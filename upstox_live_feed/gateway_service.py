"""Gateway lifecycle, catalog refresh, and guarded upstream reconnects."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .feed_connection import FeedConnectionError, UpstoxFeedConnection
from .gateway_config import GatewaySettings
from .gateway_state import Broadcaster, SnapshotStore
from .instrument_catalog import InstrumentCatalog, InstrumentCatalogError, SubscriptionPlan
from .token_reader import RuntimeSecretError, read_upstox_token

LOGGER = logging.getLogger(__name__)


class RetryBudget:
    """Small token bucket preventing rapid reconnect storms."""

    def __init__(self, capacity: float = 5.0, refill_seconds: float = 60.0) -> None:
        self.capacity = capacity
        self.tokens = capacity
        self.refill_seconds = refill_seconds
        self.updated_at = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        self.tokens = min(
            self.capacity,
            self.tokens + (now - self.updated_at) / self.refill_seconds,
        )
        self.updated_at = now

    def consume_or_wait(self) -> float:
        self._refill()
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return 0.0
        return max((1.0 - self.tokens) * self.refill_seconds, 1.0)

    def record_success(self) -> None:
        self._refill()
        self.tokens = min(self.capacity, self.tokens + 0.2)


class GatewayService:
    """Own the subscription plan, two upstream feeds, and local state."""

    def __init__(self, settings: GatewaySettings) -> None:
        self.settings = settings
        self.store = SnapshotStore()
        self.broadcaster = Broadcaster(settings.client_queue_size)
        self.plan: Optional[SubscriptionPlan] = None
        self.connection_status: Dict[str, Dict[str, Any]] = {
            "ltpc": {"state": "starting"},
            "option_greeks": {"state": "starting"},
        }
        self.last_plan_error: Optional[str] = None
        self._stop = asyncio.Event()
        self._manager_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._manager_task = asyncio.create_task(
            self._manage_catalog_and_connections(), name="gateway-manager"
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._manager_task:
            self._manager_task.cancel()
            await asyncio.gather(self._manager_task, return_exceptions=True)

    async def _new_plan(self) -> SubscriptionPlan:
        catalog = InstrumentCatalog(self.settings.instrument_cache_dir)
        paths = await asyncio.to_thread(catalog.refresh)
        return await asyncio.to_thread(
            catalog.build_plan,
            paths,
            self.settings.target_indices,
            self.settings.futures_per_underlying,
            self.settings.option_expiries_per_index,
        )

    async def _manage_catalog_and_connections(self) -> None:
        planning_budget = RetryBudget(capacity=3.0, refill_seconds=120.0)
        while not self._stop.is_set():
            collectors = []
            try:
                plan = await self._new_plan()
                self.plan = plan
                self.store.replace_metadata(plan.metadata)
                self.last_plan_error = None
                LOGGER.info("Subscription plan: %s", plan.public_summary()["counts"])
                collectors = [
                    asyncio.create_task(
                        self._collect("ltpc", plan.ltpc_keys), name="upstox-ltpc"
                    ),
                    asyncio.create_task(
                        self._collect("option_greeks", plan.greeks_keys),
                        name="upstox-option-greeks",
                    ),
                ]
                planning_budget.record_success()
                try:
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=self.settings.catalog_refresh_seconds,
                    )
                except asyncio.TimeoutError:
                    LOGGER.info("Refreshing daily instrument universe")
            except asyncio.CancelledError:
                raise
            except (InstrumentCatalogError, RuntimeSecretError) as error:
                self.last_plan_error = str(error)
                LOGGER.error("Gateway planning unavailable: %s", error)
                budget_wait = planning_budget.consume_or_wait()
                await asyncio.sleep(max(30.0, budget_wait))
            finally:
                for collector in collectors:
                    collector.cancel()
                if collectors:
                    await asyncio.gather(*collectors, return_exceptions=True)

    async def _collect(self, mode: str, instrument_keys: tuple[str, ...]) -> None:
        retry_budget = RetryBudget()
        failed_attempts = 0
        while not self._stop.is_set():
            budget_wait = retry_budget.consume_or_wait() if failed_attempts else 0.0
            if budget_wait:
                await asyncio.sleep(budget_wait)
            try:
                access_token = read_upstox_token(self.settings.token_file)
                self.connection_status[mode] = {
                    "state": "connecting",
                    "instruments": len(instrument_keys),
                }
                connection = UpstoxFeedConnection(
                    access_token=access_token,
                    instrument_keys=instrument_keys,
                    mode=mode,
                    subscription_batch_size=self.settings.subscription_batch_size,
                )
                received_event = False
                async for event in connection.events():
                    if not received_event:
                        retry_budget.record_success()
                        failed_attempts = 0
                        received_event = True
                    now = datetime.now(timezone.utc).isoformat()
                    self.connection_status[mode] = {
                        "state": "connected",
                        "instruments": len(instrument_keys),
                        "last_event_at": now,
                    }
                    self.store.update(event)
                    self.broadcaster.publish(event)
                raise FeedConnectionError("Upstream connection closed")
            except asyncio.CancelledError:
                raise
            except (FeedConnectionError, RuntimeSecretError) as error:
                failed_attempts += 1
                delay = min(60.0, 2.0 ** min(failed_attempts, 5))
                delay += random.uniform(0.0, delay * 0.25)
                self.connection_status[mode] = {
                    "state": "disconnected",
                    "instruments": len(instrument_keys),
                    "error": str(error),
                    "next_attempt_seconds": round(delay, 1),
                }
                LOGGER.warning("Upstream mode=%s unavailable; waiting %.1fs", mode, delay)
                await asyncio.sleep(delay)

    def health(self) -> Dict[str, Any]:
        return {
            "status": (
                "ok"
                if self.plan
                and all(
                    state.get("state") == "connected"
                    for state in self.connection_status.values()
                )
                else "degraded"
            ),
            "connections": self.connection_status,
            "subscription_plan": self.plan.public_summary() if self.plan else None,
            "last_plan_error": self.last_plan_error,
            "snapshots": self.store.snapshot_count,
            "events": self.store.total_events,
            "last_event_at": self.store.last_event_at,
            "local_clients": self.broadcaster.client_count,
            "dropped_client_events": self.broadcaster.total_dropped_events,
        }
