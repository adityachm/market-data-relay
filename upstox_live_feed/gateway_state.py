"""Bounded in-memory snapshots and local-client fan-out."""

from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Set


class SnapshotStore:
    """Retain only the latest normalized event for each instrument."""

    def __init__(self) -> None:
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._metadata: Mapping[str, Mapping[str, Any]] = {}
        self.market_info: Dict[str, Any] = {}
        self.total_events = 0
        self.last_event_at: Optional[str] = None

    def replace_metadata(self, metadata: Mapping[str, Mapping[str, Any]]) -> None:
        self._metadata = metadata
        allowed = frozenset(metadata)
        self._snapshots = {
            key: value for key, value in self._snapshots.items() if key in allowed
        }

    def update(self, event: Dict[str, Any]) -> None:
        self.total_events += 1
        self.last_event_at = datetime.now(timezone.utc).isoformat()
        if event.get("type") == "market_info":
            self.market_info = event
            return
        instrument_key = event.get("instrument_key")
        if isinstance(instrument_key, str):
            current = dict(self._snapshots.get(instrument_key, {}))
            current["type"] = "snapshot"
            current["instrument_key"] = instrument_key
            current["received_at"] = self.last_event_at
            feed_mode = str(event.get("feed_mode") or "unknown")
            source_timestamps = dict(current.get("source_timestamps", {}))
            source_timestamps[feed_mode] = event.get("source_timestamp")
            current["source_timestamps"] = source_timestamps
            for field in ("ltpc", "greeks", "market"):
                incoming = event.get(field)
                if not isinstance(incoming, dict) or not incoming:
                    continue
                merged = dict(current.get(field, {}))
                merged.update(incoming)
                current[field] = merged
            self._snapshots[instrument_key] = current

    def get(self, instrument_key: str) -> Optional[Dict[str, Any]]:
        event = self._snapshots.get(instrument_key)
        if event is None:
            return None
        return {
            "metadata": self._metadata.get(instrument_key, {}),
            "event": event,
        }

    @property
    def snapshot_count(self) -> int:
        return len(self._snapshots)


@dataclass
class Subscriber:
    queue: asyncio.Queue
    instrument_keys: Optional[Set[str]]
    dropped_events: int = 0


class Broadcaster:
    """Fan out ticks without allowing slow clients to block the feed."""

    def __init__(self, queue_size: int) -> None:
        self.queue_size = queue_size
        self._ids = itertools.count(1)
        self._subscribers: Dict[int, Subscriber] = {}
        self.total_dropped_events = 0

    def subscribe(self, instrument_keys: Optional[Set[str]]) -> tuple[int, Subscriber]:
        subscriber_id = next(self._ids)
        subscriber = Subscriber(
            queue=asyncio.Queue(maxsize=self.queue_size),
            instrument_keys=instrument_keys,
        )
        self._subscribers[subscriber_id] = subscriber
        return subscriber_id, subscriber

    def unsubscribe(self, subscriber_id: int) -> None:
        self._subscribers.pop(subscriber_id, None)

    def publish(self, event: Dict[str, Any]) -> None:
        instrument_key = event.get("instrument_key")
        for subscriber in tuple(self._subscribers.values()):
            if (
                subscriber.instrument_keys is not None
                and instrument_key not in subscriber.instrument_keys
            ):
                continue
            if subscriber.queue.full():
                try:
                    subscriber.queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                subscriber.dropped_events += 1
                self.total_dropped_events += 1
            subscriber.queue.put_nowait(event)

    @property
    def client_count(self) -> int:
        return len(self._subscribers)
