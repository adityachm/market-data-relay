"""Authenticated local REST and WebSocket API for normalized market data."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect, status

from .gateway_config import GatewaySettings
from .gateway_service import GatewayService
from .token_reader import api_key_matches, read_gateway_api_key


def create_app(settings: Optional[GatewaySettings] = None) -> FastAPI:
    gateway_settings = settings or GatewaySettings.from_environment()
    expected_api_key = read_gateway_api_key(gateway_settings.api_key_file)
    service = GatewayService(gateway_settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await service.start()
        try:
            yield
        finally:
            await service.stop()

    app = FastAPI(
        title="Local Upstox Market Gateway",
        version="0.2.0",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.gateway = service

    def require_api_key(x_api_key: str = Header(default="")) -> None:
        if not api_key_matches(x_api_key, expected_api_key):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key",
            )

    @app.get("/health")
    async def health() -> dict:
        return service.health()

    @app.get("/v1/plan", dependencies=[Depends(require_api_key)])
    async def plan() -> dict:
        if service.plan is None:
            raise HTTPException(status_code=503, detail="Subscription plan unavailable")
        return service.plan.public_summary()

    @app.get("/v1/snapshot", dependencies=[Depends(require_api_key)])
    async def snapshot(instrument_key: str = Query(min_length=3)) -> dict:
        value = service.store.get(instrument_key)
        if value is None:
            raise HTTPException(status_code=404, detail="Snapshot unavailable")
        return value

    @app.get("/v1/instruments", dependencies=[Depends(require_api_key)])
    async def instruments(offset: int = 0, limit: int = 100) -> dict:
        if service.plan is None:
            raise HTTPException(status_code=503, detail="Subscription plan unavailable")
        if offset < 0 or not 1 <= limit <= 500:
            raise HTTPException(status_code=400, detail="Invalid pagination")
        keys = sorted(service.plan.all_keys)
        selected = keys[offset : offset + limit]
        return {
            "total": len(keys),
            "offset": offset,
            "limit": limit,
            "items": [service.plan.metadata[key] for key in selected],
        }

    @app.websocket("/v1/stream")
    async def stream(websocket: WebSocket) -> None:
        await websocket.accept()
        subscriber_id = None
        try:
            request = await asyncio.wait_for(websocket.receive_json(), timeout=10.0)
            provided_key = request.get("api_key", "")
            if not isinstance(provided_key, str) or not api_key_matches(
                provided_key, expected_api_key
            ):
                await websocket.close(code=4401, reason="Unauthorized")
                return
            if service.plan is None:
                await websocket.close(code=1013, reason="Gateway starting")
                return

            subscribe_all = request.get("all") is True
            requested = request.get("instrument_keys", [])
            if subscribe_all:
                instrument_keys = None
            else:
                if not isinstance(requested, list) or not all(
                    isinstance(key, str) for key in requested
                ):
                    await websocket.close(code=4400, reason="Invalid instrument_keys")
                    return
                requested_set = set(requested)
                if not requested_set or len(requested_set) > gateway_settings.max_client_keys:
                    await websocket.close(code=4400, reason="Invalid subscription size")
                    return
                unknown = requested_set - service.plan.all_keys
                if unknown:
                    await websocket.close(code=4404, reason="Unknown instrument key")
                    return
                instrument_keys = requested_set

            subscriber_id, subscriber = service.broadcaster.subscribe(instrument_keys)
            await websocket.send_json(
                {
                    "type": "subscribed",
                    "all": subscribe_all,
                    "instrument_count": (
                        len(service.plan.all_keys)
                        if subscribe_all
                        else len(instrument_keys)
                    ),
                }
            )
            async def send_events() -> None:
                while True:
                    event = await subscriber.queue.get()
                    await websocket.send_json(event)

            async def wait_for_disconnect() -> None:
                while True:
                    message = await websocket.receive()
                    if message["type"] == "websocket.disconnect":
                        return

            sender = asyncio.create_task(send_events(), name="local-feed-sender")
            receiver = asyncio.create_task(
                wait_for_disconnect(), name="local-feed-disconnect-watcher"
            )
            done, pending = await asyncio.wait(
                {sender, receiver}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*done, *pending, return_exceptions=True)
        except (asyncio.TimeoutError, asyncio.CancelledError, WebSocketDisconnect):
            return
        finally:
            if subscriber_id is not None:
                service.broadcaster.unsubscribe(subscriber_id)

    return app
