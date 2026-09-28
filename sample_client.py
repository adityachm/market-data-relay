#!/usr/bin/env python3
"""Minimal local client for the gateway WebSocket."""

from __future__ import annotations

import asyncio
import json
import os

import websockets


async def main() -> None:
    gateway_url = os.getenv("GATEWAY_URL", "ws://127.0.0.1:8080/v1/stream")
    api_key = os.environ["GATEWAY_API_KEY"]
    instrument_keys = [
        key.strip()
        for key in os.getenv("INSTRUMENT_KEYS", "NSE_INDEX|Nifty 50").split(",")
        if key.strip()
    ]

    async with websockets.connect(gateway_url, max_size=1024 * 1024) as websocket:
        await websocket.send(
            json.dumps(
                {
                    "api_key": api_key,
                    "instrument_keys": instrument_keys,
                }
            )
        )
        async for message in websocket:
            print(message)


if __name__ == "__main__":
    asyncio.run(main())
