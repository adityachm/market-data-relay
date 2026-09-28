"""Upstox Market Data Feed V3 authorization and WebSocket client."""

from __future__ import annotations

import json
import logging
import ssl
import uuid
from typing import Any, AsyncIterator, Dict
from urllib.parse import urlsplit

import httpx
import websockets
from google.protobuf.json_format import MessageToDict
from upstox_client.feeder.proto import MarketDataFeedV3_pb2

from .config import FeedSettings

AUTHORIZE_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
LOGGER = logging.getLogger(__name__)


class UpstoxFeedError(RuntimeError):
    """Raised when authorization, connection, or feed decoding fails."""


class UpstoxMarketDataClient:
    """Subscribe to a single instrument through Upstox Market Data Feed V3."""

    def __init__(self, settings: FeedSettings) -> None:
        self.settings = settings

    async def _authorized_websocket_url(self) -> str:
        headers = {
            "Accept": "*/*",
            "Authorization": f"Bearer {self.settings.access_token}",
        }
        timeout = httpx.Timeout(10.0, connect=5.0)

        try:
            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=True,
            ) as client:
                response = await client.get(AUTHORIZE_URL, headers=headers)
                response.raise_for_status()
                payload = response.json()
        except httpx.HTTPStatusError as error:
            raise UpstoxFeedError(
                f"Upstox feed authorization failed with HTTP {error.response.status_code}"
            ) from error
        except (httpx.HTTPError, ValueError) as error:
            raise UpstoxFeedError("Unable to authorize the Upstox market feed") from error

        try:
            websocket_url = payload["data"]["authorized_redirect_uri"]
        except (KeyError, TypeError) as error:
            raise UpstoxFeedError(
                "Upstox authorization response did not contain a WebSocket URL"
            ) from error

        parsed_url = urlsplit(websocket_url)
        hostname = (parsed_url.hostname or "").lower()
        if (
            parsed_url.scheme != "wss"
            or not hostname
            or not (hostname == "upstox.com" or hostname.endswith(".upstox.com"))
        ):
            raise UpstoxFeedError("Upstox returned an invalid secure WebSocket URL")
        return websocket_url

    def _subscription_message(self) -> bytes:
        request = {
            "guid": str(uuid.uuid4()),
            "method": "sub",
            "data": {
                "mode": self.settings.mode,
                "instrumentKeys": [self.settings.instrument_key],
            },
        }
        return json.dumps(request, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def _decode_message(message: bytes) -> Dict[str, Any]:
        try:
            decoded = MarketDataFeedV3_pb2.FeedResponse.FromString(message)
            return MessageToDict(decoded)
        except Exception as error:
            raise UpstoxFeedError("Unable to decode an Upstox protobuf frame") from error

    async def messages(self) -> AsyncIterator[Dict[str, Any]]:
        """Yield decoded market-status, snapshot, and live-feed messages."""
        websocket_url = await self._authorized_websocket_url()
        ssl_context = ssl.create_default_context()

        LOGGER.info(
            "Connecting to Upstox for instrument=%s mode=%s",
            self.settings.instrument_key,
            self.settings.mode,
        )

        try:
            async with websockets.connect(
                websocket_url,
                ssl=ssl_context,
                open_timeout=10,
                close_timeout=10,
                ping_interval=20,
                ping_timeout=20,
                max_size=16 * 1024 * 1024,
            ) as websocket:
                await websocket.send(self._subscription_message())
                LOGGER.info("Subscription request sent")

                received = 0
                async for message in websocket:
                    if isinstance(message, str):
                        LOGGER.warning("Ignoring unexpected text WebSocket frame")
                        continue

                    yield self._decode_message(message)
                    received += 1
                    if self.settings.max_messages and received >= self.settings.max_messages:
                        LOGGER.info("Configured message limit reached")
                        return
        except UpstoxFeedError:
            raise
        except Exception as error:
            # Never include the credential-bearing authorized URL in an error message.
            raise UpstoxFeedError(
                f"Upstox WebSocket connection failed ({type(error).__name__})"
            ) from None
