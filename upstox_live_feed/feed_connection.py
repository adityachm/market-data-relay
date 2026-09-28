"""Memory-bounded multi-instrument Upstox V3 feed connection."""

from __future__ import annotations

import json
import logging
import ssl
import uuid
from typing import Any, AsyncIterator, Dict, Iterator, Sequence
from urllib.parse import urlsplit

import httpx
import websockets
from google.protobuf.json_format import MessageToDict
from upstox_client.feeder.proto import MarketDataFeedV3_pb2

AUTHORIZE_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
LOGGER = logging.getLogger(__name__)
VALID_MODES = frozenset({"ltpc", "full", "option_greeks"})
MAX_FRAME_BYTES = 4 * 1024 * 1024


class FeedConnectionError(RuntimeError):
    """Raised when an upstream feed cannot authorize, connect, or decode."""


class UpstoxFeedConnection:
    """One bounded Upstox connection for one mode and a fixed key set."""

    def __init__(
        self,
        access_token: str,
        instrument_keys: Sequence[str],
        mode: str,
        subscription_batch_size: int,
    ) -> None:
        if mode not in VALID_MODES:
            raise ValueError(f"Unsupported feed mode: {mode}")
        if not instrument_keys:
            raise ValueError("At least one instrument key is required")
        self._access_token = access_token
        self.instrument_keys = tuple(dict.fromkeys(instrument_keys))
        self.mode = mode
        self.subscription_batch_size = subscription_batch_size

    async def _authorized_url(self) -> str:
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(10.0, connect=5.0),
                follow_redirects=True,
            ) as client:
                response = await client.get(
                    AUTHORIZE_URL,
                    headers={
                        "Accept": "*/*",
                        "Authorization": f"Bearer {self._access_token}",
                    },
                )
                response.raise_for_status()
                websocket_url = response.json()["data"]["authorized_redirect_uri"]
        except httpx.HTTPStatusError as error:
            raise FeedConnectionError(
                f"Feed authorization returned HTTP {error.response.status_code}"
            ) from None
        except (httpx.HTTPError, KeyError, TypeError, ValueError):
            raise FeedConnectionError("Feed authorization failed") from None

        parsed = urlsplit(websocket_url)
        hostname = (parsed.hostname or "").lower()
        if (
            parsed.scheme != "wss"
            or not (hostname == "upstox.com" or hostname.endswith(".upstox.com"))
        ):
            raise FeedConnectionError("Feed authorization returned an invalid endpoint")
        return websocket_url

    def _subscription_requests(self) -> Iterator[bytes]:
        for offset in range(0, len(self.instrument_keys), self.subscription_batch_size):
            request = {
                "guid": str(uuid.uuid4()),
                "method": "sub",
                "data": {
                    "mode": self.mode,
                    "instrumentKeys": list(
                        self.instrument_keys[
                            offset : offset + self.subscription_batch_size
                        ]
                    ),
                },
            }
            yield json.dumps(request, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def _as_dict(message: Any) -> Dict[str, Any]:
        return MessageToDict(message, preserving_proto_field_name=True)

    @classmethod
    def _events_from_frame(cls, frame: bytes) -> Iterator[Dict[str, Any]]:
        try:
            response = MarketDataFeedV3_pb2.FeedResponse.FromString(frame)
        except Exception:
            raise FeedConnectionError("Unable to decode an Upstox protobuf frame") from None

        response_type = MarketDataFeedV3_pb2.Type.Name(response.type)
        if response_type == "market_info":
            yield {
                "type": "market_info",
                "source_timestamp": response.currentTs,
                "market_info": cls._as_dict(response.marketInfo),
            }
            return

        for instrument_key, feed in response.feeds.items():
            event: Dict[str, Any] = {
                "type": "tick",
                "instrument_key": instrument_key,
                "source_timestamp": response.currentTs,
            }
            feed_variant = feed.WhichOneof("FeedUnion")
            if feed_variant == "ltpc":
                event["ltpc"] = cls._as_dict(feed.ltpc)
            elif feed_variant == "fullFeed":
                full_variant = feed.fullFeed.WhichOneof("FullFeedUnion")
                if full_variant == "marketFF":
                    market_feed = feed.fullFeed.marketFF
                    event["ltpc"] = cls._as_dict(market_feed.ltpc)
                    event["greeks"] = cls._as_dict(market_feed.optionGreeks)
                    event["market"] = {
                        "atp": market_feed.atp,
                        "vtt": market_feed.vtt,
                        "oi": market_feed.oi,
                        "iv": market_feed.iv,
                    }
                elif full_variant == "indexFF":
                    event["ltpc"] = cls._as_dict(feed.fullFeed.indexFF.ltpc)
            elif feed_variant == "firstLevelWithGreeks":
                first_level = feed.firstLevelWithGreeks
                event["ltpc"] = cls._as_dict(first_level.ltpc)
                event["greeks"] = cls._as_dict(first_level.optionGreeks)
                event["market"] = {
                    "vtt": first_level.vtt,
                    "oi": first_level.oi,
                    "iv": first_level.iv,
                }
            else:
                continue
            yield event

    async def events(self) -> AsyncIterator[Dict[str, Any]]:
        websocket_url = await self._authorized_url()
        ssl_context = ssl.create_default_context()
        LOGGER.info(
            "Connecting upstream mode=%s instruments=%d",
            self.mode,
            len(self.instrument_keys),
        )

        try:
            async with websockets.connect(
                websocket_url,
                ssl=ssl_context,
                open_timeout=10,
                close_timeout=5,
                ping_interval=20,
                ping_timeout=20,
                max_size=MAX_FRAME_BYTES,
                max_queue=4,
                compression=None,
            ) as websocket:
                for request in self._subscription_requests():
                    await websocket.send(request)
                LOGGER.info(
                    "Subscribed upstream mode=%s instruments=%d",
                    self.mode,
                    len(self.instrument_keys),
                )

                async for frame in websocket:
                    if isinstance(frame, str):
                        continue
                    for event in self._events_from_frame(frame):
                        event["feed_mode"] = self.mode
                        yield event
        except FeedConnectionError:
            raise
        except Exception as error:
            raise FeedConnectionError(
                f"Upstream {self.mode} connection failed ({type(error).__name__})"
            ) from None
