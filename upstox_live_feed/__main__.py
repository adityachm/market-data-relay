"""Host-side Upstox login and one-instrument diagnostic feed."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import replace
from typing import Optional, Sequence

from .auth import UpstoxAuthenticationError, UpstoxTokenProvider
from .client import UpstoxFeedError, UpstoxMarketDataClient
from .config import (
    ConfigurationError,
    FeedSettings,
    LoginSettings,
    VALID_MODES,
    load_local_config,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Log in and optionally subscribe to one Upstox V3 instrument."
    )
    parser.add_argument("--instrument", help="Override UPSTOX_INSTRUMENT_KEY.")
    parser.add_argument("--mode", choices=sorted(VALID_MODES), help="Override feed mode.")
    parser.add_argument(
        "--max-messages",
        type=int,
        help="Stop after this many decoded messages; zero streams continuously.",
    )
    parser.add_argument(
        "--force-login",
        action="store_true",
        help="Discard the local token cache and perform a fresh login.",
    )
    parser.add_argument(
        "--login-only",
        action="store_true",
        help="Refresh the protected token cache without starting a feed.",
    )
    return parser


async def stream(settings: FeedSettings) -> None:
    client = UpstoxMarketDataClient(settings)
    async for message in client.messages():
        print(json.dumps(message, separators=(",", ":")), flush=True)


def configured_settings(
    args: argparse.Namespace,
    values: dict,
    access_token: str,
) -> FeedSettings:
    settings = FeedSettings.from_mapping(values, access_token)
    overrides = {}
    if args.instrument:
        if "|" not in args.instrument:
            raise ConfigurationError("--instrument must be a valid Upstox instrument key")
        overrides["instrument_key"] = args.instrument
    if args.mode:
        overrides["mode"] = args.mode
    if args.max_messages is not None:
        if args.max_messages < 0:
            raise ConfigurationError("--max-messages cannot be negative")
        overrides["max_messages"] = args.max_messages
    return replace(settings, **overrides)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        values = dict(load_local_config())
        log_level = values.get("UPSTOX_LOG_LEVEL", "INFO").upper()
        logging.basicConfig(
            level=getattr(logging, log_level, logging.INFO),
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )

        token_provider = UpstoxTokenProvider(LoginSettings.from_mapping(values))
        if args.force_login:
            token_provider.invalidate_cache()
        access_token = token_provider.get_access_token()
        if args.login_only:
            logging.getLogger(__name__).info("Protected Upstox token cache is ready")
            return 0

        settings = configured_settings(args, values, access_token)
        asyncio.run(stream(settings))
    except (ConfigurationError, UpstoxAuthenticationError, UpstoxFeedError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Feed stopped by user", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
