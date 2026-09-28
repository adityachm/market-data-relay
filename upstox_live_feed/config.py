"""Strict folder-local configuration for Upstox authentication and streaming."""

from __future__ import annotations

import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from dotenv import dotenv_values

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
RUNTIME_DIR = PROJECT_ROOT / ".runtime"
TOKEN_CACHE_FILE = RUNTIME_DIR / "upstox_token.json"
LOGIN_FAILURE_DIR = RUNTIME_DIR / "login_failures"
VALID_MODES = frozenset({"ltpc", "full", "option_greeks", "full_d30"})


class ConfigurationError(ValueError):
    """Raised when required local configuration is absent or unsafe."""


def load_local_config() -> Mapping[str, str]:
    """Load only live_feed/.env and require owner-only file permissions."""
    if not ENV_FILE.is_file():
        raise ConfigurationError(f"Create the local credential file: {ENV_FILE}")

    permissions = stat.S_IMODE(ENV_FILE.stat().st_mode)
    if permissions & 0o077:
        raise ConfigurationError(
            f"Credential file must use mode 0600, not {permissions:04o}"
        )

    raw_values = dotenv_values(ENV_FILE)
    return {
        key: value.strip()
        for key, value in raw_values.items()
        if value is not None
    }


def _required(values: Mapping[str, str], key: str) -> str:
    value = values.get(key, "").strip()
    if not value or value.startswith("replace_with_"):
        raise ConfigurationError(f"{key} must be set in {ENV_FILE}")
    return value


@dataclass(frozen=True)
class LoginSettings:
    """Credentials required by the Upstox OAuth browser flow."""

    api_key: str = field(repr=False)
    api_secret: str = field(repr=False)
    redirect_uri: str
    mobile_number: str = field(repr=False)
    pin: str = field(repr=False)
    totp_secret: str = field(repr=False)
    request_timeout_seconds: int = 30

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> "LoginSettings":
        return cls(
            api_key=_required(values, "API_KEY"),
            api_secret=_required(values, "API_SECRET"),
            redirect_uri=_required(values, "REDIRECT_URI"),
            mobile_number=_required(values, "MOBILE_NO"),
            pin=_required(values, "PIN"),
            totp_secret=_required(values, "TOTP_SECRET"),
        )


@dataclass(frozen=True)
class FeedSettings:
    """Validated settings for one Upstox instrument subscription."""

    access_token: str = field(repr=False)
    instrument_key: str
    mode: str = "ltpc"
    max_messages: int = 0
    log_level: str = "INFO"

    @classmethod
    def from_mapping(
        cls,
        values: Mapping[str, str],
        access_token: str,
    ) -> "FeedSettings":
        instrument_key = _required(values, "UPSTOX_INSTRUMENT_KEY")
        mode = values.get("UPSTOX_FEED_MODE", "ltpc").strip().lower()
        log_level = values.get("UPSTOX_LOG_LEVEL", "INFO").strip().upper()

        if "|" not in instrument_key:
            raise ConfigurationError(
                "UPSTOX_INSTRUMENT_KEY must be an Upstox instrument key such as "
                "NSE_INDEX|Nifty 50"
            )
        if mode not in VALID_MODES:
            raise ConfigurationError(
                f"UPSTOX_FEED_MODE must be one of: {', '.join(sorted(VALID_MODES))}"
            )

        raw_max_messages = values.get("UPSTOX_MAX_MESSAGES", "0").strip()
        try:
            max_messages = int(raw_max_messages)
        except ValueError as error:
            raise ConfigurationError("UPSTOX_MAX_MESSAGES must be an integer") from error
        if max_messages < 0:
            raise ConfigurationError("UPSTOX_MAX_MESSAGES cannot be negative")

        return cls(
            access_token=access_token,
            instrument_key=instrument_key,
            mode=mode,
            max_messages=max_messages,
            log_level=log_level,
        )
