"""Read mounted runtime secrets without importing browser-login dependencies."""

from __future__ import annotations

import hmac
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

INDIA_TIMEZONE = ZoneInfo("Asia/Kolkata")


class RuntimeSecretError(RuntimeError):
    """Raised when a mounted token or local API key is unavailable."""


def read_upstox_token(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        access_token = payload["access_token"]
        expires_at = datetime.fromisoformat(payload["expires_at"])
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeSecretError("Upstox token file is unavailable or invalid") from error

    if not isinstance(access_token, str) or not access_token:
        raise RuntimeSecretError("Upstox token file does not contain a token")
    if expires_at.tzinfo is None:
        raise RuntimeSecretError("Upstox token expiry must include a timezone")
    if expires_at <= datetime.now(INDIA_TIMEZONE) + timedelta(minutes=5):
        raise RuntimeSecretError("Upstox token is expired or near expiry")
    return access_token


def read_gateway_api_key(path: Path) -> str:
    try:
        api_key = path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise RuntimeSecretError("Gateway API key file is unavailable") from error
    if len(api_key) < 32:
        raise RuntimeSecretError("Gateway API key must contain at least 32 characters")
    return api_key


def api_key_matches(provided: str, expected: str) -> bool:
    return hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))
