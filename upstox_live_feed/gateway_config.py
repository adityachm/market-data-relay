"""Non-secret runtime configuration for the lightweight gateway."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNTIME_DIR = PROJECT_ROOT / ".runtime"


class GatewayConfigurationError(ValueError):
    """Raised when gateway configuration is invalid."""


def _integer(name: str, default: int, minimum: int, maximum: int) -> int:
    raw_value = os.getenv(name, str(default)).strip()
    try:
        value = int(raw_value)
    except ValueError as error:
        raise GatewayConfigurationError(f"{name} must be an integer") from error
    if not minimum <= value <= maximum:
        raise GatewayConfigurationError(
            f"{name} must be between {minimum} and {maximum}"
        )
    return value


@dataclass(frozen=True)
class GatewaySettings:
    """Validated controls that do not contain credentials."""

    host: str
    port: int
    token_file: Path
    api_key_file: Path
    instrument_cache_dir: Path
    target_indices: Tuple[str, ...]
    futures_per_underlying: int
    option_expiries_per_index: int
    catalog_refresh_seconds: int
    client_queue_size: int
    subscription_batch_size: int
    max_client_keys: int

    @classmethod
    def from_environment(cls) -> "GatewaySettings":
        default_token = RUNTIME_DIR / "upstox_token.json"
        default_api_key = RUNTIME_DIR / "gateway_api_key"
        default_cache = RUNTIME_DIR / "instruments"
        target_indices = tuple(
            symbol.strip().upper()
            for symbol in os.getenv("GATEWAY_TARGET_INDICES", "NIFTY,SENSEX").split(",")
            if symbol.strip()
        )
        if not target_indices:
            raise GatewayConfigurationError("GATEWAY_TARGET_INDICES cannot be empty")

        return cls(
            host=os.getenv("GATEWAY_HOST", "0.0.0.0").strip(),
            port=_integer("GATEWAY_PORT", 8080, 1, 65535),
            token_file=Path(
                os.getenv("UPSTOX_TOKEN_FILE", str(default_token)).strip()
            ),
            api_key_file=Path(
                os.getenv("GATEWAY_API_KEY_FILE", str(default_api_key)).strip()
            ),
            instrument_cache_dir=Path(
                os.getenv("INSTRUMENT_CACHE_DIR", str(default_cache)).strip()
            ),
            target_indices=target_indices,
            futures_per_underlying=_integer(
                "FUTURES_PER_UNDERLYING", 1, 1, 3
            ),
            option_expiries_per_index=_integer(
                "OPTION_EXPIRIES_PER_INDEX", 2, 1, 10
            ),
            catalog_refresh_seconds=_integer(
                "CATALOG_REFRESH_SECONDS", 21600, 900, 86400
            ),
            client_queue_size=_integer("CLIENT_QUEUE_SIZE", 64, 1, 1024),
            subscription_batch_size=_integer(
                "SUBSCRIPTION_BATCH_SIZE", 500, 1, 1000
            ),
            max_client_keys=_integer("MAX_CLIENT_KEYS", 1500, 1, 2000),
        )
