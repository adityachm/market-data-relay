"""Streaming Upstox instrument discovery and subscription planning."""

from __future__ import annotations

import gzip
import json
import os
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, DefaultDict, Dict, Iterable, Iterator, List, Mapping, Sequence, Tuple

import httpx
import ijson

INSTRUMENT_URLS = {
    "NSE": "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
    "BSE": "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz",
}
DERIVATIVE_SEGMENTS = frozenset({"NSE_FO", "BSE_FO"})
FUTURE_TYPE = "FUT"
OPTION_TYPES = frozenset({"CE", "PE"})

# Both categories are used concurrently, so combined limits apply.
COMBINED_LTPC_LIMIT = 2000
COMBINED_GREEKS_LIMIT = 2000
SUBSCRIPTION_SAFETY_MARGIN = 100
MAX_COMPRESSED_MASTER_BYTES = 16 * 1024 * 1024


class InstrumentCatalogError(RuntimeError):
    """Raised when instrument discovery or selection is unsafe."""


class SubscriptionLimitError(InstrumentCatalogError):
    """Raised when a generated plan exceeds the provider limit."""


@dataclass(frozen=True)
class Contract:
    instrument_key: str
    underlying_key: str
    underlying_symbol: str
    underlying_type: str
    instrument_type: str
    expiry_ms: int
    strike_price: float
    trading_symbol: str

    @property
    def expiry_date(self) -> str:
        return datetime.fromtimestamp(
            self.expiry_ms / 1000, timezone.utc
        ).date().isoformat()

    def metadata(self) -> Dict[str, Any]:
        return {
            "instrument_key": self.instrument_key,
            "underlying_key": self.underlying_key,
            "underlying_symbol": self.underlying_symbol,
            "underlying_type": self.underlying_type,
            "instrument_type": self.instrument_type,
            "expiry": self.expiry_date,
            "strike_price": self.strike_price,
            "trading_symbol": self.trading_symbol,
        }


@dataclass(frozen=True)
class SubscriptionPlan:
    generated_at: str
    ltpc_keys: Tuple[str, ...]
    greeks_keys: Tuple[str, ...]
    metadata: Mapping[str, Mapping[str, Any]]
    counts: Mapping[str, Any]

    @property
    def all_keys(self) -> frozenset[str]:
        return frozenset(self.ltpc_keys).union(self.greeks_keys)

    def public_summary(self) -> Dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "counts": dict(self.counts),
            "limits": {
                "ltpc_provider": COMBINED_LTPC_LIMIT,
                "option_greeks_provider": COMBINED_GREEKS_LIMIT,
                "safety_margin": SUBSCRIPTION_SAFETY_MARGIN,
            },
        }


class InstrumentCatalog:
    """Download daily masters and retain only records needed by the gateway."""

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir

    def refresh(self) -> Tuple[Path, ...]:
        self.cache_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        paths: List[Path] = []
        for exchange, url in INSTRUMENT_URLS.items():
            destination = self.cache_dir / f"{exchange}.json.gz"
            temporary = destination.with_suffix(".tmp")
            downloaded = 0
            try:
                with httpx.stream(
                    "GET",
                    url,
                    timeout=httpx.Timeout(45.0, connect=10.0),
                    follow_redirects=True,
                ) as response:
                    response.raise_for_status()
                    with temporary.open("wb") as output:
                        for chunk in response.iter_bytes(64 * 1024):
                            downloaded += len(chunk)
                            if downloaded > MAX_COMPRESSED_MASTER_BYTES:
                                raise InstrumentCatalogError(
                                    f"{exchange} instrument master exceeds safety limit"
                                )
                            output.write(chunk)
                        output.flush()
                        os.fsync(output.fileno())
                if downloaded == 0:
                    raise InstrumentCatalogError(
                        f"{exchange} instrument master was empty"
                    )
                os.replace(temporary, destination)
            except Exception as error:
                if temporary.exists():
                    temporary.unlink()
                if isinstance(error, InstrumentCatalogError):
                    raise
                raise InstrumentCatalogError(
                    f"Unable to download {exchange} instrument master"
                ) from error
            paths.append(destination)
        return tuple(paths)

    @staticmethod
    def _contracts(paths: Iterable[Path]) -> Iterator[Contract]:
        for path in paths:
            try:
                with gzip.open(path, "rb") as stream:
                    for item in ijson.items(stream, "item"):
                        segment = str(item.get("segment") or "")
                        instrument_type = str(item.get("instrument_type") or "")
                        if segment not in DERIVATIVE_SEGMENTS:
                            continue
                        if instrument_type not in OPTION_TYPES.union({FUTURE_TYPE}):
                            continue

                        underlying_key = str(
                            item.get("underlying_key") or item.get("asset_key") or ""
                        )
                        underlying_symbol = str(
                            item.get("underlying_symbol")
                            or item.get("asset_symbol")
                            or item.get("name")
                            or ""
                        ).upper()
                        instrument_key = str(item.get("instrument_key") or "")
                        expiry_ms = int(item.get("expiry") or 0)
                        if not all(
                            (underlying_key, underlying_symbol, instrument_key, expiry_ms)
                        ):
                            continue

                        yield Contract(
                            instrument_key=instrument_key,
                            underlying_key=underlying_key,
                            underlying_symbol=underlying_symbol,
                            underlying_type=str(
                                item.get("underlying_type")
                                or item.get("asset_type")
                                or ""
                            ),
                            instrument_type=instrument_type,
                            expiry_ms=expiry_ms,
                            strike_price=float(item.get("strike_price") or 0),
                            trading_symbol=str(item.get("trading_symbol") or ""),
                        )
            except (OSError, EOFError, ValueError) as error:
                raise InstrumentCatalogError(
                    f"Unable to parse instrument master {path.name}"
                ) from error

    def build_plan(
        self,
        paths: Iterable[Path],
        target_indices: Sequence[str],
        futures_per_underlying: int,
        option_expiries_per_index: int,
    ) -> SubscriptionPlan:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        normalized_targets = frozenset(symbol.upper() for symbol in target_indices)
        underlyings: Dict[str, str] = {}
        futures: DefaultDict[str, List[Contract]] = defaultdict(list)
        target_options: DefaultDict[str, List[Contract]] = defaultdict(list)
        listed_futures = 0
        listed_options = 0

        for contract in self._contracts(paths):
            underlyings[contract.underlying_key] = contract.underlying_symbol
            if contract.instrument_type == FUTURE_TYPE:
                listed_futures += 1
                if contract.expiry_ms >= now_ms:
                    futures[contract.underlying_key].append(contract)
            else:
                listed_options += 1
                if (
                    contract.underlying_symbol in normalized_targets
                    and contract.expiry_ms >= now_ms
                ):
                    target_options[contract.underlying_symbol].append(contract)

        selected_futures: List[Contract] = []
        for underlying_key in sorted(underlyings):
            contracts = sorted(
                futures.get(underlying_key, []),
                key=lambda item: (item.expiry_ms, item.instrument_key),
            )
            selected_futures.extend(contracts[:futures_per_underlying])

        selected_options: List[Contract] = []
        selected_expiry_counts: Dict[str, Dict[str, int]] = {}
        for symbol in sorted(normalized_targets):
            contracts = sorted(
                target_options.get(symbol, []),
                key=lambda item: (
                    item.expiry_ms,
                    item.strike_price,
                    item.instrument_type,
                    item.instrument_key,
                ),
            )
            if not contracts:
                raise InstrumentCatalogError(
                    f"No active option contracts found for target index {symbol}"
                )
            active_expiries = sorted({item.expiry_ms for item in contracts})
            chosen_expiries = frozenset(active_expiries[:option_expiries_per_index])
            chosen = [item for item in contracts if item.expiry_ms in chosen_expiries]
            selected_options.extend(chosen)
            per_expiry: DefaultDict[str, int] = defaultdict(int)
            for contract in chosen:
                per_expiry[contract.expiry_date] += 1
            selected_expiry_counts[symbol] = dict(sorted(per_expiry.items()))

        greeks_keys = tuple(
            sorted({contract.instrument_key for contract in selected_options})
        )
        # Options are placed on both upstream connections: LTPC provides price
        # changes, while option_greeks provides the Greeks without full depth.
        ltpc_keys = tuple(
            sorted(
                set(underlyings)
                .union(contract.instrument_key for contract in selected_futures)
                .union(greeks_keys)
            )
        )

        ltpc_safety_limit = COMBINED_LTPC_LIMIT - SUBSCRIPTION_SAFETY_MARGIN
        greeks_safety_limit = COMBINED_GREEKS_LIMIT - SUBSCRIPTION_SAFETY_MARGIN
        if len(ltpc_keys) > ltpc_safety_limit:
            raise SubscriptionLimitError(
                f"LTPC plan has {len(ltpc_keys)} keys; operational safety limit is "
                f"{ltpc_safety_limit}"
            )
        if len(greeks_keys) > greeks_safety_limit:
            raise SubscriptionLimitError(
                f"Option Greeks plan has {len(greeks_keys)} keys; operational safety "
                f"limit is {greeks_safety_limit}"
            )

        metadata = {
            contract.instrument_key: contract.metadata()
            for contract in selected_futures + selected_options
        }
        for underlying_key, symbol in underlyings.items():
            metadata.setdefault(
                underlying_key,
                {
                    "instrument_key": underlying_key,
                    "underlying_key": underlying_key,
                    "underlying_symbol": symbol,
                    "instrument_type": "SPOT",
                },
            )

        counts = {
            "unique_fno_underlyings": len(underlyings),
            "listed_futures": listed_futures,
            "listed_options": listed_options,
            "selected_futures": len(selected_futures),
            "ltpc_subscriptions": len(ltpc_keys),
            "option_greeks_subscriptions": len(greeks_keys),
            "selected_option_contracts": selected_expiry_counts,
            "missing_nearest_futures": len(underlyings)
            * futures_per_underlying
            - len(selected_futures),
        }
        return SubscriptionPlan(
            generated_at=datetime.now(timezone.utc).isoformat(),
            ltpc_keys=ltpc_keys,
            greeks_keys=greeks_keys,
            metadata=metadata,
            counts=counts,
        )
