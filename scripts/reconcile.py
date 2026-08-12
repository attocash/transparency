#!/usr/bin/env python3
"""Reconcile transparency CSVs with treasury activity on a live Atto node."""

from __future__ import annotations

import argparse
import base64
import csv
import io
import json
import os
import re
import sys
import tempfile
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date as CalendarDate
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from fractions import Fraction
from pathlib import Path
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo


RAW_PER_ATTO = 1_000_000_000
CSV_HEADER = (
    "date",
    "hash",
    "account",
    "counterparty",
    "allocation",
    "amount",
    "amount_eur",
    "amount_usd",
    "description",
)
ADDRESS_CSV_HEADER = ("address", "label")
EXCHANGE_RATE_CSV_HEADER = ("date", "eur", "usd")
NON_SPENDING_ALLOCATIONS = {"internal-transfer", "opening-balance"}
PRICE_METRICS = {"eur": "price.eur", "usd": "price.usd"}


class ReconciliationError(RuntimeError):
    pass


@dataclass(frozen=True)
class SourceAccount:
    label: str
    public_key: str


@dataclass(frozen=True)
class AccountMetadata:
    public_key: str
    address: str
    label: str


@dataclass(frozen=True)
class ClassificationRule:
    direction: str
    counterparty_public_key: str
    allocation: str
    description: str


@dataclass(frozen=True)
class TokenAllocation:
    name: str
    share_percent: Fraction
    ledger_allocations: tuple[str, ...]


@dataclass(frozen=True)
class AllocationStatus:
    allocation: TokenAllocation
    allocated_raw: int
    spent_raw: int

    @property
    def remaining_raw(self) -> int:
        return self.allocated_raw - self.spent_raw


@dataclass(frozen=True)
class ExchangeRate:
    date: str
    eur: Decimal | None
    usd: Decimal | None

    @property
    def is_available(self) -> bool:
        return self.eur is not None and self.usd is not None

    @property
    def is_unavailable(self) -> bool:
        return self.eur is None and self.usd is None


@dataclass(frozen=True)
class CurrentExchangeRate:
    eur_price_date: str
    eur: Decimal
    usd_price_date: str
    usd: Decimal


@dataclass(frozen=True)
class FiatValue:
    eur: Decimal
    usd: Decimal


@dataclass(frozen=True)
class AllocationValuation:
    allocation_name: str
    historical_spent: FiatValue
    current_remaining: FiatValue
    matched_entry_count: int


@dataclass(frozen=True)
class Entry:
    hash: str
    public_key: str
    height: int
    block_type: str
    subject_public_key: str
    previous_balance: int
    balance: int
    timestamp: int
    address: str = ""
    subject_address: str = ""

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> "Entry":
        return cls(
            hash=str(value["hash"]).upper(),
            public_key=str(value["publicKey"]).upper(),
            height=int(value["height"]),
            block_type=str(value["blockType"]).upper(),
            subject_public_key=str(value["subjectPublicKey"]).upper(),
            previous_balance=int(value["previousBalance"]),
            balance=int(value["balance"]),
            timestamp=int(value["timestamp"]),
            address=str(value.get("address", "")),
            subject_address=str(value.get("subjectAddress", "")),
        )

    @property
    def delta_raw(self) -> int:
        return self.balance - self.previous_balance


@dataclass(frozen=True)
class Transfer:
    sender_public_key: str
    receiver_public_key: str
    amount_raw: int
    timestamp: int


@dataclass(frozen=True)
class LoggedRow:
    path: Path
    line_number: int
    date: str
    hash: str
    allocation: str
    amount: str
    description: str
    account: str = ""
    counterparty: str = ""


@dataclass(frozen=True)
class PlannedRow:
    timestamp: int
    month: str
    values: tuple[str, ...]


@dataclass(frozen=True)
class ReconciliationPlan:
    rows: tuple[PlannedRow, ...]
    ignored_setup_hashes: tuple[str, ...]
    unknown_entries: tuple[Entry, ...]


@dataclass(frozen=True)
class Config:
    timezone_name: str
    sources: tuple[SourceAccount, ...]
    rules: tuple[ClassificationRule, ...]
    ignore_below_raw: int
    setup_window_seconds: int
    setup_max_ratio: Fraction
    include_setup_transactions: bool = False
    projection_url: str | None = None
    projection_source_labels: tuple[str, ...] = ("Genesis",)
    projection_source_prefixes: tuple[str, ...] = ("Treasury ",)
    address_labels: tuple[tuple[str, str], ...] = ()
    total_supply_raw: int = 0
    token_allocations: tuple[TokenAllocation, ...] = ()
    whitepaper_url: str = "https://atto.cash/docs/whitepaper#distribution"
    gatekeeper_metrics_url: str = (
        "https://gatekeeper.live.application.atto.cash/metrics"
    )
    price_history_lookback_days: int = 365


class NodeClient:
    def __init__(self, base_url: str, timeout_seconds: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def get_account_height(self, public_key: str) -> int:
        value = self._get_json(f"/accounts/{public_key}")
        return int(value["height"])

    def get_entry(self, transaction_hash: str) -> Entry:
        lines = self._get_ndjson(f"/accounts/entries/{transaction_hash}/stream")
        if len(lines) != 1:
            raise ReconciliationError(
                f"Expected one entry for {transaction_hash}, received {len(lines)}"
            )
        return Entry.from_json(lines[0])

    def get_account_entries(self, public_key: str, height: int) -> list[Entry]:
        query = urllib.parse.urlencode({"fromHeight": 1, "toHeight": height})
        lines = self._get_ndjson(
            f"/accounts/{public_key}/entries/stream?{query}"
        )
        entries = [Entry.from_json(value) for value in lines]
        actual_heights = [entry.height for entry in entries]
        expected_heights = list(range(1, height + 1))
        if actual_heights != expected_heights:
            raise ReconciliationError(
                f"Incomplete history for {public_key}: expected heights 1..{height}, "
                f"received {actual_heights[:1]}..{actual_heights[-1:] or []}"
            )
        return entries

    def _get_json(self, path: str) -> dict[str, Any]:
        body = self._read(path)
        value = json.loads(body)
        if not isinstance(value, dict):
            raise ReconciliationError(f"Expected JSON object from {path}")
        return value

    def _get_ndjson(self, path: str) -> list[dict[str, Any]]:
        body = self._read(path)
        values = []
        for line in body.splitlines():
            if line.strip():
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ReconciliationError(f"Expected NDJSON object from {path}")
                values.append(value)
        return values

    def _read(self, path: str) -> str:
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            headers={"Accept": "*/*", "User-Agent": "atto-transparency-reconciler/1"},
        )
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            return response.read().decode("utf-8")


def amount_to_raw(value: str) -> int:
    text = value.strip()
    sign = -1 if text.startswith("-") else 1
    unsigned = text.lstrip("+-")
    whole, separator, fraction = unsigned.partition(".")
    if not whole.isdigit() or (separator and not fraction.isdigit()):
        raise ValueError(f"Invalid ATTO amount: {value}")
    if len(fraction) > 9:
        raise ValueError(f"ATTO amount has more than nine decimals: {value}")
    fraction_raw = int(fraction.ljust(9, "0")) if fraction else 0
    return sign * (int(whole) * RAW_PER_ATTO + fraction_raw)


def raw_to_amount(value: int) -> str:
    sign = "-" if value < 0 else "+" if value > 0 else ""
    whole, fraction = divmod(abs(value), RAW_PER_ATTO)
    if fraction == 0:
        return f"{sign}{whole}.0"
    return f"{sign}{whole}.{fraction:09d}".rstrip("0")


def transfer_from_entry(entry: Entry) -> Transfer | None:
    if entry.block_type == "SEND":
        return Transfer(
            sender_public_key=entry.public_key,
            receiver_public_key=entry.subject_public_key,
            amount_raw=-entry.delta_raw,
            timestamp=entry.timestamp,
        )
    if entry.block_type in {"RECEIVE", "OPEN"}:
        return Transfer(
            sender_public_key=entry.subject_public_key,
            receiver_public_key=entry.public_key,
            amount_raw=entry.delta_raw,
            timestamp=entry.timestamp,
        )
    return None


def load_config(path: Path) -> Config:
    value = json.loads(path.read_text(encoding="utf-8"))
    tokenomics = value.get("tokenomics", {})
    valuation = value.get("valuation", {})
    sources = tuple(
        SourceAccount(str(item["label"]), str(item["public_key"]).upper())
        for item in value.get("sources", [])
    )
    rules = tuple(
        ClassificationRule(
            direction=str(item["direction"]).lower(),
            counterparty_public_key=str(item["counterparty_public_key"]).upper(),
            allocation=str(item["allocation"]),
            description=str(item["description"]),
        )
        for item in value["rules"]
    )
    return Config(
        timezone_name=str(value.get("timezone", "Europe/Amsterdam")),
        sources=sources,
        rules=rules,
        ignore_below_raw=amount_to_raw(str(value.get("ignore_below_atto", "1000"))),
        setup_window_seconds=int(value.get("setup_window_seconds", 86_400)),
        setup_max_ratio=Fraction(str(value.get("setup_max_ratio", "0.001"))),
        include_setup_transactions=bool(value.get("include_setup_transactions", False)),
        projection_url=value.get("projection_url"),
        projection_source_labels=tuple(
            str(label) for label in value.get("projection_source_labels", ["Genesis"])
        ),
        projection_source_prefixes=tuple(
            str(prefix)
            for prefix in value.get("projection_source_prefixes", ["Treasury "])
        ),
        address_labels=tuple(
            (str(address), str(label))
            for address, label in value.get("address_labels", {}).items()
        ),
        total_supply_raw=amount_to_raw(
            str(tokenomics.get("total_supply_atto", "0"))
        ),
        token_allocations=tuple(
            TokenAllocation(
                name=str(item["name"]),
                share_percent=Fraction(str(item["share_percent"])),
                ledger_allocations=tuple(
                    str(allocation) for allocation in item["ledger_allocations"]
                ),
            )
            for item in tokenomics.get("allocations", [])
        ),
        whitepaper_url=str(
            tokenomics.get(
                "whitepaper_url",
                "https://atto.cash/docs/whitepaper#distribution",
            )
        ),
        gatekeeper_metrics_url=str(
            valuation.get(
                "gatekeeper_metrics_url",
                "https://gatekeeper.live.application.atto.cash/metrics",
            )
        ).rstrip("/"),
        price_history_lookback_days=int(
            valuation.get("history_lookback_days", 365)
        ),
    )


def load_exchange_rates(path: Path) -> dict[str, ExchangeRate]:
    if not path.exists():
        return {}

    rates: dict[str, ExchangeRate] = {}
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.reader(stream)
        try:
            header = tuple(next(reader))
        except StopIteration as error:
            raise ReconciliationError(f"Empty exchange-rate file: {path}") from error
        if header != EXCHANGE_RATE_CSV_HEADER:
            raise ReconciliationError(
                f"Invalid header in {path}: expected {EXCHANGE_RATE_CSV_HEADER}, "
                f"received {header}"
            )
        for line_number, values in enumerate(reader, start=2):
            if len(values) != len(header):
                raise ReconciliationError(
                    f"Invalid column count in {path}:{line_number}: {len(values)}"
                )
            rate_date, eur, usd = values
            _parse_iso_date(rate_date, f"{path}:{line_number}")
            if bool(eur) != bool(usd):
                raise ReconciliationError(
                    f"EUR and USD availability differs in {path}:{line_number}"
                )
            if rate_date in rates:
                raise ReconciliationError(
                    f"Duplicate transaction date in {path}:{line_number}: "
                    f"{rate_date}"
                )
            rates[rate_date] = ExchangeRate(
                date=rate_date,
                eur=(
                    _positive_decimal(eur, f"{path}:{line_number} EUR rate")
                    if eur
                    else None
                ),
                usd=(
                    _positive_decimal(usd, f"{path}:{line_number} USD rate")
                    if usd
                    else None
                ),
            )
    return rates


def transaction_price_dates(ledger_rows: Sequence[PlannedRow]) -> tuple[str, ...]:
    return tuple(
        sorted({row.values[0] for row in ledger_rows})
    )


def reconcile_exchange_rates(
    path: Path,
    ledger_rows: Sequence[PlannedRow],
    metrics_url: str,
    maximum_lookback_days: int,
) -> tuple[tuple[ExchangeRate, ...], CurrentExchangeRate, int, int]:
    if maximum_lookback_days not in range(1, 366):
        raise ReconciliationError("Gatekeeper price lookback must be between 1 and 365")

    cached = load_exchange_rates(path)
    required_dates = transaction_price_dates(ledger_rows)
    required_set = set(required_dates)
    today = datetime.now(timezone.utc).date()
    rates = {
        transaction_date: rate
        for transaction_date, rate in cached.items()
        if transaction_date in required_set
        and (rate.is_available or rate.is_unavailable)
        and not _should_retry_unavailable_rate(
            rate, today, maximum_lookback_days
        )
    }
    missing_dates = [date for date in required_dates if date not in rates]
    if missing_dates:
        oldest_missing = _parse_iso_date(missing_dates[0], "ledger")
        age_days = (datetime.now(timezone.utc).date() - oldest_missing).days
        lookback_days = min(max(age_days + 2, 1), maximum_lookback_days)
        history = fetch_gatekeeper_price_history(metrics_url, lookback_days)
        for transaction_date in missing_dates:
            eur_match = _price_for_date(history["eur"], transaction_date)
            usd_match = _price_for_date(history["usd"], transaction_date)
            if eur_match is not None and usd_match is not None:
                rates[transaction_date] = ExchangeRate(
                    date=transaction_date,
                    eur=eur_match[1],
                    usd=usd_match[1],
                )
            else:
                rates[transaction_date] = ExchangeRate(
                    date=transaction_date,
                    eur=None,
                    usd=None,
                )

    current = fetch_gatekeeper_current_exchange_rate(metrics_url)
    ordered = tuple(rates[date] for date in required_dates)
    return ordered, current, len(missing_dates), len(set(cached) - required_set)


def _should_retry_unavailable_rate(
    rate: ExchangeRate,
    today: CalendarDate,
    maximum_lookback_days: int,
) -> bool:
    if not rate.is_unavailable:
        return False
    age_days = (today - _parse_iso_date(rate.date, "exchange-rate cache")).days
    return age_days < maximum_lookback_days


def fetch_gatekeeper_price_history(
    metrics_url: str, lookback_days: int
) -> dict[str, dict[str, Decimal]]:
    query = urllib.parse.urlencode(
        [
            ("names", PRICE_METRICS["eur"]),
            ("names", PRICE_METRICS["usd"]),
            ("lookback", str(lookback_days)),
        ]
    )
    values = _read_metric_collection(f"{metrics_url.rstrip('/')}/history?{query}")
    history: dict[str, dict[str, Decimal]] = {"eur": {}, "usd": {}}
    currencies = {metric: currency for currency, metric in PRICE_METRICS.items()}
    for value in values:
        metric_name = str(value.get("name", ""))
        currency = currencies.get(metric_name)
        if currency is None:
            continue
        price_date = str(value.get("date", ""))
        _parse_iso_date(price_date, f"Gatekeeper {metric_name}")
        history[currency][price_date] = _positive_decimal(
            value.get("value"), f"Gatekeeper {metric_name} on {price_date}"
        )
    for currency, prices in history.items():
        if not prices:
            raise ReconciliationError(
                f"Gatekeeper returned no {currency.upper()} price history"
            )
    return history


def fetch_gatekeeper_current_exchange_rate(
    metrics_url: str,
) -> CurrentExchangeRate:
    values = _read_metric_collection(metrics_url.rstrip("/"))
    latest: dict[str, tuple[str, Decimal]] = {}
    currencies = {metric: currency for currency, metric in PRICE_METRICS.items()}
    for value in values:
        metric_name = str(value.get("name", ""))
        currency = currencies.get(metric_name)
        if currency is None:
            continue
        price_date = str(value.get("date", ""))
        _parse_iso_date(price_date, f"Gatekeeper {metric_name}")
        candidate = (
            price_date,
            _positive_decimal(
                value.get("value"), f"Gatekeeper {metric_name} on {price_date}"
            ),
        )
        if currency not in latest or candidate[0] > latest[currency][0]:
            latest[currency] = candidate
    missing = set(PRICE_METRICS) - set(latest)
    if missing:
        raise ReconciliationError(
            "Gatekeeper returned no current price for "
            + ", ".join(sorted(currency.upper() for currency in missing))
        )
    return CurrentExchangeRate(
        eur_price_date=latest["eur"][0],
        eur=latest["eur"][1],
        usd_price_date=latest["usd"][0],
        usd=latest["usd"][1],
    )


def _read_metric_collection(url: str) -> list[dict[str, Any]]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "atto-transparency-reconciler/1",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        value = json.loads(response.read().decode("utf-8"), parse_float=Decimal)
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ReconciliationError(f"Expected a JSON metric collection from {url}")
    return value


def _price_for_date(
    prices: dict[str, Decimal], transaction_date: str
) -> tuple[str, Decimal] | None:
    _parse_iso_date(transaction_date, "ledger")
    price = prices.get(transaction_date)
    return (transaction_date, price) if price is not None else None


def _parse_iso_date(value: str, source: str) -> CalendarDate:
    try:
        return CalendarDate.fromisoformat(value)
    except ValueError as error:
        raise ReconciliationError(f"Invalid date from {source}: {value}") from error


def _positive_decimal(value: Any, source: str) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ReconciliationError(f"Invalid decimal for {source}: {value}") from error
    if not result.is_finite() or result <= 0:
        raise ReconciliationError(f"Invalid positive decimal for {source}: {value}")
    return result


def discover_accounts(
    config: Config,
) -> tuple[tuple[SourceAccount, ...], dict[str, AccountMetadata]]:
    if not config.projection_url:
        if not config.sources:
            raise ReconciliationError("No treasury source accounts are configured")
        accounts = {
            source.public_key: AccountMetadata(
                public_key=source.public_key,
                address="",
                label=source.label,
            )
            for source in config.sources
        }
        return config.sources, _apply_custom_address_labels(
            accounts, config.address_labels
        )

    request = urllib.request.Request(
        config.projection_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "atto-transparency-reconciler/1",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        projection = json.loads(response.read().decode("utf-8"))
    projected_addresses = projection.get("addresses")
    if not isinstance(projected_addresses, list):
        raise ReconciliationError("Gatekeeper projection has no addresses list")

    sources: list[SourceAccount] = []
    accounts: dict[str, AccountMetadata] = {}
    for item in projected_addresses:
        label = str(item.get("label", ""))
        address = str(item["address"])
        public_key = public_key_from_address(address)
        if public_key in accounts:
            raise ReconciliationError(
                f"Gatekeeper projection contains duplicate address for {label}"
            )
        accounts[public_key] = AccountMetadata(
            public_key=public_key,
            address=address,
            label=label,
        )
        if not (
            label in config.projection_source_labels
            or any(label.startswith(prefix) for prefix in config.projection_source_prefixes)
        ):
            continue
        sources.append(
            SourceAccount(
                label=label,
                public_key=public_key,
            )
        )
    if not any(source.label == "Genesis" for source in sources):
        raise ReconciliationError("Gatekeeper projection did not contain Genesis")
    if not any(re.fullmatch(r"Treasury \d+", source.label) for source in sources):
        raise ReconciliationError("Gatekeeper projection did not contain treasury accounts")
    if len({source.public_key for source in sources}) != len(sources):
        raise ReconciliationError("Gatekeeper projection contains duplicate source accounts")
    return (
        tuple(sorted(sources, key=_source_sort_key)),
        _apply_custom_address_labels(accounts, config.address_labels),
    )


def _apply_custom_address_labels(
    accounts: dict[str, AccountMetadata],
    address_labels: Sequence[tuple[str, str]],
) -> dict[str, AccountMetadata]:
    merged = dict(accounts)
    for address, label in address_labels:
        public_key = public_key_from_address(address)
        merged[public_key] = AccountMetadata(
            public_key=public_key,
            address=address,
            label=label,
        )
    return merged


def discover_source_accounts(config: Config) -> tuple[SourceAccount, ...]:
    sources, _ = discover_accounts(config)
    return sources


def public_key_from_address(address: str) -> str:
    prefix = "atto://"
    if not address.startswith(prefix):
        raise ReconciliationError(f"Invalid Atto address: {address}")
    encoded = address[len(prefix) :].upper()
    padding = "=" * ((8 - len(encoded) % 8) % 8)
    try:
        decoded = base64.b32decode(encoded + padding, casefold=True)
    except ValueError as error:
        raise ReconciliationError(f"Invalid Atto address: {address}") from error
    if len(decoded) != 38:
        raise ReconciliationError(
            f"Invalid Atto address payload length for {address}: {len(decoded)}"
        )
    return decoded[1:33].hex().upper()


def _source_sort_key(source: SourceAccount) -> tuple[int, int]:
    if source.label == "Genesis":
        return (0, 0)
    match = re.fullmatch(r"Treasury (\d+)", source.label)
    return (1, int(match.group(1))) if match else (2, 0)


def load_logged_rows(log_dir: Path) -> list[LoggedRow]:
    rows: list[LoggedRow] = []
    hashes: set[str] = set()
    for path in sorted(log_dir.glob("*.csv")):
        with path.open(newline="", encoding="utf-8") as stream:
            reader = csv.reader(stream)
            try:
                header = tuple(next(reader))
            except StopIteration as error:
                raise ReconciliationError(f"Empty log file: {path}") from error
            if header != CSV_HEADER:
                raise ReconciliationError(
                    f"Invalid header in {path}: expected {CSV_HEADER}, "
                    f"received {header}"
                )
            for line_number, values in enumerate(reader, start=2):
                if len(values) != len(header):
                    raise ReconciliationError(
                        f"Invalid column count in {path}:{line_number}: {len(values)}"
                    )
                (
                    date,
                    transaction_hash,
                    account,
                    counterparty,
                    allocation,
                    amount,
                    _amount_eur,
                    _amount_usd,
                    description,
                ) = values
                transaction_hash = transaction_hash.upper()
                if transaction_hash in hashes:
                    raise ReconciliationError(
                        f"Duplicate transaction hash at {path}:{line_number}: {transaction_hash}"
                    )
                amount_to_raw(amount)
                hashes.add(transaction_hash)
                rows.append(
                    LoggedRow(
                        path=path,
                        line_number=line_number,
                        date=date,
                        hash=transaction_hash,
                        allocation=allocation,
                        amount=amount,
                        description=description,
                        account=account,
                        counterparty=counterparty,
                    )
                )
    return rows


def resolve_logged_entries(
    client: NodeClient, rows: Sequence[LoggedRow], workers: int
) -> dict[str, Entry]:
    return _parallel_map(
        ((row.hash, row.hash) for row in rows),
        client.get_entry,
        workers,
    )


def fetch_source_entries(
    client: NodeClient, sources: Sequence[SourceAccount], workers: int
) -> list[Entry]:
    heights = _parallel_map(
        ((source.public_key, source.public_key) for source in sources),
        client.get_account_height,
        workers,
    )

    def fetch(item: tuple[str, int]) -> list[Entry]:
        public_key, height = item
        return client.get_account_entries(public_key, height)

    histories = _parallel_map(
        (
            (source.public_key, (source.public_key, heights[source.public_key]))
            for source in sources
        ),
        fetch,
        workers,
    )
    return sorted(
        (entry for entries in histories.values() for entry in entries),
        key=lambda entry: (entry.timestamp, entry.public_key, entry.height),
    )


def _parallel_map(
    keyed_values: Iterable[tuple[str, Any]], function: Any, workers: int
) -> dict[str, Any]:
    values = list(keyed_values)
    results: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(function, value): key for key, value in values}
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception as error:
                raise ReconciliationError(f"Failed to fetch {key}: {error}") from error
    return results


def build_plan(
    config: Config,
    logged_rows: Sequence[LoggedRow],
    source_entries: Sequence[Entry],
    account_directory: dict[str, AccountMetadata] | None = None,
) -> ReconciliationPlan:
    account_directory = account_directory or {
        source.public_key: AccountMetadata(
            public_key=source.public_key,
            address="",
            label=source.label,
        )
        for source in config.sources
    }
    source_keys = {source.public_key for source in config.sources}
    logged_hashes = {row.hash for row in logged_rows}
    candidate_entries = [
        entry
        for entry in source_entries
        if entry.block_type in {"OPEN", "SEND", "RECEIVE"}
    ]
    missing = [entry for entry in candidate_entries if entry.hash not in logged_hashes]
    ignored_setup = (
        ()
        if config.include_setup_transactions
        else tuple(
            entry.hash
            for entry in missing
            if _is_ignored_setup(entry, candidate_entries, config)
        )
    )
    ignored_set = set(ignored_setup)
    rules = {
        (rule.direction, rule.counterparty_public_key): rule
        for rule in config.rules
    }
    planned_rows: list[PlannedRow] = []
    unknown_entries: list[Entry] = []
    for entry in missing:
        if entry.hash in ignored_set:
            continue
        direction = _entry_direction(entry)
        if (
            entry.block_type == "OPEN"
            and account_directory[entry.public_key].label == "Genesis"
        ):
            allocation = "opening-balance"
            description = "Genesis opening balance"
        elif entry.subject_public_key in source_keys:
            allocation = "internal-transfer"
            transfer = transfer_from_entry(entry)
            if transfer is None:
                raise ReconciliationError(f"Cannot resolve transfer {entry.hash}")
            description = (
                f"{account_directory[transfer.sender_public_key].label} to "
                f"{account_directory[transfer.receiver_public_key].label}"
            )
        else:
            rule = rules.get((direction, entry.subject_public_key))
            if rule is None:
                unknown_entries.append(entry)
                continue
            allocation = rule.allocation
            description = rule.description
        planned_rows.append(
            planned_row_from_entry(
                entry,
                allocation,
                description,
                config.timezone_name,
                account_directory,
            )
        )
    return ReconciliationPlan(
        rows=tuple(sorted(planned_rows, key=lambda row: row.timestamp)),
        ignored_setup_hashes=ignored_setup,
        unknown_entries=tuple(unknown_entries),
    )


def _entry_direction(entry: Entry) -> str:
    return "send" if entry.block_type == "SEND" else "receive"


def planned_row_from_entry(
    entry: Entry,
    allocation: str,
    description: str,
    timezone_name: str,
    account_directory: dict[str, AccountMetadata],
) -> PlannedRow:
    local_date = datetime.fromtimestamp(
        entry.timestamp / 1000, timezone.utc
    ).astimezone(ZoneInfo(timezone_name))
    account = _account_address(
        entry.public_key, entry.address, account_directory
    )
    counterparty = _account_address(
        entry.subject_public_key, entry.subject_address, account_directory
    )
    return PlannedRow(
        timestamp=entry.timestamp,
        month=local_date.strftime("%Y-%m"),
        values=(
            local_date.date().isoformat(),
            entry.hash,
            account,
            counterparty,
            allocation,
            raw_to_amount(entry.delta_raw),
            description,
        ),
    )


def _account_address(
    public_key: str,
    entry_address: str,
    account_directory: dict[str, AccountMetadata],
) -> str:
    metadata = account_directory.get(public_key)
    return (metadata.address if metadata else "") or entry_address or public_key


def _is_ignored_setup(
    candidate: Entry, external_entries: Sequence[Entry], config: Config
) -> bool:
    if candidate.block_type != "SEND":
        return False
    candidate_amount = -candidate.delta_raw
    if candidate_amount < config.ignore_below_raw:
        return True
    window_ms = config.setup_window_seconds * 1000
    for later in external_entries:
        if (
            later.block_type == "SEND"
            and later.public_key == candidate.public_key
            and later.subject_public_key == candidate.subject_public_key
            and 0 < later.timestamp - candidate.timestamp <= window_ms
        ):
            later_amount = -later.delta_raw
            if (
                later_amount > candidate_amount
                and candidate_amount * config.setup_max_ratio.denominator
                <= later_amount * config.setup_max_ratio.numerator
            ):
                return True
    return False


def validate_logged_rows(
    rows: Sequence[LoggedRow],
    entries: dict[str, Entry],
    timezone_name: str,
    account_directory: dict[str, AccountMetadata],
) -> list[str]:
    timezone_value = ZoneInfo(timezone_name)
    warnings: list[str] = []
    for row in rows:
        entry = entries[row.hash]
        expected_raw = amount_to_raw(row.amount)
        if expected_raw != entry.delta_raw:
            warnings.append(
                f"{row.path}:{row.line_number}: amount {row.amount} does not match "
                f"network delta {raw_to_amount(entry.delta_raw)} for {row.hash}"
            )
        local_date = datetime.fromtimestamp(
            entry.timestamp / 1000, timezone.utc
        ).astimezone(timezone_value)
        network_date = local_date.date().isoformat()
        if row.date != network_date:
            warnings.append(
                f"{row.path}:{row.line_number}: date {row.date} does not match "
                f"network date {network_date} for {row.hash}"
            )
        if row.path.stem != local_date.strftime("%Y-%m"):
            warnings.append(
                f"{row.path}:{row.line_number}: row belongs in "
                f"{local_date.strftime('%Y-%m')}.csv for {row.hash}"
            )
        expected_account = _account_address(
            entry.public_key, entry.address, account_directory
        )
        if row.account and row.account != expected_account:
            warnings.append(
                f"{row.path}:{row.line_number}: account {row.account} does not match "
                f"network account {expected_account} for {row.hash}"
            )
        expected_counterparty = _account_address(
            entry.subject_public_key, entry.subject_address, account_directory
        )
        if row.counterparty and row.counterparty != expected_counterparty:
            warnings.append(
                f"{row.path}:{row.line_number}: counterparty {row.counterparty} does not "
                f"match network counterparty {expected_counterparty} for {row.hash}"
            )
    return warnings


def normalize_logged_rows(
    rows: Sequence[LoggedRow],
    entries: dict[str, Entry],
    timezone_name: str,
    account_directory: dict[str, AccountMetadata],
) -> list[PlannedRow]:
    return [
        planned_row_from_entry(
            entries[row.hash],
            row.allocation,
            row.description,
            timezone_name,
            account_directory,
        )
        for row in rows
    ]


def select_source_rows(
    rows: Sequence[LoggedRow],
    entries: dict[str, Entry],
    sources: Sequence[SourceAccount],
) -> list[LoggedRow]:
    source_keys = {source.public_key for source in sources}
    return [row for row in rows if entries[row.hash].public_key in source_keys]


def calculate_allocation_statuses(
    config: Config, ledger_rows: Sequence[PlannedRow]
) -> tuple[AllocationStatus, ...]:
    if config.total_supply_raw <= 0 or not config.token_allocations:
        raise ReconciliationError("Tokenomics allocation plan is not configured")
    total_share = sum(
        (allocation.share_percent for allocation in config.token_allocations),
        Fraction(0),
    )
    if total_share != 100:
        raise ReconciliationError(
            f"Tokenomics allocation shares total {total_share}%, expected 100%"
        )

    allocation_owners: dict[str, TokenAllocation] = {}
    for allocation in config.token_allocations:
        for ledger_allocation in allocation.ledger_allocations:
            if ledger_allocation in allocation_owners:
                raise ReconciliationError(
                    f"Ledger allocation {ledger_allocation} belongs to multiple pools"
                )
            allocation_owners[ledger_allocation] = allocation

    observed_allocations = {
        row.values[4]
        for row in ledger_rows
        if row.values[4] not in NON_SPENDING_ALLOCATIONS
    }
    unknown_allocations = observed_allocations - set(allocation_owners)
    if unknown_allocations:
        raise ReconciliationError(
            "Ledger allocations missing from tokenomics plan: "
            + ", ".join(sorted(unknown_allocations))
        )

    net_by_allocation: dict[str, int] = {}
    for row in ledger_rows:
        ledger_allocation = row.values[4]
        if ledger_allocation in NON_SPENDING_ALLOCATIONS:
            continue
        net_by_allocation[ledger_allocation] = (
            net_by_allocation.get(ledger_allocation, 0)
            + amount_to_raw(row.values[5])
        )

    statuses: list[AllocationStatus] = []
    for allocation in config.token_allocations:
        allocated = (
            Fraction(config.total_supply_raw)
            * allocation.share_percent
            / 100
        )
        if allocated.denominator != 1:
            raise ReconciliationError(
                f"Allocation for {allocation.name} is not representable in raw units"
            )
        spent_raw = -sum(
            net_by_allocation.get(ledger_allocation, 0)
            for ledger_allocation in allocation.ledger_allocations
        )
        status = AllocationStatus(
            allocation=allocation,
            allocated_raw=allocated.numerator,
            spent_raw=spent_raw,
        )
        if status.spent_raw < 0:
            raise ReconciliationError(
                f"{allocation.name} has negative net spending"
            )
        if status.remaining_raw < 0:
            raise ReconciliationError(
                f"{allocation.name} spending exceeds its allocation"
            )
        statuses.append(status)

    if sum(status.allocated_raw for status in statuses) != config.total_supply_raw:
        raise ReconciliationError("Tokenomics allocations do not equal total supply")
    return tuple(statuses)


def validate_treasury_balance(
    statuses: Sequence[AllocationStatus], source_entries: Sequence[Entry]
) -> None:
    latest_by_account: dict[str, Entry] = {}
    for entry in source_entries:
        current = latest_by_account.get(entry.public_key)
        if current is None or entry.height > current.height:
            latest_by_account[entry.public_key] = entry
    on_chain_balance = sum(entry.balance for entry in latest_by_account.values())
    planned_remaining = sum(status.remaining_raw for status in statuses)
    if on_chain_balance != planned_remaining:
        raise ReconciliationError(
            f"Allocation remainder {raw_to_amount(planned_remaining)} does not match "
            f"treasury balance {raw_to_amount(on_chain_balance)}"
        )


def calculate_allocation_valuations(
    ledger_rows: Sequence[PlannedRow],
    statuses: Sequence[AllocationStatus],
    rates: Sequence[ExchangeRate],
    current_rate: CurrentExchangeRate,
) -> tuple[AllocationValuation, ...]:
    rates_by_date = {rate.date: rate for rate in rates}
    allocation_owners = {
        ledger_allocation: status.allocation.name
        for status in statuses
        for ledger_allocation in status.allocation.ledger_allocations
    }
    historical_spend = {
        status.allocation.name: FiatValue(Decimal(0), Decimal(0))
        for status in statuses
    }
    matched_entry_count = {
        status.allocation.name: 0 for status in statuses
    }
    for row in ledger_rows:
        ledger_allocation = row.values[4]
        if ledger_allocation in NON_SPENDING_ALLOCATIONS:
            continue
        try:
            allocation_name = allocation_owners[ledger_allocation]
            rate = rates_by_date[row.values[0]]
        except KeyError as error:
            raise ReconciliationError(
                f"Missing exchange rate mapping for ledger row {row.values[1]}"
            ) from error
        if not rate.is_available:
            continue
        if rate.eur is None or rate.usd is None:
            raise ReconciliationError(
                f"Incomplete exchange rate for ledger row {row.values[1]}"
            )
        signed_atto = Decimal(amount_to_raw(row.values[5])) / Decimal(RAW_PER_ATTO)
        previous = historical_spend[allocation_name]
        historical_spend[allocation_name] = FiatValue(
            eur=previous.eur - signed_atto * rate.eur,
            usd=previous.usd - signed_atto * rate.usd,
        )
        matched_entry_count[allocation_name] += 1

    return tuple(
        AllocationValuation(
            allocation_name=status.allocation.name,
            historical_spent=historical_spend[status.allocation.name],
            current_remaining=FiatValue(
                eur=(
                    Decimal(status.remaining_raw)
                    / Decimal(RAW_PER_ATTO)
                    * current_rate.eur
                ),
                usd=(
                    Decimal(status.remaining_raw)
                    / Decimal(RAW_PER_ATTO)
                    * current_rate.usd
                ),
            ),
            matched_entry_count=matched_entry_count[status.allocation.name],
        )
        for status in statuses
    )


def write_exchange_rates(path: Path, rates: Sequence[ExchangeRate]) -> Path:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(EXCHANGE_RATE_CSV_HEADER)
    writer.writerows(
        (
            rate.date,
            format(rate.eur, "f") if rate.eur is not None else "",
            format(rate.usd, "f") if rate.usd is not None else "",
        )
        for rate in sorted(rates, key=lambda item: item.date)
    )
    _atomic_write(path, buffer.getvalue().encode("utf-8"))
    return path


def write_readme(
    path: Path,
    config: Config,
    ledger_rows: Sequence[PlannedRow],
    statuses: Sequence[AllocationStatus],
    rates: Sequence[ExchangeRate],
    current_rate: CurrentExchangeRate,
) -> Path:
    last_ledger_date = max(row.values[0] for row in ledger_rows)
    total_allocated = sum(status.allocated_raw for status in statuses)
    total_spent = sum(status.spent_raw for status in statuses)
    total_remaining = sum(status.remaining_raw for status in statuses)
    valuations = calculate_allocation_valuations(
        ledger_rows, statuses, rates, current_rate
    )
    valuations_by_allocation = {
        valuation.allocation_name: valuation for valuation in valuations
    }
    total_historical_spent = FiatValue(
        eur=sum(
            (valuation.historical_spent.eur for valuation in valuations), Decimal(0)
        ),
        usd=sum(
            (valuation.historical_spent.usd for valuation in valuations), Decimal(0)
        ),
    )
    total_current_remaining = FiatValue(
        eur=sum(
            (valuation.current_remaining.eur for valuation in valuations), Decimal(0)
        ),
        usd=sum(
            (valuation.current_remaining.usd for valuation in valuations), Decimal(0)
        ),
    )
    unavailable_rates = [rate for rate in rates if not rate.is_available]
    table_rows = [
        "| Allocation | Share | Allocated | Spent | Approx. matched spend value "
        "| Remaining | Approx. current value | Used |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for status in statuses:
        valuation = valuations_by_allocation[status.allocation.name]
        historical_value = (
            "n/a"
            if valuation.matched_entry_count == 0
            else _format_fiat(valuation.historical_spent)
        )
        table_rows.append(
            f"| {status.allocation.name} "
            f"| {_format_percentage(status.allocation.share_percent)} "
            f"| {_format_atto(status.allocated_raw)} "
            f"| {_format_atto(status.spent_raw)} "
            f"| {historical_value} "
            f"| {_format_atto(status.remaining_raw)} "
            f"| {_format_fiat(valuation.current_remaining)} "
            f"| {_format_ratio(status.spent_raw, status.allocated_raw)} |"
        )
    table_rows.append(
        f"| **Total** | **100%** | **{_format_atto(total_allocated)}** "
        f"| **{_format_atto(total_spent)}** "
        f"| **{_format_fiat(total_historical_spent)}** "
        f"| **{_format_atto(total_remaining)}** "
        f"| **{_format_fiat(total_current_remaining)}** "
        f"| **{_format_ratio(total_spent, total_allocated)}** |"
    )

    if current_rate.eur_price_date == current_rate.usd_price_date:
        current_price_date = current_rate.eur_price_date
    else:
        current_price_date = (
            f"EUR {current_rate.eur_price_date}; USD {current_rate.usd_price_date}"
        )
    availability_note = ""
    if unavailable_rates:
        availability_note = (
            f"\n\nGatekeeper has no exact price for **{len(unavailable_rates)}** ledger "
            "date(s). Their `amount_eur` and `amount_usd` fields are blank and they "
            "are excluded from historical fiat totals. No earlier or later price "
            "is substituted."
        )

    content = f"""<!-- Generated by scripts/reconcile.py --apply. -->
# Atto Treasury Transparency

> Every ATTO should be easy to account for.

Welcome! This repository is the public, network-backed accounting book for
Atto's Genesis and treasury accounts. Every row is verified against the live
network, so amounts, dates, account addresses, and counterparties come directly
from on-chain blocks rather than a manually maintained spreadsheet.

[**Whitepaper**]({config.whitepaper_url}) · [**Monthly ledger**](log/) ·
[**Known addresses**](addresses.csv) · [**Exchange rates**](exchange-rates.csv)

## Allocation status

The allocation plan comes from the [Atto whitepaper]({config.whitepaper_url}).
Figures below include treasury activity through **{last_ledger_date}** and are
denominated in **ATTO**.

**Total supply:** {_format_atto(total_allocated)} ·
**Spent so far:** {_format_atto(total_spent)} ·
**Still available:** {_format_atto(total_remaining)}

**Approximate value of date-matched spending:** {_format_fiat(total_historical_spent)} ·
**Approximate current treasury value:** {_format_fiat(total_current_remaining)}

{chr(10).join(table_rows)}

“Spent” is net treasury outflow: outgoing distributions consume an allocation,
while funds returned to treasury restore it. Genesis funding and transfers
between controlled treasury accounts do not count as spending.

Fiat spend uses the daily [Gatekeeper EUR and USD rates]({config.gatekeeper_metrics_url}/history?names=price.eur&names=price.usd&lookback={config.price_history_lookback_days})
only when the rate date exactly matches the transaction date.
Current remaining value uses the latest available rates (**{current_price_date}**):
**€{_format_rate(current_rate.eur)} / US${_format_rate(current_rate.usd)} per ATTO**.
[`exchange-rates.csv`](exchange-rates.csv) stores only dates present in the
ledger. Cached dates are reused, so future reconciliations fetch history only
for newly encountered transaction dates.{availability_note}

## How the ledger works

- Monthly files under [`log/`](log/) contain only blocks owned by Genesis or a
  Gatekeeper-labeled Treasury account.
- SEND blocks are negative; RECEIVE and OPEN blocks are positive.
- Internal transfers have a debit and credit leg and net to zero.
- [`addresses.csv`](addresses.csv) maps used, independently known addresses to
  human-readable labels. Unknown counterparties remain unlabeled in the ledger.
- The reconciler refuses to write when a transaction is missing a classification
  or an allocation is absent from the whitepaper mapping.

## Verify or update

Preview the current live reconciliation without writing:

```bash
python3 scripts/reconcile.py
```

Reconcile the monthly ledgers, refresh the address directory, and regenerate
the exchange-rate cache and this README:

```bash
python3 scripts/reconcile.py --apply
```

The default data sources are the public Atto node and Gatekeeper's address and
price endpoints. Classification rules, verified address labels, and the
whitepaper allocation mapping live in
[`scripts/reconcile-config.json`](scripts/reconcile-config.json).

## Ledger schema

```text
date,hash,account,counterparty,allocation,amount,amount_eur,amount_usd,description
```

ATTO amounts retain all nine decimal places supported by the protocol. EUR and
USD amounts are signed estimates with up to six decimal places when an
exact-date Gatekeeper rate exists; otherwise both fields are blank. Transaction
hashes provide the audit trail back to the public network.
"""
    _atomic_write(path, content.encode("utf-8"))
    return path


def _format_atto(raw_amount: int) -> str:
    if raw_amount < 0:
        raise ReconciliationError("README amounts must not be negative")
    amount = raw_to_amount(raw_amount).lstrip("+")
    whole, separator, fraction = amount.partition(".")
    formatted = f"{int(whole):,}"
    return f"{formatted}.{fraction}" if separator and fraction != "0" else formatted


def _format_fiat(value: FiatValue) -> str:
    if value.eur < 0 or value.usd < 0:
        raise ReconciliationError("README fiat values must not be negative")
    eur = value.eur.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    usd = value.usd.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return f"≈ €{eur:,.0f} / US${usd:,.0f}"


def _format_rate(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _format_csv_fiat(value: Decimal) -> str:
    rounded = abs(value).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
    unsigned = f"{rounded:.6f}".rstrip("0").rstrip(".")
    if "." not in unsigned:
        unsigned += ".00"
    else:
        whole, fraction = unsigned.split(".", maxsplit=1)
        unsigned = f"{whole}.{fraction.ljust(2, '0')}"
    sign = "-" if value < 0 else "+"
    return sign + unsigned


def _format_percentage(value: Fraction) -> str:
    return (
        f"{value.numerator}%"
        if value.denominator == 1
        else f"{float(value):.2f}%"
    )


def _format_ratio(numerator: int, denominator: int) -> str:
    hundredths = (numerator * 10_000 + denominator // 2) // denominator
    return f"{hundredths // 100}.{hundredths % 100:02d}%"


def write_accounting_book(
    log_dir: Path,
    existing_rows: Sequence[PlannedRow],
    new_rows: Sequence[PlannedRow],
    rates: Sequence[ExchangeRate],
) -> list[Path]:
    rates_by_date = {rate.date: rate for rate in rates}
    if len(rates_by_date) != len(rates):
        raise ReconciliationError("Duplicate transaction date in exchange rates")
    by_month: dict[str, list[PlannedRow]] = {}
    hashes: set[str] = set()
    for row in (*existing_rows, *new_rows):
        transaction_hash = row.values[1]
        if transaction_hash in hashes:
            raise ReconciliationError(
                f"Duplicate transaction hash while writing: {transaction_hash}"
            )
        hashes.add(transaction_hash)
        by_month.setdefault(row.month, []).append(row)

    months = {path.stem for path in log_dir.glob("*.csv")} | set(by_month)
    written: list[Path] = []
    for month in sorted(months):
        month_rows = by_month.get(month, [])
        path = log_dir / f"{month}.csv"
        month_rows.sort(key=lambda row: (row.timestamp, row.values[1]))
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(CSV_HEADER)
        writer.writerows(
            _ledger_csv_values(row, rates_by_date) for row in month_rows
        )
        _atomic_write(path, buffer.getvalue().encode("utf-8"))
        written.append(path)
    return written


def _ledger_csv_values(
    row: PlannedRow, rates_by_date: dict[str, ExchangeRate]
) -> tuple[str, ...]:
    try:
        rate = rates_by_date[row.values[0]]
    except KeyError as error:
        raise ReconciliationError(
            f"Missing exchange rate for ledger date {row.values[0]}"
        ) from error
    if not rate.is_available:
        return (*row.values[:6], "", "", row.values[6])
    if rate.eur is None or rate.usd is None:
        raise ReconciliationError(
            f"Incomplete exchange rate for ledger date {row.values[0]}"
        )
    signed_atto = Decimal(amount_to_raw(row.values[5])) / Decimal(RAW_PER_ATTO)
    return (
        *row.values[:6],
        _format_csv_fiat(signed_atto * rate.eur),
        _format_csv_fiat(signed_atto * rate.usd),
        row.values[6],
    )


def write_address_labels(
    path: Path,
    account_directory: dict[str, AccountMetadata],
    ledger_rows: Sequence[PlannedRow],
) -> Path:
    used_addresses = {
        address
        for row in ledger_rows
        for address in (row.values[2], row.values[3])
    }
    labels_by_address = {
        metadata.address: metadata.label
        for metadata in account_directory.values()
        if metadata.address and metadata.label
    }
    rows = sorted(
        (
            (address, labels_by_address[address])
            for address in used_addresses
            if address in labels_by_address
        ),
        key=lambda row: (row[1].casefold(), row[0]),
    )
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(ADDRESS_CSV_HEADER)
    writer.writerows(rows)
    _atomic_write(path, buffer.getvalue().encode("utf-8"))
    return path


def _atomic_write(path: Path, content: bytes) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def write_planned_rows(
    log_dir: Path,
    rows: Sequence[PlannedRow],
    rates: Sequence[ExchangeRate],
) -> list[Path]:
    """Write a new accounting book. Kept as a small public helper for tests."""
    return write_accounting_book(log_dir, (), rows, rates)


def print_plan(plan: ReconciliationPlan, log_dir: Path) -> None:
    if plan.rows:
        print("Missing ledger entries:")
        for row in plan.rows:
            print(f"  {log_dir / (row.month + '.csv')}: {','.join(row.values)}")
    else:
        print("No classified ledger entries are missing.")
    if plan.ignored_setup_hashes:
        print(f"Ignored {len(plan.ignored_setup_hashes)} setup/small transaction(s).")
    if plan.unknown_entries:
        print("Unclassified material entries:", file=sys.stderr)
        for entry in plan.unknown_entries:
            print(
                f"  {entry.hash}: {raw_to_amount(entry.delta_raw)} ATTO, "
                f"{entry.block_type.lower()} counterparty {entry.subject_public_key}",
                file=sys.stderr,
            )


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Migrate the book and write classified missing entries; default is dry-run.",
    )
    parser.add_argument(
        "--node",
        default="https://node-public.live.application.atto.cash",
        help="Atto node base URL.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).with_name("reconcile-config.json"),
        help="Reconciliation policy file.",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "log",
        help="Directory containing monthly CSV logs.",
    )
    parser.add_argument(
        "--addresses-file",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "addresses.csv",
        help="CSV snapshot of Gatekeeper address labels.",
    )
    parser.add_argument(
        "--readme",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "README.md",
        help="Generated treasury summary README.",
    )
    parser.add_argument(
        "--exchange-rates-file",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "exchange-rates.csv",
        help="Incremental Gatekeeper rate cache for ledger transaction dates.",
    )
    parser.add_argument("--workers", type=int, default=8)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    config = load_config(args.config)
    sources, account_directory = discover_accounts(config)
    config = Config(
        timezone_name=config.timezone_name,
        sources=sources,
        rules=config.rules,
        ignore_below_raw=config.ignore_below_raw,
        setup_window_seconds=config.setup_window_seconds,
        setup_max_ratio=config.setup_max_ratio,
        include_setup_transactions=config.include_setup_transactions,
        projection_url=config.projection_url,
        projection_source_labels=config.projection_source_labels,
        projection_source_prefixes=config.projection_source_prefixes,
        address_labels=config.address_labels,
        total_supply_raw=config.total_supply_raw,
        token_allocations=config.token_allocations,
        whitepaper_url=config.whitepaper_url,
        gatekeeper_metrics_url=config.gatekeeper_metrics_url,
        price_history_lookback_days=config.price_history_lookback_days,
    )
    rows = load_logged_rows(args.log_dir)
    client = NodeClient(args.node)
    logged_entries = resolve_logged_entries(client, rows, args.workers)
    source_entries = fetch_source_entries(client, config.sources, args.workers)
    source_rows = select_source_rows(rows, logged_entries, config.sources)
    excluded_row_count = len(rows) - len(source_rows)
    warnings = validate_logged_rows(
        source_rows, logged_entries, config.timezone_name, account_directory
    )
    plan = build_plan(config, rows, source_entries, account_directory)
    normalized_rows = normalize_logged_rows(
        source_rows,
        logged_entries,
        config.timezone_name,
        account_directory,
    )
    reconciled_rows = (*normalized_rows, *plan.rows)
    allocation_statuses = calculate_allocation_statuses(config, reconciled_rows)
    validate_treasury_balance(allocation_statuses, source_entries)
    exchange_rates, current_rate, updated_rate_count, removed_rate_count = (
        reconcile_exchange_rates(
            args.exchange_rates_file,
            reconciled_rows,
            config.gatekeeper_metrics_url,
            config.price_history_lookback_days,
        )
    )

    for warning in warnings:
        print(f"WARNING: {warning}", file=sys.stderr)
    print_plan(plan, args.log_dir)
    if plan.unknown_entries:
        print("No files changed; add classification rules and rerun.", file=sys.stderr)
        return 2
    if args.apply:
        written = write_accounting_book(
            args.log_dir, normalized_rows, plan.rows, exchange_rates
        )
        for path in written:
            print(f"Updated {path}")
        if excluded_row_count:
            print(f"Removed {excluded_row_count} non-treasury row(s)")
        print(
            f"Updated {write_address_labels(args.addresses_file, account_directory, reconciled_rows)}"
        )
        print(f"Updated {write_exchange_rates(args.exchange_rates_file, exchange_rates)}")
        if updated_rate_count:
            print(
                f"Cached or corrected {updated_rate_count} transaction-date "
                "exchange-rate row(s)"
            )
        if removed_rate_count:
            print(f"Removed {removed_rate_count} unused exchange rate(s)")
        print(
            f"Updated {write_readme(args.readme, config, reconciled_rows, allocation_statuses, exchange_rates, current_rate)}"
        )
    else:
        print(
            f"Dry-run only; --apply will normalize {len(normalized_rows)} existing "
            f"treasury row(s), remove {excluded_row_count} non-treasury row(s), "
            f"add {len(plan.rows)} missing row(s), and refresh "
            f"{args.addresses_file}, {args.exchange_rates_file}, and {args.readme}. "
            f"Exchange-rate changes: {updated_rate_count} added/corrected, "
            f"{removed_rate_count} unused."
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, ReconciliationError, json.JSONDecodeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
