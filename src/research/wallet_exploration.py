from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from src.clients.rest import (
    parse_optional_bool,
    parse_optional_datetime,
    parse_optional_decimal,
    parse_optional_int,
    parse_optional_str,
)


SUPPORTED_COHORT_METRICS = (
    "realized_pnl",
    "realized_roi",
    "hit_rate",
    "activity_volume_usdc",
    "activity_trade_count",
    "closed_position_count",
)
DEFAULT_WALLET_TABLES = (
    "wallet_profiles",
    "wallet_positions",
    "wallet_closed_positions",
    "trades",
)
_CAPTURE_SUFFIXES = frozenset({".json", ".jsonl"})


@dataclass(frozen=True, slots=True)
class WalletSeedMetadata:
    wallet_address: str
    rank: int | None
    leaderboard_pnl: Decimal | None
    leaderboard_volume: Decimal | None
    user_name: str | None
    verified_badge: bool | None


@dataclass(frozen=True, slots=True)
class WalletCohortProfile:
    wallet_address: str
    rank: int | None
    leaderboard_pnl: Decimal | None
    leaderboard_volume: Decimal | None
    user_name: str | None
    verified_badge: bool | None
    as_of_time_utc: datetime
    realized_pnl: Decimal
    realized_roi: Decimal | None
    closed_position_count: int
    winning_closed_position_count: int
    hit_rate: Decimal | None
    avg_closed_position_cost: Decimal | None
    activity_trade_count: int
    activity_volume_usdc: Decimal
    avg_trade_size_usdc: Decimal | None
    first_activity_time_utc: datetime | None
    last_activity_time_utc: datetime | None
    last_closed_position_time_utc: datetime | None


@dataclass(frozen=True, slots=True)
class WalletRawCaptureInfo:
    dataset: str
    wallet_address: str | None
    path: Path
    collection_time_utc: datetime | None
    record_count: int


@dataclass(frozen=True, slots=True)
class WalletActivityTrade:
    trade_time_utc: datetime
    side: str | None
    condition_id: str | None
    outcome: str | None
    volume_usdc: Decimal | None
    size: Decimal | None
    price: Decimal | None
    transaction_hash: str | None
    label: str


@dataclass(frozen=True, slots=True)
class WalletClosedPositionPoint:
    closed_at_utc: datetime
    condition_id: str | None
    outcome: str | None
    realized_pnl: Decimal | None
    total_bought: Decimal | None
    cumulative_realized_pnl: Decimal
    label: str


@dataclass(frozen=True, slots=True)
class WalletOpenPositionSnapshot:
    condition_id: str | None
    outcome: str | None
    current_value: Decimal | None
    size: Decimal | None
    average_price: Decimal | None
    realized_pnl: Decimal | None
    total_bought: Decimal | None
    end_time_utc: datetime | None
    label: str


def latest_raw_capture_path(raw_dir: str | Path, source: str, dataset: str) -> Path | None:
    capture_dir = Path(raw_dir) / source / dataset
    capture_paths = sorted(path for path in capture_dir.glob("date=*/*") if path.suffix in _CAPTURE_SUFFIXES)
    return capture_paths[-1] if capture_paths else None


def load_json_capture(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text())
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object capture at {path}.")
    return payload


def load_jsonl_capture(path: str | Path) -> list[dict[str, Any]]:
    rows = [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
    return [row for row in rows if isinstance(row, dict)]


def list_wallet_seed_metadata(raw_dir: str | Path) -> list[WalletSeedMetadata]:
    path = latest_raw_capture_path(raw_dir, "data_api", "wallet_seed_list")
    if path is None:
        return []

    metadata_rows: list[WalletSeedMetadata] = []
    for record in load_jsonl_capture(path):
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        wallet_address = parse_optional_str(payload.get("wallet_address"))
        if not wallet_address:
            continue
        metadata_rows.append(
            WalletSeedMetadata(
                wallet_address=wallet_address,
                rank=parse_optional_int(payload.get("rank")),
                leaderboard_pnl=parse_optional_decimal(payload.get("pnl")),
                leaderboard_volume=parse_optional_decimal(payload.get("volume")),
                user_name=parse_optional_str(payload.get("user_name")),
                verified_badge=parse_optional_bool(payload.get("verified_badge")),
            )
        )

    return sorted(
        metadata_rows,
        key=lambda row: (
            row.rank is None,
            row.rank if row.rank is not None else 0,
            row.wallet_address,
        ),
    )


def list_latest_wallet_dataset_captures(raw_dir: str | Path, dataset: str) -> list[WalletRawCaptureInfo]:
    dataset_dir = Path(raw_dir) / "data_api" / dataset
    capture_paths = sorted(path for path in dataset_dir.glob("date=*/*") if path.suffix in _CAPTURE_SUFFIXES)
    captures_by_wallet: dict[str | None, WalletRawCaptureInfo] = {}

    for path in capture_paths:
        envelope = _load_capture_envelope(path)
        metadata = envelope.get("metadata")
        wallet_address = parse_optional_str(metadata.get("wallet_address")) if isinstance(metadata, dict) else None
        candidate = WalletRawCaptureInfo(
            dataset=dataset,
            wallet_address=wallet_address,
            path=path,
            collection_time_utc=parse_optional_datetime(envelope.get("collection_time_utc")),
            record_count=_capture_record_count(path, envelope),
        )
        existing = captures_by_wallet.get(wallet_address)
        if existing is None or _capture_sort_key(candidate) >= _capture_sort_key(existing):
            captures_by_wallet[wallet_address] = candidate

    return sorted(
        captures_by_wallet.values(),
        key=lambda row: (
            row.wallet_address is None,
            row.wallet_address or "",
            row.path.name,
        ),
    )


def get_table_counts(
    warehouse_path: str | Path,
    *,
    table_names: tuple[str, ...] = DEFAULT_WALLET_TABLES,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        for table_name in table_names:
            counts[table_name] = int(connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
    return counts


def list_wallet_cohort_profiles(warehouse_path: str | Path, raw_dir: str | Path) -> list[WalletCohortProfile]:
    seed_metadata_index = {row.wallet_address: row for row in list_wallet_seed_metadata(raw_dir)}
    rows = _fetch_rows(
        warehouse_path,
        """
        WITH ranked_profiles AS (
            SELECT
                wallet_address,
                as_of_time_utc,
                realized_pnl,
                realized_roi,
                closed_position_count,
                winning_closed_position_count,
                hit_rate,
                avg_closed_position_cost,
                activity_trade_count,
                activity_volume_usdc,
                avg_trade_size_usdc,
                first_activity_time_utc,
                last_activity_time_utc,
                last_closed_position_time_utc,
                ROW_NUMBER() OVER (
                    PARTITION BY wallet_address
                    ORDER BY as_of_time_utc DESC, collection_time_utc DESC, updated_at_utc DESC, profile_id DESC
                ) AS profile_rank
            FROM wallet_profiles
        )
        SELECT
            wallet_address,
            as_of_time_utc,
            realized_pnl,
            realized_roi,
            closed_position_count,
            winning_closed_position_count,
            hit_rate,
            avg_closed_position_cost,
            activity_trade_count,
            activity_volume_usdc,
            avg_trade_size_usdc,
            first_activity_time_utc,
            last_activity_time_utc,
            last_closed_position_time_utc
        FROM ranked_profiles
        WHERE profile_rank = 1
        ORDER BY wallet_address ASC
        """,
    )

    cohort_rows = [
        _build_wallet_cohort_profile(row, seed_metadata_index.get(str(row["wallet_address"])))
        for row in rows
    ]

    return sorted(
        cohort_rows,
        key=lambda row: (
            row.rank is None,
            row.rank if row.rank is not None else 0,
            row.wallet_address,
        ),
    )


def list_wallet_activity_trades(warehouse_path: str | Path, wallet_address: str) -> list[WalletActivityTrade]:
    rows = _fetch_rows(
        warehouse_path,
        """
        SELECT
            trade_time_utc,
            side,
            condition_id,
            outcome,
            COALESCE(usdc_size, size * price) AS volume_usdc,
            size,
            price,
            transaction_hash
        FROM trades
        WHERE proxy_wallet = ? AND source = 'data_api.wallet_activity' AND trade_time_utc IS NOT NULL
        ORDER BY trade_time_utc ASC, transaction_hash ASC NULLS LAST
        """,
        [wallet_address],
    )

    return [
        WalletActivityTrade(
            trade_time_utc=parse_optional_datetime(row["trade_time_utc"]),
            side=row["side"],
            condition_id=row["condition_id"],
            outcome=row["outcome"],
            volume_usdc=row["volume_usdc"],
            size=row["size"],
            price=row["price"],
            transaction_hash=row["transaction_hash"],
            label=market_label(row["condition_id"], row["outcome"]),
        )
        for row in rows
    ]


def list_wallet_closed_position_points(
    warehouse_path: str | Path,
    wallet_address: str,
) -> list[WalletClosedPositionPoint]:
    rows = _fetch_rows(
        warehouse_path,
        """
        SELECT
            closed_at_utc,
            condition_id,
            outcome,
            realized_pnl,
            total_bought
        FROM wallet_closed_positions
        WHERE wallet_address = ? AND closed_at_utc IS NOT NULL
        ORDER BY closed_at_utc ASC, condition_id ASC NULLS LAST, outcome ASC NULLS LAST
        """,
        [wallet_address],
    )

    cumulative_realized_pnl = Decimal("0")
    points: list[WalletClosedPositionPoint] = []
    for row in rows:
        realized_pnl = row["realized_pnl"]
        if realized_pnl is not None:
            cumulative_realized_pnl += realized_pnl
        points.append(
            WalletClosedPositionPoint(
                closed_at_utc=parse_optional_datetime(row["closed_at_utc"]),
                condition_id=row["condition_id"],
                outcome=row["outcome"],
                realized_pnl=realized_pnl,
                total_bought=row["total_bought"],
                cumulative_realized_pnl=cumulative_realized_pnl,
                label=market_label(row["condition_id"], row["outcome"]),
            )
        )

    return points


def list_wallet_open_positions(warehouse_path: str | Path, wallet_address: str) -> list[WalletOpenPositionSnapshot]:
    rows = _fetch_rows(
        warehouse_path,
        """
        SELECT
            condition_id,
            outcome,
            current_value,
            size,
            average_price,
            realized_pnl,
            total_bought,
            end_time_utc
        FROM wallet_positions
        WHERE wallet_address = ?
        ORDER BY current_value DESC NULLS LAST, size DESC NULLS LAST, condition_id ASC NULLS LAST, outcome ASC NULLS LAST
        """,
        [wallet_address],
    )

    return [
        WalletOpenPositionSnapshot(
            condition_id=row["condition_id"],
            outcome=row["outcome"],
            current_value=row["current_value"],
            size=row["size"],
            average_price=row["average_price"],
            realized_pnl=row["realized_pnl"],
            total_bought=row["total_bought"],
            end_time_utc=parse_optional_datetime(row["end_time_utc"]),
            label=market_label(row["condition_id"], row["outcome"]),
        )
        for row in rows
    ]


def market_label(condition_id: str | None, outcome: str | None) -> str:
    normalized_condition_id = parse_optional_str(condition_id)
    normalized_outcome = parse_optional_str(outcome)
    if normalized_condition_id and normalized_outcome:
        return f"{normalized_condition_id} / {normalized_outcome}"
    if normalized_condition_id:
        return normalized_condition_id
    if normalized_outcome:
        return normalized_outcome
    return "<unknown>"


def wallet_display_name(wallet_address: str, *, user_name: str | None = None) -> str:
    normalized_user_name = parse_optional_str(user_name)
    if normalized_user_name:
        return normalized_user_name
    if len(wallet_address) <= 12:
        return wallet_address
    return f"{wallet_address[:6]}...{wallet_address[-4:]}"


def _fetch_rows(
    warehouse_path: str | Path,
    sql: str,
    params: list[object] | tuple[object, ...] | None = None,
) -> list[dict[str, object]]:
    query_params = params or []
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        cursor = connection.execute(sql, query_params)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _load_capture_envelope(path: Path) -> dict[str, Any]:
    if path.suffix == ".json":
        return load_json_capture(path)
    records = load_jsonl_capture(path)
    return records[0] if records else {}


def _capture_record_count(path: Path, envelope: dict[str, Any]) -> int:
    record_count = parse_optional_int(envelope.get("record_count"))
    if record_count is not None:
        return record_count
    if path.suffix == ".json":
        payload = envelope.get("payload")
        if isinstance(payload, list):
            return len(payload)
        return 1 if envelope else 0
    return len(load_jsonl_capture(path))


def _capture_sort_key(capture: WalletRawCaptureInfo) -> tuple[str, str]:
    return (
        capture.collection_time_utc.isoformat() if capture.collection_time_utc is not None else "",
        str(capture.path),
    )


def _build_wallet_cohort_profile(
    row: dict[str, object],
    seed_metadata: WalletSeedMetadata | None,
) -> WalletCohortProfile:
    return WalletCohortProfile(
        wallet_address=str(row["wallet_address"]),
        rank=seed_metadata.rank if seed_metadata is not None else None,
        leaderboard_pnl=seed_metadata.leaderboard_pnl if seed_metadata is not None else None,
        leaderboard_volume=seed_metadata.leaderboard_volume if seed_metadata is not None else None,
        user_name=seed_metadata.user_name if seed_metadata is not None else None,
        verified_badge=seed_metadata.verified_badge if seed_metadata is not None else None,
        as_of_time_utc=parse_optional_datetime(row["as_of_time_utc"]),
        realized_pnl=row["realized_pnl"],
        realized_roi=row["realized_roi"],
        closed_position_count=int(row["closed_position_count"]),
        winning_closed_position_count=int(row["winning_closed_position_count"]),
        hit_rate=row["hit_rate"],
        avg_closed_position_cost=row["avg_closed_position_cost"],
        activity_trade_count=int(row["activity_trade_count"]),
        activity_volume_usdc=row["activity_volume_usdc"],
        avg_trade_size_usdc=row["avg_trade_size_usdc"],
        first_activity_time_utc=parse_optional_datetime(row["first_activity_time_utc"]),
        last_activity_time_utc=parse_optional_datetime(row["last_activity_time_utc"]),
        last_closed_position_time_utc=parse_optional_datetime(row["last_closed_position_time_utc"]),
    )
