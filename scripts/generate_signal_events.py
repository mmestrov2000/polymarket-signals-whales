#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Sequence

import duckdb
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.clients.clob import PriceHistoryPoint  # noqa: E402
from src.clients.data_api import TradeRecord  # noqa: E402
from src.signals import WalletProfile, detect_signal_events  # noqa: E402
from src.storage import DEFAULT_WAREHOUSE_PATH, PolymarketWarehouse, TopOfBookSnapshot  # noqa: E402


@dataclass(frozen=True, slots=True)
class SkippedAsset:
    asset_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class AssetSignalEventResult:
    asset_id: str
    condition_id: str | None
    trade_count: int
    price_point_count: int
    order_book_snapshot_count: int
    event_count: int


@dataclass(frozen=True, slots=True)
class SignalEventMaterializationSummary:
    selected_asset_ids: tuple[str, ...]
    wallet_profile_count: int
    events_written: int
    skipped_assets: tuple[SkippedAsset, ...]
    asset_results: tuple[AssetSignalEventResult, ...]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Milestone 4 signal events from the normalized warehouse inputs."
    )
    parser.add_argument(
        "--warehouse-path",
        default=str(DEFAULT_WAREHOUSE_PATH),
        help=f"DuckDB path for normalized inputs and signal event outputs. Defaults to {DEFAULT_WAREHOUSE_PATH}.",
    )
    parser.add_argument(
        "--asset-id",
        dest="asset_ids",
        action="append",
        default=None,
        help="Optional asset/token id to process. Repeat to limit the run to specific assets.",
    )
    return parser.parse_args(argv)


def materialize_signal_events(
    *,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE_PATH,
    asset_ids: Sequence[str] | None = None,
    collection_time: datetime | None = None,
) -> SignalEventMaterializationSummary:
    resolved_warehouse_path = Path(warehouse_path)
    with _connect_read_only(resolved_warehouse_path) as connection:
        selected_asset_ids = tuple(asset_ids) if asset_ids else _list_asset_ids(connection)
        wallet_profiles = _load_wallet_profiles(connection)

        skipped_assets: list[SkippedAsset] = []
        asset_results: list[AssetSignalEventResult] = []
        all_events = []

        for asset_id in selected_asset_ids:
            condition_id = _resolve_condition_id(connection, asset_id)
            trades = _load_trades(connection, asset_id)
            price_points = _load_price_points(connection, asset_id)
            snapshots = _load_order_book_snapshots(connection, asset_id)

            if not trades:
                skipped_assets.append(SkippedAsset(asset_id=asset_id, reason="missing_trades"))
                continue
            if not price_points:
                skipped_assets.append(SkippedAsset(asset_id=asset_id, reason="missing_price_history"))
                continue

            detected_events = detect_signal_events(
                asset_id=asset_id,
                condition_id=condition_id,
                trades=trades,
                price_points=price_points,
                order_book_snapshots=snapshots,
                wallet_profiles=wallet_profiles,
            )
            all_events.extend(detected_events)
            asset_results.append(
                AssetSignalEventResult(
                    asset_id=asset_id,
                    condition_id=condition_id,
                    trade_count=len(trades),
                    price_point_count=len(price_points),
                    order_book_snapshot_count=len(snapshots),
                    event_count=len(detected_events),
                )
            )

    with PolymarketWarehouse(resolved_warehouse_path) as warehouse:
        events_written = warehouse.upsert_signal_events(all_events, collection_time=collection_time)

    return SignalEventMaterializationSummary(
        selected_asset_ids=selected_asset_ids,
        wallet_profile_count=len(wallet_profiles),
        events_written=events_written,
        skipped_assets=tuple(skipped_assets),
        asset_results=tuple(asset_results),
    )


def format_summary(summary: SignalEventMaterializationSummary, *, warehouse_path: Path) -> str:
    selected_assets = ", ".join(summary.selected_asset_ids) if summary.selected_asset_ids else "none"
    lines = [
        "Signal event materialization summary",
        f"Selected assets ({len(summary.selected_asset_ids)}): {selected_assets}",
        f"Wallet profiles loaded: {summary.wallet_profile_count}",
        f"Signal events written: {summary.events_written}",
        f"Warehouse path: {warehouse_path}",
    ]

    if summary.asset_results:
        lines.append(
            "Per-asset results: "
            + "; ".join(
                (
                    f"{result.asset_id} "
                    f"(condition_id={result.condition_id or 'unknown'}, "
                    f"trades={result.trade_count}, "
                    f"price_points={result.price_point_count}, "
                    f"order_book_snapshots={result.order_book_snapshot_count}, "
                    f"events={result.event_count})"
                )
                for result in summary.asset_results
            )
        )

    if summary.skipped_assets:
        lines.append(
            "Skipped assets: "
            + ", ".join(f"{item.asset_id}:{item.reason}" for item in summary.skipped_assets)
        )

    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(REPO_ROOT / ".env")

    try:
        summary = materialize_signal_events(
            warehouse_path=Path(args.warehouse_path),
            asset_ids=tuple(args.asset_ids or ()),
        )
    except Exception as exc:
        print(f"Signal event materialization failed before completion: {exc}", file=sys.stderr)
        return 1

    print(format_summary(summary, warehouse_path=Path(args.warehouse_path)))
    return 0


def _connect_read_only(database_path: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(database_path), read_only=True)


def _list_asset_ids(connection: duckdb.DuckDBPyConnection) -> tuple[str, ...]:
    rows = connection.execute(
        """
        SELECT asset_id
        FROM (
            SELECT token_id AS asset_id FROM price_history
            UNION
            SELECT asset_id FROM trades
            UNION
            SELECT asset_id FROM order_book_snapshots
        )
        WHERE asset_id IS NOT NULL
        ORDER BY asset_id ASC
        """
    ).fetchall()
    return tuple(row[0] for row in rows if row and row[0])


def _load_wallet_profiles(connection: duckdb.DuckDBPyConnection) -> tuple[WalletProfile, ...]:
    rows = connection.execute(
        """
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
        FROM wallet_profiles
        ORDER BY wallet_address ASC, as_of_time_utc ASC
        """
    ).fetchall()
    return tuple(
        WalletProfile(
            wallet_address=row[0],
            as_of_time_utc=_normalize_timestamp(row[1]),
            realized_pnl=row[2],
            realized_roi=row[3],
            closed_position_count=row[4],
            winning_closed_position_count=row[5],
            hit_rate=row[6],
            avg_closed_position_cost=row[7],
            activity_trade_count=row[8],
            activity_volume_usdc=row[9],
            avg_trade_size_usdc=row[10],
            first_activity_time_utc=_normalize_nullable_timestamp(row[11]),
            last_activity_time_utc=_normalize_nullable_timestamp(row[12]),
            last_closed_position_time_utc=_normalize_nullable_timestamp(row[13]),
        )
        for row in rows
        if row and row[0] and row[1] is not None
    )


def _resolve_condition_id(connection: duckdb.DuckDBPyConnection, asset_id: str) -> str | None:
    trade_row = connection.execute(
        """
        SELECT condition_id
        FROM trades
        WHERE asset_id = ? AND condition_id IS NOT NULL
        ORDER BY trade_time_utc DESC NULLS LAST, collection_time_utc DESC
        LIMIT 1
        """,
        [asset_id],
    ).fetchone()
    if trade_row and trade_row[0]:
        return trade_row[0]

    market_row = connection.execute(
        """
        SELECT markets.condition_id
        FROM market_tokens
        JOIN markets ON markets.market_id = market_tokens.market_id
        WHERE market_tokens.token_id = ? AND markets.condition_id IS NOT NULL
        ORDER BY markets.collection_time_utc DESC
        LIMIT 1
        """,
        [asset_id],
    ).fetchone()
    return market_row[0] if market_row and market_row[0] else None


def _load_trades(connection: duckdb.DuckDBPyConnection, asset_id: str) -> tuple[TradeRecord, ...]:
    rows = connection.execute(
        """
        SELECT
            proxy_wallet,
            asset_id,
            condition_id,
            outcome,
            side,
            size,
            price,
            trade_time_utc,
            transaction_hash,
            usdc_size
        FROM trades
        WHERE asset_id = ?
        ORDER BY trade_time_utc ASC NULLS LAST, collection_time_utc ASC, transaction_hash ASC
        """,
        [asset_id],
    ).fetchall()
    return tuple(
        TradeRecord(
            proxy_wallet=row[0],
            asset_id=row[1],
            condition_id=row[2],
            outcome=row[3],
            side=row[4],
            size=row[5],
            price=row[6],
            timestamp=_normalize_nullable_timestamp(row[7]),
            transaction_hash=row[8],
            usdc_size=row[9],
        )
        for row in rows
    )


def _load_price_points(connection: duckdb.DuckDBPyConnection, asset_id: str) -> tuple[PriceHistoryPoint, ...]:
    preferred_series = connection.execute(
        """
        SELECT interval, fidelity
        FROM (
            SELECT
                interval,
                fidelity,
                COUNT(*) AS point_count,
                MAX(collection_time_utc) AS latest_collection_time_utc
            FROM price_history
            WHERE token_id = ? AND price_time_utc IS NOT NULL
            GROUP BY interval, fidelity
        )
        ORDER BY point_count DESC, latest_collection_time_utc DESC, fidelity ASC, interval ASC
        LIMIT 1
        """,
        [asset_id],
    ).fetchone()
    if preferred_series is None:
        return ()

    rows = connection.execute(
        """
        SELECT price_time_utc, price
        FROM price_history
        WHERE token_id = ? AND interval = ? AND fidelity = ?
        ORDER BY price_time_utc ASC
        """,
        [asset_id, preferred_series[0], preferred_series[1]],
    ).fetchall()
    return tuple(
        PriceHistoryPoint(
            timestamp=_normalize_timestamp(row[0]),
            price=row[1],
        )
        for row in rows
        if row and row[0] is not None
    )


def _load_order_book_snapshots(
    connection: duckdb.DuckDBPyConnection,
    asset_id: str,
) -> tuple[TopOfBookSnapshot, ...]:
    rows = connection.execute(
        """
        SELECT
            market_id,
            asset_id,
            best_bid_price,
            best_bid_size,
            best_ask_price,
            best_ask_size,
            last_trade_price,
            tick_size,
            book_hash,
            snapshot_time_utc
        FROM order_book_snapshots
        WHERE asset_id = ?
        ORDER BY snapshot_time_utc ASC NULLS LAST, collection_time_utc ASC, book_hash ASC
        """,
        [asset_id],
    ).fetchall()
    return tuple(
        TopOfBookSnapshot(
            market_id=row[0],
            asset_id=row[1],
            best_bid_price=row[2],
            best_bid_size=row[3],
            best_ask_price=row[4],
            best_ask_size=row[5],
            last_trade_price=row[6],
            tick_size=row[7],
            book_hash=row[8],
            snapshot_time=_normalize_nullable_timestamp(row[9]),
        )
        for row in rows
    )


def _normalize_timestamp(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _normalize_nullable_timestamp(value: datetime | None) -> datetime | None:
    return _normalize_timestamp(value) if value is not None else None


if __name__ == "__main__":
    raise SystemExit(main())
