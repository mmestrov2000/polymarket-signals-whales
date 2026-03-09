from __future__ import annotations

import json
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import duckdb

from src.clients.clob import OrderBookSnapshot, PriceHistory
from src.clients.data_api import ClosedPosition, PositionSnapshot, TradeRecord
from src.clients.gamma import GammaMarket
from src.signals import SignalEvent, WalletProfile


DEFAULT_WAREHOUSE_PATH = Path("data/warehouse/polymarket.duckdb")
DECIMAL_SQL_TYPE = "DECIMAL(38, 18)"


@dataclass(frozen=True, slots=True)
class TopOfBookSnapshot:
    market_id: str | None
    asset_id: str | None
    best_bid_price: Decimal | None
    best_bid_size: Decimal | None
    best_ask_price: Decimal | None
    best_ask_size: Decimal | None
    last_trade_price: Decimal | None
    tick_size: Decimal | None
    book_hash: str | None
    snapshot_time: datetime | None

    @classmethod
    def from_order_book_snapshot(cls, snapshot: OrderBookSnapshot) -> TopOfBookSnapshot:
        best_bid_price, best_bid_size = _best_level(snapshot.bids)
        best_ask_price, best_ask_size = _best_level(snapshot.asks)
        return cls(
            market_id=snapshot.market_id,
            asset_id=snapshot.asset_id,
            best_bid_price=best_bid_price,
            best_bid_size=best_bid_size,
            best_ask_price=best_ask_price,
            best_ask_size=best_ask_size,
            last_trade_price=snapshot.last_trade_price,
            tick_size=snapshot.tick_size,
            book_hash=snapshot.book_hash,
            snapshot_time=snapshot.timestamp,
        )


@dataclass(frozen=True, slots=True)
class EventDatasetRow:
    dataset_row_id: str
    dataset_build_id: str
    dataset_split: str
    event_id: str
    asset_id: str | None
    condition_id: str | None
    event_time_utc: datetime
    source_event_collection_time_utc: datetime
    direction: str
    trigger_reason: str
    recent_trade_count: int
    recent_volume_usdc: Decimal
    volume_zscore: Decimal | None
    trade_count_zscore: Decimal | None
    order_flow_imbalance: Decimal | None
    short_return: Decimal | None
    medium_return: Decimal | None
    liquidity_features_available: bool
    latest_price: Decimal | None
    latest_mid_price: Decimal | None
    latest_spread_bps: Decimal | None
    spread_change_bps: Decimal | None
    top_of_book_depth_usdc: Decimal | None
    depth_change_ratio: Decimal | None
    depth_imbalance: Decimal | None
    active_wallet_count: int
    profiled_wallet_count: int
    sparse_wallet_set: bool
    profiled_volume_share: Decimal | None
    top_wallet_share: Decimal | None
    concentration_hhi: Decimal | None
    weighted_average_quality: Decimal | None
    weighted_average_realized_roi: Decimal | None
    weighted_average_hit_rate: Decimal | None
    weighted_average_realized_pnl: Decimal | None
    entry_price: Decimal
    entry_price_time_utc: datetime
    assumed_round_trip_cost_bps: Decimal
    primary_label_name: str
    primary_label_horizon_minutes: int
    primary_label_continuation: bool
    primary_label_reversion: bool
    primary_label_profitable: bool
    primary_directional_return_bps: Decimal
    primary_net_pnl_bps: Decimal
    primary_exit_price: Decimal
    primary_exit_time_utc: datetime
    horizon_labels_json: dict[str, object]


class PolymarketWarehouse:
    """Owns normalized DuckDB tables for market, wallet, trade, and live order-book data."""

    def __init__(self, database_path: str | Path = DEFAULT_WAREHOUSE_PATH) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = duckdb.connect(str(self.database_path))
        self.ensure_schema()

    def close(self) -> None:
        with suppress(Exception):
            self._connection.close()

    def __enter__(self) -> PolymarketWarehouse:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def ensure_schema(self) -> None:
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS markets (
                market_id VARCHAR PRIMARY KEY,
                question VARCHAR,
                slug VARCHAR,
                condition_id VARCHAR,
                active BOOLEAN,
                end_time_utc TIMESTAMP,
                liquidity {DECIMAL_SQL_TYPE},
                volume {DECIMAL_SQL_TYPE},
                source VARCHAR NOT NULL,
                collection_time_utc TIMESTAMP NOT NULL,
                updated_at_utc TIMESTAMP NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS market_tokens (
                market_id VARCHAR NOT NULL,
                token_id VARCHAR NOT NULL,
                token_index INTEGER NOT NULL,
                source VARCHAR NOT NULL,
                collection_time_utc TIMESTAMP NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS market_tokens_market_id_token_id_idx
            ON market_tokens (market_id, token_id)
            """
        )
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS price_history (
                price_id VARCHAR PRIMARY KEY,
                token_id VARCHAR NOT NULL,
                interval VARCHAR NOT NULL,
                fidelity INTEGER NOT NULL,
                price_time_utc TIMESTAMP NOT NULL,
                price {DECIMAL_SQL_TYPE},
                source VARCHAR NOT NULL,
                collection_time_utc TIMESTAMP NOT NULL,
                updated_at_utc TIMESTAMP NOT NULL
            )
            """
        )
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id VARCHAR PRIMARY KEY,
                proxy_wallet VARCHAR,
                asset_id VARCHAR,
                condition_id VARCHAR,
                outcome VARCHAR,
                side VARCHAR,
                size {DECIMAL_SQL_TYPE},
                price {DECIMAL_SQL_TYPE},
                transaction_hash VARCHAR,
                usdc_size {DECIMAL_SQL_TYPE},
                trade_time_utc TIMESTAMP,
                source VARCHAR NOT NULL,
                collection_time_utc TIMESTAMP NOT NULL,
                updated_at_utc TIMESTAMP NOT NULL
            )
            """
        )
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS order_book_snapshots (
                snapshot_id VARCHAR PRIMARY KEY,
                asset_id VARCHAR,
                market_id VARCHAR,
                best_bid_price {DECIMAL_SQL_TYPE},
                best_bid_size {DECIMAL_SQL_TYPE},
                best_ask_price {DECIMAL_SQL_TYPE},
                best_ask_size {DECIMAL_SQL_TYPE},
                mid_price {DECIMAL_SQL_TYPE},
                spread {DECIMAL_SQL_TYPE},
                last_trade_price {DECIMAL_SQL_TYPE},
                tick_size {DECIMAL_SQL_TYPE},
                book_hash VARCHAR,
                snapshot_time_utc TIMESTAMP,
                source VARCHAR NOT NULL,
                collection_time_utc TIMESTAMP NOT NULL,
                updated_at_utc TIMESTAMP NOT NULL
            )
            """
        )
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS wallet_positions (
                position_snapshot_id VARCHAR PRIMARY KEY,
                wallet_address VARCHAR NOT NULL,
                asset_id VARCHAR,
                condition_id VARCHAR,
                outcome VARCHAR,
                outcome_index INTEGER,
                size {DECIMAL_SQL_TYPE},
                average_price {DECIMAL_SQL_TYPE},
                current_value {DECIMAL_SQL_TYPE},
                realized_pnl {DECIMAL_SQL_TYPE},
                total_bought {DECIMAL_SQL_TYPE},
                end_time_utc TIMESTAMP,
                source VARCHAR NOT NULL,
                collection_time_utc TIMESTAMP NOT NULL,
                updated_at_utc TIMESTAMP NOT NULL
            )
            """
        )
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS wallet_closed_positions (
                closed_position_id VARCHAR PRIMARY KEY,
                wallet_address VARCHAR NOT NULL,
                asset_id VARCHAR,
                condition_id VARCHAR,
                outcome VARCHAR,
                average_price {DECIMAL_SQL_TYPE},
                realized_pnl {DECIMAL_SQL_TYPE},
                total_bought {DECIMAL_SQL_TYPE},
                closed_at_utc TIMESTAMP,
                end_time_utc TIMESTAMP,
                source VARCHAR NOT NULL,
                collection_time_utc TIMESTAMP NOT NULL,
                updated_at_utc TIMESTAMP NOT NULL
            )
            """
        )
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS wallet_profiles (
                profile_id VARCHAR PRIMARY KEY,
                wallet_address VARCHAR NOT NULL,
                as_of_time_utc TIMESTAMP NOT NULL,
                realized_pnl {DECIMAL_SQL_TYPE} NOT NULL,
                realized_roi {DECIMAL_SQL_TYPE},
                closed_position_count INTEGER NOT NULL,
                winning_closed_position_count INTEGER NOT NULL,
                hit_rate {DECIMAL_SQL_TYPE},
                avg_closed_position_cost {DECIMAL_SQL_TYPE},
                activity_trade_count INTEGER NOT NULL,
                activity_volume_usdc {DECIMAL_SQL_TYPE} NOT NULL,
                avg_trade_size_usdc {DECIMAL_SQL_TYPE},
                first_activity_time_utc TIMESTAMP,
                last_activity_time_utc TIMESTAMP,
                last_closed_position_time_utc TIMESTAMP,
                source VARCHAR NOT NULL,
                collection_time_utc TIMESTAMP NOT NULL,
                updated_at_utc TIMESTAMP NOT NULL
            )
            """
        )
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS signal_events (
                event_id VARCHAR PRIMARY KEY,
                asset_id VARCHAR,
                condition_id VARCHAR,
                event_time_utc TIMESTAMP NOT NULL,
                direction VARCHAR,
                trigger_reason VARCHAR NOT NULL,
                trigger_rules_json VARCHAR NOT NULL,
                recent_trade_count INTEGER NOT NULL,
                recent_volume_usdc {DECIMAL_SQL_TYPE} NOT NULL,
                volume_zscore {DECIMAL_SQL_TYPE},
                trade_count_zscore {DECIMAL_SQL_TYPE},
                order_flow_imbalance {DECIMAL_SQL_TYPE},
                short_return {DECIMAL_SQL_TYPE},
                medium_return {DECIMAL_SQL_TYPE},
                liquidity_features_available BOOLEAN NOT NULL,
                active_wallet_count INTEGER NOT NULL,
                profiled_wallet_count INTEGER NOT NULL,
                top_wallet_share {DECIMAL_SQL_TYPE},
                weighted_average_quality {DECIMAL_SQL_TYPE},
                explanation_payload_json VARCHAR NOT NULL,
                source VARCHAR NOT NULL,
                collection_time_utc TIMESTAMP NOT NULL,
                updated_at_utc TIMESTAMP NOT NULL
            )
            """
        )
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS event_dataset_rows (
                dataset_row_id VARCHAR PRIMARY KEY,
                dataset_build_id VARCHAR NOT NULL,
                dataset_split VARCHAR NOT NULL,
                event_id VARCHAR NOT NULL,
                asset_id VARCHAR,
                condition_id VARCHAR,
                event_time_utc TIMESTAMP NOT NULL,
                source_event_collection_time_utc TIMESTAMP NOT NULL,
                direction VARCHAR NOT NULL,
                trigger_reason VARCHAR NOT NULL,
                recent_trade_count INTEGER NOT NULL,
                recent_volume_usdc {DECIMAL_SQL_TYPE} NOT NULL,
                volume_zscore {DECIMAL_SQL_TYPE},
                trade_count_zscore {DECIMAL_SQL_TYPE},
                order_flow_imbalance {DECIMAL_SQL_TYPE},
                short_return {DECIMAL_SQL_TYPE},
                medium_return {DECIMAL_SQL_TYPE},
                liquidity_features_available BOOLEAN NOT NULL,
                latest_price {DECIMAL_SQL_TYPE},
                latest_mid_price {DECIMAL_SQL_TYPE},
                latest_spread_bps {DECIMAL_SQL_TYPE},
                spread_change_bps {DECIMAL_SQL_TYPE},
                top_of_book_depth_usdc {DECIMAL_SQL_TYPE},
                depth_change_ratio {DECIMAL_SQL_TYPE},
                depth_imbalance {DECIMAL_SQL_TYPE},
                active_wallet_count INTEGER NOT NULL,
                profiled_wallet_count INTEGER NOT NULL,
                sparse_wallet_set BOOLEAN NOT NULL,
                profiled_volume_share {DECIMAL_SQL_TYPE},
                top_wallet_share {DECIMAL_SQL_TYPE},
                concentration_hhi {DECIMAL_SQL_TYPE},
                weighted_average_quality {DECIMAL_SQL_TYPE},
                weighted_average_realized_roi {DECIMAL_SQL_TYPE},
                weighted_average_hit_rate {DECIMAL_SQL_TYPE},
                weighted_average_realized_pnl {DECIMAL_SQL_TYPE},
                entry_price {DECIMAL_SQL_TYPE} NOT NULL,
                entry_price_time_utc TIMESTAMP NOT NULL,
                assumed_round_trip_cost_bps {DECIMAL_SQL_TYPE} NOT NULL,
                primary_label_name VARCHAR NOT NULL,
                primary_label_horizon_minutes INTEGER NOT NULL,
                primary_label_continuation BOOLEAN NOT NULL,
                primary_label_reversion BOOLEAN NOT NULL,
                primary_label_profitable BOOLEAN NOT NULL,
                primary_directional_return_bps {DECIMAL_SQL_TYPE} NOT NULL,
                primary_net_pnl_bps {DECIMAL_SQL_TYPE} NOT NULL,
                primary_exit_price {DECIMAL_SQL_TYPE} NOT NULL,
                primary_exit_time_utc TIMESTAMP NOT NULL,
                horizon_labels_json VARCHAR NOT NULL,
                source VARCHAR NOT NULL,
                collection_time_utc TIMESTAMP NOT NULL,
                updated_at_utc TIMESTAMP NOT NULL
            )
            """
        )

    def upsert_markets(
        self,
        markets: Iterable[GammaMarket],
        *,
        source: str = "gamma.markets",
        collection_time: datetime | None = None,
    ) -> int:
        collected_at = _normalize_utc_timestamp(collection_time or datetime.now(UTC))
        market_rows: dict[str, tuple[object, ...]] = {}
        token_rows: dict[tuple[str, str], tuple[object, ...]] = {}

        for market in markets:
            if not market.market_id:
                continue

            market_rows[market.market_id] = (
                market.market_id,
                market.question,
                market.slug,
                market.condition_id,
                market.active,
                _normalize_nullable_utc_timestamp(market.end_date),
                market.liquidity,
                market.volume,
                source,
                collected_at,
                collected_at,
            )

            for token_index, token_id in enumerate(market.clob_token_ids):
                if not token_id:
                    continue
                token_rows[(market.market_id, token_id)] = (
                    market.market_id,
                    token_id,
                    token_index,
                    source,
                    collected_at,
                )

        if not market_rows:
            return 0

        market_ids = [(market_id,) for market_id in market_rows]

        self._begin_transaction()
        try:
            self._connection.executemany("DELETE FROM markets WHERE market_id = ?", market_ids)
            self._connection.executemany(
                """
                INSERT INTO markets (
                    market_id,
                    question,
                    slug,
                    condition_id,
                    active,
                    end_time_utc,
                    liquidity,
                    volume,
                    source,
                    collection_time_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                list(market_rows.values()),
            )
            self._connection.executemany("DELETE FROM market_tokens WHERE market_id = ?", market_ids)
            if token_rows:
                self._connection.executemany(
                    """
                    INSERT INTO market_tokens (
                        market_id,
                        token_id,
                        token_index,
                        source,
                        collection_time_utc
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    list(token_rows.values()),
                )
            self._commit_transaction()
        except Exception:
            self._rollback_transaction()
            raise

        return len(market_rows)

    def upsert_price_history(
        self,
        histories: Iterable[PriceHistory],
        *,
        source: str = "clob.prices_history",
        collection_time: datetime | None = None,
    ) -> int:
        collected_at = _normalize_utc_timestamp(collection_time or datetime.now(UTC))
        price_rows: dict[str, tuple[object, ...]] = {}

        for history in histories:
            if not history.token_id:
                continue
            for point in history.points:
                if point.timestamp is None:
                    continue
                price_time = _normalize_utc_timestamp(point.timestamp)
                price_id = _stable_hash(
                    history.token_id,
                    history.interval,
                    history.fidelity,
                    price_time.isoformat(),
                )
                price_rows[price_id] = (
                    price_id,
                    history.token_id,
                    history.interval,
                    history.fidelity,
                    price_time,
                    point.price,
                    source,
                    collected_at,
                    collected_at,
                )

        if not price_rows:
            return 0

        price_ids = [(price_id,) for price_id in price_rows]

        self._begin_transaction()
        try:
            self._connection.executemany("DELETE FROM price_history WHERE price_id = ?", price_ids)
            self._connection.executemany(
                """
                INSERT INTO price_history (
                    price_id,
                    token_id,
                    interval,
                    fidelity,
                    price_time_utc,
                    price,
                    source,
                    collection_time_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                list(price_rows.values()),
            )
            self._commit_transaction()
        except Exception:
            self._rollback_transaction()
            raise

        return len(price_rows)

    def upsert_trades(
        self,
        trades: Iterable[TradeRecord],
        *,
        source: str = "data_api.trades",
        collection_time: datetime | None = None,
    ) -> int:
        collected_at = _normalize_utc_timestamp(collection_time or datetime.now(UTC))
        trade_rows: dict[str, tuple[object, ...]] = {}

        for trade in trades:
            trade_time = _normalize_nullable_utc_timestamp(trade.timestamp)
            trade_id = _build_trade_id(trade, trade_time)
            trade_rows[trade_id] = (
                trade_id,
                trade.proxy_wallet,
                trade.asset_id,
                trade.condition_id,
                trade.outcome,
                trade.side,
                trade.size,
                trade.price,
                trade.transaction_hash,
                trade.usdc_size,
                trade_time,
                source,
                collected_at,
                collected_at,
            )

        if not trade_rows:
            return 0

        trade_ids = [(trade_id,) for trade_id in trade_rows]

        self._begin_transaction()
        try:
            self._connection.executemany("DELETE FROM trades WHERE trade_id = ?", trade_ids)
            self._connection.executemany(
                """
                INSERT INTO trades (
                    trade_id,
                    proxy_wallet,
                    asset_id,
                    condition_id,
                    outcome,
                    side,
                    size,
                    price,
                    transaction_hash,
                    usdc_size,
                    trade_time_utc,
                    source,
                    collection_time_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                list(trade_rows.values()),
            )
            self._commit_transaction()
        except Exception:
            self._rollback_transaction()
            raise

        return len(trade_rows)

    def upsert_wallet_positions(
        self,
        positions: Iterable[PositionSnapshot],
        *,
        source: str = "data_api.positions",
        collection_time: datetime | None = None,
    ) -> int:
        collected_at = _normalize_utc_timestamp(collection_time or datetime.now(UTC))
        position_rows: dict[str, tuple[object, ...]] = {}

        for position in positions:
            wallet_address = position.proxy_wallet
            if not wallet_address:
                continue

            position_snapshot_id = _build_wallet_position_snapshot_id(position, collected_at)
            position_rows[position_snapshot_id] = (
                position_snapshot_id,
                wallet_address,
                position.asset_id,
                position.condition_id,
                position.outcome,
                position.outcome_index,
                position.size,
                position.average_price,
                position.current_value,
                position.realized_pnl,
                position.total_bought,
                _normalize_nullable_utc_timestamp(position.end_date),
                source,
                collected_at,
                collected_at,
            )

        if not position_rows:
            return 0

        position_ids = [(position_id,) for position_id in position_rows]

        self._begin_transaction()
        try:
            self._connection.executemany(
                "DELETE FROM wallet_positions WHERE position_snapshot_id = ?",
                position_ids,
            )
            self._connection.executemany(
                """
                INSERT INTO wallet_positions (
                    position_snapshot_id,
                    wallet_address,
                    asset_id,
                    condition_id,
                    outcome,
                    outcome_index,
                    size,
                    average_price,
                    current_value,
                    realized_pnl,
                    total_bought,
                    end_time_utc,
                    source,
                    collection_time_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                list(position_rows.values()),
            )
            self._commit_transaction()
        except Exception:
            self._rollback_transaction()
            raise

        return len(position_rows)

    def upsert_wallet_closed_positions(
        self,
        positions: Iterable[ClosedPosition],
        *,
        source: str = "data_api.closed_positions",
        collection_time: datetime | None = None,
    ) -> int:
        collected_at = _normalize_utc_timestamp(collection_time or datetime.now(UTC))
        closed_position_rows: dict[str, tuple[object, ...]] = {}

        for position in positions:
            wallet_address = position.proxy_wallet
            if not wallet_address:
                continue

            closed_at = _normalize_nullable_utc_timestamp(position.closed_at)
            closed_position_id = _build_wallet_closed_position_id(position, closed_at)
            closed_position_rows[closed_position_id] = (
                closed_position_id,
                wallet_address,
                position.asset_id,
                position.condition_id,
                position.outcome,
                position.average_price,
                position.realized_pnl,
                position.total_bought,
                closed_at,
                _normalize_nullable_utc_timestamp(position.end_date),
                source,
                collected_at,
                collected_at,
            )

        if not closed_position_rows:
            return 0

        position_ids = [(position_id,) for position_id in closed_position_rows]

        self._begin_transaction()
        try:
            self._connection.executemany(
                "DELETE FROM wallet_closed_positions WHERE closed_position_id = ?",
                position_ids,
            )
            self._connection.executemany(
                """
                INSERT INTO wallet_closed_positions (
                    closed_position_id,
                    wallet_address,
                    asset_id,
                    condition_id,
                    outcome,
                    average_price,
                    realized_pnl,
                    total_bought,
                    closed_at_utc,
                    end_time_utc,
                    source,
                    collection_time_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                list(closed_position_rows.values()),
            )
            self._commit_transaction()
        except Exception:
            self._rollback_transaction()
            raise

        return len(closed_position_rows)

    def upsert_wallet_profiles(
        self,
        profiles: Iterable[WalletProfile],
        *,
        source: str = "signals.wallet_profiles",
        collection_time: datetime | None = None,
    ) -> int:
        collected_at = _normalize_utc_timestamp(collection_time or datetime.now(UTC))
        profile_rows: dict[str, tuple[object, ...]] = {}

        for profile in profiles:
            profile_id = _build_wallet_profile_id(profile)
            profile_rows[profile_id] = (
                profile_id,
                profile.wallet_address,
                _normalize_utc_timestamp(profile.as_of_time_utc),
                profile.realized_pnl,
                profile.realized_roi,
                profile.closed_position_count,
                profile.winning_closed_position_count,
                profile.hit_rate,
                profile.avg_closed_position_cost,
                profile.activity_trade_count,
                profile.activity_volume_usdc,
                profile.avg_trade_size_usdc,
                _normalize_nullable_utc_timestamp(profile.first_activity_time_utc),
                _normalize_nullable_utc_timestamp(profile.last_activity_time_utc),
                _normalize_nullable_utc_timestamp(profile.last_closed_position_time_utc),
                source,
                collected_at,
                collected_at,
            )

        if not profile_rows:
            return 0

        profile_ids = [(profile_id,) for profile_id in profile_rows]

        self._begin_transaction()
        try:
            self._connection.executemany(
                "DELETE FROM wallet_profiles WHERE profile_id = ?",
                profile_ids,
            )
            self._connection.executemany(
                """
                INSERT INTO wallet_profiles (
                    profile_id,
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
                    source,
                    collection_time_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                list(profile_rows.values()),
            )
            self._commit_transaction()
        except Exception:
            self._rollback_transaction()
            raise

        return len(profile_rows)

    def upsert_order_book_snapshots(
        self,
        snapshots: Iterable[TopOfBookSnapshot],
        *,
        source: str = "websocket.market",
        collection_time: datetime | None = None,
    ) -> int:
        collected_at = _normalize_utc_timestamp(collection_time or datetime.now(UTC))
        snapshot_rows: dict[str, tuple[object, ...]] = {}

        for snapshot in snapshots:
            if not snapshot.asset_id and not snapshot.market_id:
                continue

            snapshot_time = _normalize_nullable_utc_timestamp(snapshot.snapshot_time)
            snapshot_id = _build_order_book_snapshot_id(snapshot, snapshot_time)
            snapshot_rows[snapshot_id] = (
                snapshot_id,
                snapshot.asset_id,
                snapshot.market_id,
                snapshot.best_bid_price,
                snapshot.best_bid_size,
                snapshot.best_ask_price,
                snapshot.best_ask_size,
                _calculate_mid_price(snapshot.best_bid_price, snapshot.best_ask_price),
                _calculate_spread(snapshot.best_bid_price, snapshot.best_ask_price),
                snapshot.last_trade_price,
                snapshot.tick_size,
                snapshot.book_hash,
                snapshot_time,
                source,
                collected_at,
                collected_at,
            )

        if not snapshot_rows:
            return 0

        snapshot_ids = [(snapshot_id,) for snapshot_id in snapshot_rows]

        self._begin_transaction()
        try:
            self._connection.executemany(
                "DELETE FROM order_book_snapshots WHERE snapshot_id = ?",
                snapshot_ids,
            )
            self._connection.executemany(
                """
                INSERT INTO order_book_snapshots (
                    snapshot_id,
                    asset_id,
                    market_id,
                    best_bid_price,
                    best_bid_size,
                    best_ask_price,
                    best_ask_size,
                    mid_price,
                    spread,
                    last_trade_price,
                    tick_size,
                    book_hash,
                    snapshot_time_utc,
                    source,
                    collection_time_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                list(snapshot_rows.values()),
            )
            self._commit_transaction()
        except Exception:
            self._rollback_transaction()
            raise

        return len(snapshot_rows)

    def upsert_signal_events(
        self,
        events: Iterable[SignalEvent],
        *,
        source: str = "signals.event_detector",
        collection_time: datetime | None = None,
    ) -> int:
        collected_at = _normalize_utc_timestamp(collection_time or datetime.now(UTC))
        event_rows: dict[str, tuple[object, ...]] = {}

        for event in events:
            event_rows[event.event_id] = (
                event.event_id,
                event.asset_id,
                event.condition_id,
                _normalize_utc_timestamp(event.event_time_utc),
                event.direction,
                event.trigger_reason,
                json.dumps([rule.rule for rule in event.trigger_rules], sort_keys=True),
                event.market_features.recent_trade_count,
                event.market_features.recent_volume_usdc,
                event.market_features.volume_zscore,
                event.market_features.trade_count_zscore,
                event.market_features.order_flow_imbalance,
                event.market_features.short_return,
                event.market_features.medium_return,
                event.market_features.liquidity_features_available,
                event.wallet_summary.active_wallet_count,
                event.wallet_summary.profiled_wallet_count,
                event.wallet_summary.top_wallet_share,
                event.wallet_summary.weighted_average_quality,
                json.dumps(event.explanation_payload, sort_keys=True),
                source,
                collected_at,
                collected_at,
            )

        if not event_rows:
            return 0

        event_ids = [(event_id,) for event_id in event_rows]

        self._begin_transaction()
        try:
            self._connection.executemany("DELETE FROM signal_events WHERE event_id = ?", event_ids)
            self._connection.executemany(
                """
                INSERT INTO signal_events (
                    event_id,
                    asset_id,
                    condition_id,
                    event_time_utc,
                    direction,
                    trigger_reason,
                    trigger_rules_json,
                    recent_trade_count,
                    recent_volume_usdc,
                    volume_zscore,
                    trade_count_zscore,
                    order_flow_imbalance,
                    short_return,
                    medium_return,
                    liquidity_features_available,
                    active_wallet_count,
                    profiled_wallet_count,
                    top_wallet_share,
                    weighted_average_quality,
                    explanation_payload_json,
                    source,
                    collection_time_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                list(event_rows.values()),
            )
            self._commit_transaction()
        except Exception:
            self._rollback_transaction()
            raise

        return len(event_rows)

    def upsert_event_dataset_rows(
        self,
        rows: Iterable[EventDatasetRow],
        *,
        source: str = "research.event_dataset",
        collection_time: datetime | None = None,
    ) -> int:
        collected_at = _normalize_utc_timestamp(collection_time or datetime.now(UTC))
        dataset_rows: dict[str, tuple[object, ...]] = {}

        for row in rows:
            dataset_rows[row.dataset_row_id] = (
                row.dataset_row_id,
                row.dataset_build_id,
                row.dataset_split,
                row.event_id,
                row.asset_id,
                row.condition_id,
                _normalize_utc_timestamp(row.event_time_utc),
                _normalize_utc_timestamp(row.source_event_collection_time_utc),
                row.direction,
                row.trigger_reason,
                row.recent_trade_count,
                row.recent_volume_usdc,
                row.volume_zscore,
                row.trade_count_zscore,
                row.order_flow_imbalance,
                row.short_return,
                row.medium_return,
                row.liquidity_features_available,
                row.latest_price,
                row.latest_mid_price,
                row.latest_spread_bps,
                row.spread_change_bps,
                row.top_of_book_depth_usdc,
                row.depth_change_ratio,
                row.depth_imbalance,
                row.active_wallet_count,
                row.profiled_wallet_count,
                row.sparse_wallet_set,
                row.profiled_volume_share,
                row.top_wallet_share,
                row.concentration_hhi,
                row.weighted_average_quality,
                row.weighted_average_realized_roi,
                row.weighted_average_hit_rate,
                row.weighted_average_realized_pnl,
                row.entry_price,
                _normalize_utc_timestamp(row.entry_price_time_utc),
                row.assumed_round_trip_cost_bps,
                row.primary_label_name,
                row.primary_label_horizon_minutes,
                row.primary_label_continuation,
                row.primary_label_reversion,
                row.primary_label_profitable,
                row.primary_directional_return_bps,
                row.primary_net_pnl_bps,
                row.primary_exit_price,
                _normalize_utc_timestamp(row.primary_exit_time_utc),
                json.dumps(row.horizon_labels_json, sort_keys=True),
                source,
                collected_at,
                collected_at,
            )

        if not dataset_rows:
            return 0

        row_ids = [(row_id,) for row_id in dataset_rows]

        self._begin_transaction()
        try:
            self._connection.executemany("DELETE FROM event_dataset_rows WHERE dataset_row_id = ?", row_ids)
            self._connection.executemany(
                """
                INSERT INTO event_dataset_rows (
                    dataset_row_id,
                    dataset_build_id,
                    dataset_split,
                    event_id,
                    asset_id,
                    condition_id,
                    event_time_utc,
                    source_event_collection_time_utc,
                    direction,
                    trigger_reason,
                    recent_trade_count,
                    recent_volume_usdc,
                    volume_zscore,
                    trade_count_zscore,
                    order_flow_imbalance,
                    short_return,
                    medium_return,
                    liquidity_features_available,
                    latest_price,
                    latest_mid_price,
                    latest_spread_bps,
                    spread_change_bps,
                    top_of_book_depth_usdc,
                    depth_change_ratio,
                    depth_imbalance,
                    active_wallet_count,
                    profiled_wallet_count,
                    sparse_wallet_set,
                    profiled_volume_share,
                    top_wallet_share,
                    concentration_hhi,
                    weighted_average_quality,
                    weighted_average_realized_roi,
                    weighted_average_hit_rate,
                    weighted_average_realized_pnl,
                    entry_price,
                    entry_price_time_utc,
                    assumed_round_trip_cost_bps,
                    primary_label_name,
                    primary_label_horizon_minutes,
                    primary_label_continuation,
                    primary_label_reversion,
                    primary_label_profitable,
                    primary_directional_return_bps,
                    primary_net_pnl_bps,
                    primary_exit_price,
                    primary_exit_time_utc,
                    horizon_labels_json,
                    source,
                    collection_time_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                list(dataset_rows.values()),
            )
            self._commit_transaction()
        except Exception:
            self._rollback_transaction()
            raise

        return len(dataset_rows)

    def _begin_transaction(self) -> None:
        self._connection.execute("BEGIN TRANSACTION")

    def _commit_transaction(self) -> None:
        self._connection.execute("COMMIT")

    def _rollback_transaction(self) -> None:
        self._connection.execute("ROLLBACK")


def _build_trade_id(trade: TradeRecord, trade_time: datetime | None) -> str:
    transaction_hash = _normalize_identity(trade.transaction_hash)
    asset_id = _normalize_identity(trade.asset_id)

    if transaction_hash and asset_id:
        return _stable_hash(
            "trade",
            transaction_hash,
            asset_id,
            _normalize_identity(trade.side),
            _decimal_as_text(trade.price),
            _decimal_as_text(trade.size),
            trade_time.isoformat() if trade_time else "",
        )

    return _stable_hash(
        "trade-fallback",
        _normalize_identity(trade.proxy_wallet),
        asset_id,
        _normalize_identity(trade.condition_id),
        _normalize_identity(trade.outcome),
        _normalize_identity(trade.side),
        _decimal_as_text(trade.price),
        _decimal_as_text(trade.size),
        _decimal_as_text(trade.usdc_size),
        trade_time.isoformat() if trade_time else "",
    )


def _build_wallet_position_snapshot_id(
    position: PositionSnapshot,
    collection_time: datetime,
) -> str:
    return _stable_hash(
        "wallet-position-snapshot",
        _normalize_identity(position.proxy_wallet),
        _normalize_identity(position.asset_id),
        _normalize_identity(position.condition_id),
        _normalize_identity(position.outcome),
        str(position.outcome_index) if position.outcome_index is not None else "",
        _decimal_as_text(position.size),
        collection_time.isoformat(),
    )


def _build_wallet_closed_position_id(
    position: ClosedPosition,
    closed_at: datetime | None,
) -> str:
    return _stable_hash(
        "wallet-closed-position",
        _normalize_identity(position.proxy_wallet),
        _normalize_identity(position.asset_id),
        _normalize_identity(position.condition_id),
        _normalize_identity(position.outcome),
        _decimal_as_text(position.average_price),
        _decimal_as_text(position.realized_pnl),
        _decimal_as_text(position.total_bought),
        closed_at.isoformat() if closed_at else "",
    )


def _build_wallet_profile_id(profile: WalletProfile) -> str:
    return _stable_hash(
        "wallet-profile",
        _normalize_identity(profile.wallet_address),
        _normalize_utc_timestamp(profile.as_of_time_utc).isoformat(),
    )


def _build_order_book_snapshot_id(
    snapshot: TopOfBookSnapshot,
    snapshot_time: datetime | None,
) -> str:
    return _stable_hash(
        "order-book",
        _normalize_identity(snapshot.asset_id),
        _normalize_identity(snapshot.market_id),
        _normalize_identity(snapshot.book_hash),
        snapshot_time.isoformat() if snapshot_time else "",
        _decimal_as_text(snapshot.best_bid_price),
        _decimal_as_text(snapshot.best_ask_price),
        _decimal_as_text(snapshot.best_bid_size),
        _decimal_as_text(snapshot.best_ask_size),
    )


def _normalize_identity(value: str | None) -> str:
    return value.strip().lower() if value else ""


def _decimal_as_text(value: Decimal | None) -> str:
    return "" if value is None else format(value, "f")


def _stable_hash(*parts: object) -> str:
    payload = "|".join(str(part) for part in parts)
    return sha256(payload.encode("utf-8")).hexdigest()


def _best_level(levels: Iterable[object]) -> tuple[Decimal | None, Decimal | None]:
    for level in levels:
        price = getattr(level, "price", None)
        size = getattr(level, "size", None)
        if price is not None or size is not None:
            return price, size
    return None, None


def _calculate_mid_price(
    best_bid_price: Decimal | None,
    best_ask_price: Decimal | None,
) -> Decimal | None:
    if best_bid_price is None or best_ask_price is None:
        return None
    return (best_bid_price + best_ask_price) / Decimal("2")


def _calculate_spread(
    best_bid_price: Decimal | None,
    best_ask_price: Decimal | None,
) -> Decimal | None:
    if best_bid_price is None or best_ask_price is None:
        return None
    return best_ask_price - best_bid_price


def _normalize_utc_timestamp(value: datetime) -> datetime:
    normalized = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.replace(tzinfo=None)


def _normalize_nullable_utc_timestamp(value: datetime | None) -> datetime | None:
    return _normalize_utc_timestamp(value) if value is not None else None
