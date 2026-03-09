from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import duckdb

from src.clients.clob import OrderBookSnapshot, PriceHistory, PriceHistoryPoint
from src.clients.data_api import ClosedPosition, PositionSnapshot, TradeRecord
from src.clients.gamma import GammaMarket
from src.signals import (
    MarketAnomalyFeatures,
    SignalEvent,
    TriggerRule,
    WalletParticipantFeatures,
    WalletProfile,
    WalletSummaryFeatures,
)
from src.storage.raw import RawPayloadStore
from src.storage.warehouse import EventDatasetRow, PolymarketWarehouse, TopOfBookSnapshot


def test_raw_payload_store_uses_append_only_json_and_jsonl_layout(tmp_path) -> None:
    store = RawPayloadStore(tmp_path / "raw")
    collected_at = datetime(2026, 3, 8, 12, 0, tzinfo=UTC)

    object_capture = store.write_capture(
        "Gamma API",
        "market snapshots",
        {"id": "123", "question": "Will it rain?"},
        endpoint="/markets",
        request_params={"limit": 1, "closed": False},
        collection_time=collected_at,
    )
    list_capture = store.write_capture(
        "Gamma API",
        "market snapshots",
        [{"id": "123"}, {"id": "456"}],
        endpoint="/markets",
        request_params={"limit": 2, "closed": False},
        collection_time=collected_at,
    )

    assert object_capture.path.suffix == ".json"
    assert list_capture.path.suffix == ".jsonl"
    assert object_capture.path != list_capture.path
    assert object_capture.path.parent.name == "date=2026-03-08"
    assert object_capture.path.parent.parent.name == "market_snapshots"
    assert object_capture.path.parent.parent.parent.name == "gamma_api"

    object_record = json.loads(object_capture.path.read_text())
    assert object_record["source"] == "Gamma API"
    assert object_record["dataset"] == "market snapshots"
    assert object_record["collection_time_utc"] == "2026-03-08T12:00:00+00:00"
    assert object_record["request_params"] == {"limit": 1, "closed": False}
    assert object_record["payload"]["id"] == "123"

    jsonl_records = [json.loads(line) for line in list_capture.path.read_text().splitlines()]
    assert len(jsonl_records) == 2
    assert jsonl_records[0]["record_count"] == 2
    assert jsonl_records[0]["record_index"] == 0
    assert jsonl_records[1]["record_index"] == 1
    assert jsonl_records[1]["payload"]["id"] == "456"


def test_polymarket_warehouse_creates_schema_and_upserts_without_duplication(tmp_path) -> None:
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"
    first_collection_time = datetime(2026, 3, 8, 12, 0, tzinfo=UTC)
    second_collection_time = datetime(2026, 3, 8, 12, 30, tzinfo=UTC)

    first_market = GammaMarket(
        market_id="123",
        question="Will it rain?",
        slug="will-it-rain",
        condition_id="0xcondition123",
        clob_token_ids=("111", "222"),
        active=True,
        end_date=datetime(2026, 3, 10, 17, 0, tzinfo=UTC),
        liquidity=Decimal("1000.50"),
        volume=Decimal("2500.00"),
    )
    updated_market = GammaMarket(
        market_id="123",
        question="Will it rain tomorrow?",
        slug="will-it-rain",
        condition_id="0xcondition123",
        clob_token_ids=("111", "222"),
        active=True,
        end_date=datetime(2026, 3, 10, 17, 0, tzinfo=UTC),
        liquidity=Decimal("1000.50"),
        volume=Decimal("3500.00"),
    )
    price_history = PriceHistory(
        token_id="111",
        interval="1d",
        fidelity=5,
        points=(
            PriceHistoryPoint(
                timestamp=datetime(2026, 3, 8, 10, 0, tzinfo=UTC),
                price=Decimal("0.45"),
            ),
            PriceHistoryPoint(
                timestamp=datetime(2026, 3, 8, 11, 0, tzinfo=UTC),
                price=Decimal("0.47"),
            ),
        ),
    )
    trade = TradeRecord(
        proxy_wallet="0xwallet1",
        asset_id="111",
        condition_id="0xcondition123",
        outcome="Yes",
        side="BUY",
        size=Decimal("50"),
        price=Decimal("0.47"),
        timestamp=datetime(2026, 3, 8, 11, 5, tzinfo=UTC),
        transaction_hash="0xtradehash123",
        usdc_size=Decimal("23.5"),
    )

    with PolymarketWarehouse(database_path) as warehouse:
        assert warehouse.upsert_markets([first_market], collection_time=first_collection_time) == 1
        assert warehouse.upsert_price_history([price_history], collection_time=first_collection_time) == 2
        assert warehouse.upsert_trades([trade], collection_time=first_collection_time) == 1

        assert warehouse.upsert_markets([updated_market], collection_time=second_collection_time) == 1
        assert warehouse.upsert_price_history([price_history], collection_time=second_collection_time) == 2
        assert warehouse.upsert_trades([trade, trade], collection_time=second_collection_time) == 1

    with duckdb.connect(str(database_path), read_only=True) as connection:
        table_names = {
            row[0]
            for row in connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
        assert {"markets", "market_tokens", "order_book_snapshots", "price_history", "trades"} <= table_names

        market_count = connection.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
        market_token_count = connection.execute("SELECT COUNT(*) FROM market_tokens").fetchone()[0]
        price_count = connection.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
        trade_count = connection.execute("SELECT COUNT(*) FROM trades").fetchone()[0]

        assert market_count == 1
        assert market_token_count == 2
        assert price_count == 2
        assert trade_count == 1

        stored_market = connection.execute(
            """
            SELECT question, volume, collection_time_utc
            FROM markets
            WHERE market_id = '123'
            """
        ).fetchone()
        assert stored_market[0] == "Will it rain tomorrow?"
        assert stored_market[1] == Decimal("3500.000000000000000000")
        assert stored_market[2] == second_collection_time.replace(tzinfo=None)

        stored_trade = connection.execute(
            """
            SELECT transaction_hash, side, trade_time_utc, collection_time_utc
            FROM trades
            """
        ).fetchone()
        assert stored_trade[0] == "0xtradehash123"
        assert stored_trade[1] == "BUY"
        assert stored_trade[2] == datetime(2026, 3, 8, 11, 5)
        assert stored_trade[3] == second_collection_time.replace(tzinfo=None)


def test_polymarket_warehouse_upserts_top_of_book_snapshots_without_duplication(tmp_path) -> None:
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"
    first_collection_time = datetime(2026, 3, 8, 12, 0, tzinfo=UTC)
    second_collection_time = datetime(2026, 3, 8, 12, 5, tzinfo=UTC)

    snapshot = TopOfBookSnapshot.from_order_book_snapshot(
        OrderBookSnapshot(
            market_id="0xcondition123",
            asset_id="111",
            bids=(),
            asks=(),
            last_trade_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
            min_order_size=None,
            book_hash="0xbookhash123",
            timestamp=datetime(2026, 3, 8, 11, 59, tzinfo=UTC),
            neg_risk=False,
        )
    )
    snapshot = TopOfBookSnapshot(
        market_id=snapshot.market_id,
        asset_id=snapshot.asset_id,
        best_bid_price=Decimal("0.49"),
        best_bid_size=Decimal("120"),
        best_ask_price=Decimal("0.51"),
        best_ask_size=Decimal("140"),
        last_trade_price=snapshot.last_trade_price,
        tick_size=snapshot.tick_size,
        book_hash=snapshot.book_hash,
        snapshot_time=snapshot.snapshot_time,
    )

    with PolymarketWarehouse(database_path) as warehouse:
        assert warehouse.upsert_order_book_snapshots([snapshot], collection_time=first_collection_time) == 1
        assert (
            warehouse.upsert_order_book_snapshots(
                [snapshot, snapshot],
                collection_time=second_collection_time,
            )
            == 1
        )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        snapshot_count = connection.execute(
            "SELECT COUNT(*) FROM order_book_snapshots"
        ).fetchone()[0]
        assert snapshot_count == 1

        stored_snapshot = connection.execute(
            """
            SELECT
                asset_id,
                market_id,
                best_bid_price,
                best_ask_price,
                mid_price,
                spread,
                collection_time_utc
            FROM order_book_snapshots
            """
        ).fetchone()
        assert stored_snapshot[0] == "111"
        assert stored_snapshot[1] == "0xcondition123"
        assert stored_snapshot[2] == Decimal("0.490000000000000000")
        assert stored_snapshot[3] == Decimal("0.510000000000000000")
        assert stored_snapshot[4] == Decimal("0.500000000000000000")
        assert stored_snapshot[5] == Decimal("0.020000000000000000")
        assert stored_snapshot[6] == second_collection_time.replace(tzinfo=None)


def test_polymarket_warehouse_upserts_wallet_tables_without_duplication(tmp_path) -> None:
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"
    first_collection_time = datetime(2026, 3, 8, 12, 0, tzinfo=UTC)
    second_collection_time = datetime(2026, 3, 8, 12, 30, tzinfo=UTC)

    position = PositionSnapshot(
        proxy_wallet="0xwallet1",
        asset_id="111",
        condition_id="0xcondition123",
        size=Decimal("500"),
        average_price=Decimal("0.55"),
        current_value=Decimal("275.00"),
        realized_pnl=Decimal("25.50"),
        outcome="YES",
        outcome_index=0,
        total_bought=Decimal("300"),
        end_date=datetime(2026, 3, 31, 12, 0, tzinfo=UTC),
    )
    closed_position = ClosedPosition(
        proxy_wallet="0xwallet1",
        asset_id="111",
        condition_id="0xcondition123",
        outcome="YES",
        average_price=Decimal("0.44"),
        realized_pnl=Decimal("125.50"),
        total_bought=Decimal("500"),
        closed_at=datetime(2026, 3, 1, 10, 15, tzinfo=UTC),
        end_date=datetime(2026, 3, 31, 12, 0, tzinfo=UTC),
    )
    profile = WalletProfile(
        wallet_address="0xwallet1",
        as_of_time_utc=datetime(2026, 3, 8, 12, 0, tzinfo=UTC),
        realized_pnl=Decimal("125.50"),
        realized_roi=Decimal("0.251"),
        closed_position_count=1,
        winning_closed_position_count=1,
        hit_rate=Decimal("1"),
        avg_closed_position_cost=Decimal("500"),
        activity_trade_count=2,
        activity_volume_usdc=Decimal("90"),
        avg_trade_size_usdc=Decimal("45"),
        first_activity_time_utc=datetime(2026, 3, 1, 10, 0, tzinfo=UTC),
        last_activity_time_utc=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
        last_closed_position_time_utc=datetime(2026, 3, 1, 10, 15, tzinfo=UTC),
    )

    with PolymarketWarehouse(database_path) as warehouse:
        assert warehouse.upsert_wallet_positions([position], collection_time=first_collection_time) == 1
        assert warehouse.upsert_wallet_closed_positions([closed_position], collection_time=first_collection_time) == 1
        assert warehouse.upsert_wallet_profiles([profile], collection_time=first_collection_time) == 1

        assert warehouse.upsert_wallet_positions([position], collection_time=first_collection_time) == 1
        assert warehouse.upsert_wallet_closed_positions([closed_position], collection_time=second_collection_time) == 1
        assert warehouse.upsert_wallet_profiles([profile], collection_time=second_collection_time) == 1

    with duckdb.connect(str(database_path), read_only=True) as connection:
        table_names = {
            row[0]
            for row in connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
        assert {"wallet_positions", "wallet_closed_positions", "wallet_profiles"} <= table_names

        position_count = connection.execute("SELECT COUNT(*) FROM wallet_positions").fetchone()[0]
        closed_position_count = connection.execute("SELECT COUNT(*) FROM wallet_closed_positions").fetchone()[0]
        profile_count = connection.execute("SELECT COUNT(*) FROM wallet_profiles").fetchone()[0]

        assert position_count == 1
        assert closed_position_count == 1
        assert profile_count == 1

        stored_profile = connection.execute(
            """
            SELECT wallet_address, activity_trade_count, collection_time_utc
            FROM wallet_profiles
            """
        ).fetchone()
        assert stored_profile[0] == "0xwallet1"
        assert stored_profile[1] == 2
        assert stored_profile[2] == second_collection_time.replace(tzinfo=None)


def test_polymarket_warehouse_upserts_signal_events_without_duplication(tmp_path) -> None:
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"
    first_collection_time = datetime(2026, 3, 8, 12, 0, tzinfo=UTC)
    second_collection_time = datetime(2026, 3, 8, 12, 30, tzinfo=UTC)

    event = SignalEvent(
        event_id="event-123",
        asset_id="111",
        condition_id="0xcondition123",
        event_time_utc=datetime(2026, 3, 8, 12, 0, tzinfo=UTC),
        direction="up",
        trigger_reason="volume_spike, order_flow_imbalance",
        trigger_rules=(
            TriggerRule(
                rule="volume_spike",
                metric="volume_zscore",
                threshold=Decimal("1.5"),
                actual_value=Decimal("4.2"),
            ),
            TriggerRule(
                rule="order_flow_imbalance",
                metric="order_flow_imbalance",
                threshold=Decimal("0.45"),
                actual_value=Decimal("0.67"),
            ),
        ),
        market_features=MarketAnomalyFeatures(
            asset_id="111",
            condition_id="0xcondition123",
            event_time_utc=datetime(2026, 3, 8, 12, 0, tzinfo=UTC),
            recent_window_seconds=900,
            baseline_window_seconds=7200,
            recent_trade_count=3,
            recent_volume_usdc=Decimal("300"),
            baseline_trade_count_mean=Decimal("1.5"),
            baseline_trade_count_std=Decimal("0.5"),
            trade_count_zscore=Decimal("3"),
            baseline_volume_mean=Decimal("25"),
            baseline_volume_std=Decimal("11.18"),
            volume_zscore=Decimal("24.6"),
            buy_volume_usdc=Decimal("250"),
            sell_volume_usdc=Decimal("50"),
            order_flow_imbalance=Decimal("0.666666666666666667"),
            latest_price=Decimal("0.72"),
            short_return_window_seconds=300,
            short_return=Decimal("0.107692307692307692"),
            medium_return_window_seconds=900,
            medium_return=Decimal("0.2"),
            liquidity_features_available=True,
            latest_mid_price=Decimal("0.72"),
            latest_spread_bps=Decimal("555.55"),
            spread_change_bps=Decimal("227.6"),
            top_of_book_depth_usdc=Decimal("79.4"),
            depth_change_ratio=Decimal("-0.63"),
            depth_imbalance=Decimal("-0.11"),
        ),
        wallet_summary=WalletSummaryFeatures(
            event_time_utc=datetime(2026, 3, 8, 12, 0, tzinfo=UTC),
            window_seconds=900,
            active_wallet_count=2,
            profiled_wallet_count=2,
            sparse_wallet_set=False,
            total_wallet_volume_usdc=Decimal("300"),
            profiled_volume_share=Decimal("1"),
            top_wallet_share=Decimal("0.833333333333333333"),
            concentration_hhi=Decimal("0.722222222222222222"),
            weighted_average_quality=Decimal("0.31"),
            weighted_average_realized_roi=Decimal("0.18"),
            weighted_average_hit_rate=Decimal("0.69"),
            weighted_average_realized_pnl=Decimal("72"),
            participants=(
                WalletParticipantFeatures(
                    wallet_address="0xwalletA",
                    trade_count=2,
                    traded_volume_usdc=Decimal("250"),
                    buy_volume_usdc=Decimal("250"),
                    sell_volume_usdc=Decimal("0"),
                    net_order_flow_usdc=Decimal("250"),
                    profile_as_of_time_utc=datetime(2026, 3, 8, 10, 0, tzinfo=UTC),
                    realized_pnl=Decimal("90"),
                    realized_roi=Decimal("0.35"),
                    hit_rate=Decimal("0.75"),
                    closed_position_count=6,
                    activity_volume_usdc=Decimal("300"),
                    quality_score=Decimal("0.425"),
                ),
                WalletParticipantFeatures(
                    wallet_address="0xwalletB",
                    trade_count=1,
                    traded_volume_usdc=Decimal("50"),
                    buy_volume_usdc=Decimal("0"),
                    sell_volume_usdc=Decimal("50"),
                    net_order_flow_usdc=Decimal("-50"),
                    profile_as_of_time_utc=datetime(2026, 3, 8, 11, 0, tzinfo=UTC),
                    realized_pnl=Decimal("-15"),
                    realized_roi=Decimal("-0.15"),
                    hit_rate=Decimal("0.40"),
                    closed_position_count=3,
                    activity_volume_usdc=Decimal("200"),
                    quality_score=Decimal("-0.015"),
                ),
            ),
        ),
        explanation_payload={
            "event_time_utc": "2026-03-08T12:00:00+00:00",
            "trigger_reason": "volume_spike, order_flow_imbalance",
            "wallet_context": {
                "active_wallet_count": 2,
                "participants": [
                    {"wallet_address": "0xwalletA", "traded_volume_usdc": "250"},
                    {"wallet_address": "0xwalletB", "traded_volume_usdc": "50"},
                ],
            },
        },
    )

    with PolymarketWarehouse(database_path) as warehouse:
        assert warehouse.upsert_signal_events([event], collection_time=first_collection_time) == 1
        assert warehouse.upsert_signal_events([event, event], collection_time=second_collection_time) == 1

    with duckdb.connect(str(database_path), read_only=True) as connection:
        table_names = {
            row[0]
            for row in connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
        assert "signal_events" in table_names

        event_count = connection.execute("SELECT COUNT(*) FROM signal_events").fetchone()[0]
        assert event_count == 1

        stored_event = connection.execute(
            """
            SELECT
                trigger_reason,
                recent_volume_usdc,
                active_wallet_count,
                trigger_rules_json,
                explanation_payload_json,
                collection_time_utc
            FROM signal_events
            """
        ).fetchone()
        assert stored_event[0] == "volume_spike, order_flow_imbalance"
        assert stored_event[1] == Decimal("300.000000000000000000")
        assert stored_event[2] == 2
        assert json.loads(stored_event[3]) == ["volume_spike", "order_flow_imbalance"]
        explanation_payload = json.loads(stored_event[4])
        assert explanation_payload["wallet_context"]["participants"][0]["wallet_address"] == "0xwalletA"
        assert stored_event[5] == second_collection_time.replace(tzinfo=None)


def test_polymarket_warehouse_upserts_event_dataset_rows_without_duplication(tmp_path) -> None:
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"
    first_collection_time = datetime(2026, 3, 9, 13, 0, tzinfo=UTC)
    second_collection_time = datetime(2026, 3, 9, 13, 30, tzinfo=UTC)

    row = EventDatasetRow(
        dataset_row_id="dataset-row-123",
        dataset_build_id="build-123",
        dataset_split="train",
        event_id="event-123",
        asset_id="111",
        condition_id="0xcondition123",
        event_time_utc=datetime(2026, 3, 9, 12, 0, tzinfo=UTC),
        source_event_collection_time_utc=datetime(2026, 3, 9, 12, 30, tzinfo=UTC),
        direction="up",
        trigger_reason="volume_spike",
        recent_trade_count=3,
        recent_volume_usdc=Decimal("300"),
        volume_zscore=Decimal("2.5"),
        trade_count_zscore=Decimal("2.0"),
        order_flow_imbalance=Decimal("0.60"),
        short_return=Decimal("0.08"),
        medium_return=Decimal("0.12"),
        liquidity_features_available=True,
        latest_price=Decimal("0.72"),
        latest_mid_price=Decimal("0.72"),
        latest_spread_bps=Decimal("555.55"),
        spread_change_bps=Decimal("227.60"),
        top_of_book_depth_usdc=Decimal("79.40"),
        depth_change_ratio=Decimal("-0.63"),
        depth_imbalance=Decimal("-0.11"),
        active_wallet_count=2,
        profiled_wallet_count=2,
        sparse_wallet_set=False,
        profiled_volume_share=Decimal("1"),
        top_wallet_share=Decimal("0.83"),
        concentration_hhi=Decimal("0.72"),
        weighted_average_quality=Decimal("0.31"),
        weighted_average_realized_roi=Decimal("0.18"),
        weighted_average_hit_rate=Decimal("0.69"),
        weighted_average_realized_pnl=Decimal("72"),
        entry_price=Decimal("0.72"),
        entry_price_time_utc=datetime(2026, 3, 9, 12, 0, tzinfo=UTC),
        assumed_round_trip_cost_bps=Decimal("605.55"),
        primary_label_name="profitable_after_costs",
        primary_label_horizon_minutes=15,
        primary_label_continuation=True,
        primary_label_reversion=False,
        primary_label_profitable=True,
        primary_directional_return_bps=Decimal("833.333333333333333333"),
        primary_net_pnl_bps=Decimal("227.783333333333333333"),
        primary_exit_price=Decimal("0.78"),
        primary_exit_time_utc=datetime(2026, 3, 9, 12, 15, tzinfo=UTC),
        horizon_labels_json={
            "15m": {
                "continuation": True,
                "net_pnl_bps": "227.783333333333333333",
                "profitable_after_costs": True,
            }
        },
    )

    with PolymarketWarehouse(database_path) as warehouse:
        assert warehouse.upsert_event_dataset_rows([row], collection_time=first_collection_time) == 1
        assert warehouse.upsert_event_dataset_rows([row, row], collection_time=second_collection_time) == 1

    with duckdb.connect(str(database_path), read_only=True) as connection:
        table_names = {
            row_name[0]
            for row_name in connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
        assert "event_dataset_rows" in table_names

        row_count = connection.execute("SELECT COUNT(*) FROM event_dataset_rows").fetchone()[0]
        assert row_count == 1

        stored_row = connection.execute(
            """
            SELECT
                dataset_build_id,
                primary_label_name,
                primary_label_profitable,
                horizon_labels_json,
                collection_time_utc
            FROM event_dataset_rows
            """
        ).fetchone()
        assert stored_row[0] == "build-123"
        assert stored_row[1] == "profitable_after_costs"
        assert stored_row[2] is True
        assert json.loads(stored_row[3])["15m"]["continuation"] is True
        assert stored_row[4] == second_collection_time.replace(tzinfo=None)
