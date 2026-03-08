from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import duckdb

from src.clients.clob import OrderBookSnapshot, PriceHistory, PriceHistoryPoint
from src.clients.data_api import TradeRecord
from src.clients.gamma import GammaMarket
from src.storage.raw import RawPayloadStore
from src.storage.warehouse import PolymarketWarehouse, TopOfBookSnapshot


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
