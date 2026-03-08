from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal
from pathlib import Path

import duckdb
from websockets.asyncio.server import serve

from src.clients.polymarket_websocket import build_market_subscription
from src.ingestion.live_market_recorder import normalize_market_message, run_live_market_recorder


BOOK_MESSAGE = {
    "event_type": "book",
    "asset_id": "123",
    "market": "0xcondition123",
    "hash": "0xbookhash123",
    "last_trade_price": "0.50",
    "tick_size": "0.01",
    "timestamp": "2026-03-08T12:00:00Z",
    "bids": [{"price": "0.49", "size": "100"}],
    "asks": [{"price": "0.51", "size": "120"}],
}

TRADE_MESSAGE = {
    "event_type": "trade",
    "asset_id": "123",
    "market": "0xcondition123",
    "side": "BUY",
    "size": "25",
    "price": "0.50",
    "timestamp": "2026-03-08T12:00:01Z",
    "transactionHash": "0xtradehash123",
    "proxyWallet": "0xwallet123",
    "outcome": "Yes",
    "usdcSize": "12.5",
}


def test_normalize_market_message_extracts_top_of_book_and_trade_rows() -> None:
    snapshots, trades = normalize_market_message([BOOK_MESSAGE, TRADE_MESSAGE, {"event_type": "price_change"}])

    assert len(snapshots) == 1
    assert snapshots[0].asset_id == "123"
    assert snapshots[0].best_bid_price == Decimal("0.49")
    assert snapshots[0].best_ask_size == Decimal("120")

    assert len(trades) == 1
    assert trades[0].asset_id == "123"
    assert trades[0].side == "BUY"
    assert trades[0].transaction_hash == "0xtradehash123"


def test_run_live_market_recorder_persists_raw_messages_and_normalized_rows(tmp_path: Path) -> None:
    async def run_test() -> tuple[object, list[dict[str, object]]]:
        received_subscriptions: list[dict[str, object]] = []

        async def handler(websocket) -> None:
            received_subscriptions.append(json.loads(await websocket.recv()))
            await websocket.send(json.dumps([BOOK_MESSAGE, TRADE_MESSAGE]))

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            summary = await run_live_market_recorder(
                asset_ids=["123"],
                raw_data_dir=tmp_path / "raw",
                warehouse_path=tmp_path / "warehouse" / "polymarket.duckdb",
                ws_base_url=f"ws://127.0.0.1:{port}/ws",
                session_seconds=5,
                max_messages=1,
                reconnect_attempts=0,
                message_timeout_seconds=0.1,
                ping_interval_seconds=1.0,
                ping_timeout_seconds=1.0,
            )

        return summary, received_subscriptions

    summary, received_subscriptions = asyncio.run(run_test())

    assert received_subscriptions == [build_market_subscription(["123"])]
    assert summary.messages_received == 1
    assert summary.raw_capture_count == 1
    assert summary.order_book_rows == 1
    assert summary.trade_rows == 1
    assert summary.reconnect_count == 0
    assert summary.warnings == ()

    raw_files = list((tmp_path / "raw" / "websocket" / "live_market_channel_events").rglob("*.json"))
    assert len(raw_files) == 1
    raw_capture = json.loads(raw_files[0].read_text())
    assert raw_capture["payload"]["message"][0]["event_type"] == "book"
    assert raw_capture["payload"]["message"][1]["event_type"] == "trade"

    with duckdb.connect(str(tmp_path / "warehouse" / "polymarket.duckdb"), read_only=True) as connection:
        assert connection.execute("SELECT COUNT(*) FROM order_book_snapshots").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 1

        stored_book = connection.execute(
            """
            SELECT best_bid_price, best_ask_price, mid_price, spread, book_hash
            FROM order_book_snapshots
            """
        ).fetchone()
        assert stored_book[0] == Decimal("0.490000000000000000")
        assert stored_book[1] == Decimal("0.510000000000000000")
        assert stored_book[2] == Decimal("0.500000000000000000")
        assert stored_book[3] == Decimal("0.020000000000000000")
        assert stored_book[4] == "0xbookhash123"

        stored_trade = connection.execute(
            """
            SELECT asset_id, side, transaction_hash, usdc_size
            FROM trades
            """
        ).fetchone()
        assert stored_trade[0] == "123"
        assert stored_trade[1] == "BUY"
        assert stored_trade[2] == "0xtradehash123"
        assert stored_trade[3] == Decimal("12.500000000000000000")


def test_run_live_market_recorder_warns_and_reconnects_after_receive_timeout(
    tmp_path: Path,
    caplog,
) -> None:
    async def run_test() -> tuple[object, int]:
        connection_count = 0

        async def handler(websocket) -> None:
            nonlocal connection_count
            connection_count += 1
            await websocket.recv()

            if connection_count == 1:
                await asyncio.sleep(0.15)
                return

            await websocket.send(json.dumps(BOOK_MESSAGE))

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            summary = await run_live_market_recorder(
                asset_ids=["123"],
                raw_data_dir=tmp_path / "raw",
                warehouse_path=tmp_path / "warehouse" / "polymarket.duckdb",
                ws_base_url=f"ws://127.0.0.1:{port}/ws",
                session_seconds=5,
                max_messages=1,
                reconnect_attempts=1,
                message_timeout_seconds=0.05,
                ping_interval_seconds=1.0,
                ping_timeout_seconds=1.0,
                logger=logging.getLogger("tests.live_market_recorder"),
            )

        return summary, connection_count

    with caplog.at_level(logging.WARNING):
        summary, connection_count = asyncio.run(run_test())

    assert connection_count == 2
    assert summary.messages_received == 1
    assert summary.order_book_rows == 1
    assert summary.trade_rows == 0
    assert summary.reconnect_count == 1
    assert len(summary.warnings) == 1
    assert summary.warnings[0].kind == "receive_timeout"
    assert "receive_timeout" in caplog.text
