from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb

from src.clients.clob import PriceHistory, PriceHistoryPoint
from src.clients.data_api import TradeRecord
from src.signals import WalletProfile
from src.storage import PolymarketWarehouse, TopOfBookSnapshot


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "generate_signal_events.py"


def load_script_module():
    spec = importlib.util.spec_from_file_location("generate_signal_events", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_main_prints_summary_on_success(monkeypatch, capsys, tmp_path) -> None:
    module = load_script_module()
    summary = module.SignalEventMaterializationSummary(
        selected_asset_ids=("111", "222"),
        wallet_profile_count=4,
        events_written=3,
        skipped_assets=(module.SkippedAsset(asset_id="333", reason="missing_price_history"),),
        asset_results=(
            module.AssetSignalEventResult(
                asset_id="111",
                condition_id="0xcondition111",
                trade_count=10,
                price_point_count=20,
                order_book_snapshot_count=5,
                event_count=2,
            ),
            module.AssetSignalEventResult(
                asset_id="222",
                condition_id="0xcondition222",
                trade_count=8,
                price_point_count=12,
                order_book_snapshot_count=0,
                event_count=1,
            ),
        ),
    )

    def fake_materialize_signal_events(**kwargs):
        assert kwargs["warehouse_path"] == tmp_path / "warehouse.duckdb"
        assert kwargs["asset_ids"] == ("111", "222")
        return summary

    monkeypatch.setattr(module, "materialize_signal_events", fake_materialize_signal_events)

    exit_code = module.main(
        [
            "--warehouse-path",
            str(tmp_path / "warehouse.duckdb"),
            "--asset-id",
            "111",
            "--asset-id",
            "222",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Signal event materialization summary" in captured.out
    assert "Selected assets (2): 111, 222" in captured.out
    assert "Signal events written: 3" in captured.out
    assert "Skipped assets: 333:missing_price_history" in captured.out


def test_main_prints_failure_message(monkeypatch, capsys, tmp_path) -> None:
    module = load_script_module()

    def fake_materialize_signal_events(**_kwargs):
        raise RuntimeError("warehouse is missing")

    monkeypatch.setattr(module, "materialize_signal_events", fake_materialize_signal_events)

    exit_code = module.main(
        [
            "--warehouse-path",
            str(tmp_path / "warehouse.duckdb"),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Signal event materialization failed before completion:" in captured.err
    assert "warehouse is missing" in captured.err


def test_materialize_signal_events_reads_warehouse_and_writes_events(tmp_path) -> None:
    module = load_script_module()
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"
    collection_time = datetime(2026, 3, 9, 13, 0, tzinfo=UTC)
    event_time = datetime(2026, 3, 9, 12, 0, tzinfo=UTC)

    price_history = PriceHistory(
        token_id="111",
        interval="1m",
        fidelity=1,
        points=(
            PriceHistoryPoint(timestamp=event_time - timedelta(minutes=15), price=Decimal("0.60")),
            PriceHistoryPoint(timestamp=event_time - timedelta(minutes=5), price=Decimal("0.65")),
            PriceHistoryPoint(timestamp=event_time, price=Decimal("0.72")),
        ),
    )
    trades = (
        _trade("0xbaseline1", "BUY", "10", event_time - timedelta(minutes=70)),
        _trade("0xbaseline2", "SELL", "5", event_time - timedelta(minutes=55)),
        _trade("0xbaseline2", "SELL", "15", event_time - timedelta(minutes=52)),
        _trade("0xbaseline3", "BUY", "30", event_time - timedelta(minutes=40)),
        _trade("0xbaseline4", "SELL", "15", event_time - timedelta(minutes=25)),
        _trade("0xbaseline4", "BUY", "25", event_time - timedelta(minutes=20)),
        _trade("0xwalletA", "BUY", "100", event_time - timedelta(minutes=10)),
        _trade("0xwalletA", "BUY", "150", event_time - timedelta(minutes=7)),
        _trade("0xwalletB", "SELL", "50", event_time - timedelta(minutes=4)),
    )
    snapshots = (
        _snapshot("0.59", "200", "0.61", "200", event_time - timedelta(minutes=50)),
        _snapshot("0.60", "180", "0.62", "180", event_time - timedelta(minutes=35)),
        _snapshot("0.61", "160", "0.63", "160", event_time - timedelta(minutes=20)),
        _snapshot("0.70", "50", "0.74", "60", event_time - timedelta(minutes=1)),
    )
    profiles = (
        _profile("0xwalletA", event_time - timedelta(hours=2), "90", "0.35", 6, "0.75"),
        _profile("0xwalletB", event_time - timedelta(hours=1), "-15", "-0.15", 3, "0.40"),
    )

    with PolymarketWarehouse(database_path) as warehouse:
        assert warehouse.upsert_price_history([price_history], collection_time=collection_time) == 3
        assert warehouse.upsert_trades(trades, collection_time=collection_time) == 9
        assert warehouse.upsert_order_book_snapshots(snapshots, collection_time=collection_time) == 4
        assert warehouse.upsert_wallet_profiles(profiles, collection_time=collection_time) == 2

    summary = module.materialize_signal_events(
        warehouse_path=database_path,
        asset_ids=("111",),
        collection_time=collection_time,
    )

    assert summary.selected_asset_ids == ("111",)
    assert summary.wallet_profile_count == 2
    assert summary.events_written == 1
    assert summary.skipped_assets == ()
    assert len(summary.asset_results) == 1
    assert summary.asset_results[0].asset_id == "111"
    assert summary.asset_results[0].event_count == 1

    with duckdb.connect(str(database_path), read_only=True) as connection:
        event_count = connection.execute("SELECT COUNT(*) FROM signal_events").fetchone()[0]

    assert event_count == 1


def _trade(wallet_address: str, side: str, usdc_size: str, timestamp: datetime) -> TradeRecord:
    return TradeRecord(
        proxy_wallet=wallet_address,
        asset_id="111",
        condition_id="0xcondition123",
        outcome="YES",
        side=side,
        size=Decimal("100"),
        price=Decimal(usdc_size) / Decimal("100"),
        timestamp=timestamp,
        transaction_hash=f"{wallet_address}-{timestamp.isoformat()}",
        usdc_size=Decimal(usdc_size),
    )


def _snapshot(
    best_bid_price: str,
    best_bid_size: str,
    best_ask_price: str,
    best_ask_size: str,
    snapshot_time: datetime,
) -> TopOfBookSnapshot:
    return TopOfBookSnapshot(
        market_id="0xcondition123",
        asset_id="111",
        best_bid_price=Decimal(best_bid_price),
        best_bid_size=Decimal(best_bid_size),
        best_ask_price=Decimal(best_ask_price),
        best_ask_size=Decimal(best_ask_size),
        last_trade_price=None,
        tick_size=Decimal("0.01"),
        book_hash=f"hash-{snapshot_time.isoformat()}",
        snapshot_time=snapshot_time,
    )


def _profile(
    wallet_address: str,
    as_of_time: datetime,
    realized_pnl: str,
    realized_roi: str,
    closed_position_count: int,
    hit_rate: str,
) -> WalletProfile:
    return WalletProfile(
        wallet_address=wallet_address,
        as_of_time_utc=as_of_time,
        realized_pnl=Decimal(realized_pnl),
        realized_roi=Decimal(realized_roi),
        closed_position_count=closed_position_count,
        winning_closed_position_count=max(1, closed_position_count // 2),
        hit_rate=Decimal(hit_rate),
        avg_closed_position_cost=Decimal("100"),
        activity_trade_count=10,
        activity_volume_usdc=Decimal("300"),
        avg_trade_size_usdc=Decimal("30"),
        first_activity_time_utc=as_of_time - timedelta(days=20),
        last_activity_time_utc=as_of_time - timedelta(days=1),
        last_closed_position_time_utc=as_of_time - timedelta(days=2),
    )
