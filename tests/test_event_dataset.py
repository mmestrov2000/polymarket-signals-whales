from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import duckdb
import pytest

from src.clients.clob import PriceHistory, PriceHistoryPoint
from src.research import DatasetIntegrityError, materialize_event_dataset
from src.signals import EventDetectorConfig, SignalEvent, WalletProfile, detect_signal_events
from src.storage import PolymarketWarehouse, TopOfBookSnapshot


def test_materialize_event_dataset_builds_time_ordered_rows_and_reports(tmp_path) -> None:
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"
    output_dir = tmp_path / "dataset_builds"
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
            PriceHistoryPoint(timestamp=event_time + timedelta(minutes=5), price=Decimal("0.75")),
            PriceHistoryPoint(timestamp=event_time + timedelta(minutes=15), price=Decimal("0.78")),
            PriceHistoryPoint(timestamp=event_time + timedelta(minutes=60), price=Decimal("0.68")),
        ),
    )

    event = _build_signal_event(event_time)[0]

    with PolymarketWarehouse(database_path) as warehouse:
        assert warehouse.upsert_price_history([price_history], collection_time=collection_time) == 6
        assert warehouse.upsert_signal_events([event], collection_time=collection_time) == 1

    result = materialize_event_dataset(
        warehouse_path=database_path,
        output_dir=output_dir,
        build_id="dataset-build-test",
    )

    assert result.rows_written == 1
    assert result.build.qa_report.errors == ()
    assert result.build.qa_report.split_counts == {"train": 1}
    assert result.artifact_paths.summary_path.exists()
    assert result.artifact_paths.qa_report_path.exists()

    summary_payload = json.loads(result.artifact_paths.summary_path.read_text())
    qa_payload = json.loads(result.artifact_paths.qa_report_path.read_text())

    assert summary_payload["primary_label_name"] == "profitable_after_costs"
    assert summary_payload["primary_label_horizon_minutes"] == 15
    assert qa_payload["materialized_row_count"] == 1

    with duckdb.connect(str(database_path), read_only=True) as connection:
        stored_row = connection.execute(
            """
            SELECT
                dataset_split,
                primary_label_name,
                primary_label_horizon_minutes,
                primary_label_profitable,
                primary_net_pnl_bps,
                horizon_labels_json
            FROM event_dataset_rows
            """
        ).fetchone()

    assert stored_row[0] == "train"
    assert stored_row[1] == "profitable_after_costs"
    assert stored_row[2] == 15
    assert stored_row[3] is True
    assert stored_row[4] == Decimal("227.777777777777777778")

    horizon_labels = json.loads(stored_row[5])
    assert horizon_labels["5m"]["continuation"] is True
    assert horizon_labels["15m"]["profitable_after_costs"] is True
    assert horizon_labels["60m"]["reversion"] is True


def test_materialize_event_dataset_fails_on_wallet_feature_leakage_and_writes_report(tmp_path) -> None:
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"
    output_dir = tmp_path / "dataset_builds"
    collection_time = datetime(2026, 3, 9, 13, 0, tzinfo=UTC)
    event_time = datetime(2026, 3, 9, 12, 0, tzinfo=UTC)

    price_history = PriceHistory(
        token_id="111",
        interval="1m",
        fidelity=1,
        points=(
            PriceHistoryPoint(timestamp=event_time - timedelta(minutes=5), price=Decimal("0.70")),
            PriceHistoryPoint(timestamp=event_time, price=Decimal("0.72")),
            PriceHistoryPoint(timestamp=event_time + timedelta(minutes=15), price=Decimal("0.78")),
        ),
    )
    source_event = _build_signal_event(event_time)[0]
    leaky_event = SignalEvent(
        event_id="event-leakage",
        asset_id=source_event.asset_id,
        condition_id=source_event.condition_id,
        event_time_utc=source_event.event_time_utc,
        direction=source_event.direction,
        trigger_reason=source_event.trigger_reason,
        trigger_rules=source_event.trigger_rules,
        market_features=source_event.market_features,
        wallet_summary=source_event.wallet_summary,
        explanation_payload={
            **source_event.explanation_payload,
            "wallet_context": {
                **source_event.explanation_payload["wallet_context"],
                "participants": [
                    {
                        "wallet_address": "0xwalletA",
                        "profile_as_of_time_utc": (event_time + timedelta(minutes=1)).isoformat(),
                    }
                ],
            },
        },
    )

    with PolymarketWarehouse(database_path) as warehouse:
        assert warehouse.upsert_price_history([price_history], collection_time=collection_time) == 3
        assert warehouse.upsert_signal_events([leaky_event], collection_time=collection_time) == 1

    with pytest.raises(DatasetIntegrityError) as excinfo:
        materialize_event_dataset(
            warehouse_path=database_path,
            output_dir=output_dir,
            build_id="dataset-build-leakage",
        )

    error = excinfo.value
    assert error.report.wallet_leakage_event_ids == ("event-leakage",)
    assert error.artifact_paths.qa_report_path.exists()

    qa_payload = json.loads(error.artifact_paths.qa_report_path.read_text())
    assert qa_payload["wallet_leakage_event_ids"] == ["event-leakage"]
    assert any("Wallet leakage detected" in message for message in qa_payload["errors"])


def _build_signal_event(event_time: datetime) -> tuple[SignalEvent, ...]:
    trades = [
        _trade("0xbaseline1", "BUY", "10", event_time - timedelta(minutes=70)),
        _trade("0xbaseline2", "SELL", "5", event_time - timedelta(minutes=55)),
        _trade("0xbaseline2", "SELL", "15", event_time - timedelta(minutes=52)),
        _trade("0xbaseline3", "BUY", "30", event_time - timedelta(minutes=40)),
        _trade("0xbaseline4", "SELL", "15", event_time - timedelta(minutes=25)),
        _trade("0xbaseline4", "BUY", "25", event_time - timedelta(minutes=20)),
        _trade("0xwalletA", "BUY", "100", event_time - timedelta(minutes=10)),
        _trade("0xwalletA", "BUY", "150", event_time - timedelta(minutes=7)),
        _trade("0xwalletB", "SELL", "50", event_time - timedelta(minutes=4)),
    ]
    price_points = (
        PriceHistoryPoint(timestamp=event_time - timedelta(minutes=15), price=Decimal("0.60")),
        PriceHistoryPoint(timestamp=event_time - timedelta(minutes=5), price=Decimal("0.65")),
        PriceHistoryPoint(timestamp=event_time, price=Decimal("0.72")),
        PriceHistoryPoint(timestamp=event_time + timedelta(minutes=5), price=Decimal("0.75")),
        PriceHistoryPoint(timestamp=event_time + timedelta(minutes=15), price=Decimal("0.78")),
    )
    snapshots = (
        _snapshot("0.59", "200", "0.61", "200", event_time - timedelta(minutes=50)),
        _snapshot("0.60", "180", "0.62", "180", event_time - timedelta(minutes=35)),
        _snapshot("0.61", "160", "0.63", "160", event_time - timedelta(minutes=20)),
        _snapshot("0.70", "50", "0.74", "60", event_time - timedelta(minutes=1)),
    )
    profiles = [
        _profile("0xwalletA", event_time - timedelta(hours=2), "90", "0.35", 6, "0.75"),
        _profile("0xwalletB", event_time - timedelta(hours=1), "-15", "-0.15", 3, "0.40"),
    ]

    return detect_signal_events(
        asset_id="111",
        condition_id="0xcondition123",
        trades=trades,
        price_points=price_points,
        order_book_snapshots=snapshots,
        wallet_profiles=profiles,
        candidate_times=(event_time,),
        config=EventDetectorConfig(),
    )


def _trade(wallet_address: str, side: str, usdc_size: str, timestamp: datetime):
    from src.clients.data_api import TradeRecord

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
