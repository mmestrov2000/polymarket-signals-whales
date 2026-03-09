from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from src.clients.clob import PriceHistoryPoint
from src.clients.data_api import TradeRecord
from src.signals import EventDetectorConfig, WalletProfile, detect_signal_events
from src.storage import TopOfBookSnapshot


def test_detect_signal_events_emits_interpretable_event_with_wallet_context() -> None:
    event_time = datetime(2026, 3, 8, 12, 0, tzinfo=UTC)
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

    events = detect_signal_events(
        asset_id="111",
        condition_id="0xcondition123",
        trades=trades,
        price_points=price_points,
        order_book_snapshots=snapshots,
        wallet_profiles=profiles,
        candidate_times=(event_time,),
        config=EventDetectorConfig(),
    )

    assert len(events) == 1
    event = events[0]
    assert event.asset_id == "111"
    assert event.condition_id == "0xcondition123"
    assert event.event_time_utc == event_time
    assert event.direction == "up"
    assert "volume_spike" in event.trigger_reason
    assert "order_flow_imbalance" in event.trigger_reason
    assert event.wallet_summary.active_wallet_count == 2
    assert event.wallet_summary.profiled_wallet_count == 2
    assert event.explanation_payload["event_time_utc"] == event_time.isoformat()
    assert event.explanation_payload["direction"] == "up"
    assert event.explanation_payload["wallet_context"]["active_wallet_count"] == 2
    assert event.explanation_payload["wallet_context"]["participants"][0]["wallet_address"] == "0xwalletA"
    assert event.explanation_payload["wallet_context"]["participants"][0]["quality_score"] is not None
    assert event.explanation_payload["market_context"]["recent_volume_usdc"] == "300"


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
