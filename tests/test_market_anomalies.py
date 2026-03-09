from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from src.clients.clob import PriceHistoryPoint
from src.clients.data_api import TradeRecord
from src.signals import MarketFeatureConfig, calculate_market_anomaly_features
from src.storage import TopOfBookSnapshot


def test_calculate_market_anomaly_features_computes_spikes_returns_and_liquidity_shifts() -> None:
    event_time = datetime(2026, 3, 8, 12, 0, tzinfo=UTC)
    config = MarketFeatureConfig(
        recent_window=timedelta(minutes=15),
        baseline_window=timedelta(hours=1),
        short_return_window=timedelta(minutes=5),
        medium_return_window=timedelta(minutes=15),
        liquidity_baseline_window=timedelta(hours=1),
    )
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
        PriceHistoryPoint(timestamp=event_time - timedelta(minutes=20), price=Decimal("0.58")),
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

    features = calculate_market_anomaly_features(
        event_time=event_time,
        trades=trades,
        price_points=price_points,
        order_book_snapshots=snapshots,
        asset_id="111",
        condition_id="0xcondition123",
        config=config,
    )

    assert features.recent_trade_count == 3
    assert features.recent_volume_usdc == Decimal("300")
    assert features.baseline_trade_count_mean == Decimal("1.5")
    assert features.baseline_trade_count_std == Decimal("0.5")
    assert features.trade_count_zscore == Decimal("3")
    assert features.baseline_volume_mean == Decimal("25")
    assert Decimal("24") < features.volume_zscore < Decimal("25")
    assert features.buy_volume_usdc == Decimal("250")
    assert features.sell_volume_usdc == Decimal("50")
    assert features.order_flow_imbalance == Decimal("2") / Decimal("3")
    assert features.latest_price == Decimal("0.72")
    assert features.short_return == Decimal("0.72") / Decimal("0.65") - Decimal("1")
    assert features.medium_return == Decimal("0.72") / Decimal("0.60") - Decimal("1")
    assert features.liquidity_features_available is True
    assert features.latest_mid_price == Decimal("0.72")
    assert Decimal("220") < features.spread_change_bps < Decimal("230")
    assert Decimal("-0.7") < features.depth_change_ratio < Decimal("-0.6")
    assert Decimal("-0.2") < features.depth_imbalance < Decimal("-0.1")


def test_calculate_market_anomaly_features_gates_liquidity_metrics_without_forward_data() -> None:
    event_time = datetime(2026, 3, 8, 12, 0, tzinfo=UTC)

    features = calculate_market_anomaly_features(
        event_time=event_time,
        trades=[_trade("0xwalletA", "BUY", "100", event_time - timedelta(minutes=3))],
        price_points=[PriceHistoryPoint(timestamp=event_time, price=Decimal("0.55"))],
        asset_id="111",
        condition_id="0xcondition123",
    )

    assert features.liquidity_features_available is False
    assert features.latest_mid_price is None
    assert features.latest_spread_bps is None
    assert features.spread_change_bps is None
    assert features.top_of_book_depth_usdc is None
    assert features.depth_change_ratio is None
    assert features.depth_imbalance is None


def _trade(
    wallet_address: str,
    side: str,
    usdc_size: str,
    timestamp: datetime,
) -> TradeRecord:
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
