from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from src.clients.data_api import ClosedPosition, TradeRecord
from src.signals import build_wallet_profile, estimate_trade_usdc_size


def test_build_wallet_profile_returns_zero_counts_and_null_ratios_for_empty_history() -> None:
    as_of_time = datetime(2026, 3, 8, 12, 0, tzinfo=UTC)

    profile = build_wallet_profile("0xwallet1", as_of_time=as_of_time)

    assert profile.wallet_address == "0xwallet1"
    assert profile.as_of_time_utc == as_of_time
    assert profile.realized_pnl == Decimal("0")
    assert profile.realized_roi is None
    assert profile.closed_position_count == 0
    assert profile.winning_closed_position_count == 0
    assert profile.hit_rate is None
    assert profile.avg_closed_position_cost is None
    assert profile.activity_trade_count == 0
    assert profile.activity_volume_usdc == Decimal("0")
    assert profile.avg_trade_size_usdc is None
    assert profile.first_activity_time_utc is None
    assert profile.last_activity_time_utc is None
    assert profile.last_closed_position_time_utc is None


def test_build_wallet_profile_applies_cutoff_filters_and_volume_fallbacks() -> None:
    as_of_time = datetime(2026, 3, 5, 12, 0, tzinfo=UTC)
    closed_positions = [
        ClosedPosition(
            proxy_wallet="0xwallet1",
            asset_id="111",
            condition_id="0xcondition123",
            outcome="YES",
            average_price=Decimal("0.44"),
            realized_pnl=Decimal("20"),
            total_bought=Decimal("100"),
            closed_at=datetime(2026, 3, 1, 10, 15, tzinfo=UTC),
            end_date=datetime(2026, 3, 31, 12, 0, tzinfo=UTC),
        ),
        ClosedPosition(
            proxy_wallet="0xwallet1",
            asset_id="222",
            condition_id="0xcondition456",
            outcome="NO",
            average_price=Decimal("0.51"),
            realized_pnl=Decimal("-10"),
            total_bought=Decimal("50"),
            closed_at=datetime(2026, 3, 2, 9, 0, tzinfo=UTC),
            end_date=datetime(2026, 3, 31, 12, 0, tzinfo=UTC),
        ),
        ClosedPosition(
            proxy_wallet="0xwallet1",
            asset_id="333",
            condition_id="0xcondition789",
            outcome="YES",
            average_price=Decimal("0.70"),
            realized_pnl=Decimal("8"),
            total_bought=Decimal("40"),
            closed_at=datetime(2026, 3, 6, 9, 0, tzinfo=UTC),
            end_date=datetime(2026, 3, 31, 12, 0, tzinfo=UTC),
        ),
        ClosedPosition(
            proxy_wallet="0xwallet1",
            asset_id="444",
            condition_id="0xcondition000",
            outcome="YES",
            average_price=Decimal("0.30"),
            realized_pnl=Decimal("5"),
            total_bought=Decimal("20"),
            closed_at=None,
            end_date=datetime(2026, 3, 31, 12, 0, tzinfo=UTC),
        ),
        ClosedPosition(
            proxy_wallet="0xwallet2",
            asset_id="555",
            condition_id="0xcondition111",
            outcome="NO",
            average_price=Decimal("0.40"),
            realized_pnl=Decimal("99"),
            total_bought=Decimal("100"),
            closed_at=datetime(2026, 3, 2, 12, 0, tzinfo=UTC),
            end_date=datetime(2026, 3, 31, 12, 0, tzinfo=UTC),
        ),
    ]
    activity_trades = [
        TradeRecord(
            proxy_wallet="0xwallet1",
            asset_id="111",
            condition_id="0xcondition123",
            outcome="YES",
            side="BUY",
            size=Decimal("100"),
            price=Decimal("0.45"),
            timestamp=datetime(2026, 3, 1, 10, 0, tzinfo=UTC),
            transaction_hash="0xtx1",
            usdc_size=Decimal("45"),
        ),
        TradeRecord(
            proxy_wallet="0xwallet1",
            asset_id="222",
            condition_id="0xcondition456",
            outcome="NO",
            side="SELL",
            size=Decimal("20"),
            price=Decimal("0.50"),
            timestamp=datetime(2026, 3, 2, 11, 0, tzinfo=UTC),
            transaction_hash="0xtx2",
            usdc_size=None,
        ),
        TradeRecord(
            proxy_wallet="0xwallet1",
            asset_id="333",
            condition_id="0xcondition789",
            outcome="YES",
            side="BUY",
            size=Decimal("15"),
            price=Decimal("0.60"),
            timestamp=datetime(2026, 3, 6, 12, 1, tzinfo=UTC),
            transaction_hash="0xtx3",
            usdc_size=None,
        ),
        TradeRecord(
            proxy_wallet="0xwallet1",
            asset_id="444",
            condition_id="0xcondition999",
            outcome="YES",
            side="BUY",
            size=Decimal("50"),
            price=Decimal("0.55"),
            timestamp=None,
            transaction_hash="0xtx4",
            usdc_size=Decimal("27.5"),
        ),
        TradeRecord(
            proxy_wallet="0xwallet2",
            asset_id="555",
            condition_id="0xcondition111",
            outcome="NO",
            side="BUY",
            size=Decimal("999"),
            price=Decimal("0.99"),
            timestamp=datetime(2026, 3, 1, 9, 0, tzinfo=UTC),
            transaction_hash="0xtx5",
            usdc_size=Decimal("989.01"),
        ),
    ]

    profile = build_wallet_profile(
        "0xwallet1",
        closed_positions=closed_positions,
        activity_trades=activity_trades,
        as_of_time=as_of_time,
    )

    assert profile.realized_pnl == Decimal("10")
    assert profile.realized_roi == Decimal("10") / Decimal("150")
    assert profile.closed_position_count == 2
    assert profile.winning_closed_position_count == 1
    assert profile.hit_rate == Decimal("1") / Decimal("2")
    assert profile.avg_closed_position_cost == Decimal("75")
    assert profile.activity_trade_count == 2
    assert profile.activity_volume_usdc == Decimal("55")
    assert profile.avg_trade_size_usdc == Decimal("27.5")
    assert profile.first_activity_time_utc == datetime(2026, 3, 1, 10, 0, tzinfo=UTC)
    assert profile.last_activity_time_utc == datetime(2026, 3, 2, 11, 0, tzinfo=UTC)
    assert profile.last_closed_position_time_utc == datetime(2026, 3, 2, 9, 0, tzinfo=UTC)


def test_estimate_trade_usdc_size_prefers_reported_usdc_and_falls_back_to_notional() -> None:
    reported = TradeRecord(
        proxy_wallet="0xwallet1",
        asset_id="111",
        condition_id="0xcondition123",
        outcome="YES",
        side="BUY",
        size=Decimal("100"),
        price=Decimal("0.45"),
        timestamp=datetime(2026, 3, 1, 10, 0, tzinfo=UTC),
        transaction_hash="0xtx1",
        usdc_size=Decimal("45"),
    )
    derived = TradeRecord(
        proxy_wallet="0xwallet1",
        asset_id="222",
        condition_id="0xcondition456",
        outcome="NO",
        side="SELL",
        size=Decimal("20"),
        price=Decimal("0.50"),
        timestamp=datetime(2026, 3, 2, 11, 0, tzinfo=UTC),
        transaction_hash="0xtx2",
        usdc_size=None,
    )

    assert estimate_trade_usdc_size(reported) == Decimal("45")
    assert estimate_trade_usdc_size(derived) == Decimal("10")
