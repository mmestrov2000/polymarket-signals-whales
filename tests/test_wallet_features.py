from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from src.clients.data_api import TradeRecord
from src.signals import (
    WalletProfile,
    aggregate_wallet_activity,
    calculate_wallet_quality_score,
    summarize_wallet_quality,
)


def test_summarize_wallet_quality_handles_empty_wallet_sets_explicitly() -> None:
    event_time = datetime(2026, 3, 8, 12, 0, tzinfo=UTC)

    summary = summarize_wallet_quality(event_time=event_time, wallet_trades=(), wallet_profiles=())

    assert summary.event_time_utc == event_time
    assert summary.active_wallet_count == 0
    assert summary.profiled_wallet_count == 0
    assert summary.sparse_wallet_set is True
    assert summary.total_wallet_volume_usdc == Decimal("0")
    assert summary.profiled_volume_share is None
    assert summary.top_wallet_share is None
    assert summary.concentration_hhi is None
    assert summary.weighted_average_quality is None
    assert summary.weighted_average_realized_roi is None
    assert summary.weighted_average_hit_rate is None
    assert summary.weighted_average_realized_pnl is None
    assert summary.participants == ()


def test_summarize_wallet_quality_computes_concentration_and_weighted_quality_for_mixed_wallets() -> None:
    event_time = datetime(2026, 3, 8, 12, 0, tzinfo=UTC)
    wallet_trades = aggregate_wallet_activity(
        event_time=event_time,
        trades=[
            _trade("0xwalletA", "BUY", "40", event_time - timedelta(minutes=10)),
            _trade("0xwalletA", "BUY", "20", event_time - timedelta(minutes=8)),
            _trade("0xwalletB", "SELL", "30", event_time - timedelta(minutes=7)),
            _trade("0xwalletC", "BUY", "10", event_time - timedelta(minutes=6)),
        ],
        asset_id="111",
        condition_id="0xcondition123",
        window=timedelta(minutes=15),
    )
    profiles = [
        _profile(
            "0xwalletA",
            as_of_time=event_time - timedelta(hours=2),
            realized_pnl="100",
            realized_roi="0.40",
            closed_position_count=5,
            hit_rate="0.80",
        ),
        _profile(
            "0xwalletA",
            as_of_time=event_time + timedelta(minutes=1),
            realized_pnl="999",
            realized_roi="0.99",
            closed_position_count=10,
            hit_rate="0.95",
        ),
        _profile(
            "0xwalletB",
            as_of_time=event_time - timedelta(hours=1),
            realized_pnl="-20",
            realized_roi="-0.20",
            closed_position_count=2,
            hit_rate="0.40",
        ),
    ]

    summary = summarize_wallet_quality(
        event_time=event_time,
        wallet_trades=wallet_trades,
        wallet_profiles=profiles,
    )

    wallet_a_quality = calculate_wallet_quality_score(profiles[0])
    wallet_b_quality = calculate_wallet_quality_score(profiles[2])

    assert [participant.wallet_address for participant in summary.participants] == [
        "0xwalletA",
        "0xwalletB",
        "0xwalletC",
    ]
    assert summary.active_wallet_count == 3
    assert summary.profiled_wallet_count == 2
    assert summary.sparse_wallet_set is False
    assert summary.total_wallet_volume_usdc == Decimal("100")
    assert summary.profiled_volume_share == Decimal("0.9")
    assert summary.top_wallet_share == Decimal("0.6")
    assert summary.concentration_hhi == Decimal("0.46")
    assert summary.weighted_average_quality == (
        wallet_a_quality * Decimal("60") + wallet_b_quality * Decimal("30")
    ) / Decimal("90")
    assert summary.weighted_average_realized_roi == Decimal("0.2")
    assert summary.weighted_average_hit_rate == Decimal("2") / Decimal("3")
    assert summary.weighted_average_realized_pnl == Decimal("60")
    assert summary.participants[0].quality_score == wallet_a_quality
    assert summary.participants[1].quality_score == wallet_b_quality
    assert summary.participants[2].quality_score is None
    assert summary.participants[0].profile_as_of_time_utc == event_time - timedelta(hours=2)


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


def _profile(
    wallet_address: str,
    *,
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
        activity_volume_usdc=Decimal("250"),
        avg_trade_size_usdc=Decimal("25"),
        first_activity_time_utc=as_of_time - timedelta(days=20),
        last_activity_time_utc=as_of_time - timedelta(days=1),
        last_closed_position_time_utc=as_of_time - timedelta(days=2),
    )
