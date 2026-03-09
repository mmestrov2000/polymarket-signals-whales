from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from src.clients.data_api import ClosedPosition, TradeRecord


ZERO_DECIMAL = Decimal("0")


@dataclass(frozen=True, slots=True)
class WalletProfile:
    wallet_address: str
    as_of_time_utc: datetime
    realized_pnl: Decimal
    realized_roi: Decimal | None
    closed_position_count: int
    winning_closed_position_count: int
    hit_rate: Decimal | None
    avg_closed_position_cost: Decimal | None
    activity_trade_count: int
    activity_volume_usdc: Decimal
    avg_trade_size_usdc: Decimal | None
    first_activity_time_utc: datetime | None
    last_activity_time_utc: datetime | None
    last_closed_position_time_utc: datetime | None


def build_wallet_profile(
    wallet_address: str,
    *,
    closed_positions: Iterable[ClosedPosition] = (),
    activity_trades: Iterable[TradeRecord] = (),
    as_of_time: datetime | None = None,
) -> WalletProfile:
    normalized_as_of_time = _normalize_utc_timestamp(as_of_time or datetime.now(UTC))
    visible_closed_positions = [
        position
        for position in closed_positions
        if position.proxy_wallet == wallet_address and _is_visible_at_cutoff(position.closed_at, normalized_as_of_time)
    ]
    visible_activity = [
        trade
        for trade in activity_trades
        if trade.proxy_wallet == wallet_address and _is_visible_at_cutoff(trade.timestamp, normalized_as_of_time)
    ]

    realized_pnl = sum(
        (position.realized_pnl for position in visible_closed_positions if position.realized_pnl is not None),
        start=ZERO_DECIMAL,
    )
    closed_position_costs = [position.total_bought for position in visible_closed_positions if position.total_bought is not None]
    total_closed_position_cost = sum(closed_position_costs, start=ZERO_DECIMAL)
    realized_roi = (
        realized_pnl / total_closed_position_cost
        if total_closed_position_cost > ZERO_DECIMAL
        else None
    )

    winning_closed_position_count = sum(
        1
        for position in visible_closed_positions
        if position.realized_pnl is not None and position.realized_pnl > ZERO_DECIMAL
    )
    closed_position_count = len(visible_closed_positions)
    hit_rate = (
        Decimal(winning_closed_position_count) / Decimal(closed_position_count)
        if closed_position_count > 0
        else None
    )
    avg_closed_position_cost = (
        total_closed_position_cost / Decimal(len(closed_position_costs))
        if closed_position_costs
        else None
    )

    visible_trade_sizes = [
        trade_size
        for trade_size in (estimate_trade_usdc_size(trade) for trade in visible_activity)
        if trade_size is not None
    ]
    activity_volume_usdc = sum(visible_trade_sizes, start=ZERO_DECIMAL)
    avg_trade_size_usdc = (
        activity_volume_usdc / Decimal(len(visible_trade_sizes))
        if visible_trade_sizes
        else None
    )
    activity_timestamps = [trade.timestamp for trade in visible_activity if trade.timestamp is not None]
    closed_position_timestamps = [
        position.closed_at for position in visible_closed_positions if position.closed_at is not None
    ]

    return WalletProfile(
        wallet_address=wallet_address,
        as_of_time_utc=normalized_as_of_time,
        realized_pnl=realized_pnl,
        realized_roi=realized_roi,
        closed_position_count=closed_position_count,
        winning_closed_position_count=winning_closed_position_count,
        hit_rate=hit_rate,
        avg_closed_position_cost=avg_closed_position_cost,
        activity_trade_count=len(visible_activity),
        activity_volume_usdc=activity_volume_usdc,
        avg_trade_size_usdc=avg_trade_size_usdc,
        first_activity_time_utc=min(activity_timestamps) if activity_timestamps else None,
        last_activity_time_utc=max(activity_timestamps) if activity_timestamps else None,
        last_closed_position_time_utc=max(closed_position_timestamps) if closed_position_timestamps else None,
    )


def estimate_trade_usdc_size(trade: TradeRecord) -> Decimal | None:
    if trade.usdc_size is not None:
        return trade.usdc_size
    if trade.size is None or trade.price is None:
        return None
    return trade.size * trade.price


def _is_visible_at_cutoff(value: datetime | None, cutoff_time: datetime) -> bool:
    return value is not None and _normalize_utc_timestamp(value) <= cutoff_time


def _normalize_utc_timestamp(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
