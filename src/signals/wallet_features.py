from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from src.clients.data_api import TradeRecord
from src.signals.wallet_profiles import WalletProfile, estimate_trade_usdc_size


ZERO_DECIMAL = Decimal("0")
ONE_DECIMAL = Decimal("1")
HALF_DECIMAL = Decimal("0.5")


@dataclass(frozen=True, slots=True)
class WalletQualityConfig:
    event_window: timedelta = timedelta(minutes=15)
    full_reliability_closed_positions: int = 5

    def __post_init__(self) -> None:
        if self.event_window <= timedelta(0):
            raise ValueError("event_window must be greater than zero.")
        if self.full_reliability_closed_positions <= 0:
            raise ValueError("full_reliability_closed_positions must be greater than zero.")


@dataclass(frozen=True, slots=True)
class WalletTradeAggregate:
    wallet_address: str
    trade_count: int
    traded_volume_usdc: Decimal
    buy_volume_usdc: Decimal
    sell_volume_usdc: Decimal
    net_order_flow_usdc: Decimal


@dataclass(frozen=True, slots=True)
class WalletParticipantFeatures:
    wallet_address: str
    trade_count: int
    traded_volume_usdc: Decimal
    buy_volume_usdc: Decimal
    sell_volume_usdc: Decimal
    net_order_flow_usdc: Decimal
    profile_as_of_time_utc: datetime | None
    realized_pnl: Decimal | None
    realized_roi: Decimal | None
    hit_rate: Decimal | None
    closed_position_count: int | None
    activity_volume_usdc: Decimal | None
    quality_score: Decimal | None


@dataclass(frozen=True, slots=True)
class WalletSummaryFeatures:
    event_time_utc: datetime
    window_seconds: int
    active_wallet_count: int
    profiled_wallet_count: int
    sparse_wallet_set: bool
    total_wallet_volume_usdc: Decimal
    profiled_volume_share: Decimal | None
    top_wallet_share: Decimal | None
    concentration_hhi: Decimal | None
    weighted_average_quality: Decimal | None
    weighted_average_realized_roi: Decimal | None
    weighted_average_hit_rate: Decimal | None
    weighted_average_realized_pnl: Decimal | None
    participants: tuple[WalletParticipantFeatures, ...]


def aggregate_wallet_activity(
    *,
    event_time: datetime,
    trades: Iterable[TradeRecord],
    asset_id: str | None = None,
    condition_id: str | None = None,
    window: timedelta | None = None,
) -> tuple[WalletTradeAggregate, ...]:
    normalized_event_time = _normalize_utc_timestamp(event_time)
    lookback_window = window or WalletQualityConfig().event_window
    window_start = normalized_event_time - lookback_window
    aggregates: dict[str, WalletTradeAggregate] = {}

    for trade in trades:
        wallet_address = trade.proxy_wallet
        if not wallet_address or trade.timestamp is None:
            continue
        if not _trade_matches(trade, asset_id=asset_id, condition_id=condition_id):
            continue

        trade_time = _normalize_utc_timestamp(trade.timestamp)
        if not (window_start < trade_time <= normalized_event_time):
            continue

        trade_volume = estimate_trade_usdc_size(trade) or ZERO_DECIMAL
        side = _normalized_side(trade.side)
        buy_volume = trade_volume if side == "BUY" else ZERO_DECIMAL
        sell_volume = trade_volume if side == "SELL" else ZERO_DECIMAL
        current = aggregates.get(wallet_address)
        if current is None:
            aggregates[wallet_address] = WalletTradeAggregate(
                wallet_address=wallet_address,
                trade_count=1,
                traded_volume_usdc=trade_volume,
                buy_volume_usdc=buy_volume,
                sell_volume_usdc=sell_volume,
                net_order_flow_usdc=buy_volume - sell_volume,
            )
            continue

        aggregates[wallet_address] = WalletTradeAggregate(
            wallet_address=wallet_address,
            trade_count=current.trade_count + 1,
            traded_volume_usdc=current.traded_volume_usdc + trade_volume,
            buy_volume_usdc=current.buy_volume_usdc + buy_volume,
            sell_volume_usdc=current.sell_volume_usdc + sell_volume,
            net_order_flow_usdc=current.net_order_flow_usdc + buy_volume - sell_volume,
        )

    return tuple(
        sorted(
            aggregates.values(),
            key=lambda aggregate: (
                -aggregate.traded_volume_usdc,
                -aggregate.trade_count,
                aggregate.wallet_address,
            ),
        )
    )


def summarize_wallet_quality(
    *,
    event_time: datetime,
    wallet_trades: Iterable[WalletTradeAggregate],
    wallet_profiles: Iterable[WalletProfile] = (),
    config: WalletQualityConfig | None = None,
) -> WalletSummaryFeatures:
    quality_config = config or WalletQualityConfig()
    normalized_event_time = _normalize_utc_timestamp(event_time)
    aggregates = tuple(wallet_trades)
    total_wallet_volume = sum((aggregate.traded_volume_usdc for aggregate in aggregates), start=ZERO_DECIMAL)
    latest_profiles = _latest_profiles_by_wallet(wallet_profiles, cutoff_time=normalized_event_time)

    participants: list[WalletParticipantFeatures] = []
    profiled_volume = ZERO_DECIMAL
    weighted_quality_sum = ZERO_DECIMAL
    weighted_roi_sum = ZERO_DECIMAL
    weighted_hit_rate_sum = ZERO_DECIMAL
    weighted_realized_pnl_sum = ZERO_DECIMAL
    weighted_profile_volume = ZERO_DECIMAL

    for aggregate in aggregates:
        profile = latest_profiles.get(aggregate.wallet_address)
        quality_score = (
            calculate_wallet_quality_score(
                profile,
                full_reliability_closed_positions=quality_config.full_reliability_closed_positions,
            )
            if profile is not None
            else None
        )
        if profile is not None and aggregate.traded_volume_usdc > ZERO_DECIMAL:
            profiled_volume += aggregate.traded_volume_usdc
            weighted_profile_volume += aggregate.traded_volume_usdc
            if quality_score is not None:
                weighted_quality_sum += quality_score * aggregate.traded_volume_usdc
            if profile.realized_roi is not None:
                weighted_roi_sum += profile.realized_roi * aggregate.traded_volume_usdc
            if profile.hit_rate is not None:
                weighted_hit_rate_sum += profile.hit_rate * aggregate.traded_volume_usdc
            weighted_realized_pnl_sum += profile.realized_pnl * aggregate.traded_volume_usdc

        participants.append(
            WalletParticipantFeatures(
                wallet_address=aggregate.wallet_address,
                trade_count=aggregate.trade_count,
                traded_volume_usdc=aggregate.traded_volume_usdc,
                buy_volume_usdc=aggregate.buy_volume_usdc,
                sell_volume_usdc=aggregate.sell_volume_usdc,
                net_order_flow_usdc=aggregate.net_order_flow_usdc,
                profile_as_of_time_utc=profile.as_of_time_utc if profile is not None else None,
                realized_pnl=profile.realized_pnl if profile is not None else None,
                realized_roi=profile.realized_roi if profile is not None else None,
                hit_rate=profile.hit_rate if profile is not None else None,
                closed_position_count=profile.closed_position_count if profile is not None else None,
                activity_volume_usdc=profile.activity_volume_usdc if profile is not None else None,
                quality_score=quality_score,
            )
        )

    top_wallet_share = None
    concentration_hhi = None
    if total_wallet_volume > ZERO_DECIMAL:
        shares = tuple(aggregate.traded_volume_usdc / total_wallet_volume for aggregate in aggregates)
        top_wallet_share = max(shares, default=None)
        concentration_hhi = sum((share * share for share in shares), start=ZERO_DECIMAL)

    profiled_wallet_count = sum(1 for participant in participants if participant.profile_as_of_time_utc is not None)
    return WalletSummaryFeatures(
        event_time_utc=normalized_event_time,
        window_seconds=int(quality_config.event_window.total_seconds()),
        active_wallet_count=len(participants),
        profiled_wallet_count=profiled_wallet_count,
        sparse_wallet_set=len(participants) <= 1 or profiled_wallet_count <= 1,
        total_wallet_volume_usdc=total_wallet_volume,
        profiled_volume_share=(
            profiled_volume / total_wallet_volume
            if total_wallet_volume > ZERO_DECIMAL
            else None
        ),
        top_wallet_share=top_wallet_share,
        concentration_hhi=concentration_hhi,
        weighted_average_quality=(
            weighted_quality_sum / weighted_profile_volume
            if weighted_profile_volume > ZERO_DECIMAL
            else None
        ),
        weighted_average_realized_roi=(
            weighted_roi_sum / weighted_profile_volume
            if weighted_profile_volume > ZERO_DECIMAL
            else None
        ),
        weighted_average_hit_rate=(
            weighted_hit_rate_sum / weighted_profile_volume
            if weighted_profile_volume > ZERO_DECIMAL
            else None
        ),
        weighted_average_realized_pnl=(
            weighted_realized_pnl_sum / weighted_profile_volume
            if weighted_profile_volume > ZERO_DECIMAL
            else None
        ),
        participants=tuple(participants),
    )


def calculate_wallet_quality_score(
    profile: WalletProfile,
    *,
    full_reliability_closed_positions: int = WalletQualityConfig().full_reliability_closed_positions,
) -> Decimal:
    reliability = min(
        Decimal(profile.closed_position_count) / Decimal(full_reliability_closed_positions),
        ONE_DECIMAL,
    )
    roi_component = _clamp(profile.realized_roi or ZERO_DECIMAL, lower=Decimal("-1"), upper=ONE_DECIMAL) / Decimal("2")
    hit_rate_component = (profile.hit_rate or HALF_DECIMAL) - HALF_DECIMAL
    return reliability * (roi_component + hit_rate_component)


def _latest_profiles_by_wallet(
    profiles: Iterable[WalletProfile],
    *,
    cutoff_time: datetime,
) -> dict[str, WalletProfile]:
    latest_profiles: dict[str, WalletProfile] = {}
    for profile in profiles:
        if _normalize_utc_timestamp(profile.as_of_time_utc) > cutoff_time:
            continue
        current = latest_profiles.get(profile.wallet_address)
        if current is None or _normalize_utc_timestamp(profile.as_of_time_utc) >= _normalize_utc_timestamp(current.as_of_time_utc):
            latest_profiles[profile.wallet_address] = profile
    return latest_profiles


def _trade_matches(
    trade: TradeRecord,
    *,
    asset_id: str | None,
    condition_id: str | None,
) -> bool:
    if asset_id is None and condition_id is None:
        return True

    matched = False
    if asset_id is not None and trade.asset_id is not None:
        if trade.asset_id != asset_id:
            return False
        matched = True
    if condition_id is not None and trade.condition_id is not None:
        if trade.condition_id != condition_id:
            return False
        matched = True
    return matched


def _clamp(value: Decimal, *, lower: Decimal, upper: Decimal) -> Decimal:
    return min(max(value, lower), upper)


def _normalized_side(value: str | None) -> str | None:
    return value.strip().upper() if value else None


def _normalize_utc_timestamp(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
