from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from src.clients.clob import PriceHistoryPoint
from src.clients.data_api import TradeRecord
from src.signals.wallet_profiles import estimate_trade_usdc_size


ZERO_DECIMAL = Decimal("0")
ONE_DECIMAL = Decimal("1")
TEN_THOUSAND = Decimal("10000")


@dataclass(frozen=True, slots=True)
class MarketFeatureConfig:
    recent_window: timedelta = timedelta(minutes=15)
    baseline_window: timedelta = timedelta(hours=2)
    short_return_window: timedelta = timedelta(minutes=5)
    medium_return_window: timedelta = timedelta(minutes=15)
    liquidity_baseline_window: timedelta = timedelta(hours=1)

    def __post_init__(self) -> None:
        if self.recent_window <= timedelta(0):
            raise ValueError("recent_window must be greater than zero.")
        if self.baseline_window < self.recent_window:
            raise ValueError("baseline_window must be at least as large as recent_window.")
        if self.baseline_window % self.recent_window != timedelta(0):
            raise ValueError("baseline_window must be an exact multiple of recent_window.")
        if self.short_return_window <= timedelta(0):
            raise ValueError("short_return_window must be greater than zero.")
        if self.medium_return_window <= timedelta(0):
            raise ValueError("medium_return_window must be greater than zero.")
        if self.liquidity_baseline_window < self.recent_window:
            raise ValueError("liquidity_baseline_window must be at least as large as recent_window.")

    @property
    def baseline_bucket_count(self) -> int:
        return int(self.baseline_window / self.recent_window)


@dataclass(frozen=True, slots=True)
class MarketAnomalyFeatures:
    asset_id: str | None
    condition_id: str | None
    event_time_utc: datetime
    recent_window_seconds: int
    baseline_window_seconds: int
    recent_trade_count: int
    recent_volume_usdc: Decimal
    baseline_trade_count_mean: Decimal | None
    baseline_trade_count_std: Decimal | None
    trade_count_zscore: Decimal | None
    baseline_volume_mean: Decimal | None
    baseline_volume_std: Decimal | None
    volume_zscore: Decimal | None
    buy_volume_usdc: Decimal
    sell_volume_usdc: Decimal
    order_flow_imbalance: Decimal | None
    latest_price: Decimal | None
    short_return_window_seconds: int
    short_return: Decimal | None
    medium_return_window_seconds: int
    medium_return: Decimal | None
    liquidity_features_available: bool
    latest_mid_price: Decimal | None
    latest_spread_bps: Decimal | None
    spread_change_bps: Decimal | None
    top_of_book_depth_usdc: Decimal | None
    depth_change_ratio: Decimal | None
    depth_imbalance: Decimal | None


def calculate_market_anomaly_features(
    *,
    event_time: datetime,
    trades: Iterable[TradeRecord] = (),
    price_points: Iterable[PriceHistoryPoint] = (),
    order_book_snapshots: Iterable[object] = (),
    asset_id: str | None = None,
    condition_id: str | None = None,
    config: MarketFeatureConfig | None = None,
) -> MarketAnomalyFeatures:
    feature_config = config or MarketFeatureConfig()
    normalized_event_time = _normalize_utc_timestamp(event_time)
    matching_trades = sorted(
        (
            trade
            for trade in trades
            if _trade_matches(trade, asset_id=asset_id, condition_id=condition_id)
            and _is_visible_at_cutoff(trade.timestamp, normalized_event_time)
        ),
        key=lambda trade: _normalize_utc_timestamp(trade.timestamp or normalized_event_time),
    )
    matching_points = sorted(
        (
            point
            for point in price_points
            if point.timestamp is not None and point.price is not None
            and _normalize_utc_timestamp(point.timestamp) <= normalized_event_time
        ),
        key=lambda point: _normalize_utc_timestamp(point.timestamp or normalized_event_time),
    )
    matching_snapshots = sorted(
        (
            snapshot
            for snapshot in order_book_snapshots
            if _snapshot_matches(snapshot, asset_id=asset_id, condition_id=condition_id)
            and _is_visible_at_cutoff(getattr(snapshot, "snapshot_time", None), normalized_event_time)
        ),
        key=lambda snapshot: _normalize_utc_timestamp(getattr(snapshot, "snapshot_time", None) or normalized_event_time),
    )

    recent_trades = _trades_in_window(
        matching_trades,
        end_time=normalized_event_time,
        window=feature_config.recent_window,
    )
    baseline_trade_windows = _build_trade_windows(
        matching_trades,
        end_time=normalized_event_time,
        recent_window=feature_config.recent_window,
        bucket_count=feature_config.baseline_bucket_count,
    )

    recent_volume_usdc = sum(
        (
            size
            for size in (estimate_trade_usdc_size(trade) for trade in recent_trades)
            if size is not None
        ),
        start=ZERO_DECIMAL,
    )
    buy_volume_usdc = sum(
        (
            size
            for trade in recent_trades
            if _normalized_side(trade.side) == "BUY"
            for size in [estimate_trade_usdc_size(trade)]
            if size is not None
        ),
        start=ZERO_DECIMAL,
    )
    sell_volume_usdc = sum(
        (
            size
            for trade in recent_trades
            if _normalized_side(trade.side) == "SELL"
            for size in [estimate_trade_usdc_size(trade)]
            if size is not None
        ),
        start=ZERO_DECIMAL,
    )
    total_order_flow_volume = buy_volume_usdc + sell_volume_usdc
    order_flow_imbalance = (
        (buy_volume_usdc - sell_volume_usdc) / total_order_flow_volume
        if total_order_flow_volume > ZERO_DECIMAL
        else None
    )

    baseline_trade_counts = [Decimal(len(bucket)) for bucket in baseline_trade_windows]
    baseline_volumes = [
        sum(
            (
                size
                for size in (estimate_trade_usdc_size(trade) for trade in bucket)
                if size is not None
            ),
            start=ZERO_DECIMAL,
        )
        for bucket in baseline_trade_windows
    ]

    baseline_trade_count_mean = _mean(baseline_trade_counts)
    baseline_trade_count_std = _population_stddev(baseline_trade_counts)
    trade_count_zscore = _zscore(
        Decimal(len(recent_trades)),
        mean=baseline_trade_count_mean,
        stddev=baseline_trade_count_std,
    )
    baseline_volume_mean = _mean(baseline_volumes)
    baseline_volume_std = _population_stddev(baseline_volumes)
    volume_zscore = _zscore(
        recent_volume_usdc,
        mean=baseline_volume_mean,
        stddev=baseline_volume_std,
    )

    latest_price = _latest_price_before(matching_points, normalized_event_time)
    short_return = _calculate_return(
        matching_points,
        end_time=normalized_event_time,
        window=feature_config.short_return_window,
    )
    medium_return = _calculate_return(
        matching_points,
        end_time=normalized_event_time,
        window=feature_config.medium_return_window,
    )

    latest_mid_price = None
    latest_spread_bps = None
    spread_change_bps = None
    top_of_book_depth_usdc = None
    depth_change_ratio = None
    depth_imbalance = None
    liquidity_features_available = bool(matching_snapshots)

    if liquidity_features_available:
        latest_snapshot = matching_snapshots[-1]
        latest_mid_price = _snapshot_mid_price(latest_snapshot)
        latest_spread_bps = _snapshot_spread_bps(latest_snapshot)
        top_of_book_depth_usdc = _snapshot_total_depth(latest_snapshot)
        depth_imbalance = _snapshot_depth_imbalance(latest_snapshot)

        baseline_snapshots = [
            snapshot
            for snapshot in matching_snapshots
            if normalized_event_time - feature_config.liquidity_baseline_window
            <= _normalize_utc_timestamp(getattr(snapshot, "snapshot_time", None) or normalized_event_time)
            < normalized_event_time
        ]
        baseline_spreads = [
            spread
            for spread in (_snapshot_spread_bps(snapshot) for snapshot in baseline_snapshots[:-1])
            if spread is not None
        ]
        baseline_depths = [
            depth
            for depth in (_snapshot_total_depth(snapshot) for snapshot in baseline_snapshots[:-1])
            if depth is not None
        ]

        baseline_spread_mean = _mean(baseline_spreads)
        baseline_depth_mean = _mean(baseline_depths)
        spread_change_bps = (
            latest_spread_bps - baseline_spread_mean
            if latest_spread_bps is not None and baseline_spread_mean is not None
            else None
        )
        depth_change_ratio = (
            (top_of_book_depth_usdc / baseline_depth_mean) - ONE_DECIMAL
            if top_of_book_depth_usdc is not None
            and baseline_depth_mean is not None
            and baseline_depth_mean > ZERO_DECIMAL
            else None
        )

    return MarketAnomalyFeatures(
        asset_id=asset_id,
        condition_id=condition_id,
        event_time_utc=normalized_event_time,
        recent_window_seconds=int(feature_config.recent_window.total_seconds()),
        baseline_window_seconds=int(feature_config.baseline_window.total_seconds()),
        recent_trade_count=len(recent_trades),
        recent_volume_usdc=recent_volume_usdc,
        baseline_trade_count_mean=baseline_trade_count_mean,
        baseline_trade_count_std=baseline_trade_count_std,
        trade_count_zscore=trade_count_zscore,
        baseline_volume_mean=baseline_volume_mean,
        baseline_volume_std=baseline_volume_std,
        volume_zscore=volume_zscore,
        buy_volume_usdc=buy_volume_usdc,
        sell_volume_usdc=sell_volume_usdc,
        order_flow_imbalance=order_flow_imbalance,
        latest_price=latest_price,
        short_return_window_seconds=int(feature_config.short_return_window.total_seconds()),
        short_return=short_return,
        medium_return_window_seconds=int(feature_config.medium_return_window.total_seconds()),
        medium_return=medium_return,
        liquidity_features_available=liquidity_features_available,
        latest_mid_price=latest_mid_price,
        latest_spread_bps=latest_spread_bps,
        spread_change_bps=spread_change_bps,
        top_of_book_depth_usdc=top_of_book_depth_usdc,
        depth_change_ratio=depth_change_ratio,
        depth_imbalance=depth_imbalance,
    )


def _trade_matches(
    trade: TradeRecord,
    *,
    asset_id: str | None,
    condition_id: str | None,
) -> bool:
    return _matches_identifiers(
        asset_id=asset_id,
        condition_id=condition_id,
        candidate_asset_id=trade.asset_id,
        candidate_condition_id=trade.condition_id,
    )


def _snapshot_matches(
    snapshot: object,
    *,
    asset_id: str | None,
    condition_id: str | None,
) -> bool:
    return _matches_identifiers(
        asset_id=asset_id,
        condition_id=condition_id,
        candidate_asset_id=getattr(snapshot, "asset_id", None),
        candidate_condition_id=getattr(snapshot, "market_id", None),
    )


def _matches_identifiers(
    *,
    asset_id: str | None,
    condition_id: str | None,
    candidate_asset_id: str | None,
    candidate_condition_id: str | None,
) -> bool:
    if asset_id is None and condition_id is None:
        return True

    matched = False
    if asset_id is not None and candidate_asset_id is not None:
        if candidate_asset_id != asset_id:
            return False
        matched = True
    if condition_id is not None and candidate_condition_id is not None:
        if candidate_condition_id != condition_id:
            return False
        matched = True
    return matched


def _trades_in_window(
    trades: Iterable[TradeRecord],
    *,
    end_time: datetime,
    window: timedelta,
) -> list[TradeRecord]:
    window_start = end_time - window
    return [
        trade
        for trade in trades
        if trade.timestamp is not None
        and window_start < _normalize_utc_timestamp(trade.timestamp) <= end_time
    ]


def _build_trade_windows(
    trades: list[TradeRecord],
    *,
    end_time: datetime,
    recent_window: timedelta,
    bucket_count: int,
) -> list[list[TradeRecord]]:
    windows: list[list[TradeRecord]] = []
    for bucket_index in range(bucket_count, 0, -1):
        bucket_end = end_time - recent_window * bucket_index
        bucket_start = bucket_end - recent_window
        windows.append(
            [
                trade
                for trade in trades
                if trade.timestamp is not None
                and bucket_start < _normalize_utc_timestamp(trade.timestamp) <= bucket_end
            ]
        )
    return windows


def _latest_price_before(points: Iterable[PriceHistoryPoint], cutoff_time: datetime) -> Decimal | None:
    for point in reversed(tuple(points)):
        if point.timestamp is not None and _normalize_utc_timestamp(point.timestamp) <= cutoff_time:
            return point.price
    return None


def _calculate_return(
    points: Iterable[PriceHistoryPoint],
    *,
    end_time: datetime,
    window: timedelta,
) -> Decimal | None:
    current_price = _latest_price_before(points, end_time)
    prior_price = _latest_price_before(points, end_time - window)
    if current_price is None or prior_price is None or prior_price <= ZERO_DECIMAL:
        return None
    return (current_price / prior_price) - ONE_DECIMAL


def _snapshot_mid_price(snapshot: object) -> Decimal | None:
    best_bid = getattr(snapshot, "best_bid_price", None)
    best_ask = getattr(snapshot, "best_ask_price", None)
    if best_bid is None or best_ask is None:
        return None
    return (best_bid + best_ask) / Decimal("2")


def _snapshot_spread_bps(snapshot: object) -> Decimal | None:
    best_bid = getattr(snapshot, "best_bid_price", None)
    best_ask = getattr(snapshot, "best_ask_price", None)
    mid_price = _snapshot_mid_price(snapshot)
    if best_bid is None or best_ask is None or mid_price is None or mid_price <= ZERO_DECIMAL:
        return None
    return ((best_ask - best_bid) / mid_price) * TEN_THOUSAND


def _snapshot_total_depth(snapshot: object) -> Decimal | None:
    best_bid_price = getattr(snapshot, "best_bid_price", None)
    best_bid_size = getattr(snapshot, "best_bid_size", None)
    best_ask_price = getattr(snapshot, "best_ask_price", None)
    best_ask_size = getattr(snapshot, "best_ask_size", None)
    bid_depth = (
        best_bid_price * best_bid_size
        if best_bid_price is not None and best_bid_size is not None
        else None
    )
    ask_depth = (
        best_ask_price * best_ask_size
        if best_ask_price is not None and best_ask_size is not None
        else None
    )
    if bid_depth is None and ask_depth is None:
        return None
    return (bid_depth or ZERO_DECIMAL) + (ask_depth or ZERO_DECIMAL)


def _snapshot_depth_imbalance(snapshot: object) -> Decimal | None:
    best_bid_price = getattr(snapshot, "best_bid_price", None)
    best_bid_size = getattr(snapshot, "best_bid_size", None)
    best_ask_price = getattr(snapshot, "best_ask_price", None)
    best_ask_size = getattr(snapshot, "best_ask_size", None)
    bid_depth = (
        best_bid_price * best_bid_size
        if best_bid_price is not None and best_bid_size is not None
        else ZERO_DECIMAL
    )
    ask_depth = (
        best_ask_price * best_ask_size
        if best_ask_price is not None and best_ask_size is not None
        else ZERO_DECIMAL
    )
    total_depth = bid_depth + ask_depth
    if total_depth <= ZERO_DECIMAL:
        return None
    return (bid_depth - ask_depth) / total_depth


def _mean(values: Iterable[Decimal]) -> Decimal | None:
    numbers = tuple(values)
    if not numbers:
        return None
    return sum(numbers, start=ZERO_DECIMAL) / Decimal(len(numbers))


def _population_stddev(values: Iterable[Decimal]) -> Decimal | None:
    numbers = tuple(values)
    if len(numbers) < 2:
        return None
    mean = _mean(numbers)
    if mean is None:
        return None
    variance = sum(((value - mean) ** 2 for value in numbers), start=ZERO_DECIMAL) / Decimal(len(numbers))
    return variance.sqrt()


def _zscore(value: Decimal, *, mean: Decimal | None, stddev: Decimal | None) -> Decimal | None:
    if mean is None or stddev is None:
        return None
    if stddev == ZERO_DECIMAL:
        return ZERO_DECIMAL if value == mean else None
    return (value - mean) / stddev


def _is_visible_at_cutoff(value: datetime | None, cutoff_time: datetime) -> bool:
    return value is not None and _normalize_utc_timestamp(value) <= cutoff_time


def _normalized_side(value: str | None) -> str | None:
    return value.strip().upper() if value else None


def _normalize_utc_timestamp(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
