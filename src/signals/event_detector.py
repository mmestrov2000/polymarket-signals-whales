from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Any

from src.clients.clob import PriceHistoryPoint
from src.clients.data_api import TradeRecord
from src.signals.market_anomalies import MarketAnomalyFeatures, MarketFeatureConfig, calculate_market_anomaly_features
from src.signals.wallet_features import (
    WalletQualityConfig,
    WalletSummaryFeatures,
    aggregate_wallet_activity,
    summarize_wallet_quality,
)
from src.signals.wallet_profiles import WalletProfile


ZERO_DECIMAL = Decimal("0")


@dataclass(frozen=True, slots=True)
class EventDetectorConfig:
    market_feature_config: MarketFeatureConfig = field(default_factory=MarketFeatureConfig)
    wallet_quality_config: WalletQualityConfig = field(default_factory=WalletQualityConfig)
    min_recent_trade_count: int = 2
    min_recent_volume_usdc: Decimal = Decimal("50")
    min_volume_zscore: Decimal = Decimal("1.5")
    min_trade_count_zscore: Decimal = Decimal("1.5")
    min_abs_order_flow_imbalance: Decimal = Decimal("0.45")
    min_abs_short_return: Decimal = Decimal("0.04")
    min_abs_medium_return: Decimal = Decimal("0.06")
    min_spread_change_bps: Decimal = Decimal("10")
    min_depth_change_ratio: Decimal = Decimal("0.20")
    min_trigger_count: int = 2
    cooldown_window: timedelta = timedelta(minutes=15)

    def __post_init__(self) -> None:
        if self.min_recent_trade_count <= 0:
            raise ValueError("min_recent_trade_count must be greater than zero.")
        if self.min_recent_volume_usdc < ZERO_DECIMAL:
            raise ValueError("min_recent_volume_usdc cannot be negative.")
        if self.min_trigger_count <= 0:
            raise ValueError("min_trigger_count must be greater than zero.")
        if self.cooldown_window < timedelta(0):
            raise ValueError("cooldown_window cannot be negative.")


@dataclass(frozen=True, slots=True)
class TriggerRule:
    rule: str
    metric: str
    threshold: Decimal
    actual_value: Decimal


@dataclass(frozen=True, slots=True)
class SignalEvent:
    event_id: str
    asset_id: str | None
    condition_id: str | None
    event_time_utc: datetime
    direction: str | None
    trigger_reason: str
    trigger_rules: tuple[TriggerRule, ...]
    market_features: MarketAnomalyFeatures
    wallet_summary: WalletSummaryFeatures
    explanation_payload: dict[str, Any]


def detect_signal_events(
    *,
    asset_id: str | None,
    condition_id: str | None = None,
    trades: Iterable[TradeRecord] = (),
    price_points: Iterable[PriceHistoryPoint] = (),
    order_book_snapshots: Iterable[object] = (),
    wallet_profiles: Iterable[WalletProfile] = (),
    candidate_times: Sequence[datetime] | None = None,
    config: EventDetectorConfig | None = None,
) -> tuple[SignalEvent, ...]:
    detector_config = config or EventDetectorConfig()
    normalized_trades = tuple(trades)
    normalized_price_points = tuple(price_points)
    normalized_snapshots = tuple(order_book_snapshots)
    normalized_profiles = tuple(wallet_profiles)

    evaluation_times = tuple(
        sorted(
            {
                _normalize_utc_timestamp(candidate_time)
                for candidate_time in (
                    candidate_times
                    or _default_candidate_times(normalized_trades, normalized_price_points)
                )
            }
        )
    )
    if not evaluation_times:
        return ()

    events: list[SignalEvent] = []
    last_event_time: datetime | None = None

    for event_time in evaluation_times:
        if last_event_time is not None and event_time - last_event_time < detector_config.cooldown_window:
            continue

        market_features = calculate_market_anomaly_features(
            event_time=event_time,
            trades=normalized_trades,
            price_points=normalized_price_points,
            order_book_snapshots=normalized_snapshots,
            asset_id=asset_id,
            condition_id=condition_id,
            config=detector_config.market_feature_config,
        )
        if market_features.recent_trade_count < detector_config.min_recent_trade_count:
            continue
        if market_features.recent_volume_usdc < detector_config.min_recent_volume_usdc:
            continue

        trigger_rules = _evaluate_trigger_rules(market_features, detector_config)
        if len(trigger_rules) < detector_config.min_trigger_count:
            continue

        wallet_trades = aggregate_wallet_activity(
            event_time=event_time,
            trades=normalized_trades,
            asset_id=asset_id,
            condition_id=condition_id,
            window=detector_config.wallet_quality_config.event_window,
        )
        wallet_summary = summarize_wallet_quality(
            event_time=event_time,
            wallet_trades=wallet_trades,
            wallet_profiles=normalized_profiles,
            config=detector_config.wallet_quality_config,
        )
        direction = _determine_direction(market_features)
        trigger_reason = ", ".join(rule.rule for rule in trigger_rules)
        explanation_payload = _build_explanation_payload(
            asset_id=asset_id,
            condition_id=condition_id,
            event_time=event_time,
            direction=direction,
            trigger_rules=trigger_rules,
            market_features=market_features,
            wallet_summary=wallet_summary,
        )
        event = SignalEvent(
            event_id=_build_signal_event_id(
                asset_id=asset_id,
                condition_id=condition_id,
                event_time=event_time,
                trigger_rules=trigger_rules,
            ),
            asset_id=asset_id,
            condition_id=condition_id,
            event_time_utc=event_time,
            direction=direction,
            trigger_reason=trigger_reason,
            trigger_rules=trigger_rules,
            market_features=market_features,
            wallet_summary=wallet_summary,
            explanation_payload=explanation_payload,
        )
        events.append(event)
        last_event_time = event_time

    return tuple(events)


def _default_candidate_times(
    trades: Iterable[TradeRecord],
    price_points: Iterable[PriceHistoryPoint],
) -> tuple[datetime, ...]:
    return tuple(
        timestamp
        for timestamp in (
            *_trade_timestamps(trades),
            *_price_point_timestamps(price_points),
        )
        if timestamp is not None
    )


def _trade_timestamps(trades: Iterable[TradeRecord]) -> tuple[datetime | None, ...]:
    return tuple(
        _normalize_utc_timestamp(trade.timestamp)
        if trade.timestamp is not None
        else None
        for trade in trades
    )


def _price_point_timestamps(points: Iterable[PriceHistoryPoint]) -> tuple[datetime | None, ...]:
    return tuple(
        _normalize_utc_timestamp(point.timestamp)
        if point.timestamp is not None
        else None
        for point in points
    )


def _evaluate_trigger_rules(
    features: MarketAnomalyFeatures,
    config: EventDetectorConfig,
) -> tuple[TriggerRule, ...]:
    rules: list[TriggerRule] = []
    if features.volume_zscore is not None and features.volume_zscore >= config.min_volume_zscore:
        rules.append(
            TriggerRule(
                rule="volume_spike",
                metric="volume_zscore",
                threshold=config.min_volume_zscore,
                actual_value=features.volume_zscore,
            )
        )
    if features.trade_count_zscore is not None and features.trade_count_zscore >= config.min_trade_count_zscore:
        rules.append(
            TriggerRule(
                rule="activity_spike",
                metric="trade_count_zscore",
                threshold=config.min_trade_count_zscore,
                actual_value=features.trade_count_zscore,
            )
        )
    if (
        features.order_flow_imbalance is not None
        and abs(features.order_flow_imbalance) >= config.min_abs_order_flow_imbalance
    ):
        rules.append(
            TriggerRule(
                rule="order_flow_imbalance",
                metric="order_flow_imbalance",
                threshold=config.min_abs_order_flow_imbalance,
                actual_value=abs(features.order_flow_imbalance),
            )
        )
    if features.short_return is not None and abs(features.short_return) >= config.min_abs_short_return:
        rules.append(
            TriggerRule(
                rule="short_return_move",
                metric="short_return",
                threshold=config.min_abs_short_return,
                actual_value=abs(features.short_return),
            )
        )
    if features.medium_return is not None and abs(features.medium_return) >= config.min_abs_medium_return:
        rules.append(
            TriggerRule(
                rule="medium_return_move",
                metric="medium_return",
                threshold=config.min_abs_medium_return,
                actual_value=abs(features.medium_return),
            )
        )
    if (
        features.liquidity_features_available
        and features.spread_change_bps is not None
        and features.spread_change_bps >= config.min_spread_change_bps
    ):
        rules.append(
            TriggerRule(
                rule="spread_widening",
                metric="spread_change_bps",
                threshold=config.min_spread_change_bps,
                actual_value=features.spread_change_bps,
            )
        )
    if (
        features.liquidity_features_available
        and features.depth_change_ratio is not None
        and features.depth_change_ratio <= -config.min_depth_change_ratio
    ):
        rules.append(
            TriggerRule(
                rule="depth_thinning",
                metric="depth_change_ratio",
                threshold=config.min_depth_change_ratio,
                actual_value=abs(features.depth_change_ratio),
            )
        )
    return tuple(rules)


def _determine_direction(features: MarketAnomalyFeatures) -> str | None:
    directional_score = ZERO_DECIMAL
    if features.short_return is not None:
        directional_score += features.short_return
    if features.medium_return is not None:
        directional_score += features.medium_return
    if features.order_flow_imbalance is not None:
        directional_score += features.order_flow_imbalance

    if directional_score > ZERO_DECIMAL:
        return "up"
    if directional_score < ZERO_DECIMAL:
        return "down"
    if directional_score == ZERO_DECIMAL and (
        features.short_return is not None
        or features.medium_return is not None
        or features.order_flow_imbalance is not None
    ):
        return "mixed"
    return None


def _build_explanation_payload(
    *,
    asset_id: str | None,
    condition_id: str | None,
    event_time: datetime,
    direction: str | None,
    trigger_rules: tuple[TriggerRule, ...],
    market_features: MarketAnomalyFeatures,
    wallet_summary: WalletSummaryFeatures,
) -> dict[str, Any]:
    return {
        "asset_id": asset_id,
        "condition_id": condition_id,
        "event_time_utc": event_time.isoformat(),
        "direction": direction,
        "trigger_reason": ", ".join(rule.rule for rule in trigger_rules),
        "trigger_rules": _serialize_value(trigger_rules),
        "market_context": _serialize_value(market_features),
        "wallet_context": _serialize_value(wallet_summary),
    }


def _build_signal_event_id(
    *,
    asset_id: str | None,
    condition_id: str | None,
    event_time: datetime,
    trigger_rules: tuple[TriggerRule, ...],
) -> str:
    payload = "|".join(
        (
            asset_id or "",
            condition_id or "",
            event_time.isoformat(),
            ",".join(rule.rule for rule in trigger_rules),
        )
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _serialize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if is_dataclass(value):
        return {
            key: _serialize_value(item)
            for key, item in asdict(value).items()
        }
    if isinstance(value, dict):
        return {key: _serialize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_value(item) for item in value]
    return value


def _normalize_utc_timestamp(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
