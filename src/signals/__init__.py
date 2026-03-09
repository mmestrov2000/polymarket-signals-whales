"""Wallet and market signal feature helpers."""

from src.signals.event_detector import EventDetectorConfig, SignalEvent, TriggerRule, detect_signal_events
from src.signals.market_anomalies import MarketAnomalyFeatures, MarketFeatureConfig, calculate_market_anomaly_features
from src.signals.wallet_features import (
    WalletParticipantFeatures,
    WalletQualityConfig,
    WalletSummaryFeatures,
    WalletTradeAggregate,
    aggregate_wallet_activity,
    calculate_wallet_quality_score,
    summarize_wallet_quality,
)
from src.signals.wallet_profiles import WalletProfile, build_wallet_profile, estimate_trade_usdc_size

__all__ = [
    "EventDetectorConfig",
    "MarketAnomalyFeatures",
    "MarketFeatureConfig",
    "SignalEvent",
    "TriggerRule",
    "WalletParticipantFeatures",
    "WalletProfile",
    "WalletQualityConfig",
    "WalletSummaryFeatures",
    "WalletTradeAggregate",
    "aggregate_wallet_activity",
    "build_wallet_profile",
    "calculate_market_anomaly_features",
    "calculate_wallet_quality_score",
    "detect_signal_events",
    "estimate_trade_usdc_size",
    "summarize_wallet_quality",
]
