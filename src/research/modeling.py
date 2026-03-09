from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

import duckdb
import numpy as np

from src.storage import DEFAULT_WAREHOUSE_PATH


NUMERIC_FEATURE_NAMES = (
    "recent_trade_count",
    "recent_volume_usdc",
    "volume_zscore",
    "trade_count_zscore",
    "order_flow_imbalance",
    "short_return",
    "medium_return",
    "latest_price",
    "latest_mid_price",
    "latest_spread_bps",
    "spread_change_bps",
    "top_of_book_depth_usdc",
    "depth_change_ratio",
    "depth_imbalance",
    "active_wallet_count",
    "profiled_wallet_count",
    "profiled_volume_share",
    "top_wallet_share",
    "concentration_hhi",
    "weighted_average_quality",
    "weighted_average_realized_roi",
    "weighted_average_hit_rate",
    "weighted_average_realized_pnl",
    "assumed_round_trip_cost_bps",
)
BINARY_FEATURE_NAMES = (
    "direction_is_up",
    "liquidity_features_available",
    "sparse_wallet_set",
)

ExceptionT = TypeVar("ExceptionT", bound=Exception)


@dataclass(frozen=True, slots=True)
class LogisticRegressionConfig:
    learning_rate: float = 0.1
    max_iterations: int = 2_000
    classification_threshold: float = 0.5

    def __post_init__(self) -> None:
        if self.learning_rate <= 0:
            raise ValueError("learning_rate must be greater than zero.")
        if self.max_iterations <= 0:
            raise ValueError("max_iterations must be greater than zero.")
        if not (0 < self.classification_threshold < 1):
            raise ValueError("classification_threshold must be in the range (0, 1).")


@dataclass(frozen=True, slots=True)
class NumericFeaturePreprocessing:
    median: float
    mean: float
    std: float
    scale: float


@dataclass(frozen=True, slots=True)
class ResearchDatasetRow:
    dataset_row_id: str
    dataset_build_id: str
    dataset_split: str
    event_id: str
    asset_id: str | None
    condition_id: str | None
    event_time_utc: datetime
    source_event_collection_time_utc: datetime
    direction: str
    trigger_reason: str
    recent_trade_count: float
    recent_volume_usdc: float
    volume_zscore: float | None
    trade_count_zscore: float | None
    order_flow_imbalance: float | None
    short_return: float | None
    medium_return: float | None
    liquidity_features_available: bool
    latest_price: float | None
    latest_mid_price: float | None
    latest_spread_bps: float | None
    spread_change_bps: float | None
    top_of_book_depth_usdc: float | None
    depth_change_ratio: float | None
    depth_imbalance: float | None
    active_wallet_count: float
    profiled_wallet_count: float
    sparse_wallet_set: bool
    profiled_volume_share: float | None
    top_wallet_share: float | None
    concentration_hhi: float | None
    weighted_average_quality: float | None
    weighted_average_realized_roi: float | None
    weighted_average_hit_rate: float | None
    weighted_average_realized_pnl: float | None
    entry_price: float
    entry_price_time_utc: datetime
    assumed_round_trip_cost_bps: float
    primary_label_name: str
    primary_label_horizon_minutes: int
    primary_label_profitable: bool
    primary_net_pnl_bps: float
    primary_directional_return_bps: float
    primary_exit_price: float
    primary_exit_time_utc: datetime


@dataclass(frozen=True, slots=True)
class TrainedLogisticRegression:
    intercept: float
    weights: np.ndarray


def resolve_dataset_build_id(
    warehouse_path: str | Path = DEFAULT_WAREHOUSE_PATH,
    *,
    dataset_build_id: str | None,
    error_cls: type[ExceptionT],
) -> str:
    build_ids = load_dataset_build_ids(warehouse_path)
    if dataset_build_id is not None:
        if dataset_build_id not in build_ids:
            raise error_cls(
                f"Dataset build '{dataset_build_id}' was not found. "
                f"Available builds: {', '.join(build_ids) or 'none'}."
            )
        return dataset_build_id

    if not build_ids:
        raise error_cls("No dataset builds were found in event_dataset_rows.")
    if len(build_ids) == 1:
        return build_ids[0]
    raise error_cls(
        "Multiple dataset builds were found in event_dataset_rows. "
        f"Pass dataset_build_id explicitly. Available builds: {', '.join(build_ids)}."
    )


def load_dataset_build_ids(warehouse_path: str | Path = DEFAULT_WAREHOUSE_PATH) -> tuple[str, ...]:
    rows = _fetch_rows(
        warehouse_path,
        """
        SELECT DISTINCT dataset_build_id
        FROM event_dataset_rows
        ORDER BY dataset_build_id ASC
        """,
    )
    return tuple(str(row["dataset_build_id"]) for row in rows)


def load_dataset_rows(
    warehouse_path: str | Path = DEFAULT_WAREHOUSE_PATH,
    *,
    dataset_build_id: str,
) -> tuple[ResearchDatasetRow, ...]:
    query = """
        SELECT
            dataset_row_id,
            dataset_build_id,
            dataset_split,
            event_id,
            asset_id,
            condition_id,
            event_time_utc,
            source_event_collection_time_utc,
            direction,
            trigger_reason,
            recent_trade_count,
            recent_volume_usdc,
            volume_zscore,
            trade_count_zscore,
            order_flow_imbalance,
            short_return,
            medium_return,
            liquidity_features_available,
            latest_price,
            latest_mid_price,
            latest_spread_bps,
            spread_change_bps,
            top_of_book_depth_usdc,
            depth_change_ratio,
            depth_imbalance,
            active_wallet_count,
            profiled_wallet_count,
            sparse_wallet_set,
            profiled_volume_share,
            top_wallet_share,
            concentration_hhi,
            weighted_average_quality,
            weighted_average_realized_roi,
            weighted_average_hit_rate,
            weighted_average_realized_pnl,
            entry_price,
            entry_price_time_utc,
            assumed_round_trip_cost_bps,
            primary_label_name,
            primary_label_horizon_minutes,
            primary_label_profitable,
            primary_net_pnl_bps,
            primary_directional_return_bps,
            primary_exit_price,
            primary_exit_time_utc
        FROM event_dataset_rows
        WHERE dataset_build_id = ?
        ORDER BY event_time_utc ASC, dataset_row_id ASC
    """
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        cursor = connection.execute(query, [dataset_build_id])
        columns = [description[0] for description in cursor.description]
        raw_rows = [
            {column: value for column, value in zip(columns, values, strict=True)}
            for values in cursor.fetchall()
        ]

    return tuple(
        ResearchDatasetRow(
            dataset_row_id=str(row["dataset_row_id"]),
            dataset_build_id=str(row["dataset_build_id"]),
            dataset_split=str(row["dataset_split"]),
            event_id=str(row["event_id"]),
            asset_id=_coerce_optional_str(row.get("asset_id")),
            condition_id=_coerce_optional_str(row.get("condition_id")),
            event_time_utc=normalize_utc_timestamp(row["event_time_utc"]),
            source_event_collection_time_utc=normalize_utc_timestamp(row["source_event_collection_time_utc"]),
            direction=str(row["direction"]),
            trigger_reason=str(row["trigger_reason"]),
            recent_trade_count=_coerce_float(row["recent_trade_count"]),
            recent_volume_usdc=_coerce_float(row["recent_volume_usdc"]),
            volume_zscore=_coerce_optional_float(row.get("volume_zscore")),
            trade_count_zscore=_coerce_optional_float(row.get("trade_count_zscore")),
            order_flow_imbalance=_coerce_optional_float(row.get("order_flow_imbalance")),
            short_return=_coerce_optional_float(row.get("short_return")),
            medium_return=_coerce_optional_float(row.get("medium_return")),
            liquidity_features_available=bool(row["liquidity_features_available"]),
            latest_price=_coerce_optional_float(row.get("latest_price")),
            latest_mid_price=_coerce_optional_float(row.get("latest_mid_price")),
            latest_spread_bps=_coerce_optional_float(row.get("latest_spread_bps")),
            spread_change_bps=_coerce_optional_float(row.get("spread_change_bps")),
            top_of_book_depth_usdc=_coerce_optional_float(row.get("top_of_book_depth_usdc")),
            depth_change_ratio=_coerce_optional_float(row.get("depth_change_ratio")),
            depth_imbalance=_coerce_optional_float(row.get("depth_imbalance")),
            active_wallet_count=_coerce_float(row["active_wallet_count"]),
            profiled_wallet_count=_coerce_float(row["profiled_wallet_count"]),
            sparse_wallet_set=bool(row["sparse_wallet_set"]),
            profiled_volume_share=_coerce_optional_float(row.get("profiled_volume_share")),
            top_wallet_share=_coerce_optional_float(row.get("top_wallet_share")),
            concentration_hhi=_coerce_optional_float(row.get("concentration_hhi")),
            weighted_average_quality=_coerce_optional_float(row.get("weighted_average_quality")),
            weighted_average_realized_roi=_coerce_optional_float(row.get("weighted_average_realized_roi")),
            weighted_average_hit_rate=_coerce_optional_float(row.get("weighted_average_hit_rate")),
            weighted_average_realized_pnl=_coerce_optional_float(row.get("weighted_average_realized_pnl")),
            entry_price=_coerce_float(row["entry_price"]),
            entry_price_time_utc=normalize_utc_timestamp(row["entry_price_time_utc"]),
            assumed_round_trip_cost_bps=_coerce_float(row["assumed_round_trip_cost_bps"]),
            primary_label_name=str(row["primary_label_name"]),
            primary_label_horizon_minutes=int(row["primary_label_horizon_minutes"]),
            primary_label_profitable=bool(row["primary_label_profitable"]),
            primary_net_pnl_bps=_coerce_float(row["primary_net_pnl_bps"]),
            primary_directional_return_bps=_coerce_float(row["primary_directional_return_bps"]),
            primary_exit_price=_coerce_float(row["primary_exit_price"]),
            primary_exit_time_utc=normalize_utc_timestamp(row["primary_exit_time_utc"]),
        )
        for row in raw_rows
    )


def prepare_feature_matrices(
    *,
    train_rows: tuple[ResearchDatasetRow, ...],
    validation_rows: tuple[ResearchDatasetRow, ...],
) -> tuple[np.ndarray, np.ndarray, dict[str, NumericFeaturePreprocessing], tuple[str, ...]]:
    train_numeric = np.empty((len(train_rows), len(NUMERIC_FEATURE_NAMES)), dtype=np.float64)
    validation_numeric = np.empty((len(validation_rows), len(NUMERIC_FEATURE_NAMES)), dtype=np.float64)
    preprocessing: dict[str, NumericFeaturePreprocessing] = {}

    for index, feature_name in enumerate(NUMERIC_FEATURE_NAMES):
        raw_train = np.array(
            [_numeric_feature_value(row, feature_name) for row in train_rows],
            dtype=np.float64,
        )
        observed_train = raw_train[~np.isnan(raw_train)]
        median = float(np.median(observed_train)) if observed_train.size else 0.0
        imputed_train = np.where(np.isnan(raw_train), median, raw_train)
        mean = float(np.mean(imputed_train))
        std = float(np.std(imputed_train))
        scale = std if np.isfinite(std) and std > 0 else 1.0

        raw_validation = np.array(
            [_numeric_feature_value(row, feature_name) for row in validation_rows],
            dtype=np.float64,
        )
        imputed_validation = np.where(np.isnan(raw_validation), median, raw_validation)

        train_numeric[:, index] = (imputed_train - mean) / scale
        validation_numeric[:, index] = (imputed_validation - mean) / scale
        preprocessing[feature_name] = NumericFeaturePreprocessing(
            median=median,
            mean=mean,
            std=std,
            scale=scale,
        )

    train_binary = np.column_stack(
        [
            np.array([_binary_feature_value(row, feature_name) for row in train_rows], dtype=np.float64)
            for feature_name in BINARY_FEATURE_NAMES
        ]
    )
    validation_binary = np.column_stack(
        [
            np.array([_binary_feature_value(row, feature_name) for row in validation_rows], dtype=np.float64)
            for feature_name in BINARY_FEATURE_NAMES
        ]
    )

    train_matrix = np.concatenate((train_numeric, train_binary), axis=1)
    validation_matrix = np.concatenate((validation_numeric, validation_binary), axis=1)
    return (
        train_matrix,
        validation_matrix,
        preprocessing,
        NUMERIC_FEATURE_NAMES + BINARY_FEATURE_NAMES,
    )


def train_logistic_regression(
    *,
    features: np.ndarray,
    labels: np.ndarray,
    config: LogisticRegressionConfig,
) -> TrainedLogisticRegression:
    weights = np.zeros(features.shape[1], dtype=np.float64)
    intercept = 0.0
    row_count = float(features.shape[0])

    for _ in range(config.max_iterations):
        linear_terms = features @ weights + intercept
        probabilities = _sigmoid(linear_terms)
        errors = probabilities - labels
        weights -= config.learning_rate * ((features.T @ errors) / row_count)
        intercept -= config.learning_rate * float(np.mean(errors))

    return TrainedLogisticRegression(intercept=intercept, weights=weights)


def predict_probabilities(features: np.ndarray, *, model: TrainedLogisticRegression) -> np.ndarray:
    return _sigmoid(features @ model.weights + model.intercept)


def combined_rule_decision(row: ResearchDatasetRow) -> bool:
    return (
        row.profiled_wallet_count > 0
        and not row.sparse_wallet_set
        and row.weighted_average_quality is not None
        and row.weighted_average_quality > 0
    )


def normalize_utc_timestamp(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def serialize_value(value: object) -> Any:
    if is_dataclass(value):
        return serialize_value(asdict(value))
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): serialize_value(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [serialize_value(item) for item in value]
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def _fetch_rows(warehouse_path: str | Path, query: str) -> list[dict[str, Any]]:
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        cursor = connection.execute(query)
        columns = [description[0] for description in cursor.description]
        return [
            {column: value for column, value in zip(columns, values, strict=True)}
            for values in cursor.fetchall()
        ]


def _coerce_float(value: object) -> float:
    return float(value)


def _coerce_optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def _coerce_optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _numeric_feature_value(row: ResearchDatasetRow, feature_name: str) -> float:
    value = getattr(row, feature_name)
    return float("nan") if value is None else float(value)


def _binary_feature_value(row: ResearchDatasetRow, feature_name: str) -> float:
    if feature_name == "direction_is_up":
        return 1.0 if row.direction == "up" else 0.0
    value = getattr(row, feature_name)
    return 1.0 if value else 0.0


def _sigmoid(values: np.ndarray) -> np.ndarray:
    result = np.empty_like(values, dtype=np.float64)
    positive_mask = values >= 0
    result[positive_mask] = 1.0 / (1.0 + np.exp(-values[positive_mask]))
    exp_values = np.exp(values[~positive_mask])
    result[~positive_mask] = exp_values / (1.0 + exp_values)
    return result
