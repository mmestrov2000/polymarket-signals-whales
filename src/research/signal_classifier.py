from __future__ import annotations

import json
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np

from src.storage import DEFAULT_WAREHOUSE_PATH


DEFAULT_SIGNAL_CLASSIFIER_OUTPUT_DIR = Path("data/research/signal_classifier_runs")
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
class StrategyMetrics:
    row_count: int
    trade_count: int
    coverage: float
    profitable_trade_share: float | None
    mean_net_pnl_bps: float | None
    median_net_pnl_bps: float | None
    total_net_pnl_bps: float
    mean_directional_return_bps: float | None


@dataclass(frozen=True, slots=True)
class ClassificationMetrics:
    row_count: int
    log_loss: float
    accuracy: float
    precision: float | None
    recall: float | None


@dataclass(frozen=True, slots=True)
class CoefficientWeight:
    feature_name: str
    coefficient: float


@dataclass(frozen=True, slots=True)
class SignalClassifierArtifactPaths:
    run_dir: Path
    summary_path: Path
    coefficients_path: Path
    validation_predictions_path: Path


@dataclass(frozen=True, slots=True)
class SignalClassifierRun:
    run_id: str
    dataset_build_id: str
    feature_names: tuple[str, ...]
    numeric_feature_names: tuple[str, ...]
    binary_feature_names: tuple[str, ...]
    preprocessing: dict[str, NumericFeaturePreprocessing]
    logistic_regression_config: LogisticRegressionConfig
    strategy_metrics: dict[str, dict[str, StrategyMetrics]]
    classifier_metrics: dict[str, ClassificationMetrics]
    intercept: float
    coefficients: tuple[CoefficientWeight, ...]


@dataclass(frozen=True, slots=True)
class SignalClassifierRunResult:
    run: SignalClassifierRun
    artifact_paths: SignalClassifierArtifactPaths


@dataclass(frozen=True, slots=True)
class _DatasetRow:
    dataset_row_id: str
    dataset_build_id: str
    dataset_split: str
    event_id: str
    event_time_utc: datetime
    direction: str
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
    assumed_round_trip_cost_bps: float
    primary_label_profitable: bool
    primary_net_pnl_bps: float
    primary_directional_return_bps: float


@dataclass(frozen=True, slots=True)
class _TrainedLogisticRegression:
    intercept: float
    weights: np.ndarray


class SignalClassifierExperimentError(RuntimeError):
    """Raised when a Milestone 6 experiment cannot be run reproducibly."""


def run_signal_classifier_experiments(
    *,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE_PATH,
    output_dir: str | Path = DEFAULT_SIGNAL_CLASSIFIER_OUTPUT_DIR,
    dataset_build_id: str | None = None,
    run_id: str | None = None,
    config: LogisticRegressionConfig | None = None,
) -> SignalClassifierRunResult:
    training_config = config or LogisticRegressionConfig()
    resolved_build_id = _resolve_dataset_build_id(warehouse_path, dataset_build_id=dataset_build_id)
    rows = _load_dataset_rows(warehouse_path, dataset_build_id=resolved_build_id)
    if not rows:
        raise SignalClassifierExperimentError(
            f"Dataset build '{resolved_build_id}' has no rows in event_dataset_rows."
        )

    train_rows = tuple(row for row in rows if row.dataset_split == "train")
    validation_rows = tuple(row for row in rows if row.dataset_split == "validation")
    if not train_rows:
        raise SignalClassifierExperimentError(
            f"Dataset build '{resolved_build_id}' is missing train rows."
        )
    if not validation_rows:
        raise SignalClassifierExperimentError(
            f"Dataset build '{resolved_build_id}' is missing validation rows."
        )

    (
        train_matrix,
        validation_matrix,
        preprocessing,
        feature_names,
    ) = _prepare_feature_matrices(train_rows=train_rows, validation_rows=validation_rows)
    train_labels = np.array(
        [1.0 if row.primary_label_profitable else 0.0 for row in train_rows],
        dtype=np.float64,
    )
    validation_labels = np.array(
        [1.0 if row.primary_label_profitable else 0.0 for row in validation_rows],
        dtype=np.float64,
    )

    model = _train_logistic_regression(
        features=train_matrix,
        labels=train_labels,
        config=training_config,
    )
    train_probabilities = _predict_probabilities(train_matrix, model=model)
    validation_probabilities = _predict_probabilities(validation_matrix, model=model)

    strategy_metrics = {
        "market_only": {
            "train": _evaluate_strategy(
                rows=train_rows,
                take_trade=np.ones(len(train_rows), dtype=bool),
            ),
            "validation": _evaluate_strategy(
                rows=validation_rows,
                take_trade=np.ones(len(validation_rows), dtype=bool),
            ),
        },
        "combined_rule": {
            "train": _evaluate_strategy(
                rows=train_rows,
                take_trade=np.array([_combined_rule_decision(row) for row in train_rows], dtype=bool),
            ),
            "validation": _evaluate_strategy(
                rows=validation_rows,
                take_trade=np.array([_combined_rule_decision(row) for row in validation_rows], dtype=bool),
            ),
        },
    }

    classifier_trade_train = train_probabilities >= training_config.classification_threshold
    classifier_trade_validation = validation_probabilities >= training_config.classification_threshold
    strategy_metrics["classifier"] = {
        "train": _evaluate_strategy(rows=train_rows, take_trade=classifier_trade_train),
        "validation": _evaluate_strategy(rows=validation_rows, take_trade=classifier_trade_validation),
    }
    classifier_metrics = {
        "train": _evaluate_classifier_predictions(
            labels=train_labels,
            probabilities=train_probabilities,
            threshold=training_config.classification_threshold,
        ),
        "validation": _evaluate_classifier_predictions(
            labels=validation_labels,
            probabilities=validation_probabilities,
            threshold=training_config.classification_threshold,
        ),
    }

    coefficient_weights = tuple(
        sorted(
            (
                CoefficientWeight(feature_name=feature_name, coefficient=float(coefficient))
                for feature_name, coefficient in zip(feature_names, model.weights, strict=True)
            ),
            key=lambda item: item.coefficient,
            reverse=True,
        )
    )
    resolved_run_id = run_id or _default_run_id()
    run = SignalClassifierRun(
        run_id=resolved_run_id,
        dataset_build_id=resolved_build_id,
        feature_names=feature_names,
        numeric_feature_names=NUMERIC_FEATURE_NAMES,
        binary_feature_names=BINARY_FEATURE_NAMES,
        preprocessing=preprocessing,
        logistic_regression_config=training_config,
        strategy_metrics=strategy_metrics,
        classifier_metrics=classifier_metrics,
        intercept=float(model.intercept),
        coefficients=coefficient_weights,
    )
    artifact_paths = write_signal_classifier_artifacts(
        run=run,
        output_dir=output_dir,
        validation_rows=validation_rows,
        validation_probabilities=validation_probabilities,
        threshold=training_config.classification_threshold,
    )
    return SignalClassifierRunResult(run=run, artifact_paths=artifact_paths)


def write_signal_classifier_artifacts(
    *,
    run: SignalClassifierRun,
    output_dir: str | Path = DEFAULT_SIGNAL_CLASSIFIER_OUTPUT_DIR,
    validation_rows: tuple[_DatasetRow, ...],
    validation_probabilities: np.ndarray,
    threshold: float,
) -> SignalClassifierArtifactPaths:
    run_dir = Path(output_dir) / run.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    summary_path = run_dir / "summary.json"
    coefficients_path = run_dir / "coefficients.json"
    validation_predictions_path = run_dir / "validation_predictions.jsonl"

    summary_payload = {
        "run_id": run.run_id,
        "dataset_build_id": run.dataset_build_id,
        "feature_names": list(run.feature_names),
        "numeric_feature_names": list(run.numeric_feature_names),
        "binary_feature_names": list(run.binary_feature_names),
        "logistic_regression_config": _serialize_value(asdict(run.logistic_regression_config)),
        "preprocessing": _serialize_value(run.preprocessing),
        "strategy_metrics": _serialize_value(run.strategy_metrics),
        "classifier_metrics": _serialize_value(run.classifier_metrics),
        "top_positive_coefficients": _serialize_value(list(run.coefficients[:5])),
        "top_negative_coefficients": _serialize_value(list(sorted(run.coefficients, key=lambda item: item.coefficient)[:5])),
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True))

    coefficients_payload = {
        "run_id": run.run_id,
        "dataset_build_id": run.dataset_build_id,
        "intercept": run.intercept,
        "feature_weights": _serialize_value(run.coefficients),
    }
    coefficients_path.write_text(json.dumps(coefficients_payload, indent=2, sort_keys=True))

    with validation_predictions_path.open("w", encoding="utf-8") as handle:
        for row, probability in zip(validation_rows, validation_probabilities, strict=True):
            payload = {
                "dataset_row_id": row.dataset_row_id,
                "event_id": row.event_id,
                "dataset_split": row.dataset_split,
                "event_time_utc": row.event_time_utc,
                "direction": row.direction,
                "actual_label_profitable": row.primary_label_profitable,
                "predicted_probability": float(probability),
                "predicted_trade": bool(probability >= threshold),
                "primary_net_pnl_bps": row.primary_net_pnl_bps,
                "primary_directional_return_bps": row.primary_directional_return_bps,
            }
            handle.write(json.dumps(_serialize_value(payload), sort_keys=True))
            handle.write("\n")

    return SignalClassifierArtifactPaths(
        run_dir=run_dir,
        summary_path=summary_path,
        coefficients_path=coefficients_path,
        validation_predictions_path=validation_predictions_path,
    )


def _resolve_dataset_build_id(warehouse_path: str | Path, *, dataset_build_id: str | None) -> str:
    build_ids = _load_dataset_build_ids(warehouse_path)
    if dataset_build_id is not None:
        if dataset_build_id not in build_ids:
            raise SignalClassifierExperimentError(
                f"Dataset build '{dataset_build_id}' was not found. Available builds: {', '.join(build_ids) or 'none'}."
            )
        return dataset_build_id

    if not build_ids:
        raise SignalClassifierExperimentError("No dataset builds were found in event_dataset_rows.")
    if len(build_ids) == 1:
        return build_ids[0]
    raise SignalClassifierExperimentError(
        "Multiple dataset builds were found in event_dataset_rows. "
        f"Pass dataset_build_id explicitly. Available builds: {', '.join(build_ids)}."
    )


def _load_dataset_build_ids(warehouse_path: str | Path) -> tuple[str, ...]:
    rows = _fetch_rows(
        warehouse_path,
        """
        SELECT DISTINCT dataset_build_id
        FROM event_dataset_rows
        ORDER BY dataset_build_id ASC
        """,
    )
    return tuple(str(row["dataset_build_id"]) for row in rows)


def _load_dataset_rows(warehouse_path: str | Path, *, dataset_build_id: str) -> tuple[_DatasetRow, ...]:
    query = """
        SELECT
            dataset_row_id,
            dataset_build_id,
            dataset_split,
            event_id,
            event_time_utc,
            direction,
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
            assumed_round_trip_cost_bps,
            primary_label_profitable,
            primary_net_pnl_bps,
            primary_directional_return_bps
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
        _DatasetRow(
            dataset_row_id=str(row["dataset_row_id"]),
            dataset_build_id=str(row["dataset_build_id"]),
            dataset_split=str(row["dataset_split"]),
            event_id=str(row["event_id"]),
            event_time_utc=_normalize_utc_timestamp(row["event_time_utc"]),
            direction=str(row["direction"]),
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
            assumed_round_trip_cost_bps=_coerce_float(row["assumed_round_trip_cost_bps"]),
            primary_label_profitable=bool(row["primary_label_profitable"]),
            primary_net_pnl_bps=_coerce_float(row["primary_net_pnl_bps"]),
            primary_directional_return_bps=_coerce_float(row["primary_directional_return_bps"]),
        )
        for row in raw_rows
    )


def _prepare_feature_matrices(
    *,
    train_rows: tuple[_DatasetRow, ...],
    validation_rows: tuple[_DatasetRow, ...],
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


def _numeric_feature_value(row: _DatasetRow, feature_name: str) -> float:
    value = getattr(row, feature_name)
    return float("nan") if value is None else float(value)


def _binary_feature_value(row: _DatasetRow, feature_name: str) -> float:
    if feature_name == "direction_is_up":
        return 1.0 if row.direction == "up" else 0.0
    value = getattr(row, feature_name)
    return 1.0 if value else 0.0


def _train_logistic_regression(
    *,
    features: np.ndarray,
    labels: np.ndarray,
    config: LogisticRegressionConfig,
) -> _TrainedLogisticRegression:
    weights = np.zeros(features.shape[1], dtype=np.float64)
    intercept = 0.0
    row_count = float(features.shape[0])

    for _ in range(config.max_iterations):
        linear_terms = features @ weights + intercept
        probabilities = _sigmoid(linear_terms)
        errors = probabilities - labels
        weights -= config.learning_rate * ((features.T @ errors) / row_count)
        intercept -= config.learning_rate * float(np.mean(errors))

    return _TrainedLogisticRegression(intercept=intercept, weights=weights)


def _predict_probabilities(features: np.ndarray, *, model: _TrainedLogisticRegression) -> np.ndarray:
    return _sigmoid(features @ model.weights + model.intercept)


def _evaluate_strategy(*, rows: tuple[_DatasetRow, ...], take_trade: np.ndarray) -> StrategyMetrics:
    if take_trade.shape[0] != len(rows):
        raise ValueError("take_trade must align with rows.")

    selected_rows = [row for row, should_trade in zip(rows, take_trade, strict=True) if should_trade]
    row_count = len(rows)
    trade_count = len(selected_rows)
    coverage = float(trade_count / row_count) if row_count else 0.0

    if not selected_rows:
        return StrategyMetrics(
            row_count=row_count,
            trade_count=0,
            coverage=coverage,
            profitable_trade_share=None,
            mean_net_pnl_bps=None,
            median_net_pnl_bps=None,
            total_net_pnl_bps=0.0,
            mean_directional_return_bps=None,
        )

    profitable = np.array(
        [1.0 if row.primary_label_profitable else 0.0 for row in selected_rows],
        dtype=np.float64,
    )
    net_pnl = np.array([row.primary_net_pnl_bps for row in selected_rows], dtype=np.float64)
    directional_return = np.array(
        [row.primary_directional_return_bps for row in selected_rows],
        dtype=np.float64,
    )
    return StrategyMetrics(
        row_count=row_count,
        trade_count=trade_count,
        coverage=coverage,
        profitable_trade_share=float(np.mean(profitable)),
        mean_net_pnl_bps=float(np.mean(net_pnl)),
        median_net_pnl_bps=float(np.median(net_pnl)),
        total_net_pnl_bps=float(np.sum(net_pnl)),
        mean_directional_return_bps=float(np.mean(directional_return)),
    )


def _evaluate_classifier_predictions(
    *,
    labels: np.ndarray,
    probabilities: np.ndarray,
    threshold: float,
) -> ClassificationMetrics:
    predictions = probabilities >= threshold
    truth = labels >= 0.5

    true_positive = int(np.sum(predictions & truth))
    false_positive = int(np.sum(predictions & ~truth))
    false_negative = int(np.sum(~predictions & truth))
    clipped_probabilities = np.clip(probabilities, 1e-9, 1 - 1e-9)
    log_loss = -np.mean(
        truth.astype(np.float64) * np.log(clipped_probabilities)
        + (1 - truth.astype(np.float64)) * np.log(1 - clipped_probabilities)
    )

    precision = (
        float(true_positive / (true_positive + false_positive))
        if true_positive + false_positive
        else None
    )
    recall = (
        float(true_positive / (true_positive + false_negative))
        if true_positive + false_negative
        else None
    )
    return ClassificationMetrics(
        row_count=int(labels.shape[0]),
        log_loss=float(log_loss),
        accuracy=float(np.mean(predictions == truth)),
        precision=precision,
        recall=recall,
    )


def _combined_rule_decision(row: _DatasetRow) -> bool:
    return (
        row.profiled_wallet_count > 0
        and not row.sparse_wallet_set
        and row.weighted_average_quality is not None
        and row.weighted_average_quality > 0
    )


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


def _sigmoid(values: np.ndarray) -> np.ndarray:
    result = np.empty_like(values, dtype=np.float64)
    positive_mask = values >= 0
    result[positive_mask] = 1.0 / (1.0 + np.exp(-values[positive_mask]))
    exp_values = np.exp(values[~positive_mask])
    result[~positive_mask] = exp_values / (1.0 + exp_values)
    return result


def _normalize_utc_timestamp(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _serialize_value(value: object) -> Any:
    if is_dataclass(value):
        return _serialize_value(asdict(value))
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _serialize_value(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_serialize_value(item) for item in value]
    return value


def _default_run_id() -> str:
    return datetime.now(UTC).strftime("signal-classifier-%Y%m%dT%H%M%SZ")
