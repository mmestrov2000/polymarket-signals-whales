from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from src.research.modeling import (
    BINARY_FEATURE_NAMES,
    NUMERIC_FEATURE_NAMES,
    LogisticRegressionConfig,
    NumericFeaturePreprocessing,
    ResearchDatasetRow,
    combined_rule_decision,
    load_dataset_rows,
    predict_probabilities,
    prepare_feature_matrices,
    resolve_dataset_build_id,
    serialize_value,
    train_logistic_regression,
)
from src.storage import DEFAULT_WAREHOUSE_PATH


DEFAULT_SIGNAL_CLASSIFIER_OUTPUT_DIR = Path("data/research/signal_classifier_runs")


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
    resolved_build_id = resolve_dataset_build_id(
        warehouse_path,
        dataset_build_id=dataset_build_id,
        error_cls=SignalClassifierExperimentError,
    )
    rows = load_dataset_rows(warehouse_path, dataset_build_id=resolved_build_id)
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
    ) = prepare_feature_matrices(train_rows=train_rows, validation_rows=validation_rows)
    train_labels = np.array(
        [1.0 if row.primary_label_profitable else 0.0 for row in train_rows],
        dtype=np.float64,
    )
    validation_labels = np.array(
        [1.0 if row.primary_label_profitable else 0.0 for row in validation_rows],
        dtype=np.float64,
    )

    model = train_logistic_regression(
        features=train_matrix,
        labels=train_labels,
        config=training_config,
    )
    train_probabilities = predict_probabilities(train_matrix, model=model)
    validation_probabilities = predict_probabilities(validation_matrix, model=model)

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
                take_trade=np.array([combined_rule_decision(row) for row in train_rows], dtype=bool),
            ),
            "validation": _evaluate_strategy(
                rows=validation_rows,
                take_trade=np.array([combined_rule_decision(row) for row in validation_rows], dtype=bool),
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
    validation_rows: tuple[ResearchDatasetRow, ...],
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
        "logistic_regression_config": serialize_value(asdict(run.logistic_regression_config)),
        "preprocessing": serialize_value(run.preprocessing),
        "strategy_metrics": serialize_value(run.strategy_metrics),
        "classifier_metrics": serialize_value(run.classifier_metrics),
        "top_positive_coefficients": serialize_value(list(run.coefficients[:5])),
        "top_negative_coefficients": serialize_value(
            list(sorted(run.coefficients, key=lambda item: item.coefficient)[:5])
        ),
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True))

    coefficients_payload = {
        "run_id": run.run_id,
        "dataset_build_id": run.dataset_build_id,
        "intercept": run.intercept,
        "feature_weights": serialize_value(run.coefficients),
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
            handle.write(json.dumps(serialize_value(payload), sort_keys=True))
            handle.write("\n")

    return SignalClassifierArtifactPaths(
        run_dir=run_dir,
        summary_path=summary_path,
        coefficients_path=coefficients_path,
        validation_predictions_path=validation_predictions_path,
    )


def _evaluate_strategy(*, rows: tuple[ResearchDatasetRow, ...], take_trade: np.ndarray) -> StrategyMetrics:
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


def _default_run_id() -> str:
    return datetime.now(UTC).strftime("signal-classifier-%Y%m%dT%H%M%SZ")
