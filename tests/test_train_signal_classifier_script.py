from __future__ import annotations

import importlib.util
from pathlib import Path

from src.research.signal_classifier import (
    ClassificationMetrics,
    CoefficientWeight,
    LogisticRegressionConfig,
    NumericFeaturePreprocessing,
    SignalClassifierArtifactPaths,
    SignalClassifierExperimentError,
    SignalClassifierRun,
    SignalClassifierRunResult,
    StrategyMetrics,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "train_signal_classifier.py"


def load_script_module():
    spec = importlib.util.spec_from_file_location("train_signal_classifier", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_main_prints_summary_on_success(monkeypatch, capsys, tmp_path) -> None:
    module = load_script_module()
    artifact_paths = SignalClassifierArtifactPaths(
        run_dir=tmp_path / "run",
        summary_path=tmp_path / "run" / "summary.json",
        coefficients_path=tmp_path / "run" / "coefficients.json",
        validation_predictions_path=tmp_path / "run" / "validation_predictions.jsonl",
    )
    artifact_paths.run_dir.mkdir(parents=True)
    artifact_paths.summary_path.write_text("{}")
    artifact_paths.coefficients_path.write_text("{}")
    artifact_paths.validation_predictions_path.write_text("{}")

    run = SignalClassifierRun(
        run_id="run-123",
        dataset_build_id="build-123",
        feature_names=("weighted_average_quality",),
        numeric_feature_names=("weighted_average_quality",),
        binary_feature_names=("direction_is_up",),
        preprocessing={
            "weighted_average_quality": NumericFeaturePreprocessing(
                median=0.0,
                mean=0.0,
                std=1.0,
                scale=1.0,
            )
        },
        logistic_regression_config=LogisticRegressionConfig(),
        strategy_metrics={
            "market_only": {
                "validation": StrategyMetrics(4, 4, 1.0, 0.5, 10.0, 12.0, 40.0, 20.0),
                "train": StrategyMetrics(6, 6, 1.0, 0.5, 10.0, 12.0, 60.0, 20.0),
            },
            "combined_rule": {
                "validation": StrategyMetrics(4, 1, 0.25, 1.0, 80.0, 80.0, 80.0, 120.0),
                "train": StrategyMetrics(6, 3, 0.5, 1.0, 90.0, 90.0, 270.0, 130.0),
            },
            "classifier": {
                "validation": StrategyMetrics(4, 2, 0.5, 0.5, 15.0, 15.0, 30.0, 25.0),
                "train": StrategyMetrics(6, 3, 0.5, 1.0, 90.0, 90.0, 270.0, 130.0),
            },
        },
        classifier_metrics={
            "train": ClassificationMetrics(6, 0.2, 1.0, 1.0, 1.0),
            "validation": ClassificationMetrics(4, 0.4, 0.75, 0.5, 1.0),
        },
        intercept=0.0,
        coefficients=(CoefficientWeight("weighted_average_quality", 1.5),),
    )

    def fake_run_signal_classifier_experiments(**kwargs):
        assert kwargs["warehouse_path"] == tmp_path / "warehouse.duckdb"
        assert kwargs["dataset_build_id"] == "build-123"
        assert kwargs["output_dir"] == tmp_path / "output"
        assert kwargs["run_id"] == "run-123"
        return SignalClassifierRunResult(run=run, artifact_paths=artifact_paths)

    monkeypatch.setattr(module, "run_signal_classifier_experiments", fake_run_signal_classifier_experiments)

    exit_code = module.main(
        [
            "--warehouse-path",
            str(tmp_path / "warehouse.duckdb"),
            "--dataset-build-id",
            "build-123",
            "--output-dir",
            str(tmp_path / "output"),
            "--run-id",
            "run-123",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Signal classifier run summary" in captured.out
    assert "Run id: run-123" in captured.out
    assert "Validation trade counts: market_only=4, combined_rule=1, classifier=2" in captured.out


def test_main_prints_failure_message(monkeypatch, capsys, tmp_path) -> None:
    module = load_script_module()

    def fake_run_signal_classifier_experiments(**_kwargs):
        raise SignalClassifierExperimentError("No dataset builds were found in event_dataset_rows.")

    monkeypatch.setattr(module, "run_signal_classifier_experiments", fake_run_signal_classifier_experiments)

    exit_code = module.main(
        [
            "--warehouse-path",
            str(tmp_path / "warehouse.duckdb"),
            "--output-dir",
            str(tmp_path / "output"),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Signal classifier run failed:" in captured.err
    assert "No dataset builds were found in event_dataset_rows." in captured.err
