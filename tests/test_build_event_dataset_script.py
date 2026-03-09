from __future__ import annotations

import importlib.util
from pathlib import Path

from src.research.event_dataset import (
    DatasetArtifactPaths,
    DatasetIntegrityError,
    DatasetQAReport,
    EventDatasetBuild,
    EventDatasetBuildConfig,
    EventDatasetBuildResult,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts/build_event_dataset.py"


def load_script_module():
    spec = importlib.util.spec_from_file_location("build_event_dataset", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_main_prints_summary_on_success(monkeypatch, capsys, tmp_path) -> None:
    module = load_script_module()
    artifact_paths = DatasetArtifactPaths(
        build_dir=tmp_path / "build",
        summary_path=tmp_path / "build" / "summary.json",
        qa_report_path=tmp_path / "build" / "qa_report.json",
    )
    artifact_paths.build_dir.mkdir(parents=True)
    artifact_paths.summary_path.write_text("{}")
    artifact_paths.qa_report_path.write_text("{}")
    build = EventDatasetBuild(
        build_id="build-123",
        config=EventDatasetBuildConfig(),
        rows=(),
        qa_report=DatasetQAReport(
            build_id="build-123",
            source_event_count=1,
            materialized_row_count=0,
            dropped_non_directional_count=0,
            dropped_missing_entry_price_count=0,
            dropped_missing_primary_label_count=0,
            split_counts={"train": 2, "validation": 1},
            duplicate_event_ids=(),
            null_fraction_by_column={},
            null_heavy_columns=(),
            impossible_timestamp_event_ids=(),
            wallet_leakage_event_ids=(),
            future_label_leakage_event_ids=(),
            errors=(),
            warnings=(),
        ),
        primary_label_name="profitable_after_costs",
        primary_label_horizon_minutes=15,
    )

    def fake_materialize_event_dataset(**kwargs):
        assert kwargs["warehouse_path"] == tmp_path / "warehouse.duckdb"
        assert kwargs["output_dir"] == tmp_path / "output"
        assert kwargs["build_id"] == "build-123"
        return EventDatasetBuildResult(
            build=build,
            artifact_paths=artifact_paths,
            rows_written=3,
        )

    monkeypatch.setattr(module, "materialize_event_dataset", fake_materialize_event_dataset)

    exit_code = module.main(
        [
            "--warehouse-path",
            str(tmp_path / "warehouse.duckdb"),
            "--output-dir",
            str(tmp_path / "output"),
            "--build-id",
            "build-123",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Event dataset build summary" in captured.out
    assert "Rows written: 3" in captured.out
    assert "Split counts: train=2, validation=1" in captured.out


def test_main_prints_qa_failures(monkeypatch, capsys, tmp_path) -> None:
    module = load_script_module()
    artifact_paths = DatasetArtifactPaths(
        build_dir=tmp_path / "build",
        summary_path=tmp_path / "build" / "summary.json",
        qa_report_path=tmp_path / "build" / "qa_report.json",
    )
    artifact_paths.build_dir.mkdir(parents=True)
    artifact_paths.qa_report_path.write_text("{}")
    report = DatasetQAReport(
        build_id="build-qa-fail",
        source_event_count=1,
        materialized_row_count=1,
        dropped_non_directional_count=0,
        dropped_missing_entry_price_count=0,
        dropped_missing_primary_label_count=0,
        split_counts={"train": 1},
        duplicate_event_ids=(),
        null_fraction_by_column={},
        null_heavy_columns=(),
        impossible_timestamp_event_ids=(),
        wallet_leakage_event_ids=("event-123",),
        future_label_leakage_event_ids=(),
        errors=("Wallet leakage detected for events: event-123",),
        warnings=(),
    )

    def fake_materialize_event_dataset(**kwargs):
        raise DatasetIntegrityError(report, artifact_paths)

    monkeypatch.setattr(module, "materialize_event_dataset", fake_materialize_event_dataset)

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
    assert "Dataset QA failed:" in captured.err
    assert "Wallet leakage detected for events: event-123" in captured.err
