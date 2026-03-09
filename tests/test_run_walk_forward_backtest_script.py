from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path

from src.research.backtesting import (
    BacktestArtifactPaths,
    EquityCurvePoint,
    SliceSummary,
    StrategyBacktestSummary,
    StrategyTradeRecord,
    WalkForwardBacktestConfig,
    WalkForwardBacktestResult,
    WalkForwardBacktestRun,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "run_walk_forward_backtest.py"


def load_script_module():
    spec = importlib.util.spec_from_file_location("run_walk_forward_backtest", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_main_prints_summary_on_success(monkeypatch, capsys, tmp_path) -> None:
    module = load_script_module()
    artifact_paths = BacktestArtifactPaths(
        run_dir=tmp_path / "run",
        summary_path=tmp_path / "run" / "summary.json",
        trades_path=tmp_path / "run" / "trades.jsonl",
        equity_curve_path=tmp_path / "run" / "equity_curve.jsonl",
        report_path=tmp_path / "run" / "report.md",
    )
    artifact_paths.run_dir.mkdir(parents=True)
    artifact_paths.summary_path.write_text("{}")
    artifact_paths.trades_path.write_text("{}")
    artifact_paths.equity_curve_path.write_text("{}")
    artifact_paths.report_path.write_text("# report")

    run = WalkForwardBacktestRun(
        run_id="run-123",
        dataset_build_id="build-123",
        config=WalkForwardBacktestConfig(
            minimum_training_rows=3,
            max_open_positions=2,
            position_size_fraction=0.25,
        ),
        primary_label_name="profitable_after_costs",
        primary_label_horizon_minutes=15,
        paper_trading_decision="go",
        paper_trading_reason="classifier stayed profitable across the checked slices.",
        strategy_summaries={
            "market_only": StrategyBacktestSummary("market_only", 3, 10, 4, 0.4, 1, 0, 0.5, 10.0, 9.0, 40.0, 55.0, 4.0, 8.0, 10_400.0),
            "combined_rule": StrategyBacktestSummary("combined_rule", 3, 10, 2, 0.2, 0, 0, 1.0, 40.0, 40.0, 80.0, 90.0, 8.0, 4.0, 10_800.0),
            "classifier": StrategyBacktestSummary("classifier", 3, 10, 1, 0.1, 0, 0, 1.0, 60.0, 60.0, 60.0, 70.0, 6.0, 2.0, 10_600.0),
        },
        slice_summaries={
            "category": (
                SliceSummary("classifier", "category", "crypto", 1, 1.0, 60.0, 60.0),
            ),
            "liquidity_bucket": (
                SliceSummary("classifier", "liquidity_bucket", "high", 1, 1.0, 60.0, 60.0),
            ),
            "month": (
                SliceSummary("classifier", "month", "2026-03", 1, 1.0, 60.0, 60.0),
            ),
        },
        trade_records={
            "market_only": (
                StrategyTradeRecord(
                    strategy_name="market_only",
                    dataset_row_id="row-1",
                    event_id="event-1",
                    asset_id="111",
                    condition_id="0xcondition",
                    event_time_utc=datetime(2026, 3, 1, 10, 0, tzinfo=UTC),
                    entry_time_utc=datetime(2026, 3, 1, 10, 0, tzinfo=UTC),
                    exit_time_utc=datetime(2026, 3, 1, 10, 15, tzinfo=UTC),
                    direction="up",
                    trigger_reason="volume_spike",
                    signal_probability=None,
                    category="politics",
                    liquidity_bucket="low",
                    month_bucket="2026-03",
                    question="Will Trump win?",
                    slug="trump-election",
                    notional_usdc=2_500.0,
                    assumed_round_trip_cost_bps=40.0,
                    gross_return_bps=120.0,
                    net_return_bps=80.0,
                    gross_pnl_usdc=30.0,
                    net_pnl_usdc=20.0,
                    equity_before_usdc=10_000.0,
                    equity_after_usdc=10_020.0,
                ),
            ),
            "combined_rule": (),
            "classifier": (),
        },
        equity_curve={
            "market_only": (
                EquityCurvePoint("market_only", datetime(2026, 3, 1, 10, 0, tzinfo=UTC), "start", 10_000.0, 10_000.0, 0, 0),
                EquityCurvePoint("market_only", datetime(2026, 3, 1, 10, 15, tzinfo=UTC), "close", 10_020.0, 10_020.0, 0, 1),
            ),
            "combined_rule": (),
            "classifier": (),
        },
    )

    def fake_run_walk_forward_backtest(**kwargs):
        config = kwargs["config"]
        assert kwargs["warehouse_path"] == tmp_path / "warehouse.duckdb"
        assert kwargs["dataset_build_id"] == "build-123"
        assert kwargs["output_dir"] == tmp_path / "output"
        assert kwargs["run_id"] == "run-123"
        assert config.minimum_training_rows == 3
        assert config.max_open_positions == 2
        assert config.position_size_fraction == 0.25
        return WalkForwardBacktestResult(run=run, artifact_paths=artifact_paths)

    monkeypatch.setattr(module, "run_walk_forward_backtest", fake_run_walk_forward_backtest)

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
            "--minimum-training-rows",
            "3",
            "--max-open-positions",
            "2",
            "--position-size-fraction",
            "0.25",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Walk-forward backtest summary" in captured.out
    assert "Run id: run-123" in captured.out
    assert "Paper trading decision: go" in captured.out
    assert "Trade counts: market_only=4, combined_rule=2, classifier=1" in captured.out


def test_main_prints_failure_message(monkeypatch, capsys, tmp_path) -> None:
    module = load_script_module()

    def fake_run_walk_forward_backtest(**_kwargs):
        raise module.BacktestExperimentError("No dataset builds were found in event_dataset_rows.")

    monkeypatch.setattr(module, "run_walk_forward_backtest", fake_run_walk_forward_backtest)

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
    assert "Walk-forward backtest failed:" in captured.err
    assert "No dataset builds were found in event_dataset_rows." in captured.err
