#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.research import (  # noqa: E402
    DEFAULT_BACKTEST_OUTPUT_DIR,
    BacktestExperimentError,
    WalkForwardBacktestConfig,
    run_walk_forward_backtest,
)
from src.storage import DEFAULT_WAREHOUSE_PATH  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Milestone 7 walk-forward backtest and write comparison artifacts."
    )
    parser.add_argument(
        "--warehouse-path",
        default=str(DEFAULT_WAREHOUSE_PATH),
        help=f"DuckDB path for normalized inputs. Defaults to {DEFAULT_WAREHOUSE_PATH}.",
    )
    parser.add_argument(
        "--dataset-build-id",
        default=None,
        help="Optional dataset_build_id to evaluate. Required when multiple builds exist.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_BACKTEST_OUTPUT_DIR),
        help=f"Directory for Milestone 7 artifacts. Defaults to {DEFAULT_BACKTEST_OUTPUT_DIR}.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional deterministic run identifier. Defaults to a UTC timestamp-based id.",
    )
    parser.add_argument(
        "--minimum-training-rows",
        type=int,
        default=WalkForwardBacktestConfig().minimum_training_rows,
        help="Shared burn-in row count before walk-forward execution begins.",
    )
    parser.add_argument(
        "--max-open-positions",
        type=int,
        default=WalkForwardBacktestConfig().max_open_positions,
        help="Maximum number of concurrent open positions per strategy.",
    )
    parser.add_argument(
        "--position-size-fraction",
        type=float,
        default=WalkForwardBacktestConfig().position_size_fraction,
        help="Fixed fraction of initial capital to allocate per trade.",
    )
    return parser.parse_args(argv)


def format_summary(
    *,
    run_id: str,
    dataset_build_id: str,
    paper_trading_decision: str,
    strategy_trade_counts: dict[str, int],
    summary_path: Path,
    trades_path: Path,
    equity_curve_path: Path,
    report_path: Path,
) -> str:
    trade_count_summary = ", ".join(
        f"{strategy}={count}" for strategy, count in strategy_trade_counts.items()
    )
    return "\n".join(
        [
            "Walk-forward backtest summary",
            f"Run id: {run_id}",
            f"Dataset build id: {dataset_build_id}",
            f"Paper trading decision: {paper_trading_decision}",
            f"Trade counts: {trade_count_summary}",
            f"Summary path: {summary_path}",
            f"Trades path: {trades_path}",
            f"Equity curve path: {equity_curve_path}",
            f"Report path: {report_path}",
        ]
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(REPO_ROOT / ".env")

    config = WalkForwardBacktestConfig(
        minimum_training_rows=args.minimum_training_rows,
        max_open_positions=args.max_open_positions,
        position_size_fraction=args.position_size_fraction,
    )
    try:
        result = run_walk_forward_backtest(
            warehouse_path=Path(args.warehouse_path),
            dataset_build_id=args.dataset_build_id,
            output_dir=Path(args.output_dir),
            run_id=args.run_id,
            config=config,
        )
    except BacktestExperimentError as error:
        print(f"Walk-forward backtest failed: {error}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Walk-forward backtest failed before completion: {exc}", file=sys.stderr)
        return 1

    print(
        format_summary(
            run_id=result.run.run_id,
            dataset_build_id=result.run.dataset_build_id,
            paper_trading_decision=result.run.paper_trading_decision,
            strategy_trade_counts={
                strategy: result.run.strategy_summaries[strategy].trade_count
                for strategy in result.run.strategy_summaries
            },
            summary_path=result.artifact_paths.summary_path,
            trades_path=result.artifact_paths.trades_path,
            equity_curve_path=result.artifact_paths.equity_curve_path,
            report_path=result.artifact_paths.report_path,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
