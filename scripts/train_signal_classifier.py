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
    DEFAULT_SIGNAL_CLASSIFIER_OUTPUT_DIR,
    SignalClassifierExperimentError,
    run_signal_classifier_experiments,
)
from src.storage import DEFAULT_WAREHOUSE_PATH  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Milestone 6 baseline and classifier experiments from one dataset build."
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
        default=str(DEFAULT_SIGNAL_CLASSIFIER_OUTPUT_DIR),
        help=(
            "Directory for signal-classifier run artifacts. "
            f"Defaults to {DEFAULT_SIGNAL_CLASSIFIER_OUTPUT_DIR}."
        ),
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional deterministic run identifier. Defaults to a UTC timestamp-based id.",
    )
    return parser.parse_args(argv)


def format_summary(
    *,
    run_id: str,
    dataset_build_id: str,
    validation_rows: int,
    market_only_trades: int,
    combined_rule_trades: int,
    classifier_trades: int,
    summary_path: Path,
    coefficients_path: Path,
    validation_predictions_path: Path,
) -> str:
    return "\n".join(
        [
            "Signal classifier run summary",
            f"Run id: {run_id}",
            f"Dataset build id: {dataset_build_id}",
            f"Validation rows: {validation_rows}",
            (
                "Validation trade counts: "
                f"market_only={market_only_trades}, "
                f"combined_rule={combined_rule_trades}, "
                f"classifier={classifier_trades}"
            ),
            f"Summary path: {summary_path}",
            f"Coefficients path: {coefficients_path}",
            f"Validation predictions path: {validation_predictions_path}",
        ]
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(REPO_ROOT / ".env")

    try:
        result = run_signal_classifier_experiments(
            warehouse_path=Path(args.warehouse_path),
            dataset_build_id=args.dataset_build_id,
            output_dir=Path(args.output_dir),
            run_id=args.run_id,
        )
    except SignalClassifierExperimentError as error:
        print(f"Signal classifier run failed: {error}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Signal classifier run failed before completion: {exc}", file=sys.stderr)
        return 1

    validation_market_only = result.run.strategy_metrics["market_only"]["validation"]
    validation_combined_rule = result.run.strategy_metrics["combined_rule"]["validation"]
    validation_classifier = result.run.strategy_metrics["classifier"]["validation"]
    print(
        format_summary(
            run_id=result.run.run_id,
            dataset_build_id=result.run.dataset_build_id,
            validation_rows=validation_market_only.row_count,
            market_only_trades=validation_market_only.trade_count,
            combined_rule_trades=validation_combined_rule.trade_count,
            classifier_trades=validation_classifier.trade_count,
            summary_path=result.artifact_paths.summary_path,
            coefficients_path=result.artifact_paths.coefficients_path,
            validation_predictions_path=result.artifact_paths.validation_predictions_path,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
