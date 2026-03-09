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
    DEFAULT_EVENT_DATASET_OUTPUT_DIR,
    DatasetIntegrityError,
    materialize_event_dataset,
)
from src.storage import DEFAULT_WAREHOUSE_PATH  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize a leakage-checked event dataset from stored signal events and price history."
    )
    parser.add_argument(
        "--warehouse-path",
        default=str(DEFAULT_WAREHOUSE_PATH),
        help=f"DuckDB path for normalized inputs and dataset rows. Defaults to {DEFAULT_WAREHOUSE_PATH}.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_EVENT_DATASET_OUTPUT_DIR),
        help=f"Directory for dataset build metadata and QA reports. Defaults to {DEFAULT_EVENT_DATASET_OUTPUT_DIR}.",
    )
    parser.add_argument(
        "--build-id",
        default=None,
        help="Optional deterministic build identifier. Defaults to a UTC timestamp-based id.",
    )
    return parser.parse_args(argv)


def format_summary(*, build_id: str, rows_written: int, summary_path: Path, qa_report_path: Path, split_counts: dict[str, int]) -> str:
    split_summary = ", ".join(f"{name}={count}" for name, count in sorted(split_counts.items())) or "none"
    return "\n".join(
        [
            "Event dataset build summary",
            f"Build id: {build_id}",
            f"Rows written: {rows_written}",
            f"Split counts: {split_summary}",
            f"Summary path: {summary_path}",
            f"QA report path: {qa_report_path}",
        ]
    )


def format_failures(error: DatasetIntegrityError) -> str:
    return "\n".join(
        [
            "Dataset QA failed:",
            *[f"- {message}" for message in error.report.errors],
            f"QA report path: {error.artifact_paths.qa_report_path}",
        ]
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(REPO_ROOT / ".env")

    try:
        result = materialize_event_dataset(
            warehouse_path=Path(args.warehouse_path),
            output_dir=Path(args.output_dir),
            build_id=args.build_id,
        )
    except DatasetIntegrityError as error:
        print(format_failures(error), file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Event dataset build failed before completion: {exc}", file=sys.stderr)
        return 1

    print(
        format_summary(
            build_id=result.build.build_id,
            rows_written=result.rows_written,
            summary_path=result.artifact_paths.summary_path,
            qa_report_path=result.artifact_paths.qa_report_path,
            split_counts=result.build.qa_report.split_counts,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
