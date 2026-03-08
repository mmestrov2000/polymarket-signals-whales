#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path
from typing import Sequence

from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.ingestion import (  # noqa: E402
    DEFAULT_MARKET_SCAN_LIMIT,
    DEFAULT_PRICE_FIDELITY,
    DEFAULT_PRICE_INTERVAL,
    DEFAULT_RAW_DATA_DIR,
    DEFAULT_SAMPLE_SIZE,
    DEFAULT_TRADE_LIMIT,
    SampleMarketBackfillSummary,
    SampleMarketSelectionRule,
    run_sample_market_backfill,
)
from src.storage import DEFAULT_WAREHOUSE_PATH  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill a deterministic sample cohort of Polymarket markets into raw storage and DuckDB."
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help=f"Number of markets to backfill after ranking. Defaults to {DEFAULT_SAMPLE_SIZE}.",
    )
    parser.add_argument(
        "--gamma-limit",
        type=int,
        default=DEFAULT_MARKET_SCAN_LIMIT,
        help=f"Number of open Gamma markets to inspect for the sample rule. Defaults to {DEFAULT_MARKET_SCAN_LIMIT}.",
    )
    parser.add_argument(
        "--price-interval",
        default=DEFAULT_PRICE_INTERVAL,
        help=f"CLOB price-history interval. Defaults to {DEFAULT_PRICE_INTERVAL}.",
    )
    parser.add_argument(
        "--price-fidelity",
        type=int,
        default=DEFAULT_PRICE_FIDELITY,
        help=f"CLOB price-history fidelity. Defaults to {DEFAULT_PRICE_FIDELITY}.",
    )
    parser.add_argument(
        "--trade-limit",
        type=int,
        default=DEFAULT_TRADE_LIMIT,
        help=f"Data API trade rows to request per market. Defaults to {DEFAULT_TRADE_LIMIT}.",
    )
    parser.add_argument(
        "--raw-dir",
        default=str(DEFAULT_RAW_DATA_DIR),
        help=f"Raw payload output directory. Defaults to {DEFAULT_RAW_DATA_DIR}.",
    )
    parser.add_argument(
        "--warehouse-path",
        default=str(DEFAULT_WAREHOUSE_PATH),
        help=f"DuckDB path for normalized tables. Defaults to {DEFAULT_WAREHOUSE_PATH}.",
    )
    return parser.parse_args(argv)


def format_summary(summary: SampleMarketBackfillSummary, *, raw_dir: Path, warehouse_path: Path) -> str:
    lines = [
        "Sample market backfill summary",
        f"Selection rule: {summary.selection_rule}",
        (
            f"Selected markets ({len(summary.selected_market_ids)}): "
            + (", ".join(summary.selected_market_ids) if summary.selected_market_ids else "none")
        ),
        (
            "Normalized rows written: "
            f"markets={summary.market_rows}, prices={summary.price_rows}, trades={summary.trade_rows}"
        ),
        f"Raw captures written: {summary.raw_capture_count} under {raw_dir}",
        f"Warehouse path: {warehouse_path}",
    ]

    if summary.market_results:
        lines.append(
            "Per-market results: "
            + "; ".join(
                (
                    f"{result.market_id} "
                    f"(condition_id={result.condition_id}, "
                    f"tokens={','.join(result.token_ids)}, "
                    f"prices={result.price_rows}, trades={result.trade_rows})"
                )
                for result in summary.market_results
            )
        )

    if summary.skipped_markets:
        reason_counts = Counter(skip.reason for skip in summary.skipped_markets)
        lines.append(
            "Skipped markets: "
            + ", ".join(f"{reason}={count}" for reason, count in sorted(reason_counts.items()))
        )

    return "\n".join(lines)


def format_failures(summary: SampleMarketBackfillSummary) -> str:
    if not summary.failures:
        return ""

    return "\n".join(
        [
            "Failures:",
            *[
                f"- market_id={failure.market_id} dataset={failure.dataset} detail={failure.detail}"
                for failure in summary.failures
            ],
        ]
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(REPO_ROOT / ".env")

    selection_rule = SampleMarketSelectionRule(
        sample_size=args.sample_size,
        gamma_limit=args.gamma_limit,
    )
    raw_dir = Path(args.raw_dir)
    warehouse_path = Path(args.warehouse_path)

    try:
        summary = run_sample_market_backfill(
            raw_data_dir=raw_dir,
            warehouse_path=warehouse_path,
            selection_rule=selection_rule,
            price_interval=args.price_interval,
            price_fidelity=args.price_fidelity,
            trade_limit=args.trade_limit,
        )
    except Exception as exc:
        print(f"Sample market backfill failed before completion: {exc}", file=sys.stderr)
        return 1

    print(format_summary(summary, raw_dir=raw_dir, warehouse_path=warehouse_path))
    if summary.has_failures:
        print(format_failures(summary), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
