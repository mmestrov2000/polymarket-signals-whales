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
    DEFAULT_LEADERBOARD_LIMIT,
    DEFAULT_WALLET_ACTIVITY_LIMIT,
    DEFAULT_WALLET_CLOSED_POSITIONS_LIMIT,
    DEFAULT_WALLET_POSITIONS_LIMIT,
    WalletBackfillSummary,
    WalletUniverseSelectionRule,
    run_wallet_backfill,
)
from src.storage import DEFAULT_WAREHOUSE_PATH  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill a leaderboard-seeded wallet cohort into raw storage and DuckDB."
    )
    parser.add_argument(
        "--leaderboard-limit",
        type=int,
        default=DEFAULT_LEADERBOARD_LIMIT,
        help=f"Number of leaderboard rows to inspect for wallet seeds. Defaults to {DEFAULT_LEADERBOARD_LIMIT}.",
    )
    parser.add_argument(
        "--positions-limit",
        type=int,
        default=DEFAULT_WALLET_POSITIONS_LIMIT,
        help=f"Rows to request from /positions per wallet. Defaults to {DEFAULT_WALLET_POSITIONS_LIMIT}.",
    )
    parser.add_argument(
        "--closed-positions-limit",
        type=int,
        default=DEFAULT_WALLET_CLOSED_POSITIONS_LIMIT,
        help=(
            "Rows to request from /closed-positions per wallet. "
            f"Defaults to {DEFAULT_WALLET_CLOSED_POSITIONS_LIMIT}."
        ),
    )
    parser.add_argument(
        "--activity-limit",
        type=int,
        default=DEFAULT_WALLET_ACTIVITY_LIMIT,
        help=f"Rows to request from /activity per wallet. Defaults to {DEFAULT_WALLET_ACTIVITY_LIMIT}.",
    )
    parser.add_argument(
        "--raw-dir",
        default="data/raw",
        help="Raw payload output directory. Defaults to data/raw.",
    )
    parser.add_argument(
        "--warehouse-path",
        default=str(DEFAULT_WAREHOUSE_PATH),
        help=f"DuckDB path for normalized tables. Defaults to {DEFAULT_WAREHOUSE_PATH}.",
    )
    return parser.parse_args(argv)


def format_summary(summary: WalletBackfillSummary, *, raw_dir: Path, warehouse_path: Path) -> str:
    lines = [
        "Wallet universe backfill summary",
        f"Selection rule: {summary.selection_rule}",
        (
            f"Selected wallets ({len(summary.seed_wallet_addresses)}): "
            + (", ".join(summary.seed_wallet_addresses) if summary.seed_wallet_addresses else "none")
        ),
        (
            "Normalized rows written: "
            f"wallet_positions={summary.position_rows}, "
            f"wallet_closed_positions={summary.closed_position_rows}, "
            f"activity_trades={summary.activity_trade_rows}, "
            f"wallet_profiles={summary.wallet_profile_rows}"
        ),
        f"Raw captures written: {summary.raw_capture_count} under {raw_dir}",
        f"Warehouse path: {warehouse_path}",
    ]

    if summary.wallet_results:
        lines.append(
            "Per-wallet results: "
            + "; ".join(
                (
                    f"{result.wallet_address} "
                    f"(positions={result.position_rows}, "
                    f"closed_positions={result.closed_position_rows}, "
                    f"activity_trades={result.activity_trade_rows}, "
                    f"profiles={result.profile_rows})"
                )
                for result in summary.wallet_results
            )
        )

    if summary.skipped_wallets:
        reason_counts = Counter(skip.reason for skip in summary.skipped_wallets)
        lines.append(
            "Skipped seeds: " + ", ".join(f"{reason}={count}" for reason, count in sorted(reason_counts.items()))
        )

    return "\n".join(lines)


def format_failures(summary: WalletBackfillSummary) -> str:
    if not summary.failures:
        return ""

    return "\n".join(
        [
            "Failures:",
            *[
                f"- wallet_address={failure.wallet_address} dataset={failure.dataset} detail={failure.detail}"
                for failure in summary.failures
            ],
        ]
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(REPO_ROOT / ".env")

    selection_rule = WalletUniverseSelectionRule(leaderboard_limit=args.leaderboard_limit)
    raw_dir = Path(args.raw_dir)
    warehouse_path = Path(args.warehouse_path)

    try:
        summary = run_wallet_backfill(
            raw_data_dir=raw_dir,
            warehouse_path=warehouse_path,
            selection_rule=selection_rule,
            positions_limit=args.positions_limit,
            closed_positions_limit=args.closed_positions_limit,
            activity_limit=args.activity_limit,
        )
    except Exception as exc:
        print(f"Wallet universe backfill failed before completion: {exc}", file=sys.stderr)
        return 1

    print(format_summary(summary, raw_dir=raw_dir, warehouse_path=warehouse_path))
    if summary.has_failures:
        print(format_failures(summary), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
