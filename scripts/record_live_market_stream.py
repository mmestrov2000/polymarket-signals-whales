#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Sequence

from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.clients.polymarket_websocket import DEFAULT_WS_BASE_URL  # noqa: E402
from src.ingestion import (  # noqa: E402
    DEFAULT_LIVE_RECORDER_MESSAGE_TIMEOUT_SECONDS,
    DEFAULT_LIVE_RECORDER_OPEN_TIMEOUT_SECONDS,
    DEFAULT_LIVE_RECORDER_PING_INTERVAL_SECONDS,
    DEFAULT_LIVE_RECORDER_PING_TIMEOUT_SECONDS,
    DEFAULT_LIVE_RECORDER_RECONNECT_ATTEMPTS,
    DEFAULT_LIVE_RECORDER_SESSION_SECONDS,
    DEFAULT_RAW_DATA_DIR,
    LiveMarketRecorderSummary,
    run_live_market_recorder,
)
from src.storage import DEFAULT_WAREHOUSE_PATH  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Record live Polymarket market-channel data into raw websocket captures and "
            "normalized DuckDB top-of-book snapshots."
        )
    )
    parser.add_argument(
        "--asset-id",
        dest="asset_ids",
        action="append",
        required=True,
        help="CLOB asset/token id to subscribe to. Repeat for multiple assets.",
    )
    parser.add_argument(
        "--session-seconds",
        type=int,
        default=DEFAULT_LIVE_RECORDER_SESSION_SECONDS,
        help=(
            "Maximum recorder session length in seconds. "
            f"Defaults to {DEFAULT_LIVE_RECORDER_SESSION_SECONDS}."
        ),
    )
    parser.add_argument(
        "--message-limit",
        type=int,
        default=None,
        help="Optional cap on received websocket messages before stopping.",
    )
    parser.add_argument(
        "--reconnect-attempts",
        type=int,
        default=DEFAULT_LIVE_RECORDER_RECONNECT_ATTEMPTS,
        help=(
            "Reconnect attempts after dropped streams or receive timeouts. "
            f"Defaults to {DEFAULT_LIVE_RECORDER_RECONNECT_ATTEMPTS}."
        ),
    )
    parser.add_argument(
        "--open-timeout-seconds",
        type=float,
        default=DEFAULT_LIVE_RECORDER_OPEN_TIMEOUT_SECONDS,
        help=f"WebSocket open timeout. Defaults to {DEFAULT_LIVE_RECORDER_OPEN_TIMEOUT_SECONDS}.",
    )
    parser.add_argument(
        "--message-timeout-seconds",
        type=float,
        default=DEFAULT_LIVE_RECORDER_MESSAGE_TIMEOUT_SECONDS,
        help=(
            "Idle receive timeout before warning and reconnect. "
            f"Defaults to {DEFAULT_LIVE_RECORDER_MESSAGE_TIMEOUT_SECONDS}."
        ),
    )
    parser.add_argument(
        "--ping-interval-seconds",
        type=float,
        default=DEFAULT_LIVE_RECORDER_PING_INTERVAL_SECONDS,
        help=f"WebSocket ping interval. Defaults to {DEFAULT_LIVE_RECORDER_PING_INTERVAL_SECONDS}.",
    )
    parser.add_argument(
        "--ping-timeout-seconds",
        type=float,
        default=DEFAULT_LIVE_RECORDER_PING_TIMEOUT_SECONDS,
        help=f"WebSocket ping timeout. Defaults to {DEFAULT_LIVE_RECORDER_PING_TIMEOUT_SECONDS}.",
    )
    parser.add_argument(
        "--ws-base-url",
        default=DEFAULT_WS_BASE_URL,
        help=f"Polymarket websocket base URL. Defaults to {DEFAULT_WS_BASE_URL}.",
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
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging level for recorder progress and warnings.",
    )
    return parser.parse_args(argv)


def format_summary(
    summary: LiveMarketRecorderSummary,
    *,
    raw_dir: Path,
    warehouse_path: Path,
) -> str:
    lines = [
        "Live market recorder summary",
        f"Assets: {', '.join(summary.asset_ids)}",
        f"Session window: {summary.started_at.isoformat()} -> {summary.ended_at.isoformat()}",
        (
            "Captured: "
            f"messages={summary.messages_received}, raw_captures={summary.raw_capture_count}, "
            f"order_book_rows={summary.order_book_rows}, trade_rows={summary.trade_rows}"
        ),
        f"Reconnects used: {summary.reconnect_count}",
        f"Raw directory: {raw_dir}",
        f"Warehouse path: {warehouse_path}",
    ]

    if summary.warnings:
        warning_counts = Counter(warning.kind for warning in summary.warnings)
        lines.append(
            "Warnings: " + ", ".join(
                f"{kind}={count}" for kind, count in sorted(warning_counts.items())
            )
        )

    return "\n".join(lines)


def format_warnings(summary: LiveMarketRecorderSummary) -> str:
    if not summary.warnings:
        return ""

    return "\n".join(
        [
            "Recorder warnings:",
            *[
                (
                    f"- kind={warning.kind} attempt={warning.attempt} "
                    f"occurred_at={warning.occurred_at.isoformat()} detail={warning.detail}"
                )
                for warning in summary.warnings
            ],
        ]
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(REPO_ROOT / ".env")
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s %(name)s: %(message)s",
    )

    raw_dir = Path(args.raw_dir)
    warehouse_path = Path(args.warehouse_path)

    try:
        summary = asyncio.run(
            run_live_market_recorder(
                asset_ids=args.asset_ids,
                raw_data_dir=raw_dir,
                warehouse_path=warehouse_path,
                ws_base_url=args.ws_base_url,
                session_seconds=args.session_seconds,
                max_messages=args.message_limit,
                reconnect_attempts=args.reconnect_attempts,
                open_timeout_seconds=args.open_timeout_seconds,
                message_timeout_seconds=args.message_timeout_seconds,
                ping_interval_seconds=args.ping_interval_seconds,
                ping_timeout_seconds=args.ping_timeout_seconds,
                logger=logging.getLogger("live_market_recorder"),
            )
        )
    except Exception as exc:
        print(f"Live market recorder failed before completion: {exc}", file=sys.stderr)
        return 1

    print(format_summary(summary, raw_dir=raw_dir, warehouse_path=warehouse_path))
    if summary.has_warnings:
        print(format_warnings(summary), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
