from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import httpx

from src.clients import DataApiClient, RequestConfig
from src.ingestion import WalletBackfillJob, WalletUniverseSelectionRule
from src.research import (
    get_table_counts,
    latest_raw_capture_path,
    list_latest_wallet_dataset_captures,
    list_wallet_activity_trades,
    list_wallet_closed_position_points,
    list_wallet_cohort_profiles,
    list_wallet_open_positions,
    list_wallet_seed_metadata,
)
from src.storage import PolymarketWarehouse, RawPayloadStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "public_clients"


def load_fixture(name: str) -> object:
    return json.loads((FIXTURE_DIR / name).read_text())


def build_wallet_run(tmp_path: Path) -> tuple[Path, Path]:
    fixture = load_fixture("data_api_wallet_backfill.json")
    collection_time = datetime(2026, 3, 8, 15, 30, tzinfo=UTC)
    raw_dir = tmp_path / "raw"
    warehouse_path = tmp_path / "warehouse" / "polymarket.duckdb"

    def data_api_handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/leaderboard":
            return httpx.Response(200, json=fixture["leaderboard"])
        if path == "/positions":
            return httpx.Response(200, json=fixture["positions"][request.url.params["user"]])
        if path == "/closed-positions":
            return httpx.Response(200, json=fixture["closed_positions"][request.url.params["user"]])
        if path == "/activity":
            return httpx.Response(200, json=fixture["activity"][request.url.params["user"]])
        raise AssertionError(f"Unexpected path {path}")

    with (
        DataApiClient(
            transport=httpx.MockTransport(data_api_handler),
            request_config=RequestConfig(timeout_seconds=2.5, max_attempts=1, retry_backoff_seconds=0.0),
        ) as data_api_client,
        PolymarketWarehouse(warehouse_path) as warehouse,
    ):
        job = WalletBackfillJob(
            data_api_client=data_api_client,
            raw_store=RawPayloadStore(raw_dir),
            warehouse=warehouse,
            selection_rule=WalletUniverseSelectionRule(leaderboard_limit=4),
            positions_limit=10,
            closed_positions_limit=10,
            activity_limit=10,
            collection_time=collection_time,
            as_of_time=collection_time,
        )
        summary = job.run()

    assert summary.failures == ()
    return raw_dir, warehouse_path


def test_wallet_cohort_profiles_join_seed_metadata_and_keep_empty_wallets(tmp_path: Path) -> None:
    raw_dir, warehouse_path = build_wallet_run(tmp_path)

    seed_metadata = list_wallet_seed_metadata(raw_dir)
    cohort_rows = list_wallet_cohort_profiles(warehouse_path, raw_dir)

    assert [row.wallet_address for row in seed_metadata] == ["0xwallet1", "0xwallet2"]
    assert [row.wallet_address for row in cohort_rows] == ["0xwallet1", "0xwallet2"]

    wallet_1 = cohort_rows[0]
    assert wallet_1.rank == 1
    assert wallet_1.user_name == "whale_1"
    assert wallet_1.verified_badge is True
    assert wallet_1.leaderboard_pnl == Decimal("2500.75")
    assert wallet_1.leaderboard_volume == Decimal("100000.00")
    assert wallet_1.realized_pnl == Decimal("125.5")
    assert wallet_1.activity_trade_count == 1
    assert wallet_1.closed_position_count == 1

    wallet_2 = cohort_rows[1]
    assert wallet_2.rank == 4
    assert wallet_2.user_name == "whale_2"
    assert wallet_2.verified_badge is False
    assert wallet_2.closed_position_count == 0
    assert wallet_2.activity_trade_count == 0
    assert wallet_2.realized_roi is None
    assert wallet_2.hit_rate is None


def test_wallet_chart_queries_return_stable_ordering_and_counts(tmp_path: Path) -> None:
    raw_dir, warehouse_path = build_wallet_run(tmp_path)

    table_counts = get_table_counts(warehouse_path)
    assert table_counts == {
        "wallet_profiles": 2,
        "wallet_positions": 1,
        "wallet_closed_positions": 1,
        "trades": 1,
    }

    leaderboard_capture = latest_raw_capture_path(raw_dir, "data_api", "wallet_universe_leaderboard")
    assert leaderboard_capture is not None

    position_captures = list_latest_wallet_dataset_captures(raw_dir, "wallet_positions")
    assert [(capture.wallet_address, capture.record_count) for capture in position_captures] == [
        ("0xwallet1", 1),
        ("0xwallet2", 0),
    ]

    activity_rows = list_wallet_activity_trades(warehouse_path, "0xwallet1")
    assert len(activity_rows) == 1
    assert activity_rows[0].trade_time_utc == datetime(2026, 3, 1, 10, 0, tzinfo=UTC)
    assert activity_rows[0].side == "BUY"
    assert activity_rows[0].volume_usdc is not None
    assert activity_rows[0].label == "0xcondition123 / YES"

    closed_points = list_wallet_closed_position_points(warehouse_path, "0xwallet1")
    assert len(closed_points) == 1
    assert closed_points[0].closed_at_utc == datetime(2026, 3, 1, 10, 15, tzinfo=UTC)
    assert closed_points[0].cumulative_realized_pnl == closed_points[0].realized_pnl
    assert closed_points[0].label == "0xcondition123 / YES"

    open_positions = list_wallet_open_positions(warehouse_path, "0xwallet1")
    assert len(open_positions) == 1
    assert open_positions[0].current_value is not None
    assert open_positions[0].label == "0xcondition123 / YES"


def test_wallet_chart_queries_return_empty_lists_for_wallets_without_history(tmp_path: Path) -> None:
    raw_dir, warehouse_path = build_wallet_run(tmp_path)

    assert list_wallet_activity_trades(warehouse_path, "0xwallet2") == []
    assert list_wallet_closed_position_points(warehouse_path, "0xwallet2") == []
    assert list_wallet_open_positions(warehouse_path, "0xwallet2") == []

    activity_captures = list_latest_wallet_dataset_captures(raw_dir, "wallet_activity")
    assert [(capture.wallet_address, capture.record_count) for capture in activity_captures] == [
        ("0xwallet1", 1),
        ("0xwallet2", 0),
    ]
