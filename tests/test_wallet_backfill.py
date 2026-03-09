from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import httpx

from src.clients import DataApiClient, RequestConfig
from src.ingestion import WalletBackfillJob, WalletUniverseSelectionRule, select_wallet_seeds
from src.storage import PolymarketWarehouse, RawPayloadStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "public_clients"


def load_fixture(name: str) -> object:
    return json.loads((FIXTURE_DIR / name).read_text())


def test_select_wallet_seeds_dedupes_wallets_and_skips_blank_entries() -> None:
    payload = load_fixture("data_api_wallet_backfill.json")
    entries = DataApiClient.parse_leaderboard(payload["leaderboard"])

    selected_entries, skipped_entries = select_wallet_seeds(
        entries,
        WalletUniverseSelectionRule(leaderboard_limit=4),
    )

    assert [entry.proxy_wallet for entry in selected_entries] == ["0xwallet1", "0xwallet2"]
    assert {(entry.identifier, entry.reason) for entry in skipped_entries} == {
        ("rank:2", "missing_proxy_wallet"),
        ("0xwallet1", "duplicate_wallet"),
    }


def test_wallet_backfill_job_persists_raw_payloads_and_normalized_rows(tmp_path) -> None:
    fixture = load_fixture("data_api_wallet_backfill.json")
    collection_time = datetime(2026, 3, 8, 15, 30, tzinfo=UTC)

    def data_api_handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/leaderboard":
            assert request.url.params["limit"] == "4"
            return httpx.Response(200, json=fixture["leaderboard"])
        if path == "/positions":
            return httpx.Response(200, json=fixture["positions"][request.url.params["user"]])
        if path == "/closed-positions":
            return httpx.Response(200, json=fixture["closed_positions"][request.url.params["user"]])
        if path == "/activity":
            return httpx.Response(200, json=fixture["activity"][request.url.params["user"]])
        raise AssertionError(f"Unexpected path {path}")

    raw_store = RawPayloadStore(tmp_path / "raw")
    warehouse_path = tmp_path / "warehouse" / "polymarket.duckdb"

    with (
        DataApiClient(
            transport=httpx.MockTransport(data_api_handler),
            request_config=RequestConfig(timeout_seconds=2.5, max_attempts=1, retry_backoff_seconds=0.0),
        ) as data_api_client,
        PolymarketWarehouse(warehouse_path) as warehouse,
    ):
        job = WalletBackfillJob(
            data_api_client=data_api_client,
            raw_store=raw_store,
            warehouse=warehouse,
            selection_rule=WalletUniverseSelectionRule(leaderboard_limit=4),
            positions_limit=10,
            closed_positions_limit=10,
            activity_limit=10,
            collection_time=collection_time,
        )
        summary = job.run()

    assert summary.seed_wallet_addresses == ("0xwallet1", "0xwallet2")
    assert summary.position_rows == 1
    assert summary.closed_position_rows == 1
    assert summary.activity_trade_rows == 1
    assert summary.wallet_profile_rows == 2
    assert summary.raw_capture_count == 8
    assert summary.failures == ()
    assert summary.wallet_results[1].wallet_address == "0xwallet2"
    assert summary.wallet_results[1].position_rows == 0
    assert summary.wallet_results[1].closed_position_rows == 0
    assert summary.wallet_results[1].activity_trade_rows == 0
    assert summary.wallet_results[1].profile_rows == 1

    raw_files = sorted(path for path in (tmp_path / "raw").rglob("*") if path.is_file())
    assert len(raw_files) == 8

    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        assert connection.execute("SELECT COUNT(*) FROM wallet_positions").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM wallet_closed_positions").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM wallet_profiles").fetchone()[0] == 2

        empty_wallet_profile = connection.execute(
            """
            SELECT
                closed_position_count,
                activity_trade_count,
                realized_roi,
                hit_rate
            FROM wallet_profiles
            WHERE wallet_address = '0xwallet2'
            """
        ).fetchone()
        assert empty_wallet_profile == (0, 0, None, None)


def test_wallet_backfill_job_records_failures_and_still_emits_profile_rows(tmp_path) -> None:
    fixture = load_fixture("data_api_wallet_backfill.json")

    def data_api_handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        wallet_address = request.url.params.get("user")

        if path == "/v1/leaderboard":
            return httpx.Response(200, json=fixture["leaderboard"])
        if path == "/positions":
            return httpx.Response(200, json=fixture["positions"][wallet_address])
        if path == "/closed-positions":
            return httpx.Response(200, json=fixture["closed_positions"][wallet_address])
        if path == "/activity" and wallet_address == "0xwallet2":
            return httpx.Response(503, json={"error": "temporary upstream failure"})
        if path == "/activity":
            return httpx.Response(200, json=fixture["activity"][wallet_address])
        raise AssertionError(f"Unexpected path {path}")

    with (
        DataApiClient(
            transport=httpx.MockTransport(data_api_handler),
            request_config=RequestConfig(timeout_seconds=2.5, max_attempts=1, retry_backoff_seconds=0.0),
        ) as data_api_client,
        PolymarketWarehouse(tmp_path / "warehouse" / "polymarket.duckdb") as warehouse,
    ):
        job = WalletBackfillJob(
            data_api_client=data_api_client,
            raw_store=RawPayloadStore(tmp_path / "raw"),
            warehouse=warehouse,
            selection_rule=WalletUniverseSelectionRule(leaderboard_limit=4),
            activity_limit=10,
        )
        summary = job.run()

    assert summary.position_rows == 1
    assert summary.closed_position_rows == 1
    assert summary.activity_trade_rows == 1
    assert summary.wallet_profile_rows == 2
    assert summary.raw_capture_count == 7
    assert summary.has_failures is True
    assert summary.failures[0].wallet_address == "0xwallet2"
    assert summary.failures[0].dataset == "data_api.activity"
    assert summary.wallet_results[1].wallet_address == "0xwallet2"
    assert summary.wallet_results[1].profile_rows == 1
