from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import httpx

from src.clients import ClobClient, DataApiClient, GammaClient, GammaMarket, RequestConfig
from src.ingestion import SampleMarketBackfillJob, SampleMarketSelectionRule, select_sample_markets
from src.storage import PolymarketWarehouse, RawPayloadStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "public_clients"


def load_fixture(name: str) -> object:
    return json.loads((FIXTURE_DIR / name).read_text())


def test_select_sample_markets_ranks_deterministically_and_reports_skips() -> None:
    rule = SampleMarketSelectionRule(sample_size=2, gamma_limit=10)
    markets = [
        GammaMarket(
            market_id="m-low",
            question="Low liquidity market",
            slug="m-low",
            condition_id="0xlow",
            clob_token_ids=("101",),
            active=True,
            end_date=None,
            liquidity=Decimal("10"),
            volume=Decimal("20"),
        ),
        GammaMarket(
            market_id="m-high",
            question="High liquidity market",
            slug="m-high",
            condition_id="0xhigh",
            clob_token_ids=("201",),
            active=True,
            end_date=None,
            liquidity=Decimal("50"),
            volume=Decimal("10"),
        ),
        GammaMarket(
            market_id="m-volume",
            question="High volume market",
            slug="m-volume",
            condition_id="0xvolume",
            clob_token_ids=("301",),
            active=True,
            end_date=None,
            liquidity=Decimal("10"),
            volume=Decimal("30"),
        ),
        GammaMarket(
            market_id="m-missing-condition",
            question="Missing condition id",
            slug="m-missing-condition",
            condition_id=None,
            clob_token_ids=("401",),
            active=True,
            end_date=None,
            liquidity=Decimal("999"),
            volume=Decimal("999"),
        ),
        GammaMarket(
            market_id="m-missing-tokens",
            question="Missing token ids",
            slug="m-missing-tokens",
            condition_id="0xtokens",
            clob_token_ids=(),
            active=True,
            end_date=None,
            liquidity=Decimal("999"),
            volume=Decimal("999"),
        ),
        GammaMarket(
            market_id="m-inactive",
            question="Inactive market",
            slug="m-inactive",
            condition_id="0xinactive",
            clob_token_ids=("501",),
            active=False,
            end_date=None,
            liquidity=Decimal("999"),
            volume=Decimal("999"),
        ),
    ]

    selected_markets, skipped_markets = select_sample_markets(markets, rule)

    assert [market.market_id for market in selected_markets] == ["m-high", "m-volume"]
    assert {(item.market_id, item.reason) for item in skipped_markets} == {
        ("m-low", "not_selected_after_ranking"),
        ("m-missing-condition", "missing_condition_id"),
        ("m-missing-tokens", "missing_clob_token_ids"),
        ("m-inactive", "inactive_or_closed"),
    }


def test_sample_market_backfill_job_persists_raw_payloads_and_normalized_rows(tmp_path) -> None:
    gamma_payload = load_fixture("gamma_markets.json")
    clob_payload = load_fixture("clob.json")
    data_api_payload = load_fixture("data_api.json")
    collection_time = datetime(2026, 3, 8, 15, 30, tzinfo=UTC)

    def gamma_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        assert request.url.params["limit"] == "10"
        assert request.url.params["closed"].lower() == "false"
        return httpx.Response(200, json=gamma_payload)

    def clob_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/prices-history"
        token_id = request.url.params["market"]
        if token_id == "111":
            return httpx.Response(200, json=clob_payload["prices_history"])
        if token_id == "222":
            second_history = {
                "history": [
                    {"t": 1710172800000, "p": "0.52"},
                    {"t": 1710259200000, "p": "0.53"},
                ]
            }
            return httpx.Response(200, json=second_history)
        raise AssertionError(f"Unexpected token_id {token_id}")

    def data_api_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/trades"
        assert request.url.params["market"] == "0xcondition123"
        assert request.url.params["limit"] == "25"
        return httpx.Response(200, json=data_api_payload["trades"])

    raw_store = RawPayloadStore(tmp_path / "raw")
    warehouse_path = tmp_path / "warehouse" / "polymarket.duckdb"

    with (
        GammaClient(
            transport=httpx.MockTransport(gamma_handler),
            request_config=RequestConfig(timeout_seconds=2.5, max_attempts=1, retry_backoff_seconds=0.0),
        ) as gamma_client,
        ClobClient(
            transport=httpx.MockTransport(clob_handler),
            request_config=RequestConfig(timeout_seconds=2.5, max_attempts=1, retry_backoff_seconds=0.0),
        ) as clob_client,
        DataApiClient(
            transport=httpx.MockTransport(data_api_handler),
            request_config=RequestConfig(timeout_seconds=2.5, max_attempts=1, retry_backoff_seconds=0.0),
        ) as data_api_client,
        PolymarketWarehouse(warehouse_path) as warehouse,
    ):
        job = SampleMarketBackfillJob(
            gamma_client=gamma_client,
            clob_client=clob_client,
            data_api_client=data_api_client,
            raw_store=raw_store,
            warehouse=warehouse,
            selection_rule=SampleMarketSelectionRule(sample_size=1, gamma_limit=10),
            price_interval="1w",
            price_fidelity=5,
            trade_limit=25,
            collection_time=collection_time,
        )
        summary = job.run()

    assert summary.selected_market_ids == ("12345",)
    assert summary.market_rows == 1
    assert summary.price_rows == 4
    assert summary.trade_rows == 1
    assert summary.raw_capture_count == 4
    assert summary.failures == ()
    assert summary.market_results[0].token_ids == ("111", "222")

    raw_files = sorted(path for path in (tmp_path / "raw").rglob("*") if path.is_file())
    assert len(raw_files) == 4

    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        assert connection.execute("SELECT COUNT(*) FROM markets").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM market_tokens").fetchone()[0] == 2
        assert connection.execute("SELECT COUNT(*) FROM price_history").fetchone()[0] == 4
        assert connection.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 1

        stored_market = connection.execute(
            """
            SELECT source, collection_time_utc
            FROM markets
            WHERE market_id = '12345'
            """
        ).fetchone()
        assert stored_market[0] == "gamma.sample_market_backfill"
        assert stored_market[1] == collection_time.replace(tzinfo=None)


def test_sample_market_backfill_job_records_failures_and_continues_other_datasets(tmp_path) -> None:
    gamma_payload = load_fixture("gamma_markets.json")
    clob_payload = load_fixture("clob.json")
    data_api_payload = load_fixture("data_api.json")

    def gamma_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=gamma_payload)

    def clob_handler(request: httpx.Request) -> httpx.Response:
        token_id = request.url.params["market"]
        if token_id == "111":
            return httpx.Response(200, json=clob_payload["prices_history"])
        if token_id == "222":
            return httpx.Response(503, json={"error": "temporary upstream failure"})
        raise AssertionError(f"Unexpected token_id {token_id}")

    def data_api_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=data_api_payload["trades"])

    with (
        GammaClient(
            transport=httpx.MockTransport(gamma_handler),
            request_config=RequestConfig(timeout_seconds=2.5, max_attempts=1, retry_backoff_seconds=0.0),
        ) as gamma_client,
        ClobClient(
            transport=httpx.MockTransport(clob_handler),
            request_config=RequestConfig(timeout_seconds=2.5, max_attempts=1, retry_backoff_seconds=0.0),
        ) as clob_client,
        DataApiClient(
            transport=httpx.MockTransport(data_api_handler),
            request_config=RequestConfig(timeout_seconds=2.5, max_attempts=1, retry_backoff_seconds=0.0),
        ) as data_api_client,
        PolymarketWarehouse(tmp_path / "warehouse" / "polymarket.duckdb") as warehouse,
    ):
        job = SampleMarketBackfillJob(
            gamma_client=gamma_client,
            clob_client=clob_client,
            data_api_client=data_api_client,
            raw_store=RawPayloadStore(tmp_path / "raw"),
            warehouse=warehouse,
            selection_rule=SampleMarketSelectionRule(sample_size=1, gamma_limit=10),
            trade_limit=10,
        )
        summary = job.run()

    assert summary.market_rows == 1
    assert summary.price_rows == 2
    assert summary.trade_rows == 1
    assert summary.has_failures is True
    assert summary.failures[0].market_id == "12345"
    assert summary.failures[0].dataset == "clob.prices_history:222"
