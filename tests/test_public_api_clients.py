from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import httpx

from src.clients.clob import ClobClient
from src.clients.data_api import DataApiClient
from src.clients.gamma import GammaClient
from src.clients.rest import RequestConfig


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "public_clients"


def load_fixture(name: str) -> object:
    return json.loads((FIXTURE_DIR / name).read_text())


def test_gamma_client_normalizes_markets_from_fixture() -> None:
    gamma_payload = load_fixture("gamma_markets.json")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        assert request.url.params["limit"] == "10"
        assert request.url.params["closed"].lower() == "false"
        return httpx.Response(200, json=gamma_payload)

    with GammaClient(
        transport=httpx.MockTransport(handler),
        request_config=RequestConfig(timeout_seconds=2.5, max_attempts=1, retry_backoff_seconds=0.0),
    ) as client:
        markets = client.list_markets(limit=10, closed=False)

    assert len(markets) == 2
    assert markets[0].market_id == "12345"
    assert markets[0].condition_id == "0xcondition123"
    assert markets[0].clob_token_ids == ("111", "222")
    assert markets[0].liquidity == Decimal("12345.67")
    assert markets[1].active is False
    assert markets[1].clob_token_ids == ("333", "444")


def test_clob_client_normalizes_book_price_and_history_from_fixtures() -> None:
    clob_payload = load_fixture("clob.json")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/book":
            assert request.url.params["token_id"] == "111"
            return httpx.Response(200, json=clob_payload["book"])
        if request.url.path == "/price":
            assert request.url.params["token_id"] == "111"
            side = request.url.params["side"]
            if side == "BUY":
                return httpx.Response(200, json=clob_payload["buy_price"])
            return httpx.Response(200, json=clob_payload["sell_price"])
        if request.url.path == "/prices-history":
            assert request.url.params["market"] == "111"
            assert request.url.params["interval"] == "1w"
            assert request.url.params["fidelity"] == "5"
            return httpx.Response(200, json=clob_payload["prices_history"])
        raise AssertionError(f"Unexpected path {request.url.path}")

    with ClobClient(
        transport=httpx.MockTransport(handler),
        request_config=RequestConfig(timeout_seconds=2.5, max_attempts=1, retry_backoff_seconds=0.0),
    ) as client:
        book = client.get_book("111")
        buy_price = client.get_price("111", "BUY")
        sell_price = client.get_price("111", "SELL")
        history = client.get_prices_history("111", interval="1w", fidelity=5)

    assert book.market_id == "0xcondition123"
    assert book.bids[0].price == Decimal("0.42")
    assert book.asks[0].size == Decimal("120")
    assert book.timestamp is not None
    assert buy_price.price == Decimal("0.58")
    assert sell_price.price == Decimal("0.42")
    assert len(history.points) == 2
    assert history.points[0].price == Decimal("0.49")
    assert history.points[1].timestamp is not None


def test_data_api_client_normalizes_verified_public_endpoints_from_fixture() -> None:
    data_api_payload = load_fixture("data_api.json")

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/leaderboard":
            assert request.url.params["category"] == "OVERALL"
            assert request.url.params["timePeriod"] == "ALL"
            return httpx.Response(200, json=data_api_payload["leaderboard"])
        if path == "/positions":
            assert request.url.params["user"] == "0xwallet1"
            return httpx.Response(200, json=data_api_payload["positions"])
        if path == "/closed-positions":
            return httpx.Response(200, json=data_api_payload["closed_positions"])
        if path == "/activity":
            assert request.url.params["type"] == "TRADE"
            return httpx.Response(200, json=data_api_payload["activity"])
        if path == "/trades":
            assert request.url.params["market"] == "0xcondition123"
            return httpx.Response(200, json=data_api_payload["trades"])
        if path == "/holders":
            return httpx.Response(200, json=data_api_payload["holders"])
        if path == "/oi":
            return httpx.Response(200, json=data_api_payload["oi"])
        raise AssertionError(f"Unexpected path {path}")

    with DataApiClient(
        transport=httpx.MockTransport(handler),
        request_config=RequestConfig(timeout_seconds=2.5, max_attempts=1, retry_backoff_seconds=0.0),
    ) as client:
        leaderboard = client.parse_leaderboard(client.get_leaderboard_payload(limit=5))
        positions = client.parse_positions(client.get_positions_payload("0xwallet1", limit=5))
        closed_positions = client.parse_closed_positions(
            client.get_closed_positions_payload("0xwallet1", limit=5)
        )
        activity = client.parse_activity(client.get_activity_payload("0xwallet1", limit=10))
        trades = client.list_trades("0xcondition123", limit=10)
        holders = client.list_holders("0xcondition123", limit=10)
        open_interest = client.get_open_interest("0xcondition123")

    assert leaderboard[0].proxy_wallet == "0xwallet1"
    assert leaderboard[0].volume == Decimal("100000.00")
    assert positions[0].current_value == Decimal("275.00")
    assert positions[0].proxy_wallet == "0xwallet1"
    assert closed_positions[0].closed_at is not None
    assert activity[0].usdc_size == Decimal("45")
    assert trades[0].side == "SELL"
    assert holders[0].token_id == "111"
    assert holders[0].holders[1].amount == Decimal("900")
    assert open_interest.market_id == "0xcondition123"
    assert open_interest.value == Decimal("123456.78")


def test_public_clients_retry_timeout_and_retryable_status_before_success() -> None:
    gamma_payload = load_fixture("gamma_markets.json")
    attempts = 0
    observed_timeouts: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        observed_timeouts.append(float(request.extensions["timeout"]["read"]))
        if attempts == 1:
            raise httpx.ReadTimeout("timed out", request=request)
        if attempts == 2:
            return httpx.Response(503, json={"error": "retry"})
        return httpx.Response(200, json=gamma_payload)

    with GammaClient(
        transport=httpx.MockTransport(handler),
        request_config=RequestConfig(timeout_seconds=1.25, max_attempts=3, retry_backoff_seconds=0.0),
    ) as client:
        markets = client.list_markets(limit=1)

    assert attempts == 3
    assert observed_timeouts == [1.25, 1.25, 1.25]
    assert markets[0].market_id == "12345"
