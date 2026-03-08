from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from src.clients import ClobClient, DataApiClient, GammaClient, GammaMarket
from src.storage import DEFAULT_WAREHOUSE_PATH, PolymarketWarehouse, RawPayloadStore


DEFAULT_SAMPLE_SIZE = 3
DEFAULT_MARKET_SCAN_LIMIT = 25
DEFAULT_PRICE_INTERVAL = "1w"
DEFAULT_PRICE_FIDELITY = 5
DEFAULT_TRADE_LIMIT = 100
DEFAULT_RAW_DATA_DIR = Path("data/raw")


@dataclass(frozen=True, slots=True)
class SampleMarketSelectionRule:
    sample_size: int = DEFAULT_SAMPLE_SIZE
    gamma_limit: int = DEFAULT_MARKET_SCAN_LIMIT

    def __post_init__(self) -> None:
        if self.sample_size <= 0:
            raise ValueError("sample_size must be greater than zero.")
        if self.gamma_limit <= 0:
            raise ValueError("gamma_limit must be greater than zero.")

    def describe(self) -> str:
        return (
            "Select up to "
            f"{self.sample_size} open Gamma markets from the first {self.gamma_limit} rows, "
            "require market_id, condition_id, and at least one token_id, "
            "then rank by liquidity desc, volume desc, and market_id asc."
        )


@dataclass(frozen=True, slots=True)
class SkippedMarket:
    market_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class BackfillFailure:
    market_id: str
    dataset: str
    detail: str


@dataclass(frozen=True, slots=True)
class BackfilledMarket:
    market_id: str
    condition_id: str
    token_ids: tuple[str, ...]
    price_rows: int
    trade_rows: int


@dataclass(frozen=True, slots=True)
class SampleMarketBackfillSummary:
    selection_rule: str
    selected_market_ids: tuple[str, ...]
    market_rows: int
    price_rows: int
    trade_rows: int
    raw_capture_count: int
    skipped_markets: tuple[SkippedMarket, ...]
    failures: tuple[BackfillFailure, ...]
    market_results: tuple[BackfilledMarket, ...]

    @property
    def has_failures(self) -> bool:
        return bool(self.failures)


def select_sample_markets(
    markets: Iterable[GammaMarket],
    rule: SampleMarketSelectionRule,
) -> tuple[list[GammaMarket], list[SkippedMarket]]:
    eligible_markets: list[GammaMarket] = []
    skipped_markets: list[SkippedMarket] = []

    for market in markets:
        market_id = _market_identifier(market)
        if not market.market_id:
            skipped_markets.append(SkippedMarket(market_id=market_id, reason="missing_market_id"))
            continue
        if market.active is False:
            skipped_markets.append(SkippedMarket(market_id=market_id, reason="inactive_or_closed"))
            continue
        if not market.condition_id:
            skipped_markets.append(SkippedMarket(market_id=market_id, reason="missing_condition_id"))
            continue
        if not market.clob_token_ids:
            skipped_markets.append(SkippedMarket(market_id=market_id, reason="missing_clob_token_ids"))
            continue
        eligible_markets.append(market)

    ranked_markets = sorted(eligible_markets, key=_market_sort_key)
    selected_markets = ranked_markets[: rule.sample_size]

    for market in ranked_markets[rule.sample_size :]:
        skipped_markets.append(
            SkippedMarket(market_id=_market_identifier(market), reason="not_selected_after_ranking")
        )

    return selected_markets, skipped_markets


class SampleMarketBackfillJob:
    def __init__(
        self,
        *,
        gamma_client: GammaClient,
        clob_client: ClobClient,
        data_api_client: DataApiClient,
        raw_store: RawPayloadStore,
        warehouse: PolymarketWarehouse,
        selection_rule: SampleMarketSelectionRule | None = None,
        price_interval: str = DEFAULT_PRICE_INTERVAL,
        price_fidelity: int = DEFAULT_PRICE_FIDELITY,
        trade_limit: int = DEFAULT_TRADE_LIMIT,
        collection_time: datetime | None = None,
    ) -> None:
        if price_fidelity <= 0:
            raise ValueError("price_fidelity must be greater than zero.")
        if trade_limit <= 0:
            raise ValueError("trade_limit must be greater than zero.")

        self.gamma_client = gamma_client
        self.clob_client = clob_client
        self.data_api_client = data_api_client
        self.raw_store = raw_store
        self.warehouse = warehouse
        self.selection_rule = selection_rule or SampleMarketSelectionRule()
        self.price_interval = price_interval
        self.price_fidelity = price_fidelity
        self.trade_limit = trade_limit
        self.collection_time = collection_time

    def run(self) -> SampleMarketBackfillSummary:
        collected_at = self.collection_time or datetime.now(UTC)
        failures: list[BackfillFailure] = []
        raw_capture_count = 0

        gamma_payload = self.gamma_client.get_markets_payload(
            limit=self.selection_rule.gamma_limit,
            closed=False,
        )
        self.raw_store.write_capture(
            "gamma",
            "sample_market_selection",
            gamma_payload,
            endpoint="/markets",
            request_params={"limit": self.selection_rule.gamma_limit, "closed": False},
            collection_time=collected_at,
            metadata={"selection_rule": self.selection_rule.describe()},
        )
        raw_capture_count += 1

        markets = self.gamma_client.parse_markets(gamma_payload)
        selected_markets, skipped_markets = select_sample_markets(markets, self.selection_rule)

        if not selected_markets:
            failures.append(
                BackfillFailure(
                    market_id="selection",
                    dataset="gamma.markets",
                    detail="No eligible markets matched the sample selection rule.",
                )
            )
            return SampleMarketBackfillSummary(
                selection_rule=self.selection_rule.describe(),
                selected_market_ids=(),
                market_rows=0,
                price_rows=0,
                trade_rows=0,
                raw_capture_count=raw_capture_count,
                skipped_markets=tuple(skipped_markets),
                failures=tuple(failures),
                market_results=(),
            )

        market_rows = self.warehouse.upsert_markets(
            selected_markets,
            source="gamma.sample_market_backfill",
            collection_time=collected_at,
        )
        price_rows = 0
        trade_rows = 0
        market_results: list[BackfilledMarket] = []

        for market in selected_markets:
            market_price_rows = 0
            market_trade_rows = 0
            histories = []

            for token_id in market.clob_token_ids:
                try:
                    price_payload = self.clob_client.get_prices_history_payload(
                        token_id,
                        interval=self.price_interval,
                        fidelity=self.price_fidelity,
                    )
                    self.raw_store.write_capture(
                        "clob",
                        "sample_market_prices",
                        price_payload,
                        endpoint="/prices-history",
                        request_params={
                            "market": token_id,
                            "interval": self.price_interval,
                            "fidelity": self.price_fidelity,
                        },
                        collection_time=collected_at,
                        metadata={
                            "market_id": market.market_id,
                            "condition_id": market.condition_id,
                            "token_id": token_id,
                        },
                    )
                    raw_capture_count += 1
                    histories.append(
                        self.clob_client.parse_price_history(
                            token_id,
                            interval=self.price_interval,
                            fidelity=self.price_fidelity,
                            payload=price_payload,
                        )
                    )
                except Exception as exc:
                    failures.append(
                        BackfillFailure(
                            market_id=market.market_id or "<unknown>",
                            dataset=f"clob.prices_history:{token_id}",
                            detail=str(exc),
                        )
                    )

            if histories:
                inserted_price_rows = self.warehouse.upsert_price_history(
                    histories,
                    source="clob.sample_market_backfill",
                    collection_time=collected_at,
                )
                market_price_rows += inserted_price_rows
                price_rows += inserted_price_rows

            try:
                trade_payload = self.data_api_client.get_trades_payload(
                    market.condition_id,
                    limit=self.trade_limit,
                )
                self.raw_store.write_capture(
                    "data_api",
                    "sample_market_trades",
                    trade_payload,
                    endpoint="/trades",
                    request_params={"market": market.condition_id, "limit": self.trade_limit},
                    collection_time=collected_at,
                    metadata={"market_id": market.market_id, "condition_id": market.condition_id},
                )
                raw_capture_count += 1
                trades = self.data_api_client.parse_trades(trade_payload)
                inserted_trade_rows = self.warehouse.upsert_trades(
                    trades,
                    source="data_api.sample_market_backfill",
                    collection_time=collected_at,
                )
                market_trade_rows += inserted_trade_rows
                trade_rows += inserted_trade_rows
            except Exception as exc:
                failures.append(
                    BackfillFailure(
                        market_id=market.market_id or "<unknown>",
                        dataset="data_api.trades",
                        detail=str(exc),
                    )
                )

            market_results.append(
                BackfilledMarket(
                    market_id=market.market_id or "<unknown>",
                    condition_id=market.condition_id or "<unknown>",
                    token_ids=market.clob_token_ids,
                    price_rows=market_price_rows,
                    trade_rows=market_trade_rows,
                )
            )

        return SampleMarketBackfillSummary(
            selection_rule=self.selection_rule.describe(),
            selected_market_ids=tuple(market.market_id for market in selected_markets if market.market_id),
            market_rows=market_rows,
            price_rows=price_rows,
            trade_rows=trade_rows,
            raw_capture_count=raw_capture_count,
            skipped_markets=tuple(skipped_markets),
            failures=tuple(failures),
            market_results=tuple(market_results),
        )


def run_sample_market_backfill(
    *,
    raw_data_dir: str | Path = DEFAULT_RAW_DATA_DIR,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE_PATH,
    selection_rule: SampleMarketSelectionRule | None = None,
    price_interval: str = DEFAULT_PRICE_INTERVAL,
    price_fidelity: int = DEFAULT_PRICE_FIDELITY,
    trade_limit: int = DEFAULT_TRADE_LIMIT,
    collection_time: datetime | None = None,
) -> SampleMarketBackfillSummary:
    with (
        GammaClient() as gamma_client,
        ClobClient() as clob_client,
        DataApiClient() as data_api_client,
        PolymarketWarehouse(warehouse_path) as warehouse,
    ):
        job = SampleMarketBackfillJob(
            gamma_client=gamma_client,
            clob_client=clob_client,
            data_api_client=data_api_client,
            raw_store=RawPayloadStore(raw_data_dir),
            warehouse=warehouse,
            selection_rule=selection_rule,
            price_interval=price_interval,
            price_fidelity=price_fidelity,
            trade_limit=trade_limit,
            collection_time=collection_time,
        )
        return job.run()


def _market_sort_key(market: GammaMarket) -> tuple[Decimal, Decimal, str]:
    return (
        -_decimal_or_zero(market.liquidity),
        -_decimal_or_zero(market.volume),
        market.market_id or "",
    )


def _decimal_or_zero(value: Decimal | None) -> Decimal:
    return value if value is not None else Decimal("0")


def _market_identifier(market: GammaMarket) -> str:
    return market.market_id or market.slug or market.question or "<unknown>"
