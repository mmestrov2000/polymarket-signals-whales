from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

import httpx

from src.clients.rest import (
    RequestConfig,
    RestJsonClient,
    UnexpectedPayloadError,
    parse_optional_bool,
    parse_optional_datetime,
    parse_optional_decimal,
    parse_optional_int,
    parse_optional_str,
    resolve_base_url,
)


DEFAULT_CLOB_BASE_URL = "https://clob.polymarket.com"
QuoteSide = Literal["BUY", "SELL"]


@dataclass(frozen=True, slots=True)
class OrderBookLevel:
    price: Decimal | None
    size: Decimal | None


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    market_id: str | None
    asset_id: str | None
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    last_trade_price: Decimal | None
    tick_size: Decimal | None
    min_order_size: Decimal | None
    book_hash: str | None
    timestamp: datetime | None
    neg_risk: bool | None


@dataclass(frozen=True, slots=True)
class PriceQuote:
    token_id: str
    side: QuoteSide
    price: Decimal | None


@dataclass(frozen=True, slots=True)
class PriceHistoryPoint:
    timestamp: datetime | None
    price: Decimal | None


@dataclass(frozen=True, slots=True)
class PriceHistory:
    token_id: str
    interval: str
    fidelity: int
    points: tuple[PriceHistoryPoint, ...]


class ClobClient(RestJsonClient):
    def __init__(
        self,
        base_url: str | None = None,
        *,
        request_config: RequestConfig | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        super().__init__(
            resolve_base_url(base_url, "POLYMARKET_CLOB_BASE_URL", DEFAULT_CLOB_BASE_URL),
            request_config=request_config,
            transport=transport,
        )

    def get_prices_history_payload(
        self,
        token_id: str,
        *,
        interval: str = "1w",
        fidelity: int = 5,
    ) -> Any:
        return self.get_json(
            "/prices-history",
            params={"market": token_id, "interval": interval, "fidelity": fidelity},
        )

    def get_book(self, token_id: str) -> OrderBookSnapshot:
        payload = self.get_json("/book", params={"token_id": token_id})
        if not isinstance(payload, dict):
            raise UnexpectedPayloadError("Expected /book to return a JSON object.")

        return OrderBookSnapshot(
            market_id=parse_optional_str(payload.get("market")),
            asset_id=parse_optional_str(payload.get("asset_id")),
            bids=self._parse_levels(payload.get("bids")),
            asks=self._parse_levels(payload.get("asks")),
            last_trade_price=parse_optional_decimal(payload.get("last_trade_price")),
            tick_size=parse_optional_decimal(payload.get("tick_size")),
            min_order_size=parse_optional_decimal(payload.get("min_order_size")),
            book_hash=parse_optional_str(payload.get("hash")),
            timestamp=parse_optional_datetime(payload.get("timestamp")),
            neg_risk=parse_optional_bool(payload.get("neg_risk")),
        )

    def get_price(self, token_id: str, side: str) -> PriceQuote:
        normalized_side = side.strip().upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL.")

        payload = self.get_json("/price", params={"token_id": token_id, "side": normalized_side})
        if not isinstance(payload, dict):
            raise UnexpectedPayloadError("Expected /price to return a JSON object.")

        return PriceQuote(
            token_id=token_id,
            side=normalized_side,
            price=parse_optional_decimal(payload.get("price")),
        )

    def get_prices_history(
        self,
        token_id: str,
        *,
        interval: str = "1w",
        fidelity: int = 5,
    ) -> PriceHistory:
        payload = self.get_prices_history_payload(token_id, interval=interval, fidelity=fidelity)
        return self.parse_price_history(
            token_id,
            interval=interval,
            fidelity=fidelity,
            payload=payload,
        )

    @staticmethod
    def parse_price_history(
        token_id: str,
        *,
        interval: str,
        fidelity: int,
        payload: Any,
    ) -> PriceHistory:
        raw_points = payload.get("history") if isinstance(payload, dict) else payload
        if not isinstance(raw_points, list):
            raise UnexpectedPayloadError("Expected /prices-history to return a history list.")

        points = tuple(
            PriceHistoryPoint(
                timestamp=parse_optional_datetime(point.get("t") or point.get("timestamp")),
                price=parse_optional_decimal(point.get("p") or point.get("price")),
            )
            for point in raw_points
            if isinstance(point, dict)
        )

        return PriceHistory(
            token_id=token_id,
            interval=interval,
            fidelity=parse_optional_int(fidelity) or fidelity,
            points=points,
        )

    @staticmethod
    def _parse_levels(raw_levels: Any) -> tuple[OrderBookLevel, ...]:
        if not isinstance(raw_levels, list):
            return ()

        return tuple(
            OrderBookLevel(
                price=parse_optional_decimal(level.get("price")),
                size=parse_optional_decimal(level.get("size")),
            )
            for level in raw_levels
            if isinstance(level, dict)
        )
