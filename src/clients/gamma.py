from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx

from src.clients.rest import (
    RequestConfig,
    RestJsonClient,
    extract_records,
    parse_optional_bool,
    parse_optional_datetime,
    parse_optional_decimal,
    parse_optional_str,
    parse_string_tuple,
    resolve_base_url,
)


DEFAULT_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"


@dataclass(frozen=True, slots=True)
class GammaMarket:
    market_id: str | None
    question: str | None
    slug: str | None
    condition_id: str | None
    clob_token_ids: tuple[str, ...]
    active: bool | None
    end_date: datetime | None
    liquidity: Decimal | None
    volume: Decimal | None


class GammaClient(RestJsonClient):
    def __init__(
        self,
        base_url: str | None = None,
        *,
        request_config: RequestConfig | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        super().__init__(
            resolve_base_url(base_url, "POLYMARKET_GAMMA_BASE_URL", DEFAULT_GAMMA_BASE_URL),
            request_config=request_config,
            transport=transport,
        )

    def get_markets_payload(self, *, limit: int = 100, closed: bool = False) -> Any:
        return self.get_json("/markets", params={"limit": limit, "closed": closed})

    def list_markets(self, *, limit: int = 100, closed: bool = False) -> list[GammaMarket]:
        return self.parse_markets(self.get_markets_payload(limit=limit, closed=closed))

    @classmethod
    def parse_markets(cls, payload: Any) -> list[GammaMarket]:
        return [cls._parse_market(record) for record in extract_records(payload, wrapper_keys=("data", "markets"))]

    @staticmethod
    def _parse_market(record: dict[str, Any]) -> GammaMarket:
        return GammaMarket(
            market_id=parse_optional_str(record.get("id")),
            question=parse_optional_str(record.get("question")),
            slug=parse_optional_str(record.get("slug")),
            condition_id=parse_optional_str(record.get("conditionId") or record.get("condition_id")),
            clob_token_ids=parse_string_tuple(record.get("clobTokenIds") or record.get("clob_token_ids")),
            active=parse_optional_bool(record.get("active")),
            end_date=parse_optional_datetime(record.get("endDate") or record.get("end_date")),
            liquidity=parse_optional_decimal(record.get("liquidity")),
            volume=parse_optional_decimal(record.get("volume")),
        )
