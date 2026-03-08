from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx

from src.clients.rest import (
    RequestConfig,
    RestJsonClient,
    UnexpectedPayloadError,
    extract_records,
    flatten_nested_records,
    parse_optional_bool,
    parse_optional_datetime,
    parse_optional_decimal,
    parse_optional_int,
    parse_optional_str,
    resolve_base_url,
)


DEFAULT_DATA_API_BASE_URL = "https://data-api.polymarket.com"


@dataclass(frozen=True, slots=True)
class LeaderboardEntry:
    proxy_wallet: str | None
    rank: int | None
    pnl: Decimal | None
    volume: Decimal | None
    user_name: str | None
    verified_badge: bool | None


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    proxy_wallet: str | None
    asset_id: str | None
    condition_id: str | None
    size: Decimal | None
    average_price: Decimal | None
    current_value: Decimal | None
    realized_pnl: Decimal | None
    outcome: str | None
    outcome_index: int | None
    total_bought: Decimal | None
    end_date: datetime | None


@dataclass(frozen=True, slots=True)
class ClosedPosition:
    proxy_wallet: str | None
    asset_id: str | None
    condition_id: str | None
    outcome: str | None
    average_price: Decimal | None
    realized_pnl: Decimal | None
    total_bought: Decimal | None
    closed_at: datetime | None
    end_date: datetime | None


@dataclass(frozen=True, slots=True)
class TradeRecord:
    proxy_wallet: str | None
    asset_id: str | None
    condition_id: str | None
    outcome: str | None
    side: str | None
    size: Decimal | None
    price: Decimal | None
    timestamp: datetime | None
    transaction_hash: str | None
    usdc_size: Decimal | None


@dataclass(frozen=True, slots=True)
class HolderRecord:
    proxy_wallet: str | None
    asset_id: str | None
    amount: Decimal | None
    outcome_index: int | None


@dataclass(frozen=True, slots=True)
class HolderGroup:
    token_id: str | None
    holders: tuple[HolderRecord, ...]


@dataclass(frozen=True, slots=True)
class OpenInterestSnapshot:
    market_id: str | None
    value: Decimal | None


class DataApiClient(RestJsonClient):
    def __init__(
        self,
        base_url: str | None = None,
        *,
        request_config: RequestConfig | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        super().__init__(
            resolve_base_url(base_url, "POLYMARKET_DATA_API_BASE_URL", DEFAULT_DATA_API_BASE_URL),
            request_config=request_config,
            transport=transport,
        )

    def get_trades_payload(self, market: str, *, limit: int = 100) -> Any:
        return self.get_json("/trades", params={"market": market, "limit": limit})

    def list_leaderboard(
        self,
        *,
        category: str = "OVERALL",
        time_period: str = "ALL",
        order_by: str = "PNL",
        limit: int = 100,
    ) -> list[LeaderboardEntry]:
        payload = self.get_json(
            "/v1/leaderboard",
            params={
                "category": category,
                "timePeriod": time_period,
                "orderBy": order_by,
                "limit": limit,
            },
        )
        records = extract_records(payload, wrapper_keys=("data", "leaderboard", "users", "results"))
        return [self._parse_leaderboard_entry(record) for record in records]

    def list_positions(self, user: str, *, limit: int = 100, sort_by: str = "TOKENS") -> list[PositionSnapshot]:
        payload = self.get_json("/positions", params={"user": user, "limit": limit, "sortBy": sort_by})
        records = flatten_nested_records(payload, "positions", wrapper_keys=("data", "results", "positions"))
        return [self._parse_position_snapshot(record) for record in records]

    def list_closed_positions(
        self,
        user: str,
        *,
        limit: int = 100,
        sort_by: str = "TIMESTAMP",
    ) -> list[ClosedPosition]:
        payload = self.get_json(
            "/closed-positions",
            params={"user": user, "limit": limit, "sortBy": sort_by},
        )
        records = flatten_nested_records(payload, "positions", wrapper_keys=("data", "results", "positions"))
        return [self._parse_closed_position(record) for record in records]

    def list_activity(
        self,
        user: str,
        *,
        limit: int = 100,
        activity_type: str = "TRADE",
        sort_by: str = "TIMESTAMP",
        sort_direction: str = "DESC",
    ) -> list[TradeRecord]:
        payload = self.get_json(
            "/activity",
            params={
                "user": user,
                "limit": limit,
                "type": activity_type,
                "sortBy": sort_by,
                "sortDirection": sort_direction,
            },
        )
        records = extract_records(payload, wrapper_keys=("data", "activity", "results"))
        return [self._parse_trade_record(record) for record in records]

    def list_trades(self, market: str, *, limit: int = 100) -> list[TradeRecord]:
        return self.parse_trades(self.get_trades_payload(market, limit=limit))

    def list_holders(self, market: str, *, limit: int = 100) -> list[HolderGroup]:
        payload = self.get_json("/holders", params={"market": market, "limit": limit})
        groups = self._extract_holder_groups(payload)
        return [self._parse_holder_group(group) for group in groups]

    def get_open_interest(self, market: str) -> OpenInterestSnapshot:
        payload = self.get_json("/oi", params={"market": market})
        records = extract_records(payload, wrapper_keys=("data", "results"))
        if not records:
            raise UnexpectedPayloadError("Expected /oi to return a JSON object.")
        return self._parse_open_interest(records[0])

    @staticmethod
    def _parse_leaderboard_entry(record: dict[str, Any]) -> LeaderboardEntry:
        return LeaderboardEntry(
            proxy_wallet=parse_optional_str(record.get("proxyWallet")),
            rank=parse_optional_int(record.get("rank")),
            pnl=parse_optional_decimal(record.get("pnl")),
            volume=parse_optional_decimal(record.get("vol") or record.get("volume")),
            user_name=parse_optional_str(record.get("userName") or record.get("username")),
            verified_badge=parse_optional_bool(record.get("verifiedBadge")),
        )

    @staticmethod
    def _parse_position_snapshot(record: dict[str, Any]) -> PositionSnapshot:
        return PositionSnapshot(
            proxy_wallet=parse_optional_str(record.get("proxyWallet") or record.get("user")),
            asset_id=parse_optional_str(record.get("asset")),
            condition_id=parse_optional_str(record.get("conditionId") or record.get("market")),
            size=parse_optional_decimal(record.get("size")),
            average_price=parse_optional_decimal(record.get("avgPrice")),
            current_value=parse_optional_decimal(record.get("currentValue")),
            realized_pnl=parse_optional_decimal(record.get("realizedPnl")),
            outcome=parse_optional_str(record.get("outcome")),
            outcome_index=parse_optional_int(record.get("outcomeIndex")),
            total_bought=parse_optional_decimal(record.get("totalBought")),
            end_date=parse_optional_datetime(record.get("endDate")),
        )

    @staticmethod
    def _parse_closed_position(record: dict[str, Any]) -> ClosedPosition:
        return ClosedPosition(
            proxy_wallet=parse_optional_str(record.get("proxyWallet") or record.get("user")),
            asset_id=parse_optional_str(record.get("asset")),
            condition_id=parse_optional_str(record.get("conditionId") or record.get("market")),
            outcome=parse_optional_str(record.get("outcome")),
            average_price=parse_optional_decimal(record.get("avgPrice")),
            realized_pnl=parse_optional_decimal(record.get("realizedPnl")),
            total_bought=parse_optional_decimal(record.get("totalBought")),
            closed_at=parse_optional_datetime(record.get("timestamp")),
            end_date=parse_optional_datetime(record.get("endDate")),
        )

    @staticmethod
    def _parse_trade_record(record: dict[str, Any]) -> TradeRecord:
        return TradeRecord(
            proxy_wallet=parse_optional_str(record.get("proxyWallet") or record.get("user")),
            asset_id=parse_optional_str(record.get("asset")),
            condition_id=parse_optional_str(record.get("conditionId") or record.get("market")),
            outcome=parse_optional_str(record.get("outcome")),
            side=parse_optional_str(record.get("side")),
            size=parse_optional_decimal(record.get("size")),
            price=parse_optional_decimal(record.get("price")),
            timestamp=parse_optional_datetime(record.get("timestamp")),
            transaction_hash=parse_optional_str(record.get("transactionHash")),
            usdc_size=parse_optional_decimal(record.get("usdcSize")),
        )

    @staticmethod
    def _parse_holder_group(record: dict[str, Any]) -> HolderGroup:
        raw_holders = record.get("holders")
        if not isinstance(raw_holders, list):
            raise UnexpectedPayloadError("Expected /holders items to contain a holders list.")

        holders = tuple(
            HolderRecord(
                proxy_wallet=parse_optional_str(holder.get("proxyWallet") or holder.get("user")),
                asset_id=parse_optional_str(holder.get("asset")),
                amount=parse_optional_decimal(holder.get("amount")),
                outcome_index=parse_optional_int(holder.get("outcomeIndex")),
            )
            for holder in raw_holders
            if isinstance(holder, dict)
        )
        return HolderGroup(
            token_id=parse_optional_str(record.get("token") or record.get("asset") or record.get("asset_id")),
            holders=holders,
        )

    @staticmethod
    def _parse_open_interest(record: dict[str, Any]) -> OpenInterestSnapshot:
        return OpenInterestSnapshot(
            market_id=parse_optional_str(record.get("market")),
            value=parse_optional_decimal(record.get("value")),
        )

    @classmethod
    def parse_trades(cls, payload: Any) -> list[TradeRecord]:
        records = extract_records(payload, wrapper_keys=("data", "trades", "results"))
        return [cls._parse_trade_record(record) for record in records]

    @staticmethod
    def _extract_holder_groups(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        if isinstance(payload, dict):
            for key in ("data", "results", "groups"):
                candidate = payload.get(key)
                if isinstance(candidate, list):
                    return [item for item in candidate if isinstance(item, dict)]
            if isinstance(payload.get("holders"), list):
                return [payload]

        raise UnexpectedPayloadError("Expected /holders to return a list of token groups.")
