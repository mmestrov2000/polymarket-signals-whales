from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from src.clients import ClobClient, TradeRecord
from src.clients.polymarket_websocket import (
    DEFAULT_WS_BASE_URL,
    build_market_subscription,
    decode_websocket_message,
    market_channel_url,
)
from src.clients.rest import parse_optional_datetime, parse_optional_decimal, parse_optional_str
from src.storage import DEFAULT_WAREHOUSE_PATH, PolymarketWarehouse, RawPayloadStore, TopOfBookSnapshot


DEFAULT_LIVE_RECORDER_RAW_DIR = Path("data/raw")
DEFAULT_LIVE_RECORDER_SESSION_SECONDS = 300
DEFAULT_LIVE_RECORDER_OPEN_TIMEOUT_SECONDS = 10.0
DEFAULT_LIVE_RECORDER_MESSAGE_TIMEOUT_SECONDS = 30.0
DEFAULT_LIVE_RECORDER_PING_INTERVAL_SECONDS = 20.0
DEFAULT_LIVE_RECORDER_PING_TIMEOUT_SECONDS = 20.0
DEFAULT_LIVE_RECORDER_RECONNECT_ATTEMPTS = 5
DEFAULT_LIVE_RECORDER_RAW_DATASET = "live_market_channel_events"
WEBSOCKET_SOURCE = "websocket.live_market_recorder"


@dataclass(frozen=True, slots=True)
class RecorderWarning:
    kind: str
    attempt: int
    occurred_at: datetime
    detail: str


@dataclass(frozen=True, slots=True)
class LiveMarketRecorderSummary:
    asset_ids: tuple[str, ...]
    started_at: datetime
    ended_at: datetime
    messages_received: int
    raw_capture_count: int
    order_book_rows: int
    trade_rows: int
    reconnect_count: int
    warnings: tuple[RecorderWarning, ...]

    @property
    def has_warnings(self) -> bool:
        return bool(self.warnings)


class LiveMarketRecorder:
    def __init__(
        self,
        *,
        asset_ids: Sequence[str],
        raw_store: RawPayloadStore,
        warehouse: PolymarketWarehouse,
        ws_base_url: str = DEFAULT_WS_BASE_URL,
        session_seconds: int | None = DEFAULT_LIVE_RECORDER_SESSION_SECONDS,
        max_messages: int | None = None,
        reconnect_attempts: int = DEFAULT_LIVE_RECORDER_RECONNECT_ATTEMPTS,
        open_timeout_seconds: float = DEFAULT_LIVE_RECORDER_OPEN_TIMEOUT_SECONDS,
        message_timeout_seconds: float = DEFAULT_LIVE_RECORDER_MESSAGE_TIMEOUT_SECONDS,
        ping_interval_seconds: float = DEFAULT_LIVE_RECORDER_PING_INTERVAL_SECONDS,
        ping_timeout_seconds: float = DEFAULT_LIVE_RECORDER_PING_TIMEOUT_SECONDS,
        raw_dataset: str = DEFAULT_LIVE_RECORDER_RAW_DATASET,
        logger: logging.Logger | None = None,
    ) -> None:
        if session_seconds is not None and session_seconds <= 0:
            raise ValueError("session_seconds must be greater than zero when provided.")
        if max_messages is not None and max_messages <= 0:
            raise ValueError("max_messages must be greater than zero when provided.")
        if session_seconds is None and max_messages is None:
            raise ValueError("Either session_seconds or max_messages must be provided.")
        if reconnect_attempts < 0:
            raise ValueError("reconnect_attempts cannot be negative.")
        if open_timeout_seconds <= 0:
            raise ValueError("open_timeout_seconds must be greater than zero.")
        if message_timeout_seconds <= 0:
            raise ValueError("message_timeout_seconds must be greater than zero.")

        subscription_payload = build_market_subscription(asset_ids)
        self.asset_ids = tuple(subscription_payload["assets_ids"])
        self.channel_url = market_channel_url(ws_base_url)
        self.subscription_payload = subscription_payload
        self.raw_store = raw_store
        self.warehouse = warehouse
        self.session_seconds = session_seconds
        self.max_messages = max_messages
        self.reconnect_attempts = reconnect_attempts
        self.open_timeout_seconds = open_timeout_seconds
        self.message_timeout_seconds = message_timeout_seconds
        self.ping_interval_seconds = ping_interval_seconds
        self.ping_timeout_seconds = ping_timeout_seconds
        self.raw_dataset = raw_dataset
        self.logger = logger or logging.getLogger(__name__)

    async def run(self) -> LiveMarketRecorderSummary:
        started_at = datetime.now(UTC)
        deadline = (
            started_at + timedelta(seconds=self.session_seconds)
            if self.session_seconds is not None
            else None
        )
        warnings: list[RecorderWarning] = []
        reconnects_remaining = self.reconnect_attempts
        reconnect_count = 0
        messages_received = 0
        raw_capture_count = 0
        order_book_rows = 0
        trade_rows = 0
        attempt = 0
        should_stop = False

        def record_warning(kind: str, detail: str) -> bool:
            nonlocal reconnect_count, reconnects_remaining

            warning = RecorderWarning(
                kind=kind,
                attempt=attempt,
                occurred_at=datetime.now(UTC),
                detail=detail,
            )
            warnings.append(warning)
            self.logger.warning("Live market recorder warning (%s) on attempt %d: %s", kind, attempt, detail)

            if reconnects_remaining <= 0:
                self.logger.warning(
                    "Stopping live market recorder after reconnect budget was exhausted."
                )
                return False

            reconnects_remaining -= 1
            reconnect_count += 1
            self.logger.info("Reconnecting market stream for assets=%s", ",".join(self.asset_ids))
            return True

        while not should_stop:
            if self.max_messages is not None and messages_received >= self.max_messages:
                break

            if deadline is not None and datetime.now(UTC) >= deadline:
                break

            attempt += 1

            try:
                open_timeout = _bounded_timeout(self.open_timeout_seconds, deadline)
                if open_timeout is not None and open_timeout <= 0:
                    break

                async with connect(
                    self.channel_url,
                    open_timeout=open_timeout or self.open_timeout_seconds,
                    ping_interval=self.ping_interval_seconds,
                    ping_timeout=self.ping_timeout_seconds,
                ) as websocket:
                    self.logger.info(
                        "Connected to Polymarket market stream for assets=%s",
                        ",".join(self.asset_ids),
                    )
                    await websocket.send(json.dumps(self.subscription_payload))

                    while not should_stop:
                        if self.max_messages is not None and messages_received >= self.max_messages:
                            should_stop = True
                            break

                        receive_timeout = _bounded_timeout(self.message_timeout_seconds, deadline)
                        if receive_timeout is not None and receive_timeout <= 0:
                            should_stop = True
                            break

                        try:
                            raw_message = await asyncio.wait_for(
                                websocket.recv(),
                                timeout=receive_timeout or self.message_timeout_seconds,
                            )
                        except asyncio.TimeoutError:
                            if deadline is not None and datetime.now(UTC) >= deadline:
                                should_stop = True
                                break

                            if not record_warning(
                                "receive_timeout",
                                f"No market-channel message arrived within {self.message_timeout_seconds} seconds.",
                            ):
                                should_stop = True
                            break

                        received_at = datetime.now(UTC)
                        raw_text, payload = decode_websocket_message(raw_message)
                        raw_capture_count += self._persist_raw_message(
                            payload=payload,
                            raw_text=raw_text,
                            attempt=attempt,
                            message_index=messages_received,
                            received_at=received_at,
                        )
                        snapshots, trades = normalize_market_message(payload)
                        order_book_rows += self.warehouse.upsert_order_book_snapshots(
                            snapshots,
                            source=WEBSOCKET_SOURCE,
                            collection_time=received_at,
                        )
                        trade_rows += self.warehouse.upsert_trades(
                            trades,
                            source=WEBSOCKET_SOURCE,
                            collection_time=received_at,
                        )
                        messages_received += 1
            except ConnectionClosed as exc:
                received_close = getattr(exc, "rcvd", None)
                detail = (
                    "connection closed"
                    f" code={getattr(received_close, 'code', None)}"
                    f" reason={getattr(received_close, 'reason', '')}"
                )
                if not record_warning("connection_closed", detail):
                    should_stop = True
            except TimeoutError as exc:
                if deadline is not None and datetime.now(UTC) >= deadline:
                    should_stop = True
                    continue

                if not record_warning("open_timeout", str(exc) or "Timed out opening the market stream."):
                    should_stop = True

        ended_at = datetime.now(UTC)

        if messages_received == 0:
            raise RuntimeError("Live market recorder finished without capturing any market-channel messages.")

        return LiveMarketRecorderSummary(
            asset_ids=self.asset_ids,
            started_at=started_at,
            ended_at=ended_at,
            messages_received=messages_received,
            raw_capture_count=raw_capture_count,
            order_book_rows=order_book_rows,
            trade_rows=trade_rows,
            reconnect_count=reconnect_count,
            warnings=tuple(warnings),
        )

    def _persist_raw_message(
        self,
        *,
        payload: Any,
        raw_text: str,
        attempt: int,
        message_index: int,
        received_at: datetime,
    ) -> int:
        self.raw_store.write_capture(
            "websocket",
            self.raw_dataset,
            {
                "attempt": attempt,
                "channel": "market",
                "received_at_utc": received_at,
                "raw_text": raw_text,
                "message": payload,
            },
            endpoint=self.channel_url,
            request_params=self.subscription_payload,
            collection_time=received_at,
            metadata={
                "message_index": message_index,
                "event_types": _extract_event_types(payload),
            },
        )
        return 1


async def run_live_market_recorder(
    *,
    asset_ids: Sequence[str],
    raw_data_dir: str | Path = DEFAULT_LIVE_RECORDER_RAW_DIR,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE_PATH,
    ws_base_url: str = DEFAULT_WS_BASE_URL,
    session_seconds: int | None = DEFAULT_LIVE_RECORDER_SESSION_SECONDS,
    max_messages: int | None = None,
    reconnect_attempts: int = DEFAULT_LIVE_RECORDER_RECONNECT_ATTEMPTS,
    open_timeout_seconds: float = DEFAULT_LIVE_RECORDER_OPEN_TIMEOUT_SECONDS,
    message_timeout_seconds: float = DEFAULT_LIVE_RECORDER_MESSAGE_TIMEOUT_SECONDS,
    ping_interval_seconds: float = DEFAULT_LIVE_RECORDER_PING_INTERVAL_SECONDS,
    ping_timeout_seconds: float = DEFAULT_LIVE_RECORDER_PING_TIMEOUT_SECONDS,
    raw_dataset: str = DEFAULT_LIVE_RECORDER_RAW_DATASET,
    logger: logging.Logger | None = None,
) -> LiveMarketRecorderSummary:
    with PolymarketWarehouse(warehouse_path) as warehouse:
        recorder = LiveMarketRecorder(
            asset_ids=asset_ids,
            raw_store=RawPayloadStore(raw_data_dir),
            warehouse=warehouse,
            ws_base_url=ws_base_url,
            session_seconds=session_seconds,
            max_messages=max_messages,
            reconnect_attempts=reconnect_attempts,
            open_timeout_seconds=open_timeout_seconds,
            message_timeout_seconds=message_timeout_seconds,
            ping_interval_seconds=ping_interval_seconds,
            ping_timeout_seconds=ping_timeout_seconds,
            raw_dataset=raw_dataset,
            logger=logger,
        )
        return await recorder.run()


def normalize_market_message(payload: Any) -> tuple[tuple[TopOfBookSnapshot, ...], tuple[TradeRecord, ...]]:
    snapshots: list[TopOfBookSnapshot] = []
    trades: list[TradeRecord] = []

    for item in _iter_message_items(payload):
        if _looks_like_order_book(item):
            snapshots.append(
                TopOfBookSnapshot.from_order_book_snapshot(
                    ClobClient.parse_order_book_snapshot(item)
                )
            )
            continue

        trade = _parse_trade_record(item)
        if trade is not None:
            trades.append(trade)

    return tuple(snapshots), tuple(trades)


def _iter_message_items(payload: Any) -> tuple[dict[str, Any], ...]:
    if isinstance(payload, dict):
        return (payload,)
    if isinstance(payload, list):
        return tuple(item for item in payload if isinstance(item, dict))
    return ()


def _looks_like_order_book(item: dict[str, Any]) -> bool:
    event_type = parse_optional_str(item.get("event_type") or item.get("type"))
    return (event_type or "").lower() == "book" or "bids" in item or "asks" in item


def _parse_trade_record(item: dict[str, Any]) -> TradeRecord | None:
    event_type = (parse_optional_str(item.get("event_type") or item.get("type")) or "").lower()
    has_trade_identifier = any(
        key in item for key in ("transactionHash", "transaction_hash", "trade_id", "tradeId")
    )

    if event_type not in {"trade", "last_trade", "lasttrade"} and not has_trade_identifier:
        return None

    if "price" not in item or "size" not in item:
        return None

    trade = TradeRecord(
        proxy_wallet=parse_optional_str(
            item.get("proxyWallet") or item.get("proxy_wallet") or item.get("user")
        ),
        asset_id=parse_optional_str(item.get("asset_id") or item.get("asset")),
        condition_id=parse_optional_str(
            item.get("market") or item.get("conditionId") or item.get("condition_id")
        ),
        outcome=parse_optional_str(item.get("outcome")),
        side=parse_optional_str(item.get("side")),
        size=parse_optional_decimal(item.get("size")),
        price=parse_optional_decimal(item.get("price")),
        timestamp=parse_optional_datetime(item.get("timestamp")),
        transaction_hash=parse_optional_str(
            item.get("transactionHash") or item.get("transaction_hash")
        ),
        usdc_size=parse_optional_decimal(item.get("usdcSize") or item.get("usdc_size")),
    )

    if not trade.asset_id and not trade.condition_id:
        return None
    return trade


def _extract_event_types(payload: Any) -> list[str]:
    event_types = {
        event_type
        for item in _iter_message_items(payload)
        if (event_type := parse_optional_str(item.get("event_type") or item.get("type")))
    }
    return sorted(event_types)


def _bounded_timeout(timeout_seconds: float, deadline: datetime | None) -> float | None:
    if deadline is None:
        return timeout_seconds

    remaining_seconds = (deadline - datetime.now(UTC)).total_seconds()
    return max(0.0, min(timeout_seconds, remaining_seconds))
