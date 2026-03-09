from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from src.clients import DataApiClient, LeaderboardEntry
from src.signals import build_wallet_profile
from src.storage import DEFAULT_WAREHOUSE_PATH, PolymarketWarehouse, RawPayloadStore


DEFAULT_LEADERBOARD_CATEGORY = "OVERALL"
DEFAULT_LEADERBOARD_TIME_PERIOD = "ALL"
DEFAULT_LEADERBOARD_ORDER_BY = "PNL"
DEFAULT_LEADERBOARD_LIMIT = 25
DEFAULT_WALLET_POSITIONS_LIMIT = 100
DEFAULT_WALLET_CLOSED_POSITIONS_LIMIT = 100
DEFAULT_WALLET_ACTIVITY_LIMIT = 100
DEFAULT_RAW_DATA_DIR = Path("data/raw")


@dataclass(frozen=True, slots=True)
class WalletUniverseSelectionRule:
    leaderboard_limit: int = DEFAULT_LEADERBOARD_LIMIT
    category: str = DEFAULT_LEADERBOARD_CATEGORY
    time_period: str = DEFAULT_LEADERBOARD_TIME_PERIOD
    order_by: str = DEFAULT_LEADERBOARD_ORDER_BY

    def __post_init__(self) -> None:
        if self.leaderboard_limit <= 0:
            raise ValueError("leaderboard_limit must be greater than zero.")

    def describe(self) -> str:
        return (
            f"Use the first {self.leaderboard_limit} leaderboard rows for category={self.category}, "
            f"time_period={self.time_period}, order_by={self.order_by}, keep non-empty proxyWallet values, "
            "and dedupe wallets in API order."
        )


@dataclass(frozen=True, slots=True)
class SkippedWalletSeed:
    identifier: str
    reason: str


@dataclass(frozen=True, slots=True)
class WalletBackfillFailure:
    wallet_address: str
    dataset: str
    detail: str


@dataclass(frozen=True, slots=True)
class BackfilledWallet:
    wallet_address: str
    position_rows: int
    closed_position_rows: int
    activity_trade_rows: int
    profile_rows: int


@dataclass(frozen=True, slots=True)
class WalletBackfillSummary:
    selection_rule: str
    seed_wallet_addresses: tuple[str, ...]
    position_rows: int
    closed_position_rows: int
    activity_trade_rows: int
    wallet_profile_rows: int
    raw_capture_count: int
    skipped_wallets: tuple[SkippedWalletSeed, ...]
    failures: tuple[WalletBackfillFailure, ...]
    wallet_results: tuple[BackfilledWallet, ...]

    @property
    def has_failures(self) -> bool:
        return bool(self.failures)


def select_wallet_seeds(
    entries: list[LeaderboardEntry],
    rule: WalletUniverseSelectionRule,
) -> tuple[list[LeaderboardEntry], list[SkippedWalletSeed]]:
    selected_entries: list[LeaderboardEntry] = []
    skipped_entries: list[SkippedWalletSeed] = []
    seen_wallets: set[str] = set()

    for entry in entries[: rule.leaderboard_limit]:
        wallet_address = entry.proxy_wallet
        if not wallet_address:
            skipped_entries.append(
                SkippedWalletSeed(identifier=_leaderboard_identifier(entry), reason="missing_proxy_wallet")
            )
            continue
        if wallet_address in seen_wallets:
            skipped_entries.append(SkippedWalletSeed(identifier=wallet_address, reason="duplicate_wallet"))
            continue
        seen_wallets.add(wallet_address)
        selected_entries.append(entry)

    return selected_entries, skipped_entries


class WalletBackfillJob:
    def __init__(
        self,
        *,
        data_api_client: DataApiClient,
        raw_store: RawPayloadStore,
        warehouse: PolymarketWarehouse,
        selection_rule: WalletUniverseSelectionRule | None = None,
        positions_limit: int = DEFAULT_WALLET_POSITIONS_LIMIT,
        closed_positions_limit: int = DEFAULT_WALLET_CLOSED_POSITIONS_LIMIT,
        activity_limit: int = DEFAULT_WALLET_ACTIVITY_LIMIT,
        collection_time: datetime | None = None,
        as_of_time: datetime | None = None,
    ) -> None:
        if positions_limit <= 0:
            raise ValueError("positions_limit must be greater than zero.")
        if closed_positions_limit <= 0:
            raise ValueError("closed_positions_limit must be greater than zero.")
        if activity_limit <= 0:
            raise ValueError("activity_limit must be greater than zero.")

        self.data_api_client = data_api_client
        self.raw_store = raw_store
        self.warehouse = warehouse
        self.selection_rule = selection_rule or WalletUniverseSelectionRule()
        self.positions_limit = positions_limit
        self.closed_positions_limit = closed_positions_limit
        self.activity_limit = activity_limit
        self.collection_time = collection_time
        self.as_of_time = as_of_time

    def run(self) -> WalletBackfillSummary:
        collected_at = self.collection_time or datetime.now(UTC)
        as_of_time = self.as_of_time or collected_at
        failures: list[WalletBackfillFailure] = []
        raw_capture_count = 0

        leaderboard_payload = self.data_api_client.get_leaderboard_payload(
            category=self.selection_rule.category,
            time_period=self.selection_rule.time_period,
            order_by=self.selection_rule.order_by,
            limit=self.selection_rule.leaderboard_limit,
        )
        self.raw_store.write_capture(
            "data_api",
            "wallet_universe_leaderboard",
            leaderboard_payload,
            endpoint="/v1/leaderboard",
            request_params={
                "category": self.selection_rule.category,
                "timePeriod": self.selection_rule.time_period,
                "orderBy": self.selection_rule.order_by,
                "limit": self.selection_rule.leaderboard_limit,
            },
            collection_time=collected_at,
            metadata={"selection_rule": self.selection_rule.describe()},
        )
        raw_capture_count += 1

        leaderboard_entries = self.data_api_client.parse_leaderboard(leaderboard_payload)
        selected_entries, skipped_wallets = select_wallet_seeds(leaderboard_entries, self.selection_rule)
        seed_wallet_addresses = tuple(entry.proxy_wallet for entry in selected_entries if entry.proxy_wallet)

        self.raw_store.write_capture(
            "data_api",
            "wallet_seed_list",
            [_seed_payload(entry) for entry in selected_entries],
            endpoint="/v1/leaderboard",
            request_params={
                "category": self.selection_rule.category,
                "timePeriod": self.selection_rule.time_period,
                "orderBy": self.selection_rule.order_by,
                "limit": self.selection_rule.leaderboard_limit,
            },
            collection_time=collected_at,
            metadata={"selection_rule": self.selection_rule.describe()},
        )
        raw_capture_count += 1

        if not seed_wallet_addresses:
            failures.append(
                WalletBackfillFailure(
                    wallet_address="wallet_universe",
                    dataset="data_api.wallet_universe",
                    detail="No wallet seeds matched the leaderboard-only selection rule.",
                )
            )
            return WalletBackfillSummary(
                selection_rule=self.selection_rule.describe(),
                seed_wallet_addresses=(),
                position_rows=0,
                closed_position_rows=0,
                activity_trade_rows=0,
                wallet_profile_rows=0,
                raw_capture_count=raw_capture_count,
                skipped_wallets=tuple(skipped_wallets),
                failures=tuple(failures),
                wallet_results=(),
            )

        total_position_rows = 0
        total_closed_position_rows = 0
        total_activity_trade_rows = 0
        total_wallet_profile_rows = 0
        wallet_results: list[BackfilledWallet] = []

        for wallet_address in seed_wallet_addresses:
            wallet_position_rows = 0
            wallet_closed_position_rows = 0
            wallet_activity_trade_rows = 0
            wallet_profile_rows = 0
            parsed_closed_positions = []
            parsed_activity = []

            try:
                positions_payload = self.data_api_client.get_positions_payload(
                    wallet_address,
                    limit=self.positions_limit,
                )
                self.raw_store.write_capture(
                    "data_api",
                    "wallet_positions",
                    positions_payload,
                    endpoint="/positions",
                    request_params={"user": wallet_address, "limit": self.positions_limit, "sortBy": "TOKENS"},
                    collection_time=collected_at,
                    metadata={"wallet_address": wallet_address},
                )
                raw_capture_count += 1
                parsed_positions = self.data_api_client.parse_positions(positions_payload)
                inserted_rows = self.warehouse.upsert_wallet_positions(
                    parsed_positions,
                    source="data_api.wallet_positions",
                    collection_time=collected_at,
                )
                wallet_position_rows += inserted_rows
                total_position_rows += inserted_rows
            except Exception as exc:
                failures.append(
                    WalletBackfillFailure(
                        wallet_address=wallet_address,
                        dataset="data_api.positions",
                        detail=str(exc),
                    )
                )

            try:
                closed_positions_payload = self.data_api_client.get_closed_positions_payload(
                    wallet_address,
                    limit=self.closed_positions_limit,
                )
                self.raw_store.write_capture(
                    "data_api",
                    "wallet_closed_positions",
                    closed_positions_payload,
                    endpoint="/closed-positions",
                    request_params={
                        "user": wallet_address,
                        "limit": self.closed_positions_limit,
                        "sortBy": "TIMESTAMP",
                    },
                    collection_time=collected_at,
                    metadata={"wallet_address": wallet_address},
                )
                raw_capture_count += 1
                parsed_closed_positions = self.data_api_client.parse_closed_positions(closed_positions_payload)
                inserted_rows = self.warehouse.upsert_wallet_closed_positions(
                    parsed_closed_positions,
                    source="data_api.wallet_closed_positions",
                    collection_time=collected_at,
                )
                wallet_closed_position_rows += inserted_rows
                total_closed_position_rows += inserted_rows
            except Exception as exc:
                failures.append(
                    WalletBackfillFailure(
                        wallet_address=wallet_address,
                        dataset="data_api.closed_positions",
                        detail=str(exc),
                    )
                )

            try:
                activity_payload = self.data_api_client.get_activity_payload(
                    wallet_address,
                    limit=self.activity_limit,
                )
                self.raw_store.write_capture(
                    "data_api",
                    "wallet_activity",
                    activity_payload,
                    endpoint="/activity",
                    request_params={
                        "user": wallet_address,
                        "limit": self.activity_limit,
                        "type": "TRADE",
                        "sortBy": "TIMESTAMP",
                        "sortDirection": "DESC",
                    },
                    collection_time=collected_at,
                    metadata={"wallet_address": wallet_address},
                )
                raw_capture_count += 1
                parsed_activity = self.data_api_client.parse_activity(activity_payload)
                inserted_rows = self.warehouse.upsert_trades(
                    parsed_activity,
                    source="data_api.wallet_activity",
                    collection_time=collected_at,
                )
                wallet_activity_trade_rows += inserted_rows
                total_activity_trade_rows += inserted_rows
            except Exception as exc:
                failures.append(
                    WalletBackfillFailure(
                        wallet_address=wallet_address,
                        dataset="data_api.activity",
                        detail=str(exc),
                    )
                )

            try:
                wallet_profile = build_wallet_profile(
                    wallet_address,
                    closed_positions=parsed_closed_positions,
                    activity_trades=parsed_activity,
                    as_of_time=as_of_time,
                )
                inserted_rows = self.warehouse.upsert_wallet_profiles(
                    [wallet_profile],
                    source="signals.wallet_profiles",
                    collection_time=collected_at,
                )
                wallet_profile_rows += inserted_rows
                total_wallet_profile_rows += inserted_rows
            except Exception as exc:
                failures.append(
                    WalletBackfillFailure(
                        wallet_address=wallet_address,
                        dataset="signals.wallet_profiles",
                        detail=str(exc),
                    )
                )

            wallet_results.append(
                BackfilledWallet(
                    wallet_address=wallet_address,
                    position_rows=wallet_position_rows,
                    closed_position_rows=wallet_closed_position_rows,
                    activity_trade_rows=wallet_activity_trade_rows,
                    profile_rows=wallet_profile_rows,
                )
            )

        return WalletBackfillSummary(
            selection_rule=self.selection_rule.describe(),
            seed_wallet_addresses=seed_wallet_addresses,
            position_rows=total_position_rows,
            closed_position_rows=total_closed_position_rows,
            activity_trade_rows=total_activity_trade_rows,
            wallet_profile_rows=total_wallet_profile_rows,
            raw_capture_count=raw_capture_count,
            skipped_wallets=tuple(skipped_wallets),
            failures=tuple(failures),
            wallet_results=tuple(wallet_results),
        )


def run_wallet_backfill(
    *,
    raw_data_dir: str | Path = DEFAULT_RAW_DATA_DIR,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE_PATH,
    selection_rule: WalletUniverseSelectionRule | None = None,
    positions_limit: int = DEFAULT_WALLET_POSITIONS_LIMIT,
    closed_positions_limit: int = DEFAULT_WALLET_CLOSED_POSITIONS_LIMIT,
    activity_limit: int = DEFAULT_WALLET_ACTIVITY_LIMIT,
    collection_time: datetime | None = None,
    as_of_time: datetime | None = None,
) -> WalletBackfillSummary:
    with DataApiClient() as data_api_client, PolymarketWarehouse(warehouse_path) as warehouse:
        job = WalletBackfillJob(
            data_api_client=data_api_client,
            raw_store=RawPayloadStore(raw_data_dir),
            warehouse=warehouse,
            selection_rule=selection_rule,
            positions_limit=positions_limit,
            closed_positions_limit=closed_positions_limit,
            activity_limit=activity_limit,
            collection_time=collection_time,
            as_of_time=as_of_time,
        )
        return job.run()


def _leaderboard_identifier(entry: LeaderboardEntry) -> str:
    if entry.proxy_wallet:
        return entry.proxy_wallet
    if entry.rank is not None:
        return f"rank:{entry.rank}"
    return "<unknown>"


def _seed_payload(entry: LeaderboardEntry) -> dict[str, object]:
    return {
        "wallet_address": entry.proxy_wallet,
        "rank": entry.rank,
        "pnl": str(entry.pnl) if entry.pnl is not None else None,
        "volume": str(entry.volume) if entry.volume is not None else None,
        "user_name": entry.user_name,
        "verified_badge": entry.verified_badge,
    }
