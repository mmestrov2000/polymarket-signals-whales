from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any

import duckdb

from src.storage import DEFAULT_WAREHOUSE_PATH, EventDatasetRow, PolymarketWarehouse


ZERO_DECIMAL = Decimal("0")
TEN_THOUSAND = Decimal("10000")
DEFAULT_EVENT_DATASET_OUTPUT_DIR = Path("data/research/event_datasets")


@dataclass(frozen=True, slots=True)
class TradeCostAssumptions:
    """Round-trip cost model used for leakage-safe post-event labels."""

    fee_bps_per_side: Decimal = Decimal("10")
    slippage_bps_per_side: Decimal = Decimal("15")
    fallback_spread_bps: Decimal = Decimal("25")

    def __post_init__(self) -> None:
        if self.fee_bps_per_side < ZERO_DECIMAL:
            raise ValueError("fee_bps_per_side cannot be negative.")
        if self.slippage_bps_per_side < ZERO_DECIMAL:
            raise ValueError("slippage_bps_per_side cannot be negative.")
        if self.fallback_spread_bps < ZERO_DECIMAL:
            raise ValueError("fallback_spread_bps cannot be negative.")

    def round_trip_cost_bps(self, observed_spread_bps: Decimal | None) -> Decimal:
        effective_spread_bps = observed_spread_bps if observed_spread_bps is not None else self.fallback_spread_bps
        return effective_spread_bps + (self.fee_bps_per_side + self.slippage_bps_per_side) * Decimal("2")


@dataclass(frozen=True, slots=True)
class EventLabelConfig:
    horizons: tuple[timedelta, ...] = (
        timedelta(minutes=5),
        timedelta(minutes=15),
        timedelta(minutes=60),
    )
    primary_horizon: timedelta = timedelta(minutes=15)
    primary_target: str = "profitable_after_costs"

    def __post_init__(self) -> None:
        if not self.horizons:
            raise ValueError("At least one horizon must be configured.")
        if any(horizon <= timedelta(0) for horizon in self.horizons):
            raise ValueError("All horizons must be greater than zero.")
        if self.primary_horizon not in self.horizons:
            raise ValueError("primary_horizon must be included in horizons.")
        if self.primary_target not in {"continuation", "reversion", "profitable_after_costs"}:
            raise ValueError("primary_target must be continuation, reversion, or profitable_after_costs.")


@dataclass(frozen=True, slots=True)
class DatasetSplitConfig:
    validation_fraction: Decimal = Decimal("0.20")
    minimum_validation_rows: int = 1

    def __post_init__(self) -> None:
        if not (ZERO_DECIMAL <= self.validation_fraction < Decimal("1")):
            raise ValueError("validation_fraction must be in the range [0, 1).")
        if self.minimum_validation_rows < 0:
            raise ValueError("minimum_validation_rows cannot be negative.")


@dataclass(frozen=True, slots=True)
class DatasetQAConfig:
    max_null_fraction: Decimal = Decimal("0.35")
    checked_columns: tuple[str, ...] = (
        "asset_id",
        "event_time_utc",
        "direction",
        "recent_trade_count",
        "recent_volume_usdc",
        "active_wallet_count",
        "entry_price",
        "entry_price_time_utc",
        "primary_label_profitable",
        "primary_net_pnl_bps",
        "primary_exit_price",
        "primary_exit_time_utc",
    )

    def __post_init__(self) -> None:
        if not (ZERO_DECIMAL <= self.max_null_fraction < Decimal("1")):
            raise ValueError("max_null_fraction must be in the range [0, 1).")


@dataclass(frozen=True, slots=True)
class EventDatasetBuildConfig:
    label_config: EventLabelConfig = field(default_factory=EventLabelConfig)
    cost_assumptions: TradeCostAssumptions = field(default_factory=TradeCostAssumptions)
    split_config: DatasetSplitConfig = field(default_factory=DatasetSplitConfig)
    qa_config: DatasetQAConfig = field(default_factory=DatasetQAConfig)


@dataclass(frozen=True, slots=True)
class HorizonLabel:
    horizon_minutes: int
    target_time_utc: datetime
    exit_time_utc: datetime | None
    exit_price: Decimal | None
    directional_return_bps: Decimal | None
    net_pnl_bps: Decimal | None
    continuation: bool | None
    reversion: bool | None
    profitable_after_costs: bool | None


@dataclass(frozen=True, slots=True)
class DatasetArtifactPaths:
    build_dir: Path
    summary_path: Path
    qa_report_path: Path


@dataclass(frozen=True, slots=True)
class DatasetQAReport:
    build_id: str
    source_event_count: int
    materialized_row_count: int
    dropped_non_directional_count: int
    dropped_missing_entry_price_count: int
    dropped_missing_primary_label_count: int
    split_counts: dict[str, int]
    duplicate_event_ids: tuple[str, ...]
    null_fraction_by_column: dict[str, str]
    null_heavy_columns: tuple[str, ...]
    impossible_timestamp_event_ids: tuple[str, ...]
    wallet_leakage_event_ids: tuple[str, ...]
    future_label_leakage_event_ids: tuple[str, ...]
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def has_failures(self) -> bool:
        return bool(self.errors)


@dataclass(frozen=True, slots=True)
class EventDatasetBuild:
    build_id: str
    config: EventDatasetBuildConfig
    rows: tuple[EventDatasetRow, ...]
    qa_report: DatasetQAReport
    primary_label_name: str
    primary_label_horizon_minutes: int


@dataclass(frozen=True, slots=True)
class EventDatasetBuildResult:
    build: EventDatasetBuild
    artifact_paths: DatasetArtifactPaths
    rows_written: int


class DatasetIntegrityError(RuntimeError):
    def __init__(self, report: DatasetQAReport, artifact_paths: DatasetArtifactPaths) -> None:
        self.report = report
        self.artifact_paths = artifact_paths
        super().__init__(self._build_message())

    def _build_message(self) -> str:
        return (
            f"Dataset QA failed for build {self.report.build_id}. "
            f"See {self.artifact_paths.qa_report_path} for details."
        )


@dataclass(frozen=True, slots=True)
class _StoredSignalEvent:
    event_id: str
    asset_id: str | None
    condition_id: str | None
    event_time_utc: datetime
    source_event_collection_time_utc: datetime
    direction: str | None
    trigger_reason: str
    recent_trade_count: int
    recent_volume_usdc: Decimal
    volume_zscore: Decimal | None
    trade_count_zscore: Decimal | None
    order_flow_imbalance: Decimal | None
    short_return: Decimal | None
    medium_return: Decimal | None
    liquidity_features_available: bool
    active_wallet_count: int
    profiled_wallet_count: int
    top_wallet_share: Decimal | None
    weighted_average_quality: Decimal | None
    explanation_payload: dict[str, Any]


def materialize_event_dataset(
    *,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE_PATH,
    output_dir: str | Path = DEFAULT_EVENT_DATASET_OUTPUT_DIR,
    build_id: str | None = None,
    config: EventDatasetBuildConfig | None = None,
) -> EventDatasetBuildResult:
    dataset_config = config or EventDatasetBuildConfig()
    resolved_build_id = build_id or _default_build_id()
    stored_events = _load_signal_events(warehouse_path)
    price_history = _load_price_history(warehouse_path)
    build = build_event_dataset(
        stored_events=stored_events,
        price_history=price_history,
        build_id=resolved_build_id,
        config=dataset_config,
    )
    artifact_paths = write_event_dataset_artifacts(build=build, output_dir=output_dir)
    if build.qa_report.has_failures:
        raise DatasetIntegrityError(build.qa_report, artifact_paths)

    with PolymarketWarehouse(warehouse_path) as warehouse:
        rows_written = warehouse.upsert_event_dataset_rows(build.rows)

    return EventDatasetBuildResult(
        build=build,
        artifact_paths=artifact_paths,
        rows_written=rows_written,
    )


def build_event_dataset(
    *,
    stored_events: tuple[_StoredSignalEvent, ...],
    price_history: dict[str, tuple[tuple[datetime, Decimal], ...]],
    build_id: str,
    config: EventDatasetBuildConfig | None = None,
) -> EventDatasetBuild:
    dataset_config = config or EventDatasetBuildConfig()
    sorted_events = tuple(sorted(stored_events, key=lambda event: (event.event_time_utc, event.event_id)))
    unsplit_rows: list[EventDatasetRow] = []

    dropped_non_directional_count = 0
    dropped_missing_entry_price_count = 0
    dropped_missing_primary_label_count = 0
    wallet_leakage_event_ids: list[str] = []
    future_label_leakage_event_ids: list[str] = []
    impossible_timestamp_event_ids: list[str] = []

    for event in sorted_events:
        if event.direction not in {"up", "down"}:
            dropped_non_directional_count += 1
            continue

        event_price_points = price_history.get(event.asset_id or "")
        if not event_price_points:
            dropped_missing_entry_price_count += 1
            continue

        entry_point = _latest_point_at_or_before(event_price_points, event.event_time_utc)
        if entry_point is None:
            dropped_missing_entry_price_count += 1
            continue

        if entry_point[0] > event.event_time_utc:
            future_label_leakage_event_ids.append(event.event_id)
            impossible_timestamp_event_ids.append(event.event_id)
            continue

        market_context = _json_object(event.explanation_payload.get("market_context"))
        wallet_context = _json_object(event.explanation_payload.get("wallet_context"))
        wallet_profile_times = _participant_profile_times(wallet_context)
        if any(profile_time > event.event_time_utc for profile_time in wallet_profile_times):
            wallet_leakage_event_ids.append(event.event_id)

        observed_spread_bps = _parse_decimal(market_context.get("latest_spread_bps"))
        round_trip_cost_bps = dataset_config.cost_assumptions.round_trip_cost_bps(observed_spread_bps)

        horizon_labels: dict[str, dict[str, Any]] = {}
        primary_label: HorizonLabel | None = None

        for horizon in dataset_config.label_config.horizons:
            label = _build_horizon_label(
                event_direction=event.direction,
                event_time=event.event_time_utc,
                entry_price=entry_point[1],
                price_points=event_price_points,
                horizon=horizon,
                round_trip_cost_bps=round_trip_cost_bps,
            )
            horizon_labels[_format_horizon_key(horizon)] = _serialize_value(asdict(label))
            if label.exit_time_utc is not None and label.exit_time_utc <= event.event_time_utc:
                future_label_leakage_event_ids.append(event.event_id)
                impossible_timestamp_event_ids.append(event.event_id)
            if horizon == dataset_config.label_config.primary_horizon:
                primary_label = label

        if primary_label is None or primary_label.exit_time_utc is None or primary_label.exit_price is None:
            dropped_missing_primary_label_count += 1
            continue
        if primary_label.net_pnl_bps is None or primary_label.directional_return_bps is None:
            dropped_missing_primary_label_count += 1
            continue
        if (
            primary_label.continuation is None
            or primary_label.reversion is None
            or primary_label.profitable_after_costs is None
        ):
            dropped_missing_primary_label_count += 1
            continue

        row = EventDatasetRow(
            dataset_row_id=_build_dataset_row_id(build_id=build_id, event_id=event.event_id),
            dataset_build_id=build_id,
            dataset_split="pending",
            event_id=event.event_id,
            asset_id=event.asset_id,
            condition_id=event.condition_id,
            event_time_utc=event.event_time_utc,
            source_event_collection_time_utc=event.source_event_collection_time_utc,
            direction=event.direction,
            trigger_reason=event.trigger_reason,
            recent_trade_count=event.recent_trade_count,
            recent_volume_usdc=event.recent_volume_usdc,
            volume_zscore=event.volume_zscore,
            trade_count_zscore=event.trade_count_zscore,
            order_flow_imbalance=event.order_flow_imbalance,
            short_return=event.short_return,
            medium_return=event.medium_return,
            liquidity_features_available=event.liquidity_features_available,
            latest_price=_parse_decimal(market_context.get("latest_price")),
            latest_mid_price=_parse_decimal(market_context.get("latest_mid_price")),
            latest_spread_bps=observed_spread_bps,
            spread_change_bps=_parse_decimal(market_context.get("spread_change_bps")),
            top_of_book_depth_usdc=_parse_decimal(market_context.get("top_of_book_depth_usdc")),
            depth_change_ratio=_parse_decimal(market_context.get("depth_change_ratio")),
            depth_imbalance=_parse_decimal(market_context.get("depth_imbalance")),
            active_wallet_count=event.active_wallet_count,
            profiled_wallet_count=event.profiled_wallet_count,
            sparse_wallet_set=bool(wallet_context.get("sparse_wallet_set", False)),
            profiled_volume_share=_parse_decimal(wallet_context.get("profiled_volume_share")),
            top_wallet_share=event.top_wallet_share,
            concentration_hhi=_parse_decimal(wallet_context.get("concentration_hhi")),
            weighted_average_quality=event.weighted_average_quality,
            weighted_average_realized_roi=_parse_decimal(wallet_context.get("weighted_average_realized_roi")),
            weighted_average_hit_rate=_parse_decimal(wallet_context.get("weighted_average_hit_rate")),
            weighted_average_realized_pnl=_parse_decimal(wallet_context.get("weighted_average_realized_pnl")),
            entry_price=entry_point[1],
            entry_price_time_utc=entry_point[0],
            assumed_round_trip_cost_bps=round_trip_cost_bps,
            primary_label_name=dataset_config.label_config.primary_target,
            primary_label_horizon_minutes=primary_label.horizon_minutes,
            primary_label_continuation=primary_label.continuation,
            primary_label_reversion=primary_label.reversion,
            primary_label_profitable=primary_label.profitable_after_costs,
            primary_directional_return_bps=primary_label.directional_return_bps,
            primary_net_pnl_bps=primary_label.net_pnl_bps,
            primary_exit_price=primary_label.exit_price,
            primary_exit_time_utc=primary_label.exit_time_utc,
            horizon_labels_json=horizon_labels,
        )
        unsplit_rows.append(row)

    rows = _assign_time_splits(tuple(unsplit_rows), dataset_config.split_config)
    qa_report = _build_qa_report(
        build_id=build_id,
        rows=rows,
        source_event_count=len(sorted_events),
        dropped_non_directional_count=dropped_non_directional_count,
        dropped_missing_entry_price_count=dropped_missing_entry_price_count,
        dropped_missing_primary_label_count=dropped_missing_primary_label_count,
        wallet_leakage_event_ids=wallet_leakage_event_ids,
        future_label_leakage_event_ids=future_label_leakage_event_ids,
        impossible_timestamp_event_ids=impossible_timestamp_event_ids,
        config=dataset_config.qa_config,
    )
    return EventDatasetBuild(
        build_id=build_id,
        config=dataset_config,
        rows=rows,
        qa_report=qa_report,
        primary_label_name=dataset_config.label_config.primary_target,
        primary_label_horizon_minutes=int(dataset_config.label_config.primary_horizon.total_seconds() // 60),
    )


def write_event_dataset_artifacts(
    *,
    build: EventDatasetBuild,
    output_dir: str | Path = DEFAULT_EVENT_DATASET_OUTPUT_DIR,
) -> DatasetArtifactPaths:
    build_dir = Path(output_dir) / build.build_id
    build_dir.mkdir(parents=True, exist_ok=True)

    summary_path = build_dir / "summary.json"
    qa_report_path = build_dir / "qa_report.json"

    summary_path.write_text(
        json.dumps(
            {
                "build_id": build.build_id,
                "primary_label_name": build.primary_label_name,
                "primary_label_horizon_minutes": build.primary_label_horizon_minutes,
                "trade_cost_assumptions": _serialize_value(asdict(build.config.cost_assumptions)),
                "label_config": _serialize_value(asdict(build.config.label_config)),
                "split_config": _serialize_value(asdict(build.config.split_config)),
                "row_count": len(build.rows),
            },
            indent=2,
            sort_keys=True,
        )
    )
    qa_report_path.write_text(
        json.dumps(
            _serialize_value(asdict(build.qa_report)),
            indent=2,
            sort_keys=True,
        )
    )

    return DatasetArtifactPaths(
        build_dir=build_dir,
        summary_path=summary_path,
        qa_report_path=qa_report_path,
    )


def _load_signal_events(warehouse_path: str | Path) -> tuple[_StoredSignalEvent, ...]:
    rows = _fetch_rows(
        warehouse_path,
        """
        SELECT
            event_id,
            asset_id,
            condition_id,
            event_time_utc,
            collection_time_utc,
            direction,
            trigger_reason,
            recent_trade_count,
            recent_volume_usdc,
            volume_zscore,
            trade_count_zscore,
            order_flow_imbalance,
            short_return,
            medium_return,
            liquidity_features_available,
            active_wallet_count,
            profiled_wallet_count,
            top_wallet_share,
            weighted_average_quality,
            explanation_payload_json
        FROM signal_events
        ORDER BY event_time_utc ASC, event_id ASC
        """,
    )

    return tuple(
        _StoredSignalEvent(
            event_id=str(row["event_id"]),
            asset_id=_coerce_optional_str(row.get("asset_id")),
            condition_id=_coerce_optional_str(row.get("condition_id")),
            event_time_utc=_normalize_utc_timestamp(row["event_time_utc"]),
            source_event_collection_time_utc=_normalize_utc_timestamp(row["collection_time_utc"]),
            direction=_coerce_optional_str(row.get("direction")),
            trigger_reason=str(row["trigger_reason"]),
            recent_trade_count=int(row["recent_trade_count"]),
            recent_volume_usdc=_coerce_decimal(row["recent_volume_usdc"]),
            volume_zscore=_coerce_optional_decimal(row.get("volume_zscore")),
            trade_count_zscore=_coerce_optional_decimal(row.get("trade_count_zscore")),
            order_flow_imbalance=_coerce_optional_decimal(row.get("order_flow_imbalance")),
            short_return=_coerce_optional_decimal(row.get("short_return")),
            medium_return=_coerce_optional_decimal(row.get("medium_return")),
            liquidity_features_available=bool(row["liquidity_features_available"]),
            active_wallet_count=int(row["active_wallet_count"]),
            profiled_wallet_count=int(row["profiled_wallet_count"]),
            top_wallet_share=_coerce_optional_decimal(row.get("top_wallet_share")),
            weighted_average_quality=_coerce_optional_decimal(row.get("weighted_average_quality")),
            explanation_payload=_json_object(json.loads(str(row["explanation_payload_json"]))),
        )
        for row in rows
    )


def _load_price_history(warehouse_path: str | Path) -> dict[str, tuple[tuple[datetime, Decimal], ...]]:
    rows = _fetch_rows(
        warehouse_path,
        """
        SELECT token_id, price_time_utc, price
        FROM (
            SELECT
                token_id,
                price_time_utc,
                price,
                ROW_NUMBER() OVER (
                    PARTITION BY token_id, price_time_utc
                    ORDER BY collection_time_utc DESC, updated_at_utc DESC, fidelity ASC, price_id DESC
                ) AS row_rank
            FROM price_history
            WHERE price IS NOT NULL
        )
        WHERE row_rank = 1
        ORDER BY token_id ASC, price_time_utc ASC
        """,
    )

    grouped: dict[str, list[tuple[datetime, Decimal]]] = {}
    for row in rows:
        token_id = _coerce_optional_str(row.get("token_id"))
        if not token_id:
            continue
        grouped.setdefault(token_id, []).append(
            (
                _normalize_utc_timestamp(row["price_time_utc"]),
                _coerce_decimal(row["price"]),
            )
        )

    return {token_id: tuple(points) for token_id, points in grouped.items()}


def _build_horizon_label(
    *,
    event_direction: str,
    event_time: datetime,
    entry_price: Decimal,
    price_points: tuple[tuple[datetime, Decimal], ...],
    horizon: timedelta,
    round_trip_cost_bps: Decimal,
) -> HorizonLabel:
    horizon_minutes = int(horizon.total_seconds() // 60)
    target_time = event_time + horizon
    exit_point = _first_point_at_or_after(price_points, target_time)
    if exit_point is None:
        return HorizonLabel(
            horizon_minutes=horizon_minutes,
            target_time_utc=target_time,
            exit_time_utc=None,
            exit_price=None,
            directional_return_bps=None,
            net_pnl_bps=None,
            continuation=None,
            reversion=None,
            profitable_after_costs=None,
        )

    direction_multiplier = Decimal("1") if event_direction == "up" else Decimal("-1")
    raw_return = (exit_point[1] - entry_price) / entry_price
    directional_return = raw_return * direction_multiplier
    directional_return_bps = directional_return * TEN_THOUSAND
    net_pnl_bps = directional_return_bps - round_trip_cost_bps

    return HorizonLabel(
        horizon_minutes=horizon_minutes,
        target_time_utc=target_time,
        exit_time_utc=exit_point[0],
        exit_price=exit_point[1],
        directional_return_bps=directional_return_bps,
        net_pnl_bps=net_pnl_bps,
        continuation=directional_return > ZERO_DECIMAL,
        reversion=directional_return < ZERO_DECIMAL,
        profitable_after_costs=net_pnl_bps > ZERO_DECIMAL,
    )


def _assign_time_splits(
    rows: tuple[EventDatasetRow, ...],
    split_config: DatasetSplitConfig,
) -> tuple[EventDatasetRow, ...]:
    if len(rows) <= 1:
        return tuple(
            EventDatasetRow(
                **{
                    **asdict(row),
                    "dataset_split": "train",
                }
            )
            for row in rows
        )

    validation_count = max(
        split_config.minimum_validation_rows,
        int(len(rows) * float(split_config.validation_fraction)),
    )
    validation_count = min(validation_count, len(rows) - 1)
    split_index = len(rows) - validation_count

    assigned_rows: list[EventDatasetRow] = []
    for index, row in enumerate(rows):
        assigned_rows.append(
            EventDatasetRow(
                **{
                    **asdict(row),
                    "dataset_split": "train" if index < split_index else "validation",
                }
            )
        )
    return tuple(assigned_rows)


def _build_qa_report(
    *,
    build_id: str,
    rows: tuple[EventDatasetRow, ...],
    source_event_count: int,
    dropped_non_directional_count: int,
    dropped_missing_entry_price_count: int,
    dropped_missing_primary_label_count: int,
    wallet_leakage_event_ids: list[str],
    future_label_leakage_event_ids: list[str],
    impossible_timestamp_event_ids: list[str],
    config: DatasetQAConfig,
) -> DatasetQAReport:
    duplicate_event_ids = _duplicate_event_ids(rows)
    split_counts = _split_counts(rows)
    null_fraction_by_column = _null_fraction_by_column(rows)
    null_heavy_columns = tuple(
        sorted(
            column
            for column in config.checked_columns
            if Decimal(null_fraction_by_column.get(column, "0")) > config.max_null_fraction
        )
    )

    split_order_violation = _has_split_order_violation(rows)
    errors = list(_unique_preserving_order(_build_error_messages(
        duplicate_event_ids=duplicate_event_ids,
        null_heavy_columns=null_heavy_columns,
        impossible_timestamp_event_ids=impossible_timestamp_event_ids,
        wallet_leakage_event_ids=wallet_leakage_event_ids,
        future_label_leakage_event_ids=future_label_leakage_event_ids,
        split_order_violation=split_order_violation,
        materialized_row_count=len(rows),
    )))
    warnings = list(_unique_preserving_order(_build_warning_messages(
        source_event_count=source_event_count,
        materialized_row_count=len(rows),
        dropped_non_directional_count=dropped_non_directional_count,
        dropped_missing_entry_price_count=dropped_missing_entry_price_count,
        dropped_missing_primary_label_count=dropped_missing_primary_label_count,
    )))

    return DatasetQAReport(
        build_id=build_id,
        source_event_count=source_event_count,
        materialized_row_count=len(rows),
        dropped_non_directional_count=dropped_non_directional_count,
        dropped_missing_entry_price_count=dropped_missing_entry_price_count,
        dropped_missing_primary_label_count=dropped_missing_primary_label_count,
        split_counts=split_counts,
        duplicate_event_ids=duplicate_event_ids,
        null_fraction_by_column=null_fraction_by_column,
        null_heavy_columns=null_heavy_columns,
        impossible_timestamp_event_ids=tuple(sorted(set(impossible_timestamp_event_ids))),
        wallet_leakage_event_ids=tuple(sorted(set(wallet_leakage_event_ids))),
        future_label_leakage_event_ids=tuple(sorted(set(future_label_leakage_event_ids))),
        errors=tuple(errors),
        warnings=tuple(warnings),
    )


def _duplicate_event_ids(rows: tuple[EventDatasetRow, ...]) -> tuple[str, ...]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row.event_id] = counts.get(row.event_id, 0) + 1
    return tuple(sorted(event_id for event_id, count in counts.items() if count > 1))


def _null_fraction_by_column(rows: tuple[EventDatasetRow, ...]) -> dict[str, str]:
    if not rows:
        return {}

    row_dicts = [asdict(row) for row in rows]
    column_names = tuple(row_dicts[0].keys())
    row_count = Decimal(len(row_dicts))
    fractions: dict[str, str] = {}
    for column in column_names:
        null_count = sum(1 for row in row_dicts if row[column] is None)
        fractions[column] = format(Decimal(null_count) / row_count, "f")
    return fractions


def _has_split_order_violation(rows: tuple[EventDatasetRow, ...]) -> bool:
    validation_started = False
    for row in rows:
        if row.dataset_split == "validation":
            validation_started = True
            continue
        if validation_started and row.dataset_split == "train":
            return True
    return False


def _build_error_messages(
    *,
    duplicate_event_ids: tuple[str, ...],
    null_heavy_columns: tuple[str, ...],
    impossible_timestamp_event_ids: list[str],
    wallet_leakage_event_ids: list[str],
    future_label_leakage_event_ids: list[str],
    split_order_violation: bool,
    materialized_row_count: int,
) -> tuple[str, ...]:
    messages: list[str] = []
    if materialized_row_count == 0:
        messages.append("Dataset materialized zero rows after filtering.")
    if duplicate_event_ids:
        messages.append(f"Duplicate event ids detected: {', '.join(duplicate_event_ids)}")
    if null_heavy_columns:
        messages.append(f"Null-heavy checked columns: {', '.join(null_heavy_columns)}")
    if impossible_timestamp_event_ids:
        messages.append(
            "Impossible dataset timestamps detected for events: "
            + ", ".join(sorted(set(impossible_timestamp_event_ids)))
        )
    if wallet_leakage_event_ids:
        messages.append(
            "Wallet leakage detected for events: " + ", ".join(sorted(set(wallet_leakage_event_ids)))
        )
    if future_label_leakage_event_ids:
        messages.append(
            "Future label leakage detected for events: "
            + ", ".join(sorted(set(future_label_leakage_event_ids)))
        )
    if split_order_violation:
        messages.append("Dataset splits are not time ordered.")
    return tuple(messages)


def _build_warning_messages(
    *,
    source_event_count: int,
    materialized_row_count: int,
    dropped_non_directional_count: int,
    dropped_missing_entry_price_count: int,
    dropped_missing_primary_label_count: int,
) -> tuple[str, ...]:
    messages: list[str] = []
    if source_event_count > materialized_row_count:
        messages.append(
            f"Dropped {source_event_count - materialized_row_count} source events during dataset materialization."
        )
    if dropped_non_directional_count:
        messages.append(f"Dropped {dropped_non_directional_count} non-directional events.")
    if dropped_missing_entry_price_count:
        messages.append(f"Dropped {dropped_missing_entry_price_count} events missing entry prices.")
    if dropped_missing_primary_label_count:
        messages.append(f"Dropped {dropped_missing_primary_label_count} events missing primary labels.")
    return tuple(messages)


def _participant_profile_times(wallet_context: dict[str, Any]) -> tuple[datetime, ...]:
    raw_participants = wallet_context.get("participants")
    if not isinstance(raw_participants, list):
        return ()

    times: list[datetime] = []
    for participant in raw_participants:
        if not isinstance(participant, dict):
            continue
        profile_time = _parse_datetime(participant.get("profile_as_of_time_utc"))
        if profile_time is not None:
            times.append(profile_time)
    return tuple(times)


def _latest_point_at_or_before(
    price_points: tuple[tuple[datetime, Decimal], ...],
    cutoff_time: datetime,
) -> tuple[datetime, Decimal] | None:
    latest_match: tuple[datetime, Decimal] | None = None
    for point in price_points:
        if point[0] <= cutoff_time:
            latest_match = point
            continue
        break
    return latest_match


def _first_point_at_or_after(
    price_points: tuple[tuple[datetime, Decimal], ...],
    cutoff_time: datetime,
) -> tuple[datetime, Decimal] | None:
    for point in price_points:
        if point[0] >= cutoff_time:
            return point
    return None


def _fetch_rows(warehouse_path: str | Path, query: str) -> list[dict[str, Any]]:
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        cursor = connection.execute(query)
        columns = [description[0] for description in cursor.description]
        return [
            {column: value for column, value in zip(columns, values, strict=True)}
            for values in cursor.fetchall()
        ]


def _split_counts(rows: tuple[EventDatasetRow, ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row.dataset_split] = counts.get(row.dataset_split, 0) + 1
    return counts


def _build_dataset_row_id(*, build_id: str, event_id: str) -> str:
    return sha256(f"{build_id}|{event_id}".encode("utf-8")).hexdigest()


def _format_horizon_key(horizon: timedelta) -> str:
    return f"{int(horizon.total_seconds() // 60)}m"


def _default_build_id() -> str:
    return datetime.now(UTC).strftime("event-dataset-%Y%m%dT%H%M%SZ")


def _parse_decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _parse_datetime(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return _normalize_utc_timestamp(value)
    return _normalize_utc_timestamp(datetime.fromisoformat(str(value)))


def _normalize_utc_timestamp(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _json_object(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _coerce_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


def _coerce_decimal(value: object) -> Decimal:
    decimal_value = _parse_decimal(value)
    if decimal_value is None:
        raise ValueError("Expected a decimal-compatible value.")
    return decimal_value


def _coerce_optional_decimal(value: object) -> Decimal | None:
    return _parse_decimal(value)


def _serialize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, timedelta):
        return int(value.total_seconds())
    if isinstance(value, dict):
        return {key: _serialize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_value(item) for item in value]
    return value


def _unique_preserving_order(values: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    unique_values: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique_values.append(value)
    return tuple(unique_values)
