from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from src.research import SignalClassifierExperimentError, run_signal_classifier_experiments
from src.storage import EventDatasetRow, PolymarketWarehouse


def test_run_signal_classifier_experiments_builds_artifacts_and_metrics(tmp_path) -> None:
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"
    output_dir = tmp_path / "signal_classifier_runs"
    collection_time = datetime(2026, 3, 9, 13, 0, tzinfo=UTC)
    rows = _build_synthetic_dataset_rows("build-signal")

    with PolymarketWarehouse(database_path) as warehouse:
        assert warehouse.upsert_event_dataset_rows(rows, collection_time=collection_time) == len(rows)

    result = run_signal_classifier_experiments(
        warehouse_path=database_path,
        output_dir=output_dir,
        dataset_build_id="build-signal",
        run_id="signal-classifier-test",
    )

    market_only_validation = result.run.strategy_metrics["market_only"]["validation"]
    combined_rule_validation = result.run.strategy_metrics["combined_rule"]["validation"]
    classifier_validation = result.run.strategy_metrics["classifier"]["validation"]

    assert result.run.dataset_build_id == "build-signal"
    assert market_only_validation.row_count == 4
    assert combined_rule_validation.row_count == market_only_validation.row_count
    assert classifier_validation.row_count == market_only_validation.row_count
    assert market_only_validation.trade_count == 4
    assert combined_rule_validation.trade_count == 1

    weighted_average_quality_coefficient = next(
        coefficient.coefficient
        for coefficient in result.run.coefficients
        if coefficient.feature_name == "weighted_average_quality"
    )
    assert weighted_average_quality_coefficient > 0

    summary_payload = json.loads(result.artifact_paths.summary_path.read_text())
    coefficients_payload = json.loads(result.artifact_paths.coefficients_path.read_text())
    prediction_lines = result.artifact_paths.validation_predictions_path.read_text().strip().splitlines()

    assert summary_payload["dataset_build_id"] == "build-signal"
    assert summary_payload["strategy_metrics"]["combined_rule"]["validation"]["trade_count"] == 1
    assert coefficients_payload["dataset_build_id"] == "build-signal"
    assert len(prediction_lines) == 4


def test_run_signal_classifier_experiments_fails_when_no_dataset_builds(tmp_path) -> None:
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"

    with PolymarketWarehouse(database_path):
        pass

    with pytest.raises(SignalClassifierExperimentError, match="No dataset builds were found"):
        run_signal_classifier_experiments(warehouse_path=database_path, output_dir=tmp_path / "output")


def test_run_signal_classifier_experiments_requires_explicit_build_selection(tmp_path) -> None:
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"
    collection_time = datetime(2026, 3, 9, 13, 0, tzinfo=UTC)
    build_a_rows = _build_synthetic_dataset_rows("build-a")[:2]
    build_b_rows = _build_synthetic_dataset_rows("build-b")[:2]

    with PolymarketWarehouse(database_path) as warehouse:
        assert warehouse.upsert_event_dataset_rows(build_a_rows, collection_time=collection_time) == len(build_a_rows)
        assert warehouse.upsert_event_dataset_rows(build_b_rows, collection_time=collection_time) == len(build_b_rows)

    with pytest.raises(SignalClassifierExperimentError, match="Multiple dataset builds were found"):
        run_signal_classifier_experiments(warehouse_path=database_path, output_dir=tmp_path / "output")


@pytest.mark.parametrize(
    ("split_to_keep", "missing_split"),
    (
        ("validation", "train"),
        ("train", "validation"),
    ),
)
def test_run_signal_classifier_experiments_fails_when_required_split_missing(
    tmp_path,
    split_to_keep: str,
    missing_split: str,
) -> None:
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"
    collection_time = datetime(2026, 3, 9, 13, 0, tzinfo=UTC)
    rows = tuple(
        row
        for row in _build_synthetic_dataset_rows("build-split")
        if row.dataset_split == split_to_keep
    )

    with PolymarketWarehouse(database_path) as warehouse:
        assert warehouse.upsert_event_dataset_rows(rows, collection_time=collection_time) == len(rows)

    with pytest.raises(SignalClassifierExperimentError, match=f"missing {missing_split} rows"):
        run_signal_classifier_experiments(
            warehouse_path=database_path,
            output_dir=tmp_path / "output",
            dataset_build_id="build-split",
        )


def _build_synthetic_dataset_rows(build_id: str) -> tuple[EventDatasetRow, ...]:
    base_time = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    train_qualities = (1.2, 1.0, 0.8, -0.8, -1.0, -1.2)
    validation_configs = (
        {"quality": 0.9, "profitable": True, "sparse": False, "profiled_wallet_count": 2, "net_pnl_bps": 120},
        {"quality": -0.7, "profitable": False, "sparse": False, "profiled_wallet_count": 2, "net_pnl_bps": -95},
        {"quality": 0.8, "profitable": False, "sparse": True, "profiled_wallet_count": 1, "net_pnl_bps": -60},
        {"quality": None, "profitable": False, "sparse": False, "profiled_wallet_count": 0, "net_pnl_bps": -40},
    )

    rows = [
        _make_dataset_row(
            build_id=build_id,
            row_index=index,
            dataset_split="train",
            event_time=base_time + timedelta(minutes=index),
            weighted_average_quality=quality,
            profitable=quality > 0,
            sparse_wallet_set=False,
            profiled_wallet_count=2,
            net_pnl_bps=100 if quality > 0 else -100,
        )
        for index, quality in enumerate(train_qualities)
    ]

    for offset, config in enumerate(validation_configs, start=len(rows)):
        rows.append(
            _make_dataset_row(
                build_id=build_id,
                row_index=offset,
                dataset_split="validation",
                event_time=base_time + timedelta(minutes=offset),
                weighted_average_quality=config["quality"],
                profitable=config["profitable"],
                sparse_wallet_set=config["sparse"],
                profiled_wallet_count=config["profiled_wallet_count"],
                net_pnl_bps=config["net_pnl_bps"],
            )
        )

    return tuple(rows)


def _make_dataset_row(
    *,
    build_id: str,
    row_index: int,
    dataset_split: str,
    event_time: datetime,
    weighted_average_quality: float | None,
    profitable: bool,
    sparse_wallet_set: bool,
    profiled_wallet_count: int,
    net_pnl_bps: int,
) -> EventDatasetRow:
    net_pnl_decimal = Decimal(str(net_pnl_bps))
    directional_return_decimal = net_pnl_decimal + Decimal("40")
    profiled_volume_share = Decimal("1") if profiled_wallet_count > 0 else None
    active_wallet_count = 1 if sparse_wallet_set else 2

    return EventDatasetRow(
        dataset_row_id=f"{build_id}-row-{row_index}",
        dataset_build_id=build_id,
        dataset_split=dataset_split,
        event_id=f"{build_id}-event-{row_index}",
        asset_id="111",
        condition_id="0xcondition123",
        event_time_utc=event_time,
        source_event_collection_time_utc=event_time + timedelta(minutes=1),
        direction="up",
        trigger_reason="volume_spike",
        recent_trade_count=3,
        recent_volume_usdc=Decimal("300"),
        volume_zscore=Decimal("2.5"),
        trade_count_zscore=Decimal("2.0"),
        order_flow_imbalance=Decimal("0.60"),
        short_return=Decimal("0.08"),
        medium_return=Decimal("0.12"),
        liquidity_features_available=True,
        latest_price=Decimal("0.72"),
        latest_mid_price=Decimal("0.72"),
        latest_spread_bps=Decimal("10"),
        spread_change_bps=Decimal("5"),
        top_of_book_depth_usdc=Decimal("150"),
        depth_change_ratio=Decimal("0.10"),
        depth_imbalance=Decimal("0.05"),
        active_wallet_count=active_wallet_count,
        profiled_wallet_count=profiled_wallet_count,
        sparse_wallet_set=sparse_wallet_set,
        profiled_volume_share=profiled_volume_share,
        top_wallet_share=Decimal("0.60"),
        concentration_hhi=Decimal("0.45"),
        weighted_average_quality=(
            Decimal(str(weighted_average_quality))
            if weighted_average_quality is not None
            else None
        ),
        weighted_average_realized_roi=Decimal("0.10"),
        weighted_average_hit_rate=Decimal("0.60"),
        weighted_average_realized_pnl=Decimal("25"),
        entry_price=Decimal("0.72"),
        entry_price_time_utc=event_time,
        assumed_round_trip_cost_bps=Decimal("40"),
        primary_label_name="profitable_after_costs",
        primary_label_horizon_minutes=15,
        primary_label_continuation=profitable,
        primary_label_reversion=not profitable,
        primary_label_profitable=profitable,
        primary_directional_return_bps=directional_return_decimal,
        primary_net_pnl_bps=net_pnl_decimal,
        primary_exit_price=Decimal("0.75") if profitable else Decimal("0.69"),
        primary_exit_time_utc=event_time + timedelta(minutes=15),
        horizon_labels_json={
            "15m": {
                "continuation": profitable,
                "profitable_after_costs": profitable,
                "net_pnl_bps": str(net_pnl_decimal),
            }
        },
    )
