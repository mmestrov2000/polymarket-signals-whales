from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import numpy as np
import pytest

import src.research.backtesting as backtesting_module
from src.clients.gamma import GammaMarket
from src.research import (
    BacktestExperimentError,
    LogisticRegressionConfig,
    WalkForwardBacktestConfig,
    run_walk_forward_backtest,
)
from src.storage import EventDatasetRow, PolymarketWarehouse


def test_run_walk_forward_backtest_builds_artifacts_and_report(tmp_path) -> None:
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"
    output_dir = tmp_path / "backtest_runs"
    collection_time = datetime(2026, 3, 9, 13, 0, tzinfo=UTC)

    with PolymarketWarehouse(database_path) as warehouse:
        assert warehouse.upsert_markets(_build_markets(), collection_time=collection_time) == 3
        assert warehouse.upsert_event_dataset_rows(
            _build_backtest_rows("build-backtest"),
            collection_time=collection_time,
        ) == 6

    result = run_walk_forward_backtest(
        warehouse_path=database_path,
        output_dir=output_dir,
        dataset_build_id="build-backtest",
        run_id="walk-forward-test",
        config=WalkForwardBacktestConfig(
            minimum_training_rows=2,
            max_open_positions=2,
            position_size_fraction=0.20,
            logistic_regression_config=LogisticRegressionConfig(max_iterations=500),
        ),
    )

    market_only_summary = result.run.strategy_summaries["market_only"]
    combined_rule_summary = result.run.strategy_summaries["combined_rule"]
    classifier_summary = result.run.strategy_summaries["classifier"]

    assert result.run.dataset_build_id == "build-backtest"
    assert market_only_summary.trade_count == 4
    assert combined_rule_summary.trade_count == 3
    assert 0 < classifier_summary.trade_count <= market_only_summary.trade_count
    assert result.artifact_paths.summary_path.exists()
    assert result.artifact_paths.trades_path.exists()
    assert result.artifact_paths.equity_curve_path.exists()
    assert result.artifact_paths.report_path.exists()

    summary_payload = json.loads(result.artifact_paths.summary_path.read_text())
    report_text = result.artifact_paths.report_path.read_text()
    trade_lines = [
        json.loads(line)
        for line in result.artifact_paths.trades_path.read_text().splitlines()
        if line.strip()
    ]
    equity_lines = [
        json.loads(line)
        for line in result.artifact_paths.equity_curve_path.read_text().splitlines()
        if line.strip()
    ]

    first_market_only_trade = next(
        record for record in trade_lines if record["strategy_name"] == "market_only"
    )

    assert summary_payload["paper_trading_decision"] in {"go", "no-go"}
    assert summary_payload["paper_trading_reason"]
    assert summary_payload["slice_summaries"]["category"]
    assert summary_payload["slice_summaries"]["liquidity_bucket"]
    assert summary_payload["slice_summaries"]["month"]
    assert "## Simulation Assumptions" in report_text
    assert "## Category Breakdown" in report_text
    assert "## Liquidity Breakdown" in report_text
    assert "## Monthly Breakdown" in report_text
    assert first_market_only_trade["notional_usdc"] == 2_000.0
    assert first_market_only_trade["assumed_round_trip_cost_bps"] == 40.0
    assert first_market_only_trade["gross_pnl_usdc"] > first_market_only_trade["net_pnl_usdc"]
    assert {entry["event_type"] for entry in equity_lines} == {"start", "close"}


def test_run_walk_forward_backtest_uses_only_observed_labels_for_classifier(tmp_path, monkeypatch) -> None:
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"
    output_dir = tmp_path / "backtest_runs"
    collection_time = datetime(2026, 3, 9, 13, 0, tzinfo=UTC)

    with PolymarketWarehouse(database_path) as warehouse:
        assert warehouse.upsert_markets(_build_markets(), collection_time=collection_time) == 3
        assert warehouse.upsert_event_dataset_rows(
            _build_backtest_rows("build-sequencing"),
            collection_time=collection_time,
        ) == 6

    class _FakeModel:
        pass

    monkeypatch.setattr(
        backtesting_module,
        "train_logistic_regression",
        lambda **_kwargs: _FakeModel(),
    )
    monkeypatch.setattr(
        backtesting_module,
        "predict_probabilities",
        lambda features, *, model: np.ones(features.shape[0], dtype=np.float64),
    )

    result = run_walk_forward_backtest(
        warehouse_path=database_path,
        output_dir=output_dir,
        dataset_build_id="build-sequencing",
        run_id="walk-forward-sequencing",
        config=WalkForwardBacktestConfig(minimum_training_rows=2),
    )

    classifier_trades = result.run.trade_records["classifier"]

    assert result.run.strategy_summaries["classifier"].trade_count == 3
    assert classifier_trades[0].dataset_row_id == "build-sequencing-row-3"
    assert all(
        trade.entry_time_utc >= datetime(2026, 3, 1, 10, 17, tzinfo=UTC)
        for trade in classifier_trades
    )


def test_run_walk_forward_backtest_requires_more_rows_than_burn_in(tmp_path) -> None:
    database_path = tmp_path / "warehouse" / "polymarket.duckdb"
    collection_time = datetime(2026, 3, 9, 13, 0, tzinfo=UTC)

    with PolymarketWarehouse(database_path) as warehouse:
        assert warehouse.upsert_event_dataset_rows(
            _build_backtest_rows("build-short")[:2],
            collection_time=collection_time,
        ) == 2

    with pytest.raises(BacktestExperimentError, match="more rows than the configured burn-in window"):
        run_walk_forward_backtest(
            warehouse_path=database_path,
            output_dir=tmp_path / "backtest_runs",
            dataset_build_id="build-short",
            config=WalkForwardBacktestConfig(minimum_training_rows=2),
        )


def _build_markets() -> tuple[GammaMarket, ...]:
    return (
        GammaMarket(
            market_id="market-politics",
            question="Will Trump win the election?",
            slug="trump-election-2026",
            condition_id="0xpolitics",
            clob_token_ids=("111",),
            active=True,
            end_date=datetime(2026, 11, 3, tzinfo=UTC),
            liquidity=Decimal("100"),
            volume=Decimal("1000"),
        ),
        GammaMarket(
            market_id="market-sports",
            question="Will the Lakers win the playoffs?",
            slug="nba-lakers-playoffs",
            condition_id="0xsports",
            clob_token_ids=("222",),
            active=True,
            end_date=datetime(2026, 6, 15, tzinfo=UTC),
            liquidity=Decimal("1000"),
            volume=Decimal("1500"),
        ),
        GammaMarket(
            market_id="market-crypto",
            question="Will Bitcoin trade above $100k?",
            slug="bitcoin-100k",
            condition_id="0xcrypto",
            clob_token_ids=("333",),
            active=True,
            end_date=datetime(2026, 12, 31, tzinfo=UTC),
            liquidity=Decimal("10000"),
            volume=Decimal("5000"),
        ),
    )


def _build_backtest_rows(build_id: str) -> tuple[EventDatasetRow, ...]:
    configs = (
        {
            "row_index": 0,
            "dataset_split": "train",
            "asset_id": "111",
            "condition_id": "0xpolitics",
            "event_time": datetime(2026, 3, 1, 9, 55, tzinfo=UTC),
            "weighted_average_quality": 2.0,
            "profitable": True,
            "net_pnl_bps": 120,
        },
        {
            "row_index": 1,
            "dataset_split": "train",
            "asset_id": "222",
            "condition_id": "0xsports",
            "event_time": datetime(2026, 3, 1, 10, 1, tzinfo=UTC),
            "weighted_average_quality": -2.0,
            "profitable": False,
            "net_pnl_bps": -100,
        },
        {
            "row_index": 2,
            "dataset_split": "train",
            "asset_id": "333",
            "condition_id": "0xcrypto",
            "event_time": datetime(2026, 3, 1, 10, 12, tzinfo=UTC),
            "weighted_average_quality": 1.5,
            "profitable": True,
            "net_pnl_bps": 110,
        },
        {
            "row_index": 3,
            "dataset_split": "train",
            "asset_id": "111",
            "condition_id": "0xpolitics",
            "event_time": datetime(2026, 3, 1, 10, 17, tzinfo=UTC),
            "weighted_average_quality": 1.2,
            "profitable": True,
            "net_pnl_bps": 90,
        },
        {
            "row_index": 4,
            "dataset_split": "validation",
            "asset_id": "222",
            "condition_id": "0xsports",
            "event_time": datetime(2026, 4, 1, 9, 0, tzinfo=UTC),
            "weighted_average_quality": -1.5,
            "profitable": False,
            "net_pnl_bps": -70,
        },
        {
            "row_index": 5,
            "dataset_split": "validation",
            "asset_id": "333",
            "condition_id": "0xcrypto",
            "event_time": datetime(2026, 4, 1, 9, 20, tzinfo=UTC),
            "weighted_average_quality": 1.0,
            "profitable": True,
            "net_pnl_bps": 80,
        },
    )
    return tuple(
        _make_dataset_row(build_id=build_id, **config)
        for config in configs
    )


def _make_dataset_row(
    *,
    build_id: str,
    row_index: int,
    dataset_split: str,
    asset_id: str,
    condition_id: str,
    event_time: datetime,
    weighted_average_quality: float,
    profitable: bool,
    net_pnl_bps: int,
) -> EventDatasetRow:
    net_pnl_decimal = Decimal(str(net_pnl_bps))
    directional_return_decimal = net_pnl_decimal + Decimal("40")
    exit_time = event_time + timedelta(minutes=15)
    exit_price = Decimal("0.75") if profitable else Decimal("0.69")

    return EventDatasetRow(
        dataset_row_id=f"{build_id}-row-{row_index}",
        dataset_build_id=build_id,
        dataset_split=dataset_split,
        event_id=f"{build_id}-event-{row_index}",
        asset_id=asset_id,
        condition_id=condition_id,
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
        active_wallet_count=2,
        profiled_wallet_count=2,
        sparse_wallet_set=False,
        profiled_volume_share=Decimal("1"),
        top_wallet_share=Decimal("0.60"),
        concentration_hhi=Decimal("0.45"),
        weighted_average_quality=Decimal(str(weighted_average_quality)),
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
        primary_exit_price=exit_price,
        primary_exit_time_utc=exit_time,
        horizon_labels_json={
            "15m": {
                "continuation": profitable,
                "profitable_after_costs": profitable,
                "net_pnl_bps": str(net_pnl_decimal),
            }
        },
    )
