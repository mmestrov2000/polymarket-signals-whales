from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import duckdb
import numpy as np

from src.research.modeling import (
    LogisticRegressionConfig,
    ResearchDatasetRow,
    combined_rule_decision,
    load_dataset_rows,
    normalize_utc_timestamp,
    predict_probabilities,
    prepare_feature_matrices,
    resolve_dataset_build_id,
    serialize_value,
    train_logistic_regression,
)
from src.storage import DEFAULT_WAREHOUSE_PATH


DEFAULT_BACKTEST_OUTPUT_DIR = Path("data/research/backtest_runs")
STRATEGY_NAMES = ("market_only", "combined_rule", "classifier")
CATEGORY_KEYWORDS = (
    ("politics", ("trump", "biden", "election", "vote", "senate", "house", "congress", "president", "governor")),
    ("sports", ("nba", "nfl", "mlb", "nhl", "soccer", "football", "basketball", "baseball", "tennis", "golf", "ufc", "f1", "formula-1")),
    ("crypto", ("bitcoin", "btc", "ethereum", "eth", "solana", "sol", "crypto", "token", "blockchain", "doge", "xrp")),
    ("business", ("stock", "shares", "earnings", "ipo", "nasdaq", "s&p", "dow", "tesla", "apple", "microsoft", "google", "amazon", "meta", "company", "ceo")),
    ("world", ("ukraine", "russia", "china", "israel", "gaza", "war", "ceasefire", "nato", "europe", "world", "earthquake", "hurricane", "pandemic")),
    ("entertainment", ("oscar", "grammy", "emmy", "movie", "film", "tv", "album", "song", "music", "celebrity", "actor", "actress", "box-office")),
)


@dataclass(frozen=True, slots=True)
class WalkForwardBacktestConfig:
    initial_capital_usdc: float = 10_000.0
    position_size_fraction: float = 0.20
    max_open_positions: int = 5
    minimum_training_rows: int = 50
    logistic_regression_config: LogisticRegressionConfig = field(default_factory=LogisticRegressionConfig)

    def __post_init__(self) -> None:
        if self.initial_capital_usdc <= 0:
            raise ValueError("initial_capital_usdc must be greater than zero.")
        if not (0 < self.position_size_fraction <= 1):
            raise ValueError("position_size_fraction must be in the range (0, 1].")
        if self.max_open_positions <= 0:
            raise ValueError("max_open_positions must be greater than zero.")
        if self.minimum_training_rows <= 0:
            raise ValueError("minimum_training_rows must be greater than zero.")

    @property
    def position_notional_usdc(self) -> float:
        return self.initial_capital_usdc * self.position_size_fraction


@dataclass(frozen=True, slots=True)
class StrategyTradeRecord:
    strategy_name: str
    dataset_row_id: str
    event_id: str
    asset_id: str | None
    condition_id: str | None
    event_time_utc: datetime
    entry_time_utc: datetime
    exit_time_utc: datetime
    direction: str
    trigger_reason: str
    signal_probability: float | None
    category: str
    liquidity_bucket: str
    month_bucket: str
    question: str | None
    slug: str | None
    notional_usdc: float
    assumed_round_trip_cost_bps: float
    gross_return_bps: float
    net_return_bps: float
    gross_pnl_usdc: float
    net_pnl_usdc: float
    equity_before_usdc: float
    equity_after_usdc: float


@dataclass(frozen=True, slots=True)
class EquityCurvePoint:
    strategy_name: str
    timestamp_utc: datetime
    event_type: Literal["start", "close"]
    realized_equity_usdc: float
    cash_usdc: float
    open_position_count: int
    closed_trade_count: int


@dataclass(frozen=True, slots=True)
class SliceSummary:
    strategy_name: str
    slice_type: str
    slice_value: str
    trade_count: int
    hit_rate: float | None
    total_net_pnl_usdc: float
    mean_net_pnl_usdc: float | None


@dataclass(frozen=True, slots=True)
class StrategyBacktestSummary:
    strategy_name: str
    burn_in_row_count: int
    execution_row_count: int
    trade_count: int
    coverage: float
    skipped_capacity_count: int
    skipped_cash_count: int
    hit_rate: float | None
    mean_net_pnl_usdc: float | None
    median_net_pnl_usdc: float | None
    total_net_pnl_usdc: float
    total_gross_pnl_usdc: float
    total_return_pct: float
    max_drawdown_pct: float
    ending_equity_usdc: float


@dataclass(frozen=True, slots=True)
class BacktestArtifactPaths:
    run_dir: Path
    summary_path: Path
    trades_path: Path
    equity_curve_path: Path
    report_path: Path


@dataclass(frozen=True, slots=True)
class WalkForwardBacktestRun:
    run_id: str
    dataset_build_id: str
    config: WalkForwardBacktestConfig
    primary_label_name: str
    primary_label_horizon_minutes: int
    paper_trading_decision: str
    paper_trading_reason: str
    strategy_summaries: dict[str, StrategyBacktestSummary]
    slice_summaries: dict[str, tuple[SliceSummary, ...]]
    trade_records: dict[str, tuple[StrategyTradeRecord, ...]]
    equity_curve: dict[str, tuple[EquityCurvePoint, ...]]


@dataclass(frozen=True, slots=True)
class WalkForwardBacktestResult:
    run: WalkForwardBacktestRun
    artifact_paths: BacktestArtifactPaths


@dataclass(frozen=True, slots=True)
class _MarketMetadata:
    condition_id: str
    question: str | None
    slug: str | None
    liquidity: float | None
    category: str
    liquidity_bucket: str


@dataclass(frozen=True, slots=True)
class _OpenPosition:
    row: ResearchDatasetRow
    signal_probability: float | None
    notional_usdc: float


@dataclass(frozen=True, slots=True)
class _StrategySimulation:
    summary: StrategyBacktestSummary
    trades: tuple[StrategyTradeRecord, ...]
    equity_curve: tuple[EquityCurvePoint, ...]


class BacktestExperimentError(RuntimeError):
    """Raised when a Milestone 7 walk-forward backtest cannot be reproduced."""


def run_walk_forward_backtest(
    *,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE_PATH,
    output_dir: str | Path = DEFAULT_BACKTEST_OUTPUT_DIR,
    dataset_build_id: str | None = None,
    run_id: str | None = None,
    config: WalkForwardBacktestConfig | None = None,
) -> WalkForwardBacktestResult:
    backtest_config = config or WalkForwardBacktestConfig()
    resolved_build_id = resolve_dataset_build_id(
        warehouse_path,
        dataset_build_id=dataset_build_id,
        error_cls=BacktestExperimentError,
    )
    rows = load_dataset_rows(warehouse_path, dataset_build_id=resolved_build_id)
    if not rows:
        raise BacktestExperimentError(
            f"Dataset build '{resolved_build_id}' has no rows in event_dataset_rows."
        )
    if len(rows) <= backtest_config.minimum_training_rows:
        raise BacktestExperimentError(
            "Walk-forward backtest requires more rows than the configured burn-in window. "
            f"Found {len(rows)} rows and minimum_training_rows={backtest_config.minimum_training_rows}."
        )

    label_definitions = {
        (row.primary_label_name, row.primary_label_horizon_minutes)
        for row in rows
    }
    if len(label_definitions) != 1:
        raise BacktestExperimentError(
            "Walk-forward backtest requires a single primary label definition per dataset build."
        )
    primary_label_name, primary_label_horizon_minutes = next(iter(label_definitions))
    market_metadata = _load_latest_market_metadata(warehouse_path)

    strategy_summaries: dict[str, StrategyBacktestSummary] = {}
    trade_records: dict[str, tuple[StrategyTradeRecord, ...]] = {}
    equity_curve: dict[str, tuple[EquityCurvePoint, ...]] = {}
    for strategy_name in STRATEGY_NAMES:
        simulation = _run_strategy_simulation(
            strategy_name=strategy_name,
            rows=rows,
            market_metadata=market_metadata,
            config=backtest_config,
        )
        strategy_summaries[strategy_name] = simulation.summary
        trade_records[strategy_name] = simulation.trades
        equity_curve[strategy_name] = simulation.equity_curve

    slice_summaries = {
        "category": _build_slice_summaries(trade_records=trade_records, slice_type="category"),
        "liquidity_bucket": _build_slice_summaries(trade_records=trade_records, slice_type="liquidity_bucket"),
        "month": _build_slice_summaries(trade_records=trade_records, slice_type="month_bucket"),
    }
    paper_decision, paper_reason = _determine_paper_trading_decision(
        strategy_summaries=strategy_summaries,
        slice_summaries=slice_summaries,
    )

    resolved_run_id = run_id or _default_run_id()
    run = WalkForwardBacktestRun(
        run_id=resolved_run_id,
        dataset_build_id=resolved_build_id,
        config=backtest_config,
        primary_label_name=primary_label_name,
        primary_label_horizon_minutes=primary_label_horizon_minutes,
        paper_trading_decision=paper_decision,
        paper_trading_reason=paper_reason,
        strategy_summaries=strategy_summaries,
        slice_summaries=slice_summaries,
        trade_records=trade_records,
        equity_curve=equity_curve,
    )
    artifact_paths = write_walk_forward_backtest_artifacts(run=run, output_dir=output_dir)
    return WalkForwardBacktestResult(run=run, artifact_paths=artifact_paths)


def write_walk_forward_backtest_artifacts(
    *,
    run: WalkForwardBacktestRun,
    output_dir: str | Path = DEFAULT_BACKTEST_OUTPUT_DIR,
) -> BacktestArtifactPaths:
    run_dir = Path(output_dir) / run.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    summary_path = run_dir / "summary.json"
    trades_path = run_dir / "trades.jsonl"
    equity_curve_path = run_dir / "equity_curve.jsonl"
    report_path = run_dir / "report.md"

    summary_payload = {
        "run_id": run.run_id,
        "dataset_build_id": run.dataset_build_id,
        "primary_label_name": run.primary_label_name,
        "primary_label_horizon_minutes": run.primary_label_horizon_minutes,
        "config": serialize_value(asdict(run.config)),
        "paper_trading_decision": run.paper_trading_decision,
        "paper_trading_reason": run.paper_trading_reason,
        "strategy_summaries": serialize_value(run.strategy_summaries),
        "slice_summaries": serialize_value(run.slice_summaries),
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True))

    with trades_path.open("w", encoding="utf-8") as handle:
        for strategy_name in STRATEGY_NAMES:
            for record in run.trade_records[strategy_name]:
                handle.write(json.dumps(serialize_value(record), sort_keys=True))
                handle.write("\n")

    with equity_curve_path.open("w", encoding="utf-8") as handle:
        for strategy_name in STRATEGY_NAMES:
            for point in run.equity_curve[strategy_name]:
                handle.write(json.dumps(serialize_value(point), sort_keys=True))
                handle.write("\n")

    report_path.write_text(_render_report(run))
    return BacktestArtifactPaths(
        run_dir=run_dir,
        summary_path=summary_path,
        trades_path=trades_path,
        equity_curve_path=equity_curve_path,
        report_path=report_path,
    )


def _run_strategy_simulation(
    *,
    strategy_name: str,
    rows: tuple[ResearchDatasetRow, ...],
    market_metadata: dict[str, _MarketMetadata],
    config: WalkForwardBacktestConfig,
) -> _StrategySimulation:
    execution_rows = rows[config.minimum_training_rows :]
    fixed_notional = config.position_notional_usdc
    cash_usdc = config.initial_capital_usdc
    realized_equity = config.initial_capital_usdc
    open_positions: list[_OpenPosition] = []
    trades: list[StrategyTradeRecord] = []
    equity_curve = [
        EquityCurvePoint(
            strategy_name=strategy_name,
            timestamp_utc=execution_rows[0].entry_price_time_utc,
            event_type="start",
            realized_equity_usdc=realized_equity,
            cash_usdc=cash_usdc,
            open_position_count=0,
            closed_trade_count=0,
        )
    ]
    skipped_capacity_count = 0
    skipped_cash_count = 0

    for index, row in enumerate(rows[config.minimum_training_rows :], start=config.minimum_training_rows):
        current_time = row.entry_price_time_utc
        realized_equity, cash_usdc = _settle_positions(
            strategy_name=strategy_name,
            cutoff_time=current_time,
            open_positions=open_positions,
            trades=trades,
            equity_curve=equity_curve,
            market_metadata=market_metadata,
            realized_equity=realized_equity,
            cash_usdc=cash_usdc,
        )
        should_trade, signal_probability = _strategy_decision(
            strategy_name=strategy_name,
            rows=rows,
            row_index=index,
            config=config,
        )
        if not should_trade:
            continue
        if len(open_positions) >= config.max_open_positions:
            skipped_capacity_count += 1
            continue
        if cash_usdc + 1e-9 < fixed_notional:
            skipped_cash_count += 1
            continue

        open_positions.append(
            _OpenPosition(
                row=row,
                signal_probability=signal_probability,
                notional_usdc=fixed_notional,
            )
        )
        cash_usdc -= fixed_notional

    realized_equity, cash_usdc = _settle_positions(
        strategy_name=strategy_name,
        cutoff_time=None,
        open_positions=open_positions,
        trades=trades,
        equity_curve=equity_curve,
        market_metadata=market_metadata,
        realized_equity=realized_equity,
        cash_usdc=cash_usdc,
    )

    trade_count = len(trades)
    coverage = float(trade_count / len(execution_rows)) if execution_rows else 0.0
    net_pnls = np.array([trade.net_pnl_usdc for trade in trades], dtype=np.float64) if trades else np.array([])
    gross_pnls = np.array([trade.gross_pnl_usdc for trade in trades], dtype=np.float64) if trades else np.array([])
    winning_trades = np.array([1.0 if trade.net_pnl_usdc > 0 else 0.0 for trade in trades], dtype=np.float64)
    summary = StrategyBacktestSummary(
        strategy_name=strategy_name,
        burn_in_row_count=config.minimum_training_rows,
        execution_row_count=len(execution_rows),
        trade_count=trade_count,
        coverage=coverage,
        skipped_capacity_count=skipped_capacity_count,
        skipped_cash_count=skipped_cash_count,
        hit_rate=float(np.mean(winning_trades)) if winning_trades.size else None,
        mean_net_pnl_usdc=float(np.mean(net_pnls)) if net_pnls.size else None,
        median_net_pnl_usdc=float(np.median(net_pnls)) if net_pnls.size else None,
        total_net_pnl_usdc=float(np.sum(net_pnls)) if net_pnls.size else 0.0,
        total_gross_pnl_usdc=float(np.sum(gross_pnls)) if gross_pnls.size else 0.0,
        total_return_pct=((realized_equity / config.initial_capital_usdc) - 1.0) * 100.0,
        max_drawdown_pct=_compute_max_drawdown_pct(equity_curve),
        ending_equity_usdc=realized_equity,
    )
    return _StrategySimulation(
        summary=summary,
        trades=tuple(trades),
        equity_curve=tuple(equity_curve),
    )


def _strategy_decision(
    *,
    strategy_name: str,
    rows: tuple[ResearchDatasetRow, ...],
    row_index: int,
    config: WalkForwardBacktestConfig,
) -> tuple[bool, float | None]:
    row = rows[row_index]
    if strategy_name == "market_only":
        return True, None
    if strategy_name == "combined_rule":
        return combined_rule_decision(row), None
    if strategy_name != "classifier":
        raise ValueError(f"Unsupported strategy_name: {strategy_name}")

    training_rows = _observed_training_rows(
        prior_rows=rows[:row_index],
        decision_time_utc=row.entry_price_time_utc,
    )
    if len(training_rows) < config.minimum_training_rows:
        return False, None

    train_matrix, validation_matrix, _, _ = prepare_feature_matrices(
        train_rows=training_rows,
        validation_rows=(row,),
    )
    train_labels = np.array(
        [1.0 if training_row.primary_label_profitable else 0.0 for training_row in training_rows],
        dtype=np.float64,
    )
    model = train_logistic_regression(
        features=train_matrix,
        labels=train_labels,
        config=config.logistic_regression_config,
    )
    probability = float(predict_probabilities(validation_matrix, model=model)[0])
    return probability >= config.logistic_regression_config.classification_threshold, probability


def _observed_training_rows(
    *,
    prior_rows: tuple[ResearchDatasetRow, ...],
    decision_time_utc: datetime,
) -> tuple[ResearchDatasetRow, ...]:
    return tuple(
        row
        for row in prior_rows
        if row.entry_price_time_utc < decision_time_utc
        and row.primary_exit_time_utc <= decision_time_utc
    )


def _settle_positions(
    *,
    strategy_name: str,
    cutoff_time: datetime | None,
    open_positions: list[_OpenPosition],
    trades: list[StrategyTradeRecord],
    equity_curve: list[EquityCurvePoint],
    market_metadata: dict[str, _MarketMetadata],
    realized_equity: float,
    cash_usdc: float,
) -> tuple[float, float]:
    eligible_positions = [
        position
        for position in open_positions
        if cutoff_time is None or position.row.primary_exit_time_utc <= cutoff_time
    ]
    eligible_positions.sort(
        key=lambda position: (position.row.primary_exit_time_utc, position.row.dataset_row_id)
    )
    for position in eligible_positions:
        open_positions.remove(position)
        row = position.row
        metadata = market_metadata.get(
            row.condition_id or "",
            _MarketMetadata(
                condition_id=row.condition_id or "",
                question=None,
                slug=None,
                liquidity=None,
                category="uncategorized",
                liquidity_bucket="unknown",
            ),
        )
        equity_before = realized_equity
        gross_pnl_usdc = position.notional_usdc * (row.primary_directional_return_bps / 10_000.0)
        net_pnl_usdc = position.notional_usdc * (row.primary_net_pnl_bps / 10_000.0)
        cash_usdc += position.notional_usdc + net_pnl_usdc
        realized_equity += net_pnl_usdc
        trades.append(
            StrategyTradeRecord(
                strategy_name=strategy_name,
                dataset_row_id=row.dataset_row_id,
                event_id=row.event_id,
                asset_id=row.asset_id,
                condition_id=row.condition_id,
                event_time_utc=row.event_time_utc,
                entry_time_utc=row.entry_price_time_utc,
                exit_time_utc=row.primary_exit_time_utc,
                direction=row.direction,
                trigger_reason=row.trigger_reason,
                signal_probability=position.signal_probability,
                category=metadata.category,
                liquidity_bucket=metadata.liquidity_bucket,
                month_bucket=row.event_time_utc.strftime("%Y-%m"),
                question=metadata.question,
                slug=metadata.slug,
                notional_usdc=position.notional_usdc,
                assumed_round_trip_cost_bps=row.assumed_round_trip_cost_bps,
                gross_return_bps=row.primary_directional_return_bps,
                net_return_bps=row.primary_net_pnl_bps,
                gross_pnl_usdc=gross_pnl_usdc,
                net_pnl_usdc=net_pnl_usdc,
                equity_before_usdc=equity_before,
                equity_after_usdc=realized_equity,
            )
        )
        equity_curve.append(
            EquityCurvePoint(
                strategy_name=strategy_name,
                timestamp_utc=row.primary_exit_time_utc,
                event_type="close",
                realized_equity_usdc=realized_equity,
                cash_usdc=cash_usdc,
                open_position_count=len(open_positions),
                closed_trade_count=len(trades),
            )
        )
    return realized_equity, cash_usdc


def _build_slice_summaries(
    *,
    trade_records: dict[str, tuple[StrategyTradeRecord, ...]],
    slice_type: Literal["category", "liquidity_bucket", "month_bucket"],
) -> tuple[SliceSummary, ...]:
    results: list[SliceSummary] = []
    for strategy_name in STRATEGY_NAMES:
        grouped: dict[str, list[StrategyTradeRecord]] = defaultdict(list)
        for trade in trade_records[strategy_name]:
            grouped[getattr(trade, slice_type)].append(trade)
        for slice_value in sorted(grouped):
            trades = grouped[slice_value]
            net_pnls = np.array([trade.net_pnl_usdc for trade in trades], dtype=np.float64)
            wins = np.array([1.0 if trade.net_pnl_usdc > 0 else 0.0 for trade in trades], dtype=np.float64)
            results.append(
                SliceSummary(
                    strategy_name=strategy_name,
                    slice_type="month" if slice_type == "month_bucket" else slice_type,
                    slice_value=slice_value,
                    trade_count=len(trades),
                    hit_rate=float(np.mean(wins)) if wins.size else None,
                    total_net_pnl_usdc=float(np.sum(net_pnls)) if net_pnls.size else 0.0,
                    mean_net_pnl_usdc=float(np.mean(net_pnls)) if net_pnls.size else None,
                )
            )
    return tuple(results)


def _determine_paper_trading_decision(
    *,
    strategy_summaries: dict[str, StrategyBacktestSummary],
    slice_summaries: dict[str, tuple[SliceSummary, ...]],
) -> tuple[str, str]:
    qualifying_strategies: list[StrategyBacktestSummary] = []
    for strategy_name in STRATEGY_NAMES:
        summary = strategy_summaries[strategy_name]
        if summary.trade_count == 0:
            continue
        if summary.total_net_pnl_usdc <= 0:
            continue
        if summary.hit_rate is None or summary.hit_rate <= 0.50:
            continue
        if summary.max_drawdown_pct >= 20.0:
            continue

        disqualifying_slice = False
        for summaries in slice_summaries.values():
            if any(
                slice_summary.strategy_name == strategy_name
                and slice_summary.trade_count >= 5
                and slice_summary.total_net_pnl_usdc <= 0
                for slice_summary in summaries
            ):
                disqualifying_slice = True
                break
        if not disqualifying_slice:
            qualifying_strategies.append(summary)

    if qualifying_strategies:
        winner = max(qualifying_strategies, key=lambda summary: summary.total_net_pnl_usdc)
        return (
            "go",
            (
                f"{winner.strategy_name} met the overall profitability, hit-rate, and drawdown gates "
                "without failing any slice that had at least 5 trades."
            ),
        )

    return (
        "no-go",
        "No strategy cleared the overall profitability, hit-rate, drawdown, and slice-consistency gates.",
    )


def _compute_max_drawdown_pct(equity_curve: list[EquityCurvePoint]) -> float:
    if not equity_curve:
        return 0.0
    peak_equity = equity_curve[0].realized_equity_usdc
    max_drawdown = 0.0
    for point in equity_curve:
        peak_equity = max(peak_equity, point.realized_equity_usdc)
        if peak_equity <= 0:
            continue
        drawdown = (peak_equity - point.realized_equity_usdc) / peak_equity
        max_drawdown = max(max_drawdown, drawdown)
    return max_drawdown * 100.0


def _load_latest_market_metadata(warehouse_path: str | Path) -> dict[str, _MarketMetadata]:
    query = """
        SELECT condition_id, question, slug, liquidity
        FROM (
            SELECT
                condition_id,
                question,
                slug,
                liquidity,
                ROW_NUMBER() OVER (
                    PARTITION BY condition_id
                    ORDER BY collection_time_utc DESC, updated_at_utc DESC, market_id ASC
                ) AS row_rank
            FROM markets
            WHERE condition_id IS NOT NULL
        )
        WHERE row_rank = 1
        ORDER BY condition_id ASC
    """
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        rows = connection.execute(query).fetchall()

    raw_metadata = [
        {
            "condition_id": str(condition_id),
            "question": str(question) if question is not None else None,
            "slug": str(slug) if slug is not None else None,
            "liquidity": float(liquidity) if liquidity is not None else None,
        }
        for condition_id, question, slug, liquidity in rows
    ]
    liquidity_thresholds = _liquidity_thresholds(
        [row["liquidity"] for row in raw_metadata if row["liquidity"] is not None]
    )
    metadata: dict[str, _MarketMetadata] = {}
    for row in raw_metadata:
        category = _derive_market_category(slug=row["slug"], question=row["question"])
        metadata[row["condition_id"]] = _MarketMetadata(
            condition_id=row["condition_id"],
            question=row["question"],
            slug=row["slug"],
            liquidity=row["liquidity"],
            category=category,
            liquidity_bucket=_liquidity_bucket(row["liquidity"], liquidity_thresholds),
        )
    return metadata


def _liquidity_thresholds(liquidity_values: list[float]) -> tuple[float, float] | None:
    if not liquidity_values:
        return None
    values = np.array(sorted(liquidity_values), dtype=np.float64)
    lower, upper = np.quantile(values, [1 / 3, 2 / 3])
    return float(lower), float(upper)


def _liquidity_bucket(liquidity: float | None, thresholds: tuple[float, float] | None) -> str:
    if liquidity is None or thresholds is None:
        return "unknown"
    lower, upper = thresholds
    if liquidity <= lower:
        return "low"
    if liquidity <= upper:
        return "medium"
    return "high"


def _derive_market_category(*, slug: str | None, question: str | None) -> str:
    for text in (slug, question):
        if text is None:
            continue
        lowered = text.lower()
        for category, keywords in CATEGORY_KEYWORDS:
            if any(keyword in lowered for keyword in keywords):
                return category
    return "uncategorized"


def _render_report(run: WalkForwardBacktestRun) -> str:
    sections = [
        "# Walk-Forward Backtest Report",
        "",
        f"- Run id: `{run.run_id}`",
        f"- Dataset build id: `{run.dataset_build_id}`",
        f"- Primary label: `{run.primary_label_name}` @ {run.primary_label_horizon_minutes}m",
        f"- Paper trading decision: `{run.paper_trading_decision}`",
        f"- Decision rationale: {run.paper_trading_reason}",
        "",
        "## Simulation Assumptions",
        f"- Initial capital: {run.config.initial_capital_usdc:.2f} USDC",
        f"- Position size fraction: {run.config.position_size_fraction:.2%}",
        f"- Max open positions per strategy: {run.config.max_open_positions}",
        f"- Minimum observed training rows before execution: {run.config.minimum_training_rows}",
        "",
        "## Overall Comparison",
        _render_overall_table(run.strategy_summaries),
        "",
        "## Category Breakdown",
        _render_slice_table(run.slice_summaries["category"]),
        "",
        "## Liquidity Breakdown",
        _render_slice_table(run.slice_summaries["liquidity_bucket"]),
        "",
        "## Monthly Breakdown",
        _render_slice_table(run.slice_summaries["month"]),
        "",
    ]
    return "\n".join(sections)


def _render_overall_table(strategy_summaries: dict[str, StrategyBacktestSummary]) -> str:
    lines = [
        "| strategy | trades | coverage | hit rate | total net pnl usdc | total return % | max drawdown % | skipped capacity | skipped cash |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for strategy_name in STRATEGY_NAMES:
        summary = strategy_summaries[strategy_name]
        lines.append(
            "| {strategy} | {trades} | {coverage:.2%} | {hit_rate} | {net_pnl:.2f} | {return_pct:.2f} | {drawdown:.2f} | {capacity} | {cash} |".format(
                strategy=strategy_name,
                trades=summary.trade_count,
                coverage=summary.coverage,
                hit_rate=_format_optional_percentage(summary.hit_rate),
                net_pnl=summary.total_net_pnl_usdc,
                return_pct=summary.total_return_pct,
                drawdown=summary.max_drawdown_pct,
                capacity=summary.skipped_capacity_count,
                cash=summary.skipped_cash_count,
            )
        )
    return "\n".join(lines)


def _render_slice_table(slice_summaries: tuple[SliceSummary, ...]) -> str:
    lines = [
        "| strategy | slice | value | trades | hit rate | total net pnl usdc | mean net pnl usdc |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for summary in slice_summaries:
        lines.append(
            "| {strategy} | {slice_type} | {slice_value} | {trades} | {hit_rate} | {total_net_pnl:.2f} | {mean_net_pnl} |".format(
                strategy=summary.strategy_name,
                slice_type=summary.slice_type,
                slice_value=summary.slice_value,
                trades=summary.trade_count,
                hit_rate=_format_optional_percentage(summary.hit_rate),
                total_net_pnl=summary.total_net_pnl_usdc,
                mean_net_pnl=(
                    f"{summary.mean_net_pnl_usdc:.2f}"
                    if summary.mean_net_pnl_usdc is not None
                    else "n/a"
                ),
            )
        )
    return "\n".join(lines)


def _format_optional_percentage(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2%}"


def _default_run_id() -> str:
    return datetime.now(UTC).strftime("walk-forward-backtest-%Y%m%dT%H%M%SZ")
