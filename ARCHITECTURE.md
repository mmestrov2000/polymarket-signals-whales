# Architecture

## Architectural Goal

Support a narrow, local-first research workflow that turns a large static archive into:
- comparable Polymarket and Kalshi probability tables
- final resolution labels
- calibration statistics for extreme probabilities
- visual outputs and a short decision memo

The architecture is intentionally optimized for fast exploratory truth-finding, not live services or trading infrastructure.

## Scope Boundaries

### Implemented Now

- rewritten canonical docs for the exploration pivot
- minimal analysis scaffold
- placeholder notebooks for dataset inventory and extreme-probability analysis

### Current Build Target

- archive inventory and extraction planning
- venue-specific schema mapping
- canonical market, contract, tick, and resolution tables
- sampling views for descriptive and inferential analysis
- statistical summaries and figures

### Deferred

- live API collection
- wallet analysis
- backtesting
- paper trading
- execution or risk systems

## Active Versus Legacy Code

The repository already contains client, ingestion, and storage modules from an earlier bot-oriented direction.

Current rule:
- new work should target archive analysis and visualization first
- inherited modules under `src/clients/`, `src/ingestion/`, and `src/signals/` are legacy context, not the active roadmap
- `src/storage/` may still be reused if its DuckDB helpers fit the new workflow

## Repository Layout

```text
.
├─ PROJECT_SPEC.md
├─ ARCHITECTURE.md
├─ TASKS.md
├─ README.md
├─ notebooks/
│  ├─ extreme_probability/
│  │  ├─ 00_dataset_inventory.ipynb
│  │  └─ 01_extreme_probability_analysis.ipynb
│  └─ polymarket_connection_checks/
│     └─ 00_api_connection.ipynb
├─ reports/
│  ├─ figures/
│  └─ summaries/
├─ src/
│  ├─ analysis/
│  ├─ datasets/
│  ├─ visualization/
│  ├─ research/
│  ├─ storage/
│  ├─ clients/
│  ├─ ingestion/
│  └─ signals/
└─ tests/
```

The `polymarket_connection_checks` notebook and related client code remain in the tree as inherited assets. The active exploration workflow starts in `notebooks/extreme_probability/`.

## Component Responsibilities

| Path | Responsibility | Notes |
| --- | --- | --- |
| `notebooks/extreme_probability/` | Interactive archive inspection, exploratory analysis, and figure generation | First execution target for the pivot |
| `src/datasets/` | Archive discovery, extraction manifests, schema mapping, and canonical table builders | Owns input normalization |
| `src/analysis/` | Bucketing, calibration metrics, confidence intervals, bootstrap or sensitivity logic | Owns statistical interpretation |
| `src/visualization/` | Reusable plotting and report-table helpers | Keeps notebooks lighter and more consistent |
| `src/research/` | Lightweight orchestration helpers for notebook and report workflows | Can host report assembly utilities |
| `src/storage/` | Reusable local storage helpers, especially DuckDB integration | Reuse only where it fits the new scope |
| `reports/figures/` | Saved charts and exported visuals | Output, not source of truth |
| `reports/summaries/` | Short written conclusions or research memos | Final go or no-go output |
| `tests/` | Unit and repo-guardrail tests | Prefer small fixtures over large integration tests |

## Data Flow

1. Discover local archives under `data/raw/` and record a file inventory.
2. Filter out irrelevant entries such as Apple metadata files or non-analysis assets.
3. Extract or stage only the required partitions into `data/staging/` when direct archive access is too slow.
4. Normalize venue-specific raw files into canonical tables for markets, contracts, ticks, and final outcomes.
5. Build analysis views:
   - all usable ticks for descriptive summaries
   - market-aware or threshold-entry samples for primary inference
6. Compute calibration tables, confidence intervals, and sensitivity summaries.
7. Save figures and a short memo under `reports/`.

## Storage Design

Use a simple local analytical stack:
- compressed raw archives preserved under `data/raw/`
- selective extraction or staging under `data/staging/`
- DuckDB as the default analytical database under `data/warehouse/`
- optional Parquet outputs under `data/derived/`

### Proposed Layout

```text
data/
├─ raw/
│  └─ *.tar.zst
├─ staging/
│  ├─ extracted/
│  └─ fixtures/
├─ warehouse/
│  └─ extreme_probability.duckdb
└─ derived/
   ├─ canonical/
   └─ analysis/
```

### Raw Storage Rules

- never modify the original archive in place
- keep extraction selective and reproducible
- store inventory metadata such as file path, venue, format, and extraction status
- explicitly ignore Apple `._*` artifacts and any similar archive noise

## Canonical Data Model

Initial canonical tables:

| Table | Grain | Purpose |
| --- | --- | --- |
| `archive_inventory` | one row per relevant file or partition | Tracks what exists in the local archive |
| `market_catalog` | one row per market | Venue-neutral market metadata |
| `contract_catalog` | one row per binary contract | Maps YES and NO conventions into a common model |
| `tick_observations` | one row per usable observation | Stores normalized probability and source metadata |
| `resolution_outcomes` | one row per resolved contract | Stores final YES or NO outcome |
| `threshold_entry_events` | one row per threshold-crossing event | Reduces repeated-tick bias for inference |
| `calibration_summaries` | one row per venue and bucket | Stores empirical rate, quoted mean, gap, and uncertainty |

Primary keys or join keys:
- `venue`
- `market_id`
- `contract_id`
- `observation_time_utc`
- `resolution_time_utc`

All timestamps must be normalized to UTC before cross-venue comparison.

## Analysis Rules

- normalize every probability to `[0, 1]`
- store the raw price source explicitly, such as `trade_price`, `quote_mid`, or `close_price`
- do not silently mix different price-source definitions inside one summary
- exclude unresolved or ambiguously resolved contracts from the first-pass analysis
- report both tick-weighted and market-aware summaries
- use uncertainty estimates that respect market-level dependence, such as Wilson intervals plus market-clustered bootstrap where practical

## External Dependencies

Expected current-scope Python dependencies:
- `duckdb` for local analytics
- `polars` or `pandas` for transforms
- `matplotlib`, `seaborn`, or `altair` for visualization
- `scipy` or `statsmodels` for simple inference helpers
- `jupyter` for notebooks
- `pytest` for tests

No external services are required for the active exploration scope.

## Reliability and Reproducibility

Minimum expectations:
- every derived table should be rebuildable from the local archive and documented transforms
- notebook scaffolds should clearly state the questions being answered
- bucket definitions must be fixed and versioned in code or docs
- tests should cover normalization, bucketing, and sampling edge cases
- figures should be saveable to a deterministic output path under `reports/`

## Security and Secrets

Current scope does not require trading credentials or live API authentication.

If legacy API utilities remain in the repository, their environment settings should be treated as unrelated to the active exploration work and not expanded unless this project explicitly changes direction again.

## Now Versus Later

| Status | Item | Reason |
| --- | --- | --- |
| Now | archive inventory and normalization | Without a comparable dataset, every statistical claim is suspect |
| Now | calibration analysis and figures | They directly answer the thesis |
| Later in another project | backtesting and execution | They only matter if this study finds a credible effect |
