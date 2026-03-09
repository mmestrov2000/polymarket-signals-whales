# Milestone 3 Wallet Exploration

This runbook exercises the current Milestone 3 wallet backfill and profile surface against real Polymarket data without adding new production behavior.

It keeps each session isolated under `data/runs/<run_id>/` so wallet raw captures, DuckDB tables, and notebook outputs stay reproducible and easy to inspect.

## Prerequisites

Bootstrap the local environment first and confirm the notebook dependencies are present:

```bash
scripts/bootstrap_env.sh
source .venv/bin/activate
.venv/bin/python -c "import duckdb, matplotlib, ipywidgets, notebook; print('imports ok')"
```

If the import check fails, rerun `scripts/bootstrap_env.sh` when network access is available.

## Start An Isolated Run

Create a run id and isolated output paths:

```bash
export RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export POLYMARKET_EXPLORATION_RUN_ID="$RUN_ID"
export RAW_DIR="data/runs/$RUN_ID/raw"
export WAREHOUSE_PATH="data/runs/$RUN_ID/warehouse/polymarket.duckdb"
```

The wallet notebook will create these paths as needed and call `run_wallet_backfill(...)` directly against them.

## Open The Wallet Notebook

Launch Jupyter Lab against the wallet walkthrough:

```bash
jupyter lab notebooks/wallet_data_exploration/00_wallet_walkthrough.ipynb
```

Run the notebook top to bottom. It will:

- verify `duckdb`, `matplotlib`, `ipywidgets`, and `notebook`
- reuse `POLYMARKET_EXPLORATION_RUN_ID` for isolated per-run storage
- execute `run_wallet_backfill(...)` with adjustable leaderboard, positions, closed-positions, and activity limits
- print the selected wallets, skipped seeds, row counts, raw capture counts, and any partial failures
- inspect the latest raw leaderboard, seed-list, and per-wallet raw capture paths
- visualize cohort-level wallet rankings and a wallet-comparison scatter plot
- expose a wallet selector for drilldowns into activity trades, cumulative closed-position realized PnL, and open-position snapshots

## Expected Artifacts

After the notebook completes, you should have:

- raw leaderboard captures under `data/runs/$RUN_ID/raw/data_api/wallet_universe_leaderboard/`
- raw seed-list captures under `data/runs/$RUN_ID/raw/data_api/wallet_seed_list/`
- per-wallet raw captures under `data/runs/$RUN_ID/raw/data_api/wallet_positions/`, `wallet_closed_positions/`, and `wallet_activity/`
- a normalized DuckDB warehouse at `data/runs/$RUN_ID/warehouse/polymarket.duckdb`

## What To Review

After the notebook completes, check:

- whether the selected wallet seeds match the expected leaderboard rule and dedupe behavior
- whether `wallet_profiles` contains one row per seeded wallet, including empty-history wallets
- whether realized PnL, ROI, hit rate, and activity volume rankings agree with the raw seed metadata
- whether the per-wallet drilldown charts match the recent trade and position tables
- whether any partial endpoint failures were surfaced inline instead of being hidden
