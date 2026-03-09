# Milestone 1-7 Guided Walkthrough

This runbook is the fastest way to get a concrete feel for what the repository can do after Milestone 7.

The best path is:

1. use the notebooks for Milestones 1-3, where visual inspection matters most
2. use the CLI runners for Milestones 4-7, where the main outputs are datasets, metrics, and reports

If you keep one shared run id and one shared warehouse path for the whole session, you can move from raw API checks all the way to a walk-forward backtest without mixing artifacts between runs.

## Prerequisites

Bootstrap the local environment first:

```bash
scripts/bootstrap_env.sh
source .venv/bin/activate
cp .env.example .env
```

Confirm the notebook and research dependencies are available:

```bash
.venv/bin/python -c "import duckdb, ipywidgets, matplotlib, notebook, websockets; print('imports ok')"
```

If that import check fails, rerun `scripts/bootstrap_env.sh` when network access is available.

## Create One Shared Session

Use one run id for the whole tour:

```bash
export RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export POLYMARKET_EXPLORATION_RUN_ID="$RUN_ID"
export RAW_DIR="data/runs/$RUN_ID/raw"
export WAREHOUSE_PATH="data/runs/$RUN_ID/warehouse/polymarket.duckdb"
```

Everything below will point at those same paths.

## Step 1: API Reality Check

Open the connection notebook:

```bash
jupyter lab notebooks/polymarket_connection_checks/00_api_connection.ipynb
```

Run it top to bottom.

What this proves:

- Gamma, CLOB, Data API, and websocket connectivity are real
- the identifiers you will rely on later actually exist in live payloads
- the raw capture layout under `data/raw/` matches the repository docs

What to look at:

- whether market ids, condition ids, and token ids line up cleanly
- whether wallet-facing Data API fields still expose `proxyWallet`, `side`, `size`, and timestamps
- whether websocket messages still look like top-of-book or trade-shaped payloads rather than a broken schema

## Step 2: Market Data Walkthrough

For a visual pass through Milestone 2, use the market notebook:

```bash
jupyter lab notebooks/market_data_exploration/00_live_market_walkthrough.ipynb
```

Run it top to bottom with the shared `POLYMARKET_EXPLORATION_RUN_ID`.

What this proves:

- sample market backfill works end to end
- raw captures and DuckDB normalization agree with each other
- the live recorder can store top-of-book snapshots and sometimes trades

What to look at:

- historical price history for both outcomes
- live spread and mid-price movement
- whether the websocket raw payloads match the normalized `order_book_snapshots` rows

## Step 3: Wallet Data Walkthrough

Open the wallet notebook next:

```bash
jupyter lab notebooks/wallet_data_exploration/00_wallet_walkthrough.ipynb
```

Run it top to bottom with the same `POLYMARKET_EXPLORATION_RUN_ID`.

What this proves:

- the leaderboard-seeded wallet universe is reproducible
- wallet positions, closed positions, activity, and wallet profiles land in the same warehouse
- the wallet metrics used later are inspectable, not hidden

What to look at:

- the selected wallet seed list and any skipped wallets
- wallet profile rankings by realized PnL, ROI, and hit rate
- one-wallet drilldowns to verify that charts match the raw capture tables

## Step 4: Add More Historical Coverage For Milestones 4-7

The notebooks are enough for a visual tour, but the dataset, classifier, and backtest need more rows than a single-market demo usually creates.

Backfill a wider cohort into the same warehouse:

```bash
.venv/bin/python scripts/backfill_sample_markets.py \
  --sample-size 10 \
  --trade-limit 200 \
  --raw-dir "$RAW_DIR" \
  --warehouse-path "$WAREHOUSE_PATH"
```

If you want a lighter run first, start with `--sample-size 5` and scale up only if the later steps produce too few events.

## Step 5: Materialize Milestone 4 Signal Events

Generate interpretable candidate events from the normalized warehouse:

```bash
.venv/bin/python scripts/generate_signal_events.py \
  --warehouse-path "$WAREHOUSE_PATH"
```

What this proves:

- market anomaly features and wallet summary features can be combined into stored `signal_events`
- the event detector can replay against collected historical data without notebook-only logic

What to look at:

- the per-asset event counts in the CLI summary
- any skipped assets caused by missing trades or missing price history

Quick warehouse check:

```bash
.venv/bin/python - <<'PY'
import duckdb
import os

with duckdb.connect(os.environ["WAREHOUSE_PATH"], read_only=True) as connection:
    print(connection.execute(
        """
        SELECT asset_id, COUNT(*) AS event_count
        FROM signal_events
        GROUP BY asset_id
        ORDER BY event_count DESC, asset_id ASC
        """
    ).fetchall())
PY
```

## Step 6: Build The Milestone 5 Event Dataset

Materialize the leakage-checked training rows:

```bash
.venv/bin/python scripts/build_event_dataset.py \
  --warehouse-path "$WAREHOUSE_PATH"
```

What this proves:

- labels, trade-cost assumptions, and QA checks run end to end
- the repository can convert stored `signal_events` into reproducible `event_dataset_rows`

What to look at:

- the printed `build_id`
- the generated `summary.json` and `qa_report.json`
- whether the split counts are large enough for the next two steps

## Step 7: Train The Milestone 6 Baselines And Classifier

Use the dataset build id from the previous step:

```bash
export DATASET_BUILD_ID="<paste-build-id-here>"

.venv/bin/python scripts/train_signal_classifier.py \
  --warehouse-path "$WAREHOUSE_PATH" \
  --dataset-build-id "$DATASET_BUILD_ID"
```

What this proves:

- market-only, combined-rule, and logistic-regression paths all run from the same dataset
- validation metrics and coefficient directions are reproducible artifacts rather than notebook output

What to look at:

- whether the combined rule outperforms the market-only baseline
- whether the classifier trades at all on validation rows
- the sign and rank order in `coefficients.json`

## Step 8: Run The Milestone 7 Walk-Forward Backtest

The default walk-forward burn-in is `50` rows. For a smoke test on a smaller local dataset, lower it explicitly:

```bash
.venv/bin/python scripts/run_walk_forward_backtest.py \
  --warehouse-path "$WAREHOUSE_PATH" \
  --dataset-build-id "$DATASET_BUILD_ID" \
  --minimum-training-rows 10
```

Use the default `50` again once you have enough data and want a more meaningful check.

What this proves:

- the strategy candidates can be replayed in timestamp order
- cost assumptions, position sizing, and capital limits are applied consistently
- the repository can make an evidence-based paper-trading decision

What to look at:

- `report.md` first
- then `summary.json`, `trades.jsonl`, and `equity_curve.jsonl`
- whether the paper-trading decision is `go` or `no-go`
- whether classifier results stay competitive after costs and sequencing

## Recommended Reading Order For Artifacts

If you want the shortest teacher-style tour, inspect results in this order:

1. notebook outputs from `00_api_connection.ipynb`
2. notebook charts from `00_live_market_walkthrough.ipynb`
3. notebook charts from `00_wallet_walkthrough.ipynb`
4. `signal_events` counts in DuckDB
5. event dataset `qa_report.json`
6. classifier `summary.json` and `coefficients.json`
7. backtest `report.md`

## If A Later Step Produces Too Little Data

That usually means the collection sample is too small, not that the code is broken.

The first fixes to try are:

- rerun `scripts/backfill_sample_markets.py` with a larger `--sample-size`
- increase `--trade-limit` to capture more market activity
- keep the same `WAREHOUSE_PATH` so the warehouse accumulates more assets before rerunning steps 5-8
- only lower `--minimum-training-rows` for a smoke test, not for a serious strategy readout
