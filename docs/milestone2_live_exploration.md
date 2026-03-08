# Milestone 2 Live Exploration

This runbook exercises the current Milestone 2 surface against real Polymarket data without building new system behavior.

It keeps each session isolated under `data/runs/<run_id>/` so you can inspect raw captures and DuckDB outputs without mixing runs together.

## Prerequisites

Bootstrap the local environment first. The current local `.venv` may be missing `duckdb` and `websockets`, so do not skip the import check.

```bash
scripts/bootstrap_env.sh
source .venv/bin/activate
.venv/bin/python -c "import duckdb, matplotlib, notebook, websockets; print('imports ok')"
```

If the import check fails, rerun `scripts/bootstrap_env.sh` when network access is available.

## Start An Isolated Run

Create a run id and isolated output paths:

```bash
export RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export RAW_DIR="data/runs/$RUN_ID/raw"
export WAREHOUSE_PATH="data/runs/$RUN_ID/warehouse/polymarket.duckdb"
```

Backfill one active market using the existing Milestone 2 command:

```bash
.venv/bin/python scripts/backfill_sample_markets.py \
  --sample-size 1 \
  --raw-dir "$RAW_DIR" \
  --warehouse-path "$WAREHOUSE_PATH"
```

Expected artifacts after the backfill:

- raw Gamma market selection capture under `data/runs/$RUN_ID/raw/gamma/sample_market_selection/`
- raw CLOB price history captures under `data/runs/$RUN_ID/raw/clob/sample_market_prices/`
- raw Data API trade captures under `data/runs/$RUN_ID/raw/data_api/sample_market_trades/`
- normalized DuckDB warehouse at `data/runs/$RUN_ID/warehouse/polymarket.duckdb`

## Inspect The Selected Market And Tokens

Read the selected market and its token ids directly from DuckDB:

```bash
.venv/bin/python - <<'PY'
import duckdb
import os

warehouse_path = os.environ["WAREHOUSE_PATH"]
with duckdb.connect(warehouse_path, read_only=True) as connection:
    market_row = connection.execute(
        """
        SELECT market_id, question, condition_id, liquidity, volume, end_time_utc
        FROM markets
        ORDER BY collection_time_utc DESC, market_id ASC
        LIMIT 1
        """
    ).fetchone()
    print("Selected market:", market_row)

    token_rows = connection.execute(
        """
        SELECT token_index, token_id
        FROM market_tokens
        WHERE market_id = ?
        ORDER BY token_index ASC
        """,
        [market_row[0]],
    ).fetchall()
    print("Tokens:")
    for token_row in token_rows:
        print(token_row)
PY
```

Build the `--asset-id` arguments for the live recorder:

```bash
export ASSET_ARGS="$(
  .venv/bin/python - <<'PY'
import duckdb
import os

with duckdb.connect(os.environ["WAREHOUSE_PATH"], read_only=True) as connection:
    token_ids = [
        row[0]
        for row in connection.execute(
            """
            SELECT token_id
            FROM market_tokens
            ORDER BY token_index ASC
            """
        ).fetchall()
    ]
print(" ".join(f"--asset-id {token_id}" for token_id in token_ids))
PY
)"
printf 'Recorder args: %s\n' "$ASSET_ARGS"
```

## Record A Live Session

Record a 300-second live session for every token in the selected market:

```bash
.venv/bin/python scripts/record_live_market_stream.py \
  $ASSET_ARGS \
  --session-seconds 300 \
  --raw-dir "$RAW_DIR" \
  --warehouse-path "$WAREHOUSE_PATH"
```

Expected live artifacts after the session:

- raw websocket captures under `data/runs/$RUN_ID/raw/websocket/live_market_channel_events/`
- normalized rows in `order_book_snapshots`
- normalized rows in `trades` when trade-shaped websocket messages are present

## Open The Walkthrough Notebook

Point the notebook at the same isolated run by exporting `POLYMARKET_EXPLORATION_RUN_ID`:

```bash
export POLYMARKET_EXPLORATION_RUN_ID="$RUN_ID"
jupyter lab notebooks/market_data_exploration/00_live_market_walkthrough.ipynb
```

Run the notebook top to bottom. It will:

- verify `duckdb`, `websockets`, `matplotlib`, and `notebook`
- reuse the existing collector scripts against the isolated run paths
- inspect normalized row counts and raw websocket message samples
- enrich token labels from the raw Gamma capture when `outcomes` are present
- plot `price_history`, `order_book_snapshots`, and `trades`
- tolerate low-activity sessions where no live trades are captured

## What To Review

After the notebook completes, check:

- whether the selected market and token ids make sense
- whether `price_history` contains enough points for both outcomes
- whether `order_book_snapshots` show stable mid-price and spread evolution
- whether live `trades` exist and how buy versus sell flow clusters by minute
- whether the raw websocket payloads match the normalized rows you expect
