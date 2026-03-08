# Tasks

Status legend:
- `completed` means the task is already done in the repository
- `pending` means it is approved and ready for implementation
- `blocked` means it depends on a prior research result

## Milestone 0 - Project Bootstrap

### T0.1 Finalize canonical product docs
Status: `completed`

Goal:
- replace template docs with a Polymarket-specific spec, architecture, and milestone plan

Implementation steps:
1. Rewrite `PROJECT_SPEC.md` around a research-first, data-validation-first scope.
2. Rewrite `ARCHITECTURE.md` with only the components needed for discovery, collection, and offline evaluation.
3. Rewrite `TASKS.md` into small milestone-scoped tasks with acceptance criteria.

Acceptance criteria:
- the three canonical docs are consistent with each other
- the MVP is explicitly defined as a research platform, not a live trader
- risks, unknown APIs, and data constraints are documented

### T0.2 Create the minimal current-scope scaffold
Status: `completed`

Goal:
- create the notebook and package directories required for implementation to start immediately

Implementation steps:
1. Add `notebooks/polymarket_connection_checks/00_api_connection.ipynb`.
2. Create empty module directories for `src/clients`, `src/ingestion`, `src/storage`, `src/signals`, `src/research`, and `tests`.
3. Keep the scaffold limited to current-scope components only.

Acceptance criteria:
- the connection-check notebook path exists
- the current-scope source directories exist
- no later-phase execution or trading modules are added yet

### T0.3 Add Python project configuration and environment template
Status: `completed`

Goal:
- make the repository runnable for Python development without guessing package or environment setup

Implementation steps:
1. Add a Python project file such as `pyproject.toml` with baseline tooling and dependencies.
2. Add `.env.example` documenting expected Polymarket and storage configuration values.
3. Update `README.md` with local setup and notebook usage instructions.

Acceptance criteria:
- a fresh clone can bootstrap a Python environment locally
- required environment variables are documented
- developers can identify how to run the notebook and future scripts

### T0.4 Establish CI guardrails for the tracked repository
Status: `completed`

Goal:
- make CI reflect the actual tracked repository and remain useful as implementation begins

Implementation steps:
1. Remove template-era validation requirements for local-only directories that are intentionally not tracked.
2. Add a Python-based CI baseline that bootstraps the environment, validates repository structure, and runs tests.
3. Add deterministic tests that simulate a clean checkout and validate notebook structure.

Acceptance criteria:
- CI passes on a clean clone without relying on ignored local files
- the workflow bootstraps Python and runs automated tests
- the baseline checks are specific to this project rather than the original template

## Milestone 1 - Polymarket API Connectivity

### T1.1 Verify Gamma and CLOB public REST access in the notebook
Status: `completed`

Goal:
- confirm that market metadata, price, and public market-state endpoints are reachable and useful

Implementation steps:
1. Add notebook cells that call Gamma market metadata endpoints.
2. Add notebook cells that call the planned public CLOB endpoints.
3. Print or persist representative payload samples and key identifiers.

Acceptance criteria:
- the notebook can reach the chosen Gamma and CLOB endpoints from a local environment
- sample payloads are captured for later schema design
- required identifiers for cross-endpoint joins are documented

### T1.2 Verify Data API wallet and trade-related endpoints in the notebook
Status: `completed`

Goal:
- prove which wallet-level and trade-history endpoints can support whale analysis

Implementation steps:
1. Add notebook checks for wallet, holder, position, trade-history, and open-interest endpoints that appear relevant.
2. Record which fields are returned for wallet identity, side, size, outcome, and timestamps.
3. Save a short note on missing fields or weak coverage.

Acceptance criteria:
- the repository has a written record of which wallet endpoints are usable
- sample responses exist for at least one wallet-related endpoint
- known gaps for wallet attribution are documented

Notes:
- `notebooks/polymarket_connection_checks/00_api_connection.ipynb` now checks Data API leaderboard, positions, closed positions, activity, trades, holders, and open-interest endpoints.
- The notebook saves Data API samples under `data/raw/data_api/connection_checks/` and prints field-coverage plus wallet-attribution caveats inline.
- Live verification showed `GET /activity` and `GET /trades` expose `proxyWallet`, `side`, `size`, `outcome`, and `timestamp`, while `GET /holders` and `GET /oi` are useful concentration inputs but not trade logs.
- `GET /positions` remained reachable but returned an empty snapshot for the sampled leaderboard wallet, so open-position coverage should be treated as wallet-dependent rather than guaranteed.

### T1.3 Verify WebSocket connectivity and message shape
Status: `completed`

Goal:
- prove that a live stream can be opened and that the message schema is understandable enough for a recorder

Implementation steps:
1. Add a notebook section or helper code that opens the target WebSocket feed.
2. Subscribe to a small set of markets or channels.
3. Capture and inspect example messages, including reconnect behavior if possible.

Acceptance criteria:
- a local run receives at least one live message from the target feed
- message samples are stored or pasted into notebook outputs
- reconnect or heartbeat expectations are noted

Notes:
- `src/clients/polymarket_websocket.py` now provides a thin public market-channel helper that normalizes the `wss://.../ws/market` URL, sends the documented `assets_ids` subscription payload, persists captured samples, and records connection plus reconnect events.
- `notebooks/polymarket_connection_checks/00_api_connection.ipynb` now reuses the selected Gamma/CLOB token id, captures live market-channel samples under `data/raw/websocket/connection_checks/`, and prints message-shape summaries plus representative payloads.
- Live verification on 2026-03-08 captured a list-wrapped `book` payload containing `asset_id`, `market`, `hash`, `last_trade_price`, `tick_size`, `timestamp`, `bids`, and `asks`, which is sufficient to design a later recorder.
- The public market-channel docs do not currently document an application-level heartbeat, so the helper treats observed close or timeout events as reconnect triggers and resends the same subscription once for the notebook check.

### T1.4 Produce an endpoint capability matrix
Status: `completed`

Goal:
- convert notebook findings into a short implementation reference for later milestones

Implementation steps:
1. Summarize each validated endpoint, required parameters, and useful fields.
2. Mark each endpoint as usable now, usable later with auth, or unsuitable.
3. List unresolved questions, rate limits, and known caveats.

Acceptance criteria:
- the repo contains a concise capability summary produced from Milestone 1 checks
- future implementation tasks can point to confirmed fields instead of assumptions
- unknowns are explicit rather than hidden in notebook output

Notes:
- `src/clients/endpoint_capabilities.py` now captures the Milestone 1 endpoint matrix as a structured source of truth, including required inputs, useful fields, join keys, rate-limit caveats, and unresolved questions.
- `docs/endpoint_capability_matrix.md` now provides the concise implementation reference for later tasks, covering Gamma, CLOB REST, the public CLOB market-channel WebSocket, Data API endpoints, and deferred authenticated CLOB execution work.
- Live public spot-checks on 2026-03-08 tightened the matrix for CLOB `/price`, Data API `/holders`, Data API `/oi`, and a secondary non-empty `/positions` wallet snapshot while preserving the notebook caveat that `/positions` coverage is wallet-dependent.

## Milestone 2 - Market Data Collection

### T2.1 Implement thin public API clients
Status: `completed`

Goal:
- create reusable Python clients for the confirmed Polymarket API surfaces

Implementation steps:
1. Implement thin clients for Gamma, CLOB, and any usable Data API endpoints.
2. Add request timeout, retry, and basic parsing behavior.
3. Cover response normalization with fixture-based tests.

Acceptance criteria:
- each client can fetch a representative endpoint used in Milestone 1
- parsing behavior is covered by tests using saved payload samples
- client modules contain no research-specific business logic

Notes:
- `src/clients/rest.py` now provides the shared public REST foundation for request timeouts, retryable-status handling, and thin normalization helpers for decimals, datetimes, and nested record wrappers.
- `src/clients/gamma.py`, `src/clients/clob.py`, and `src/clients/data_api.py` now implement reusable public clients for the Milestone 1 verified Gamma, CLOB, and Data API endpoints without embedding research-specific logic.
- `tests/test_public_api_clients.py` and `tests/fixtures/public_clients/` now cover representative endpoint fetching, fixture-based normalization, and retry behavior through `httpx.MockTransport`.

### T2.2 Implement raw storage and normalized schemas
Status: `completed`

Goal:
- make collection reproducible and analytics-ready

Implementation steps:
1. Define raw payload storage layout and metadata fields.
2. Add initial DuckDB schema definitions for markets, prices, and trades.
3. Add helpers for append-only raw writes and idempotent normalized upserts.

Acceptance criteria:
- raw payloads can be stored without overwriting prior captures
- normalized tables exist for markets, prices, and trades
- repeated ingestion does not create uncontrolled duplication

Notes:
- `src/storage/raw.py` now defines append-only raw capture helpers under `data/raw/<source>/<dataset>/date=YYYY-MM-DD/` with capture metadata including endpoint, request params, and UTC collection time, using JSON for object payloads and JSONL for list-like payloads.
- `src/storage/warehouse.py` now provisions DuckDB tables for `markets`, `market_tokens`, `price_history`, and `trades`, and provides idempotent upsert helpers for `GammaMarket`, `PriceHistory`, and `TradeRecord` inputs.
- `tests/test_storage.py` covers append-only raw writes, schema creation, and repeated upserts without duplicate normalized rows.

### T2.3 Build a sample market backfill command
Status: `completed`

Goal:
- backfill a small, explicit market cohort to prove the collection path works end to end

Implementation steps:
1. Define a small sample market-selection rule.
2. Fetch metadata, prices, and trades for that cohort.
3. Persist both raw payloads and normalized records.

Acceptance criteria:
- a developer can run one command to backfill the chosen sample set
- collected data is readable from the analytical store
- the command reports failures and skipped items clearly

Notes:
- `src/ingestion/sample_market_backfill.py` now defines a deterministic sample selection rule for open Gamma markets, a backfill job that stores raw Gamma/CLOB/Data API payloads, and DuckDB upserts for market metadata, price history, and market trades.
- `scripts/backfill_sample_markets.py` provides the one-command entry point with configurable sample size, Gamma scan depth, price-history params, raw output path, and warehouse path, and prints a clear summary plus any failures.
- `tests/test_sample_market_backfill.py` and `tests/test_backfill_sample_markets_script.py` cover deterministic selection, end-to-end raw/normalized persistence, partial-failure reporting, and the script output contract.

### T2.4 Build a live recorder for trades and top-of-book snapshots
Status: `pending`

Goal:
- start forward collection for the features that historical APIs may not support well

Implementation steps:
1. Add a WebSocket-driven collector for live messages.
2. Persist raw stream events and normalized trade or top-of-book records.
3. Add reconnect handling and a simple dropped-stream warning path.

Acceptance criteria:
- the recorder can run continuously for a meaningful session
- captured stream data lands in both raw and normalized storage
- connection interruptions are logged instead of silently ignored

## Milestone 3 - Whale Wallet Data Collection

### T3.1 Define the wallet universe
Status: `pending`

Goal:
- choose which wallets are worth profiling without requiring full-chain attribution

Implementation steps:
1. Define inclusion rules such as leaderboard wallets, high-volume wallets, and wallets active during detected events.
2. Document the tradeoff between coverage and implementation complexity.
3. Store the resulting wallet seed list in a reproducible format.

Acceptance criteria:
- the wallet universe definition is written and reproducible
- the seed list can be regenerated from confirmed data sources
- the approach does not depend on speculative wallet clustering

### T3.2 Implement wallet-history collection
Status: `pending`

Goal:
- collect the raw inputs needed to score wallet quality

Implementation steps:
1. Implement collection of confirmed wallet position, trade, and outcome endpoints.
2. Normalize wallet history into analytical tables.
3. Preserve collection timestamps and source metadata.

Acceptance criteria:
- wallet-history records exist for a sample wallet cohort
- normalization keeps wallet address, market linkage, and timestamps intact
- collection failures are observable and retryable

### T3.3 Build the wallet profile table
Status: `pending`

Goal:
- compute time-aware wallet metrics suitable for event-time research

Implementation steps:
1. Define wallet metrics such as ROI, hit rate, category performance, trade size, and recency.
2. Ensure every metric is computable as of a historical cutoff time.
3. Write tests around metric computation and edge cases such as low sample size.

Acceptance criteria:
- a wallet profile row can be generated for a sample cohort
- the profile includes reliability or sample-size context
- metrics do not leak future outcomes into historical events

## Milestone 4 - Signal Feature Engineering

### T4.1 Implement market anomaly feature calculations
Status: `pending`

Goal:
- compute market-only signals from trades, prices, and forward-collected liquidity state

Implementation steps:
1. Add rolling baseline and z-score calculations for volume and activity.
2. Add order-flow and return-window features.
3. Add liquidity-shift features where forward-collected data is available.

Acceptance criteria:
- feature calculations run on normalized market data
- every feature definition is documented and testable
- unsupported historical liquidity features are gated behind forward data availability

### T4.2 Implement wallet-quality feature calculations
Status: `pending`

Goal:
- summarize the wallets behind a candidate move into usable event-time features

Implementation steps:
1. Convert wallet profile metrics into event-time wallet summary features.
2. Add concentration metrics such as top-wallet share and weighted average quality.
3. Add tests for empty, sparse, and mixed-quality wallet sets.

Acceptance criteria:
- wallet-summary features can be computed for a candidate event
- concentration and quality metrics are deterministic
- sparse-wallet cases are handled explicitly

### T4.3 Build the event detector and explanation payload
Status: `pending`

Goal:
- produce interpretable candidate events that combine market triggers and active-wallet context

Implementation steps:
1. Define trigger rules for market anomalies.
2. Attach active-wallet summaries to each triggered event.
3. Emit an explanation payload showing why the event fired.

Acceptance criteria:
- event records are generated from historical data
- each event includes trigger reason, timestamps, and participating-wallet context
- the output is understandable enough to review in notebooks or tests

## Milestone 5 - Dataset Generation

### T5.1 Define labels and trade-cost assumptions
Status: `pending`

Goal:
- make the research target executable and leakage-safe

Implementation steps:
1. Define continuation, reversion, and net-PnL labels for fixed post-event horizons.
2. Define fee, spread, and slippage assumptions for simulated entry and exit.
3. Document which label will serve as the primary training target.

Acceptance criteria:
- label definitions are explicit and reproducible
- cost assumptions are written down and encoded in code
- label generation does not use information from after prediction time

### T5.2 Build the event dataset pipeline
Status: `pending`

Goal:
- join market features, wallet features, and labels into training rows

Implementation steps:
1. Define the event dataset schema.
2. Join trigger-time market and wallet features to future labels.
3. Split datasets by time for future model evaluation.

Acceptance criteria:
- a backtest-ready dataset can be materialized from stored inputs
- each row is traceable back to source events and collection timestamps
- train and validation splits are time ordered

### T5.3 Add dataset QA and leakage checks
Status: `pending`

Goal:
- catch data errors before model training begins

Implementation steps:
1. Add checks for duplicate events, null-heavy columns, and impossible timestamps.
2. Add checks that wallet features only use past data.
3. Produce a small QA report for each dataset build.

Acceptance criteria:
- dataset builds fail loudly on obvious integrity issues
- leakage checks cover wallet features and future labels
- a QA summary is saved with each dataset run

## Milestone 6 - Signal Classifier Training

### T6.1 Run baseline rule-based experiments
Status: `pending`

Goal:
- establish whether simple spike-following or wallet-conditioned rules show any edge

Implementation steps:
1. Implement a market-only baseline.
2. Implement a combined market-plus-wallet baseline.
3. Compare both baselines under the agreed cost assumptions.

Acceptance criteria:
- the repo can reproduce the baseline experiment results
- spike-only and combined strategies are compared on the same dataset split
- results include net-of-costs metrics

### T6.2 Train and evaluate a simple classifier
Status: `pending`

Goal:
- test whether a lightweight model improves on hand-built rules without overfitting

Implementation steps:
1. Train a simple model such as logistic regression or gradient-boosted trees.
2. Evaluate it with time-based validation.
3. Inspect feature importance or coefficient direction for sanity.

Acceptance criteria:
- model training runs end to end on the generated dataset
- evaluation is performed on out-of-sample time windows
- feature effects are reviewable and not obviously leakage-driven

## Milestone 7 - Backtesting Engine

### T7.1 Implement a walk-forward backtest runner
Status: `pending`

Goal:
- evaluate candidate strategies under realistic sequencing and cost assumptions

Implementation steps:
1. Add a walk-forward execution simulator driven by event timestamps.
2. Apply entry, exit, and sizing rules consistently across strategies.
3. Report trade-level and portfolio-level metrics.

Acceptance criteria:
- strategies can be replayed on historical event streams
- fees, spread, and slippage are included
- output includes PnL, hit rate, and drawdown-style metrics

### T7.2 Publish a strategy comparison report
Status: `pending`

Goal:
- decide whether the strategy is strong enough to justify online paper trading

Implementation steps:
1. Compare market-only, combined-rule, and classifier-based strategies.
2. Break results down by category, liquidity bucket, and time period.
3. Record the go or no-go decision for paper trading.

Acceptance criteria:
- there is a single report or notebook summarizing strategy comparison
- category and liquidity breakdowns are included
- the next milestone decision is evidence-based

## Milestone 8 - Paper Trading

### T8.1 Build real-time scoring and alerting
Status: `pending`

Goal:
- reuse the research pipeline online without sending real orders

Implementation steps:
1. Consume live market updates from the recorder path.
2. Compute current event features and scores in near real time.
3. Emit alerts or logs for actionable paper signals.

Acceptance criteria:
- real-time scoring runs against live data
- actionable events are logged with explanation payloads
- no authenticated trading is required

### T8.2 Build the paper trading loop and journal
Status: `pending`

Goal:
- observe real-time decision quality before any live execution work begins

Implementation steps:
1. Simulate entries and exits using the online scores and the agreed cost model.
2. Persist paper trades and daily summaries.
3. Review paper performance against offline expectations.

Acceptance criteria:
- paper trades are produced and persisted
- daily summaries show PnL and strategy diagnostics
- discrepancies between paper and offline behavior can be investigated

## Milestone 9 - Autonomous Trading

### T9.1 Implement authenticated trading sandbox and hard risk controls
Status: `pending`

Goal:
- make sure order placement can be tested safely before any live rollout

Implementation steps:
1. Implement authenticated request signing and order-submission helpers.
2. Add position limits, notional caps, kill switch logic, and daily loss guards.
3. Test the full path in the safest available environment before touching real capital.

Acceptance criteria:
- order requests can be signed and submitted in a controlled environment
- risk controls are enforced before any order is sent
- all trading actions are logged for auditability

### T9.2 Write the limited live deployment playbook
Status: `pending`

Goal:
- define the conditions under which the bot may place real trades

Implementation steps:
1. Document rollout prerequisites, monitoring, and rollback procedures.
2. Define capital limits and success or failure thresholds.
3. Record operator checklists for startup, shutdown, and incident response.

Acceptance criteria:
- live deployment has a written go-live checklist
- rollback steps are explicit
- the bot cannot be considered production-ready without this playbook
