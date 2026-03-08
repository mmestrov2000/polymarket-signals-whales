# Polymarket Whale Signals Bot

A research-first Python project for detecting potentially informed Polymarket activity, scoring the wallets behind that activity, and determining whether those signals are worth following.

The project does not start with automated trading. It starts by proving that the required Polymarket data actually exists, can be collected reliably, and supports leakage-safe research.

## Project Goals

- collect Polymarket market, trade, and wallet data
- detect abnormal market behavior that may indicate informed flow
- score wallet quality using historical performance and specialization
- combine market signals and wallet signals into actionable event rows
- evaluate whether the combined signal is profitable after realistic costs
- graduate to paper trading first, then limited live trading only if evidence supports it

## Strategy Summary

The working thesis is:

1. Market anomalies such as volume spikes, order flow imbalance, or liquidity shifts may indicate informed activity.
2. Those anomalies become more useful when the wallets behind them have a strong historical profile.
3. A signal should only be actionable when both the market signal and the wallet-quality signal are strong.

This is explicitly not a naive whale-copying bot.

## Current Scope

Current development is focused on:
- validating Polymarket API connectivity and payload shape
- building thin clients for confirmed endpoints
- collecting raw and normalized market data
- collecting wallet-history data where available
- generating event datasets for offline research and backtesting

Out of scope for the current milestone:
- real-money trading
- large-scale deployment
- speculative architecture that depends on unverified API capabilities

## Milestone Path

- Milestone 0: project bootstrap
- Milestone 1: Polymarket API connectivity verification
- Milestone 2: market data collection
- Milestone 3: whale wallet data collection
- Milestone 4: signal feature engineering
- Milestone 5: dataset generation
- Milestone 6: signal classifier training
- Milestone 7: backtesting engine
- Milestone 8: paper trading
- Milestone 9: autonomous trading

The detailed plan lives in [TASKS.md](TASKS.md).

## Repository Layout

```text
.
├─ PROJECT_SPEC.md
├─ ARCHITECTURE.md
├─ TASKS.md
├─ AGENTS.md
├─ docs/
├─ notebooks/
│  └─ polymarket_connection_checks/
│     └─ 00_api_connection.ipynb
├─ scripts/
├─ src/
│  ├─ clients/
│  ├─ ingestion/
│  ├─ storage/
│  ├─ signals/
│  └─ research/
└─ tests/
```

## Canonical Docs

- [PROJECT_SPEC.md](PROJECT_SPEC.md): product goals, scope, risks, MVP, and research questions
- [ARCHITECTURE.md](ARCHITECTURE.md): current-scope architecture, data flow, and storage design
- [TASKS.md](TASKS.md): ordered milestone backlog with implementation steps and acceptance criteria
- [docs/PLAYBOOK.md](docs/PLAYBOOK.md): team workflow and delivery conventions
- [AGENTS.md](AGENTS.md): repository rules for contributors and tools

## Getting Started

### Prerequisites

- Python 3.12 or newer
- Git

### Local Setup

```bash
git clone https://github.com/mmestrov2000/polymarket-signals-whales.git
cd polymarket-signals-whales
scripts/bootstrap_env.sh
source .venv/bin/activate
```

Copy the environment template before adding real credentials:

```bash
cp .env.example .env
```

### First Read

1. Read `PROJECT_SPEC.md`.
2. Read `ARCHITECTURE.md`.
3. Read `TASKS.md`.
4. Start implementation from the next pending task, not from ad hoc code changes.

### First Execution Target

The first concrete build target is the connection verification notebook:

`notebooks/polymarket_connection_checks/00_api_connection.ipynb`

Its job is to verify:
- Gamma API connectivity
- CLOB API public access
- wallet-related Data API coverage
- WebSocket connectivity and message shape
- key identifiers needed for later joins

For Milestone 1 notebook work, launch `jupyter notebook` or `jupyter lab`, run the notebook top to bottom, and review the saved sample payloads under `data/raw/gamma/connection_checks/` and `data/raw/clob/connection_checks/`.

## Development Workflow

- use one branch per scoped task
- do not commit directly to `main`
- keep changes aligned with `PROJECT_SPEC.md`, `ARCHITECTURE.md`, and `TASKS.md`
- keep tasks small and testable
- prefer proving data availability before building strategy logic

## Validation

Run the same baseline checks locally before opening a PR:

```bash
scripts/validate_repo.sh
.venv/bin/pytest
```

## Technical Direction

Planned core technologies:
- Python for all clients, ingestion, feature engineering, and research code
- Jupyter notebooks for connection checks and exploratory validation
- DuckDB plus local raw payload storage for reproducible analytics

Likely Polymarket sources:
- Gamma API for market metadata
- CLOB API for trades, prices, and market state
- WebSocket feeds for live updates
- Data API for wallet, holder, position, and trade-history research

## Current Status

Completed:
- project spec, architecture, and task plan
- minimal repository scaffold
- connection-check notebook placeholder

Next:
- Python project configuration and `.env.example`
- Milestone 1 endpoint validation notebook work

## Important Risks

- some wallet-attribution fields may be missing or inconsistent across endpoints
- historical order book depth may not be available at the quality needed for backtests
- apparent strategy edge may disappear after spread, slippage, fees, and latency
- wallet-quality features can be misleading if not computed in a time-safe way

## Disclaimer

This repository is for research and software development. It is not financial advice, and any future live trading functionality should only be used with explicit risk controls and limited capital.
