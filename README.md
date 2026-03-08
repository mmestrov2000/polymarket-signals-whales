# Extreme Probability Market Calibration Study

A local-first Python research project for testing whether extreme prices in Polymarket and Kalshi are systematically miscalibrated.

The working thesis is simple:
- low-probability contracts below `10%` are overvalued
- high-probability contracts above `90%` are undervalued

This repository is intentionally narrow. Its job is to analyze the archive, produce visual and statistical evidence, and end with a clear go or no-go decision. It is not a trading bot project.

## Project Goals

- inventory the local Polymarket and Kalshi archive
- normalize comparable binary-market and resolution tables
- compare market-implied probability with realized resolution frequency
- measure calibration gaps in extreme buckets
- visualize the results clearly enough to decide whether a follow-on execution project is warranted

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
│  ├─ extreme_probability/
│  │  ├─ 00_dataset_inventory.ipynb
│  │  └─ 01_extreme_probability_analysis.ipynb
│  └─ polymarket_connection_checks/
├─ reports/
│  ├─ figures/
│  └─ summaries/
├─ scripts/
├─ src/
│  ├─ datasets/
│  ├─ analysis/
│  ├─ visualization/
│  ├─ research/
│  └─ storage/
└─ tests/
```

Legacy note:
- the repository still contains client and ingestion code from an earlier bot-oriented direction
- the active source of truth is the archive-analysis workflow described in the canonical docs

## Canonical Docs

- [PROJECT_SPEC.md](PROJECT_SPEC.md): goals, scope, hypotheses, acceptance criteria, and risks
- [ARCHITECTURE.md](ARCHITECTURE.md): active workflow, data model, and repo layout
- [TASKS.md](TASKS.md): ordered backlog for archive inventory, normalization, analysis, and reporting
- [docs/endpoint_capability_matrix.md](docs/endpoint_capability_matrix.md): legacy API reference retained from the earlier bot-oriented direction
- [docs/PLAYBOOK.md](docs/PLAYBOOK.md): team workflow and delivery conventions
- [AGENTS.md](AGENTS.md): repository rules for contributors and tools

## Getting Started

### Prerequisites

- Python 3.12 or newer
- Git
- enough local disk to work with a roughly `33 GB` compressed data archive plus staged extracts

### Local Setup

```bash
git clone https://github.com/mmestrov2000/polymarket-signals-whales.git
cd polymarket-signals-whales
scripts/bootstrap_env.sh
source .venv/bin/activate
```

Copy the environment template if you need one for legacy utilities:

```bash
cp .env.example .env
```

### First Read

1. Read `PROJECT_SPEC.md`.
2. Read `ARCHITECTURE.md`.
3. Read `TASKS.md`.
4. Start with the next pending archive-analysis task.

### First Execution Targets

The active notebooks are:

- `notebooks/extreme_probability/00_dataset_inventory.ipynb`
- `notebooks/extreme_probability/01_extreme_probability_analysis.ipynb`

Their jobs are:
- inventory the local archive
- define the canonical market-priced probability field
- analyze low and high probability calibration
- generate figures and decision-ready summaries

Expected local input:
- a Polymarket and Kalshi archive under `data/raw/*.tar.zst`

## Development Workflow

- use one branch per scoped task
- do not commit directly to `main`
- keep changes aligned with `PROJECT_SPEC.md`, `ARCHITECTURE.md`, and `TASKS.md`
- prefer the quickest credible path to an answer over broad platform-building
- keep execution and bot work out of scope unless the research explicitly justifies a new project

## Validation

Run the baseline checks locally before opening a PR:

```bash
scripts/validate_repo.sh
.venv/bin/pytest
```

## Technical Direction

Planned current-scope technologies:
- Python for normalization, analysis, and plotting helpers
- Jupyter notebooks for inventory and exploratory analysis
- DuckDB plus Parquet for local analytics
- lightweight statistics for calibration gaps and uncertainty checks

## Current Status

Completed:
- pivoted project docs
- minimal archive-analysis scaffold
- repo guardrails aligned with the new workflow

Next:
- inventory the archive contents
- define the canonical binary-market schema
- build the first calibration tables

## Important Risks

- archive schema differences between Polymarket and Kalshi may complicate direct comparison
- repeated ticks can overstate effective sample size
- price-source choice matters for the meaning of "market probability"
- selective extraction may be necessary to keep the workflow practical on one machine

## Disclaimer

This repository is for research and software development. It is not financial advice. If the thesis looks promising, any future trading work should live in a separate project with its own risk controls.
