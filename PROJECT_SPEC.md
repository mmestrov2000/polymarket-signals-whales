# Extreme Probability Market Calibration Study

## Project Overview

This repository is a local-first research project focused on one narrow question:

Are extreme prices in binary prediction markets systematically miscalibrated?

The working thesis is:
- contracts priced below `10%` are overvalued
- contracts priced above `90%` are undervalued
- the effect is driven by long-shot preference, where traders overpay for large payoffs and underpay for highly likely outcomes

The project will use a local dataset archive of roughly `33 GB` containing Polymarket and Kalshi data. The goal is to reach a statistically defensible conclusion quickly. This repository stops at data analysis, visualization, and an explicit go or no-go conclusion. If the effect looks real enough to trade, execution work moves to a separate project later.

Primary user:
- the maintainer or researcher validating whether extreme-probability bias is real, stable, and large enough to justify a follow-on trading project

Desired outcome:
- a reproducible local workflow that turns raw venue data into comparable probability and resolution tables, calibration statistics, and clear figures

## Core Research Questions

- What exactly is inside the local Polymarket and Kalshi archive?
- Which files and fields are usable for binary market analysis?
- How should venue-specific schemas be mapped into a comparable canonical model?
- For observations below `10%`, is the realized resolution frequency lower than the market-implied probability?
- For observations above `90%`, is the realized resolution frequency higher than the market-implied probability?
- Does the effect hold under more careful sampling that avoids over-weighting long-lived markets?
- Is the pattern consistent across both venues, or is it venue-specific?

## Hypotheses

### H1. Long-shot bias in low-probability contracts

For observations with market-implied probability below `0.10`, the empirical YES resolution rate will be lower than the average quoted probability.

Interpretation:
- `calibration_gap = empirical_yes_rate - average_quoted_probability`
- H1 predicts a negative calibration gap in low-probability buckets

### H2. Favorite bias in high-probability contracts

For observations with market-implied probability above `0.90`, the empirical YES resolution rate will be higher than the average quoted probability.

Interpretation:
- H2 predicts a positive calibration gap in high-probability buckets

### H3. The effect survives basic robustness checks

The directional result should still appear when comparing:
- tick-weighted summaries
- market-weighted or threshold-entry summaries
- Polymarket versus Kalshi

## Current Scope

In scope now:
- inspect the local archive and document available files, schemas, and constraints
- extract or stage only the data needed for analysis
- normalize comparable binary-market and resolution tables across venues
- define a canonical market-priced probability field for each venue
- create calibration summaries for extreme probability buckets
- run simple statistical checks and uncertainty estimates
- produce visualizations and a short written conclusion

Out of scope now:
- live API integration
- wallet or whale analysis
- paper trading or live trading
- execution infrastructure
- full strategy backtesting
- any feature work that is not directly required to answer the calibration question

## Data Sources

### Primary dataset

- source archive: `https://s3.jbecker.dev/data.tar.zst`
- expected local placement: `data/raw/*.tar.zst`
- observed local archive footprint: about `34G` under `data/`
- observed archive roots include `data/polymarket/` and `data/kalshi/`

### First-pass scope cuts

To reach a conclusion quickly, the first analysis pass should prefer:
- resolved binary markets only
- markets with clear timestamp and final outcome fields
- observations that can be normalized to a probability in `[0, 1]`

The first pass should exclude or defer:
- ambiguous multi-outcome markets
- records with unclear resolution mapping
- price sources that cannot be compared across venues without extra assumptions

## Key Definitions

- `market-priced probability`: the venue-specific probability quoted by the dataset, normalized to `[0, 1]`
- `real probability`: the empirical resolution frequency within a defined bucket of observations
- `calibration gap`: `empirical_yes_rate - average_quoted_probability`
- `low-probability bucket`: any normalized probability below `0.10`
- `high-probability bucket`: any normalized probability above `0.90`

### Sampling rule

The repository should report both:
- descriptive tick-level summaries using all usable observations
- primary inference using a sampling rule that reduces repeated-tick bias, such as threshold-entry events or market-weighted summaries

The project must not rely on all ticks alone for its main conclusion if that would let a small set of slow-moving markets dominate the result.

## Deliverables

- an archive inventory notebook
- a canonical DuckDB or Parquet analysis dataset
- bucket-level calibration tables for low and high probability regions
- cross-venue comparison tables for Polymarket and Kalshi
- figures that show quoted probability versus realized frequency
- a short memo stating whether the thesis is supported, unsupported, or inconclusive

## Acceptance Criteria

- the repo can identify and read the relevant archive files for both venues
- the canonical dataset contains comparable probability and resolution fields for a first-pass cohort of resolved binary markets
- the analysis reports calibration gaps for low and high extremes with explicit uncertainty estimates
- at least one robustness check compares tick-weighted and market-aware sampling
- the repo produces figures for each venue and at least one combined comparison
- the final memo states whether the effect is strong enough to justify a separate trading project

## Test Strategy

Testing should stay focused on correctness of the analysis pipeline, not on broad framework coverage.

Required test areas:
- archive discovery and file inventory helpers
- venue-specific schema normalization
- probability normalization and bucket assignment
- resolution mapping
- sampling logic for threshold-entry or market-weighted views
- notebook and scaffold guardrails for the active research workflow

Preferred approach:
- use tiny extracted fixtures from the local archive
- keep tests deterministic and cheap enough for normal CI
- treat statistical output tests as shape or invariant checks, not exact floating-point snapshots unless the inputs are fixed fixtures

## Risks and Constraints

### Data risks

- the compressed archive may require careful selective extraction to avoid unnecessary disk usage
- Polymarket and Kalshi may use different market, contract, and resolution conventions
- some files may contain metadata noise such as Apple `._*` entries that must be filtered out
- not every file that looks relevant may contain a clean probability field

### Analysis risks

- repeated ticks from the same market can create a false sense of sample size
- observed prices may differ by source type, such as trades versus quotes
- low-frequency edge cases near resolution may dominate extreme buckets if sampling is naive
- segmentation by category or time to expiry can reduce sample sizes quickly

### Operational constraints

- all work should stay local-first and runnable on a single developer machine
- the project should prefer DuckDB and columnar workflows over heavyweight infrastructure
- the team should optimize for a fast conclusion rather than an exhaustive market microstructure study

## Development Principles

- Optimize for the quickest credible path to truth.
- Keep the first pass narrow and binary-market focused.
- Document every assumption that affects comparability across venues.
- Prefer simple calibration tables and plots before deeper modeling.
- Treat inherited bot-oriented code as legacy context, not the active roadmap.
- If the thesis survives, move execution and trading work into a new project rather than expanding this one prematurely.
