# Tasks

Status legend:
- `completed` means the task is already done in this pivoted foundation
- `pending` means it is approved and ready for implementation
- `blocked` means it depends on a prior research result

## Milestone 0 - Pivot and Analysis Bootstrap

### T0.1 Rewrite the canonical project docs
Status: `completed`

Goal:
- replace the bot-oriented roadmap with a narrow archive-analysis roadmap focused on extreme-probability calibration

Implementation steps:
1. Rewrite `PROJECT_SPEC.md` around the long-shot and favorite bias thesis.
2. Rewrite `ARCHITECTURE.md` around static archive analysis, canonical tables, and visual outputs.
3. Rewrite `TASKS.md` into milestones that stop at a research conclusion rather than trading automation.

Acceptance criteria:
- the three canonical docs are consistent with each other
- live API, wallet, and trading work are out of the active scope
- the project goal is explicitly to reach a fast, defensible conclusion from the local archive

### T0.2 Create the minimal exploration scaffold
Status: `completed`

Goal:
- create the notebook, source, and report paths required for the exploration workflow

Implementation steps:
1. Add placeholder notebooks under `notebooks/extreme_probability/`.
2. Add source directories for `src/datasets`, `src/analysis`, and `src/visualization`.
3. Add report output directories under `reports/`.

Acceptance criteria:
- the first-pass exploration paths exist in the tracked repository
- the scaffold matches the rewritten architecture
- no new execution-oriented modules are introduced as part of the pivot

### T0.3 Update repo guardrails for the new workflow
Status: `completed`

Goal:
- make validation and CI point at the active exploration scaffold rather than the old connectivity notebook

Implementation steps:
1. Update repo validation to require the extreme-probability notebooks and report scaffold.
2. Update tests to validate the new notebook placeholders and their research focus.
3. Update CI notebook validation to check the new active notebooks.

Acceptance criteria:
- repository guardrails describe the active workflow
- CI can validate the new notebook JSON
- the canonical docs and repository checks no longer contradict each other

## Milestone 1 - Dataset Inventory and Schema Mapping

### T1.1 Inventory the archive contents and extraction constraints
Status: `pending`

Goal:
- understand exactly what is available before writing normalization code

Implementation steps:
1. Enumerate top-level Polymarket and Kalshi directories, file formats, and partition patterns.
2. Estimate extraction footprint and identify any files that can be ignored immediately.
3. Persist a simple inventory manifest or table for later tasks.

Acceptance criteria:
- the archive structure is documented for both venues
- irrelevant file noise such as Apple metadata entries is explicitly identified
- the team knows which files must be extracted or staged for the first-pass analysis

### T1.2 Define the canonical binary-market mapping
Status: `pending`

Goal:
- map venue-specific market and contract fields into one comparable model

Implementation steps:
1. Identify market ids, contract ids, outcome labels, timestamps, and resolution fields for each venue.
2. Document how YES and NO conventions differ between Polymarket and Kalshi.
3. Exclude unsupported record types from the first pass.

Acceptance criteria:
- each venue has a written field mapping into the canonical model
- the first-pass cohort is limited to resolved binary contracts with clear outcomes
- unresolved schema questions are explicit rather than hidden in notebooks

### T1.3 Choose the canonical priced-probability field
Status: `pending`

Goal:
- decide what "market-priced probability" means in the dataset for each venue

Implementation steps:
1. Inspect candidate price fields such as trade price, quote price, or end-of-interval close.
2. Choose a primary field for the first-pass analysis and document any fallback fields.
3. Define the normalization rule to `[0, 1]` and the initial extreme buckets.

Acceptance criteria:
- the first-pass analysis has one documented priced-probability definition per venue
- price-source differences are not mixed silently
- bucket boundaries for low and high extremes are fixed

## Milestone 2 - Canonical Dataset Build

### T2.1 Implement archive discovery and inventory helpers
Status: `pending`

Goal:
- make archive inspection reproducible rather than notebook-only

Implementation steps:
1. Add helpers that discover relevant files under `data/raw/` or staged extraction paths.
2. Filter ignored files and classify records by venue and data type.
3. Write inventory outputs that can feed normalization jobs.

Acceptance criteria:
- archive discovery works from a local checkout
- inventory output is deterministic for a fixed archive
- ignored noise files do not leak into later steps

### T2.2 Normalize markets, contracts, and resolution outcomes
Status: `pending`

Goal:
- build the comparable backbone of the analysis dataset

Implementation steps:
1. Normalize venue-specific market metadata into canonical market tables.
2. Normalize binary contract identifiers and outcome labels.
3. Normalize final resolution outcomes into a comparable YES or NO field.

Acceptance criteria:
- the canonical tables can support cross-venue joins
- every included contract has a clear final outcome
- unsupported edge cases are filtered or documented

### T2.3 Normalize tick observations
Status: `pending`

Goal:
- build the main table of probability observations used by the study

Implementation steps:
1. Parse the selected probability field from the relevant raw records.
2. Normalize timestamps and source metadata.
3. Persist a `tick_observations` table with venue, contract, time, probability, and price-source fields.

Acceptance criteria:
- observations are normalized to `[0, 1]`
- timestamps are comparable across venues
- each observation records its source type explicitly

### T2.4 Build market-aware sampling views
Status: `pending`

Goal:
- prevent repeated ticks from dominating the primary conclusion

Implementation steps:
1. Implement at least one threshold-entry or market-weighted sampling rule.
2. Preserve a full tick-weighted view for descriptive reporting.
3. Document the difference between descriptive and inferential views.

Acceptance criteria:
- the repo can compare all-tick and market-aware summaries
- the primary inference path does not rely on raw tick counts alone
- sampling rules are documented and testable

## Milestone 3 - Statistical Analysis

### T3.1 Compute calibration tables for extreme buckets
Status: `pending`

Goal:
- measure whether low and high probability buckets are miscalibrated

Implementation steps:
1. Compute average quoted probability and empirical YES rate per bucket.
2. Compute the calibration gap for each bucket.
3. Produce venue-level and combined summaries.

Acceptance criteria:
- low and high extreme buckets are summarized explicitly
- Polymarket and Kalshi can be compared side by side
- the output is readable without notebook-only inspection

### T3.2 Add uncertainty estimates and sensitivity checks
Status: `pending`

Goal:
- separate signal from noise before drawing a conclusion

Implementation steps:
1. Add confidence intervals or market-clustered bootstrap estimates.
2. Compare tick-weighted and market-aware results.
3. Check whether the directional effect survives basic robustness tests.

Acceptance criteria:
- the main tables include uncertainty information
- the repo shows at least one meaningful sensitivity analysis
- any instability in the effect is made explicit

### T3.3 Segment the effect by venue and simple covariates
Status: `pending`

Goal:
- learn whether the bias is broad or concentrated

Implementation steps:
1. Compare Polymarket and Kalshi separately.
2. Add at least one simple segmentation such as time to expiry, category, or calendar period if available.
3. Flag sample-size limitations for small segments.

Acceptance criteria:
- the repo reports venue-specific effects
- at least one segmentation view exists
- small-sample caveats are visible in the output

## Milestone 4 - Visualizations and Decision Memo

### T4.1 Produce the core figures
Status: `pending`

Goal:
- make the calibration results easy to inspect visually

Implementation steps:
1. Build bucketed calibration plots for each venue.
2. Build at least one combined cross-venue comparison figure.
3. Save the figures under `reports/figures/`.

Acceptance criteria:
- each venue has at least one saved visualization
- there is at least one combined comparison figure
- figures can be regenerated from the canonical dataset

### T4.2 Write the research conclusion memo
Status: `pending`

Goal:
- end the project with an explicit decision rather than open-ended exploration

Implementation steps:
1. Summarize the dataset coverage and key caveats.
2. State whether low-probability overvaluation and high-probability undervaluation appear supported.
3. Recommend either stopping or creating a separate execution project.

Acceptance criteria:
- the conclusion is written under `reports/summaries/`
- the memo states supported, unsupported, or inconclusive
- the memo includes the next-step recommendation

## Milestone 5 - Follow-on Decision

### T5.1 Decide whether to spin out a trading project
Status: `blocked`

Goal:
- keep execution work out of this repo unless the evidence is strong enough

Implementation steps:
1. Review the conclusion memo after Milestone 4.
2. Decide whether the effect is large and stable enough to justify a separate project.
3. Record the decision in `TASKS.md` or the final report.

Acceptance criteria:
- there is a written go or no-go decision
- the current repository remains analysis-first regardless of the decision
- any future bot work is explicitly scoped into a separate project
