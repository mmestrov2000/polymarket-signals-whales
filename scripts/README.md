# Scripts Overview

Scripts are optional accelerators. Team workflow does not depend on them.

## General Helpers
- `bootstrap_env.sh`: initialize local Python environment for this repo.
- `agent_worktree_start.sh`: convenience wrapper to create a worktree branch.
- `agent_worktree_finish.sh`: convenience wrapper to push branch and optionally open a PR.
- `generate_signal_events.py`: materialize Milestone 4 signal events from the normalized warehouse.
- `build_event_dataset.py`: materialize a QA-checked event dataset and write per-build metadata artifacts.
- `train_signal_classifier.py`: compare Milestone 6 rule baselines with a numpy logistic-regression classifier and write reproducible run artifacts.
- `run_walk_forward_backtest.py`: replay strategy candidates on historical event streams and write comparison artifacts, including the paper-trading decision report.

## Local Codex Helpers
- `local/install_codex_skills.sh`: install template skills into local Codex profile.

Use equivalent native tooling (`git`, your package manager, GitHub UI/CLI) if you do not want script-based workflows.
