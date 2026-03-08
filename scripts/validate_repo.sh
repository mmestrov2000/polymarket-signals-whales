#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

required_files=(
  "AGENTS.md"
  "PROJECT_SPEC.md"
  "ARCHITECTURE.md"
  "TASKS.md"
  "README.md"
  ".env.example"
  "pyproject.toml"
  "requirements-dev.txt"
  "docs/PLAYBOOK.md"
  "notebooks/polymarket_connection_checks/00_api_connection.ipynb"
)

required_dirs=(
  "docs"
  "notebooks"
  "src"
  "tests"
  "scripts"
  ".github/workflows"
)

missing=0

for file in "${required_files[@]}"; do
  if [[ ! -f "$file" ]]; then
    echo "Missing required file: $file"
    missing=1
  fi
done

for dir in "${required_dirs[@]}"; do
  if [[ ! -d "$dir" ]]; then
    echo "Missing required directory: $dir"
    missing=1
  fi
done

if [[ ! -x "scripts/bootstrap_env.sh" ]]; then
  echo "Expected executable script: scripts/bootstrap_env.sh"
  missing=1
fi

if [[ ! -x "scripts/validate_repo.sh" ]]; then
  echo "Expected executable script: scripts/validate_repo.sh"
  missing=1
fi

if [[ "$missing" -ne 0 ]]; then
  echo "Repository validation failed"
  exit 1
fi

echo "Repository validation passed"
