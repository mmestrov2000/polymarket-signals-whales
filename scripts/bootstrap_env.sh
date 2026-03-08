#!/usr/bin/env bash
set -euo pipefail

if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv
  echo "Created virtual environment at .venv"
fi

# shellcheck disable=SC1091
source .venv/bin/activate

# Keep bootstrap resilient in offline/hackathon environments.
if ! python -m pip install --upgrade pip setuptools wheel; then
  echo "Warning: could not upgrade packaging tools (offline or restricted network)."
  echo "Continuing with existing virtual environment tooling."
fi

if [[ -f "requirements.txt" ]]; then
  if ! pip install -r requirements.txt; then
    echo "Warning: failed to install requirements.txt."
    echo "If you are offline, rerun when network is available."
  fi
fi

if [[ -f "requirements-dev.txt" ]]; then
  if ! pip install -r requirements-dev.txt; then
    echo "Warning: failed to install requirements-dev.txt."
    echo "If you are offline, rerun when network is available."
  fi
fi

if [[ -f "pyproject.toml" && ! -f "requirements.txt" ]]; then
  if grep -qE "^\[project\]" pyproject.toml; then
    if ! pip install -e .; then
      echo "Warning: failed to install local project package."
      echo "If you are offline, rerun when network/dependencies are available."
    fi
  elif grep -qE "^\[tool\.poetry\]" pyproject.toml; then
    echo "Detected Poetry project. Install dependencies with: poetry install"
  fi
fi

echo "Virtual environment is ready and active for this shell."
echo "Run: source .venv/bin/activate"
