from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATHS = [
    REPO_ROOT / "notebooks/extreme_probability/00_dataset_inventory.ipynb",
    REPO_ROOT / "notebooks/extreme_probability/01_extreme_probability_analysis.ipynb",
]


def load_notebook(path: Path) -> dict:
    return json.loads(path.read_text())


def test_validate_repo_passes_for_tracked_checkout(tmp_path: Path) -> None:
    tracked_files = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()

    for relative_path in tracked_files:
        source = REPO_ROOT / relative_path
        destination = tmp_path / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)

    result = subprocess.run(
        ["bash", "scripts/validate_repo.sh"],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("notebook_path", NOTEBOOK_PATHS)
def test_analysis_notebook_is_valid_json(notebook_path: Path) -> None:
    notebook = load_notebook(notebook_path)

    assert notebook["nbformat"] == 4
    assert any(cell["cell_type"] == "markdown" for cell in notebook["cells"])


def test_dataset_inventory_notebook_contains_pivot_scope() -> None:
    notebook = load_notebook(NOTEBOOK_PATHS[0])
    notebook_source = "\n".join("".join(cell["source"]) for cell in notebook["cells"])

    assert "33GB" in notebook_source
    assert "Polymarket" in notebook_source
    assert "Kalshi" in notebook_source
    assert "binary market" in notebook_source
    assert "canonical market-priced probability" in notebook_source
    assert "resolution" in notebook_source


def test_extreme_probability_analysis_notebook_contains_research_focus() -> None:
    notebook = load_notebook(NOTEBOOK_PATHS[1])
    notebook_source = "\n".join("".join(cell["source"]) for cell in notebook["cells"])

    assert "low probability" in notebook_source
    assert "high probability" in notebook_source
    assert "calibration gap" in notebook_source
    assert "threshold-entry" in notebook_source
    assert "Wilson" in notebook_source
    assert "visualization" in notebook_source


def test_gitignore_keeps_local_only_assets_out_of_git() -> None:
    gitignore_content = (REPO_ROOT / ".gitignore").read_text()

    assert "data/" in gitignore_content
    assert "prompts/" in gitignore_content
    assert "scripts/local/" in gitignore_content
    assert "skills/" in gitignore_content
    assert "!.env.example" in gitignore_content
