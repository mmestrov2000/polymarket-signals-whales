from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


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


def test_connection_notebook_is_valid_json() -> None:
    notebook_path = REPO_ROOT / "notebooks/polymarket_connection_checks/00_api_connection.ipynb"
    notebook = json.loads(notebook_path.read_text())

    assert notebook["nbformat"] == 4
    assert any(cell["cell_type"] == "markdown" for cell in notebook["cells"])


def test_gitignore_keeps_local_only_assets_out_of_git() -> None:
    gitignore_content = (REPO_ROOT / ".gitignore").read_text()

    assert "prompts/" in gitignore_content
    assert "scripts/local/" in gitignore_content
    assert "skills/" in gitignore_content
    assert "!.env.example" in gitignore_content
