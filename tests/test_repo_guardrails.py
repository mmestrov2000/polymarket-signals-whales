from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def load_connection_notebook() -> dict:
    notebook_path = REPO_ROOT / "notebooks/polymarket_connection_checks/00_api_connection.ipynb"
    return json.loads(notebook_path.read_text())


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
    notebook = load_connection_notebook()

    assert notebook["nbformat"] == 4
    assert any(cell["cell_type"] == "markdown" for cell in notebook["cells"])


def test_connection_notebook_contains_milestone_1_checks() -> None:
    notebook = load_connection_notebook()
    notebook_source = "\n".join("".join(cell["source"]) for cell in notebook["cells"])

    assert "load_dotenv" in notebook_source
    assert "sys.path.insert(0, str(REPO_ROOT))" in notebook_source
    assert "save_sample_payload" in notebook_source
    assert "parse_clob_token_ids" in notebook_source
    assert "extract_records" in notebook_source
    assert "summarize_observed_fields" in notebook_source
    assert "/markets" in notebook_source
    assert "/book" in notebook_source
    assert "/price" in notebook_source
    assert "/prices-history" in notebook_source
    assert "POLYMARKET_DATA_API_BASE_URL" in notebook_source
    assert "data/raw/data_api/connection_checks" in notebook_source
    assert "/v1/leaderboard" in notebook_source
    assert "/positions" in notebook_source
    assert "/closed-positions" in notebook_source
    assert "/activity" in notebook_source
    assert "/trades" in notebook_source
    assert "/holders" in notebook_source
    assert "/oi" in notebook_source
    assert "POLYMARKET_WS_URL" in notebook_source
    assert "capture_market_channel_samples" in notebook_source
    assert "message_shapes" in notebook_source
    assert "data/raw/websocket/connection_checks" in notebook_source
    assert "Selected Data API wallet seed" in notebook_source
    assert "wallet_identity" in notebook_source
    assert "Known gaps or blockers" in notebook_source
    assert "conditionId" in notebook_source
    assert "asset_id" in notebook_source
    assert "Gamma.conditionId == CLOB book.market" in notebook_source
    assert "Gamma.clobTokenIds[0] == CLOB book.asset_id" in notebook_source
    assert "Deferred to T1.2" not in notebook_source
    assert "Deferred to T1.3" not in notebook_source
    assert "TODO: add Gamma API checks and persist representative payload samples." not in notebook_source
    assert "TODO: add CLOB API checks for market state, price history, and trades." not in notebook_source


def test_gitignore_keeps_local_only_assets_out_of_git() -> None:
    gitignore_content = (REPO_ROOT / ".gitignore").read_text()

    assert "data/" in gitignore_content
    assert "prompts/" in gitignore_content
    assert "scripts/local/" in gitignore_content
    assert "skills/" in gitignore_content
    assert "!.env.example" in gitignore_content
