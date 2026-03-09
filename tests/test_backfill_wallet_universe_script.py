from __future__ import annotations

import importlib.util
from pathlib import Path

from src.ingestion import BackfilledWallet, SkippedWalletSeed, WalletBackfillFailure, WalletBackfillSummary


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts/backfill_wallet_universe.py"


def load_script_module():
    spec = importlib.util.spec_from_file_location("backfill_wallet_universe", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_main_prints_summary_and_failure_details(monkeypatch, capsys, tmp_path) -> None:
    module = load_script_module()
    summary = WalletBackfillSummary(
        selection_rule="Use the first 3 leaderboard rows and dedupe wallets in API order.",
        seed_wallet_addresses=("0xwallet1", "0xwallet2"),
        position_rows=1,
        closed_position_rows=1,
        activity_trade_rows=0,
        wallet_profile_rows=2,
        raw_capture_count=7,
        skipped_wallets=(SkippedWalletSeed(identifier="rank:2", reason="missing_proxy_wallet"),),
        failures=(
            WalletBackfillFailure(
                wallet_address="0xwallet2",
                dataset="data_api.activity",
                detail="503 Service Unavailable",
            ),
        ),
        wallet_results=(
            BackfilledWallet(
                wallet_address="0xwallet1",
                position_rows=1,
                closed_position_rows=1,
                activity_trade_rows=0,
                profile_rows=1,
            ),
            BackfilledWallet(
                wallet_address="0xwallet2",
                position_rows=0,
                closed_position_rows=0,
                activity_trade_rows=0,
                profile_rows=1,
            ),
        ),
    )

    def fake_run_wallet_backfill(**kwargs):
        assert kwargs["raw_data_dir"] == tmp_path / "raw"
        assert kwargs["warehouse_path"] == tmp_path / "warehouse.duckdb"
        return summary

    monkeypatch.setattr(module, "run_wallet_backfill", fake_run_wallet_backfill)

    exit_code = module.main(
        [
            "--leaderboard-limit",
            "3",
            "--raw-dir",
            str(tmp_path / "raw"),
            "--warehouse-path",
            str(tmp_path / "warehouse.duckdb"),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Wallet universe backfill summary" in captured.out
    assert "Selected wallets (2): 0xwallet1, 0xwallet2" in captured.out
    assert "Skipped seeds: missing_proxy_wallet=1" in captured.out
    assert "Failures:" in captured.err
    assert "dataset=data_api.activity" in captured.err
