from __future__ import annotations

import importlib.util
from pathlib import Path

from src.ingestion import BackfillFailure, BackfilledMarket, SampleMarketBackfillSummary, SkippedMarket


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts/backfill_sample_markets.py"


def load_script_module():
    spec = importlib.util.spec_from_file_location("backfill_sample_markets", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_main_prints_summary_and_failure_details(monkeypatch, capsys, tmp_path) -> None:
    module = load_script_module()
    summary = SampleMarketBackfillSummary(
        selection_rule="Choose one ranked market.",
        selected_market_ids=("12345",),
        market_rows=1,
        price_rows=2,
        trade_rows=0,
        raw_capture_count=3,
        skipped_markets=(SkippedMarket(market_id="67890", reason="not_selected_after_ranking"),),
        failures=(
            BackfillFailure(
                market_id="12345",
                dataset="data_api.trades",
                detail="503 Service Unavailable",
            ),
        ),
        market_results=(
            BackfilledMarket(
                market_id="12345",
                condition_id="0xcondition123",
                token_ids=("111", "222"),
                price_rows=2,
                trade_rows=0,
            ),
        ),
    )

    def fake_run_sample_market_backfill(**kwargs):
        assert kwargs["raw_data_dir"] == tmp_path / "raw"
        assert kwargs["warehouse_path"] == tmp_path / "warehouse.duckdb"
        return summary

    monkeypatch.setattr(module, "run_sample_market_backfill", fake_run_sample_market_backfill)

    exit_code = module.main(
        [
            "--sample-size",
            "1",
            "--gamma-limit",
            "5",
            "--raw-dir",
            str(tmp_path / "raw"),
            "--warehouse-path",
            str(tmp_path / "warehouse.duckdb"),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Sample market backfill summary" in captured.out
    assert "Selected markets (1): 12345" in captured.out
    assert "Skipped markets: not_selected_after_ranking=1" in captured.out
    assert "Failures:" in captured.err
    assert "dataset=data_api.trades" in captured.err
