from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts/create_clob_api_credentials.py"


def load_script_module():
    spec = importlib.util.spec_from_file_location("create_clob_api_credentials", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_upsert_env_content_replaces_existing_values_and_appends_missing_ones() -> None:
    module = load_script_module()

    existing = (
        "CLOB_API_KEY=\n"
        "CLOB_API_SECRET=old-secret\n"
        "POLYMARKET_PRIVATE_KEY=0x" + "1" * 64 + "\n"
    )

    updated = module.upsert_env_content(
        existing,
        {
            "CLOB_API_KEY": "new-key",
            "CLOB_API_SECRET": "new-secret",
            "CLOB_API_PASSPHRASE": "new-passphrase",
        },
    )

    assert "CLOB_API_KEY=new-key" in updated
    assert "CLOB_API_SECRET=new-secret" in updated
    assert "CLOB_API_PASSPHRASE=new-passphrase" in updated
    assert "POLYMARKET_PRIVATE_KEY=0x" + "1" * 64 in updated


def test_validate_private_key_rejects_wallet_address_shape() -> None:
    module = load_script_module()

    with pytest.raises(ValueError, match="32-byte hex private key"):
        module.validate_private_key("0xCDCdbD2AcD02baa714D02c3a5dEDe5CDF0E32D36")


def test_validate_private_key_accepts_hex_private_key() -> None:
    module = load_script_module()

    private_key = "0x" + "a" * 64
    assert module.validate_private_key(private_key) == private_key


def test_validate_signature_type_accepts_supported_values() -> None:
    module = load_script_module()

    assert module.validate_signature_type(None) is None
    assert module.validate_signature_type("0") == 0
    assert module.validate_signature_type("1") == 1
    assert module.validate_signature_type("2") == 2


def test_validate_signature_type_rejects_unknown_value() -> None:
    module = load_script_module()

    with pytest.raises(ValueError, match="must be one of 0, 1, or 2"):
        module.validate_signature_type("3")


def test_validate_funder_address_is_required_for_proxy_signature_types() -> None:
    module = load_script_module()

    with pytest.raises(ValueError, match="required when POLYMARKET_SIGNATURE_TYPE is 1 or 2"):
        module.validate_funder_address(None, 2)


def test_validate_funder_address_accepts_evm_address() -> None:
    module = load_script_module()

    funder = "0x" + "b" * 40
    assert module.validate_funder_address(funder, 2) == funder
