#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.exceptions import PolyApiException


PRIVATE_KEY_PATTERN = re.compile(r"^0x[a-fA-F0-9]{64}$")
ADDRESS_PATTERN = re.compile(r"^0x[a-fA-F0-9]{40}$")
DEFAULT_HOST = "https://clob.polymarket.com"
DEFAULT_CHAIN_ID = 137


def find_repo_root(start: Path) -> Path:
    for candidate in [start, *start.parents]:
        if (candidate / ".env.example").exists() and (candidate / "scripts").exists():
            return candidate
    raise RuntimeError("Could not locate repository root.")


def validate_private_key(raw_value: str | None) -> str:
    if raw_value is None or not raw_value.strip():
        raise ValueError("POLYMARKET_PRIVATE_KEY is missing from the env file.")

    private_key = raw_value.strip()
    if not PRIVATE_KEY_PATTERN.fullmatch(private_key):
        raise ValueError(
            "POLYMARKET_PRIVATE_KEY must be a 32-byte hex private key like 0x<64 hex chars>. "
            "The current value looks like a wallet address or invalid key."
        )

    return private_key


def validate_signature_type(raw_value: str | None) -> int | None:
    if raw_value is None or not raw_value.strip():
        return None

    normalized = raw_value.strip()
    if normalized not in {"0", "1", "2"}:
        raise ValueError("POLYMARKET_SIGNATURE_TYPE must be one of 0, 1, or 2.")

    return int(normalized)


def validate_funder_address(raw_value: str | None, signature_type: int | None) -> str | None:
    if raw_value is None or not raw_value.strip():
        if signature_type in {1, 2}:
            raise ValueError(
                "POLYMARKET_FUNDER_ADDRESS is required when POLYMARKET_SIGNATURE_TYPE is 1 or 2."
            )
        return None

    funder_address = raw_value.strip()
    if not ADDRESS_PATTERN.fullmatch(funder_address):
        raise ValueError("POLYMARKET_FUNDER_ADDRESS must be an EVM address like 0x<40 hex chars>.")

    return funder_address


def upsert_env_content(existing: str, updates: dict[str, str]) -> str:
    lines = existing.splitlines()
    remaining = dict(updates)
    updated_lines: list[str] = []

    for line in lines:
        if "=" not in line or line.lstrip().startswith("#"):
            updated_lines.append(line)
            continue

        key, _sep, _value = line.partition("=")
        if key in remaining:
            updated_lines.append(f"{key}={remaining.pop(key)}")
            continue

        updated_lines.append(line)

    for key, value in remaining.items():
        updated_lines.append(f"{key}={value}")

    return "\n".join(updated_lines) + "\n"


def write_api_credentials(env_path: Path, api_key: str, api_secret: str, api_passphrase: str) -> None:
    existing_content = env_path.read_text() if env_path.exists() else ""
    updated_content = upsert_env_content(
        existing_content,
        {
            "CLOB_API_KEY": api_key,
            "CLOB_API_SECRET": api_secret,
            "CLOB_API_PASSPHRASE": api_passphrase,
        },
    )
    env_path.write_text(updated_content)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create Polymarket CLOB API credentials from POLYMARKET_PRIVATE_KEY and save them to .env."
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to the env file to read and update. Defaults to .env in the repository root.",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=f"CLOB host URL. Defaults to {DEFAULT_HOST}.",
    )
    parser.add_argument(
        "--chain-id",
        type=int,
        default=DEFAULT_CHAIN_ID,
        help=f"Chain ID for the CLOB signer. Defaults to {DEFAULT_CHAIN_ID} (Polygon mainnet).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = find_repo_root(Path.cwd().resolve())
    env_path = Path(args.env_file)
    if not env_path.is_absolute():
        env_path = repo_root / env_path

    load_dotenv(env_path)

    private_key = validate_private_key(os.getenv("POLYMARKET_PRIVATE_KEY"))
    host = os.getenv("POLYMARKET_CLOB_BASE_URL", args.host)
    signature_type = validate_signature_type(os.getenv("POLYMARKET_SIGNATURE_TYPE"))
    funder_address = validate_funder_address(os.getenv("POLYMARKET_FUNDER_ADDRESS"), signature_type)

    client = ClobClient(
        host=host,
        chain_id=args.chain_id,
        key=private_key,
        signature_type=signature_type,
        funder=funder_address,
    )

    try:
        creds = client.create_or_derive_api_creds()
    except PolyApiException as exc:
        guidance = [
            "Polymarket rejected API credential creation/derivation.",
            f"Server response: {exc}",
            "Check that POLYMARKET_PRIVATE_KEY is the private key for the signing wallet.",
            "If you trade through a Polymarket account, set POLYMARKET_SIGNATURE_TYPE and POLYMARKET_FUNDER_ADDRESS in .env:",
            "- EOA wallet: signature type 0, funder optional",
            "- Magic/email account: signature type 1, funder = proxy wallet address",
            "- Browser-wallet Polymarket account: signature type 2, funder = proxy wallet address",
        ]
        raise RuntimeError("\n".join(guidance)) from exc

    if creds is None:
        raise RuntimeError("py_clob_client returned no API credentials.")

    write_api_credentials(
        env_path,
        api_key=creds.api_key,
        api_secret=creds.api_secret,
        api_passphrase=creds.api_passphrase,
    )

    print(f"Saved CLOB API credentials to {env_path}")
    print("Updated keys: CLOB_API_KEY, CLOB_API_SECRET, CLOB_API_PASSPHRASE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
