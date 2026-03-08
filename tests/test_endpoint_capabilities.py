from __future__ import annotations

from pathlib import Path

from src.clients.endpoint_capabilities import (
    DEFERRED_AUTHENTICATED_CAPABILITIES,
    VERIFIED_PUBLIC_ENDPOINT_CAPABILITIES,
    render_endpoint_capability_matrix,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_endpoint_capabilities_cover_milestone_1_surfaces() -> None:
    capability_map = {
        (capability.surface, capability.endpoint): capability
        for capability in VERIFIED_PUBLIC_ENDPOINT_CAPABILITIES
    }

    assert {
        ("Gamma REST", "/markets"),
        ("CLOB REST", "/book"),
        ("CLOB REST", "/price"),
        ("CLOB REST", "/prices-history"),
        ("CLOB WebSocket", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
        ("Data API", "/v1/leaderboard"),
        ("Data API", "/positions"),
        ("Data API", "/closed-positions"),
        ("Data API", "/activity"),
        ("Data API", "/trades"),
        ("Data API", "/holders"),
        ("Data API", "/oi"),
    } == set(capability_map)

    assert all(capability.status == "usable_now" for capability in VERIFIED_PUBLIC_ENDPOINT_CAPABILITIES)
    assert any(
        capability.status == "usable_later_with_auth"
        for capability in DEFERRED_AUTHENTICATED_CAPABILITIES
    )

    activity = capability_map[("Data API", "/activity")]
    assert {"proxyWallet", "side", "size", "outcome", "timestamp"} <= set(activity.useful_fields)

    positions = capability_map[("Data API", "/positions")]
    assert any("wallet-dependent" in note for note in positions.notes)

    websocket = capability_map[
        ("CLOB WebSocket", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
    ]
    assert {"assets_ids", "type=market", "custom_feature_enabled"} <= set(websocket.required_inputs)


def test_rendered_capability_matrix_matches_checked_in_reference() -> None:
    reference_path = REPO_ROOT / "docs/endpoint_capability_matrix.md"

    assert reference_path.read_text() == render_endpoint_capability_matrix()


def test_repo_wiring_points_to_capability_matrix() -> None:
    validate_repo_source = (REPO_ROOT / "scripts/validate_repo.sh").read_text()
    readme_source = (REPO_ROOT / "README.md").read_text()

    assert "docs/endpoint_capability_matrix.md" in validate_repo_source
    assert "docs/endpoint_capability_matrix.md" in readme_source
