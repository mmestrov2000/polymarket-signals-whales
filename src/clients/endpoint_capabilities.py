from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


CapabilityStatus = Literal["usable_now", "usable_later_with_auth", "unsuitable"]

STATUS_LABELS: dict[CapabilityStatus, str] = {
    "usable_now": "usable now",
    "usable_later_with_auth": "usable later with auth",
    "unsuitable": "unsuitable",
}

MILESTONE_1_VERIFIED_ON = "2026-03-08"


@dataclass(frozen=True, slots=True)
class EndpointCapability:
    surface: str
    method: str
    endpoint: str
    status: CapabilityStatus
    primary_use: str
    required_inputs: tuple[str, ...]
    useful_fields: tuple[str, ...]
    notes: tuple[str, ...] = ()


VERIFIED_PUBLIC_ENDPOINT_CAPABILITIES: tuple[EndpointCapability, ...] = (
    EndpointCapability(
        surface="Gamma REST",
        method="GET",
        endpoint="/markets",
        status="usable_now",
        primary_use="Market discovery and stable join-key selection.",
        required_inputs=("limit", "closed"),
        useful_fields=(
            "id",
            "question",
            "slug",
            "conditionId",
            "clobTokenIds",
            "active",
            "endDate",
            "liquidity",
            "volume",
        ),
        notes=(
            "Milestone 1 selected the first market row exposing both conditionId and clobTokenIds.",
            "No explicit rate-limit headers were observed on 2026-03-08.",
        ),
    ),
    EndpointCapability(
        surface="CLOB REST",
        method="GET",
        endpoint="/book",
        status="usable_now",
        primary_use="Top-of-book state and Gamma/CLOB identifier alignment.",
        required_inputs=("token_id",),
        useful_fields=(
            "market",
            "asset_id",
            "bids",
            "asks",
            "last_trade_price",
            "tick_size",
            "min_order_size",
            "hash",
            "timestamp",
            "neg_risk",
        ),
        notes=(
            "Gamma.conditionId matched CLOB book.market in the Milestone 1 check.",
            "Gamma.clobTokenIds[0] matched CLOB book.asset_id in the Milestone 1 check.",
        ),
    ),
    EndpointCapability(
        surface="CLOB REST",
        method="GET",
        endpoint="/price",
        status="usable_now",
        primary_use="Current one-sided quote lookup for spread snapshots.",
        required_inputs=("token_id", "side=BUY|SELL"),
        useful_fields=("price",),
        notes=(
            "BUY and SELL must be requested separately.",
            "The live response shape observed on 2026-03-08 was a single-key payload: price.",
        ),
    ),
    EndpointCapability(
        surface="CLOB REST",
        method="GET",
        endpoint="/prices-history",
        status="usable_now",
        primary_use="Historical price series backfill for the selected token.",
        required_inputs=("market=<token_id>", "interval", "fidelity"),
        useful_fields=("history[].t", "history[].p"),
        notes=(
            "The notebook verified market=<token_id>, interval=1w, fidelity=5.",
            "A 1w query required fidelity >= 5 in the live Milestone 1 check.",
        ),
    ),
    EndpointCapability(
        surface="CLOB WebSocket",
        method="SUBSCRIBE",
        endpoint="wss://ws-subscriptions-clob.polymarket.com/ws/market",
        status="usable_now",
        primary_use="Live market-channel capture for later forward collectors.",
        required_inputs=("assets_ids", "type=market", "custom_feature_enabled"),
        useful_fields=(
            "event_type",
            "asset_id",
            "market",
            "hash",
            "last_trade_price",
            "tick_size",
            "timestamp",
            "bids",
            "asks",
        ),
        notes=(
            "The notebook captured list-wrapped book messages with the fields above on 2026-03-08.",
            "No application-level heartbeat is documented; treat idle timeouts and close events as reconnect triggers.",
        ),
    ),
    EndpointCapability(
        surface="Data API",
        method="GET",
        endpoint="/v1/leaderboard",
        status="usable_now",
        primary_use="Wallet seed discovery for downstream wallet-centric checks.",
        required_inputs=("category", "timePeriod", "orderBy", "limit"),
        useful_fields=("proxyWallet", "rank", "pnl", "vol", "userName", "verifiedBadge"),
        notes=(
            "Milestone 1 used the top leaderboard wallet as the default wallet seed when available.",
            "No explicit rate-limit headers were observed on 2026-03-08.",
        ),
    ),
    EndpointCapability(
        surface="Data API",
        method="GET",
        endpoint="/positions",
        status="usable_now",
        primary_use="Open-position snapshots for wallet holdings and market linkage.",
        required_inputs=("user", "limit", "sortBy"),
        useful_fields=(
            "proxyWallet",
            "asset",
            "conditionId",
            "size",
            "avgPrice",
            "currentValue",
            "realizedPnl",
            "outcome",
            "outcomeIndex",
            "totalBought",
            "endDate",
        ),
        notes=(
            "The sampled leaderboard wallet in the notebook returned no rows, so coverage is wallet-dependent rather than guaranteed.",
            "A secondary public spot-check on 2026-03-08 confirmed the snapshot fields above on another leaderboard wallet.",
        ),
    ),
    EndpointCapability(
        surface="Data API",
        method="GET",
        endpoint="/closed-positions",
        status="usable_now",
        primary_use="Wallet-level closed-position history with timestamps and outcomes.",
        required_inputs=("user", "limit", "sortBy"),
        useful_fields=(
            "proxyWallet",
            "asset",
            "conditionId",
            "outcome",
            "avgPrice",
            "realizedPnl",
            "totalBought",
            "timestamp",
            "endDate",
        ),
        notes=(
            "Useful for outcome and realized-PnL summaries.",
            "This is position-level history, not fill-by-fill trade sequencing.",
        ),
    ),
    EndpointCapability(
        surface="Data API",
        method="GET",
        endpoint="/activity",
        status="usable_now",
        primary_use="Wallet-centric trade log for whale activity analysis.",
        required_inputs=("user", "limit", "type=TRADE", "sortBy", "sortDirection"),
        useful_fields=(
            "proxyWallet",
            "asset",
            "conditionId",
            "outcome",
            "side",
            "size",
            "price",
            "timestamp",
            "transactionHash",
            "usdcSize",
        ),
        notes=(
            "Milestone 1 identified this as the best wallet-centric public trade log.",
            "It exposes wallet identity plus side, size, outcome, and timestamp in one record.",
        ),
    ),
    EndpointCapability(
        surface="Data API",
        method="GET",
        endpoint="/trades",
        status="usable_now",
        primary_use="Market-centric trade log with wallet attribution checks.",
        required_inputs=("market=<conditionId>", "limit"),
        useful_fields=(
            "proxyWallet",
            "asset",
            "conditionId",
            "outcome",
            "side",
            "size",
            "price",
            "timestamp",
            "transactionHash",
        ),
        notes=(
            "Milestone 1 confirmed proxyWallet, side, size, outcome, and timestamp on live samples.",
            "This is the best public market-centric trade endpoint for wallet attribution checks.",
        ),
    ),
    EndpointCapability(
        surface="Data API",
        method="GET",
        endpoint="/holders",
        status="usable_now",
        primary_use="Holder concentration snapshots grouped by token.",
        required_inputs=("market=<conditionId>", "limit"),
        useful_fields=(
            "token",
            "holders[].proxyWallet",
            "holders[].asset",
            "holders[].amount",
            "holders[].outcomeIndex",
        ),
        notes=(
            "The live response is a list of token groups, each with a holders array.",
            "Useful for concentration inputs, but not a trade log and not time-sequenced.",
        ),
    ),
    EndpointCapability(
        surface="Data API",
        method="GET",
        endpoint="/oi",
        status="usable_now",
        primary_use="Market-level open-interest snapshots.",
        required_inputs=("market=<conditionId>",),
        useful_fields=("market", "value"),
        notes=(
            "Useful for market-level size context only.",
            "It does not expose wallet identity, side, or trade timing.",
        ),
    ),
)

DEFERRED_AUTHENTICATED_CAPABILITIES: tuple[EndpointCapability, ...] = (
    EndpointCapability(
        surface="CLOB Auth",
        method="SIGNED REST",
        endpoint="order placement, order status, balances",
        status="usable_later_with_auth",
        primary_use="Execution work for Milestones 8-9 only.",
        required_inputs=(
            "POLYMARKET_PRIVATE_KEY",
            "CLOB_API_KEY",
            "CLOB_API_SECRET",
            "CLOB_API_PASSPHRASE",
        ),
        useful_fields=("status", "order id", "balances"),
        notes=(
            "Not part of the Milestone 1 public notebook validation path.",
            "scripts/create_clob_api_credentials.py documents the credential flow and proxy-wallet signature caveats.",
        ),
    ),
)

CONFIRMED_JOIN_KEYS: tuple[str, ...] = (
    "Gamma.conditionId == CLOB /book market for the sampled market.",
    "Gamma.clobTokenIds[0] == CLOB /book asset_id for the sampled market.",
    "Data API trade endpoints expose both conditionId and asset alongside proxyWallet.",
    "WebSocket market messages expose asset_id and market for the same CLOB token universe.",
)

RATE_LIMIT_AND_CAVEAT_NOTES: tuple[str, ...] = (
    "No explicit rate-limit headers were observed on 2026-03-08 for sampled Gamma /markets, CLOB /book, or Data API /v1/leaderboard responses.",
    "Absolute request ceilings, pagination limits, and burst tolerance remain undocumented in the repository and should be treated as unresolved.",
    "The public CLOB market channel does not currently document an application-level heartbeat.",
    "Data API /positions coverage is wallet-dependent; a sampled leaderboard wallet returned an empty snapshot.",
)

UNRESOLVED_QUESTIONS: tuple[str, ...] = (
    "What are the real public rate limits and pagination ceilings for Gamma, CLOB, and the Data API under sustained collection?",
    "Can Data API market and wallet trade endpoints be backfilled far enough for later research windows without silent gaps?",
    "Are there any public endpoints with stronger historical depth or order-book sequencing than the currently verified /book, /price, /prices-history, and market-channel feed?",
    "Which authenticated CLOB execution endpoints should be treated as the canonical later source for order status and balances once Milestone 8 starts?",
)


def iter_all_capabilities() -> tuple[EndpointCapability, ...]:
    return VERIFIED_PUBLIC_ENDPOINT_CAPABILITIES + DEFERRED_AUTHENTICATED_CAPABILITIES


def render_endpoint_capability_matrix() -> str:
    lines = [
        "# Polymarket Endpoint Capability Matrix",
        "",
        f"This reference distills the Milestone 1 connectivity checks last verified on {MILESTONE_1_VERIFIED_ON}.",
        "",
        "Status legend:",
        "- `usable_now`: verified and useful for the current research-first scope",
        "- `usable_later_with_auth`: reserved for later milestones and requires credentials",
        "- `unsuitable`: reachable but not reliable for the intended current use",
        "",
        "## Capability Matrix",
        "",
        "| Surface | Method | Endpoint | Status | Primary use | Required inputs | Useful fields | Notes |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]

    for capability in iter_all_capabilities():
        lines.append(
            "| "
            + " | ".join(
                [
                    capability.surface,
                    capability.method,
                    f"`{capability.endpoint}`",
                    f"`{STATUS_LABELS[capability.status]}`",
                    capability.primary_use,
                    ", ".join(f"`{item}`" for item in capability.required_inputs),
                    ", ".join(f"`{field}`" for field in capability.useful_fields),
                    " ".join(capability.notes),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Confirmed Join Keys",
            "",
            *[f"- {item}" for item in CONFIRMED_JOIN_KEYS],
            "",
            "## Rate Limits And Caveats",
            "",
            *[f"- {item}" for item in RATE_LIMIT_AND_CAVEAT_NOTES],
            "",
            "## Unresolved Questions",
            "",
            *[f"- {item}" for item in UNRESOLVED_QUESTIONS],
            "",
        ]
    )
    return "\n".join(lines)


__all__ = [
    "CONFIRMED_JOIN_KEYS",
    "DEFERRED_AUTHENTICATED_CAPABILITIES",
    "EndpointCapability",
    "MILESTONE_1_VERIFIED_ON",
    "RATE_LIMIT_AND_CAVEAT_NOTES",
    "STATUS_LABELS",
    "UNRESOLVED_QUESTIONS",
    "VERIFIED_PUBLIC_ENDPOINT_CAPABILITIES",
    "iter_all_capabilities",
    "render_endpoint_capability_matrix",
]
