# Polymarket Endpoint Capability Matrix

This reference distills the Milestone 1 connectivity checks last verified on 2026-03-08.

Status legend:
- `usable_now`: verified and useful for the current research-first scope
- `usable_later_with_auth`: reserved for later milestones and requires credentials
- `unsuitable`: reachable but not reliable for the intended current use

## Capability Matrix

| Surface | Method | Endpoint | Status | Primary use | Required inputs | Useful fields | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Gamma REST | GET | `/markets` | `usable now` | Market discovery and stable join-key selection. | `limit`, `closed` | `id`, `question`, `slug`, `conditionId`, `clobTokenIds`, `active`, `endDate`, `liquidity`, `volume` | Milestone 1 selected the first market row exposing both conditionId and clobTokenIds. No explicit rate-limit headers were observed on 2026-03-08. |
| CLOB REST | GET | `/book` | `usable now` | Top-of-book state and Gamma/CLOB identifier alignment. | `token_id` | `market`, `asset_id`, `bids`, `asks`, `last_trade_price`, `tick_size`, `min_order_size`, `hash`, `timestamp`, `neg_risk` | Gamma.conditionId matched CLOB book.market in the Milestone 1 check. Gamma.clobTokenIds[0] matched CLOB book.asset_id in the Milestone 1 check. |
| CLOB REST | GET | `/price` | `usable now` | Current one-sided quote lookup for spread snapshots. | `token_id`, `side=BUY|SELL` | `price` | BUY and SELL must be requested separately. The live response shape observed on 2026-03-08 was a single-key payload: price. |
| CLOB REST | GET | `/prices-history` | `usable now` | Historical price series backfill for the selected token. | `market=<token_id>`, `interval`, `fidelity` | `history[].t`, `history[].p` | The notebook verified market=<token_id>, interval=1w, fidelity=5. A 1w query required fidelity >= 5 in the live Milestone 1 check. |
| CLOB WebSocket | SUBSCRIBE | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | `usable now` | Live market-channel capture for later forward collectors. | `assets_ids`, `type=market`, `custom_feature_enabled` | `event_type`, `asset_id`, `market`, `hash`, `last_trade_price`, `tick_size`, `timestamp`, `bids`, `asks` | The notebook captured list-wrapped book messages with the fields above on 2026-03-08. No application-level heartbeat is documented; treat idle timeouts and close events as reconnect triggers. |
| Data API | GET | `/v1/leaderboard` | `usable now` | Wallet seed discovery for downstream wallet-centric checks. | `category`, `timePeriod`, `orderBy`, `limit` | `proxyWallet`, `rank`, `pnl`, `vol`, `userName`, `verifiedBadge` | Milestone 1 used the top leaderboard wallet as the default wallet seed when available. No explicit rate-limit headers were observed on 2026-03-08. |
| Data API | GET | `/positions` | `usable now` | Open-position snapshots for wallet holdings and market linkage. | `user`, `limit`, `sortBy` | `proxyWallet`, `asset`, `conditionId`, `size`, `avgPrice`, `currentValue`, `realizedPnl`, `outcome`, `outcomeIndex`, `totalBought`, `endDate` | The sampled leaderboard wallet in the notebook returned no rows, so coverage is wallet-dependent rather than guaranteed. A secondary public spot-check on 2026-03-08 confirmed the snapshot fields above on another leaderboard wallet. |
| Data API | GET | `/closed-positions` | `usable now` | Wallet-level closed-position history with timestamps and outcomes. | `user`, `limit`, `sortBy` | `proxyWallet`, `asset`, `conditionId`, `outcome`, `avgPrice`, `realizedPnl`, `totalBought`, `timestamp`, `endDate` | Useful for outcome and realized-PnL summaries. This is position-level history, not fill-by-fill trade sequencing. |
| Data API | GET | `/activity` | `usable now` | Wallet-centric trade log for whale activity analysis. | `user`, `limit`, `type=TRADE`, `sortBy`, `sortDirection` | `proxyWallet`, `asset`, `conditionId`, `outcome`, `side`, `size`, `price`, `timestamp`, `transactionHash`, `usdcSize` | Milestone 1 identified this as the best wallet-centric public trade log. It exposes wallet identity plus side, size, outcome, and timestamp in one record. |
| Data API | GET | `/trades` | `usable now` | Market-centric trade log with wallet attribution checks. | `market=<conditionId>`, `limit` | `proxyWallet`, `asset`, `conditionId`, `outcome`, `side`, `size`, `price`, `timestamp`, `transactionHash` | Milestone 1 confirmed proxyWallet, side, size, outcome, and timestamp on live samples. This is the best public market-centric trade endpoint for wallet attribution checks. |
| Data API | GET | `/holders` | `usable now` | Holder concentration snapshots grouped by token. | `market=<conditionId>`, `limit` | `token`, `holders[].proxyWallet`, `holders[].asset`, `holders[].amount`, `holders[].outcomeIndex` | The live response is a list of token groups, each with a holders array. Useful for concentration inputs, but not a trade log and not time-sequenced. |
| Data API | GET | `/oi` | `usable now` | Market-level open-interest snapshots. | `market=<conditionId>` | `market`, `value` | Useful for market-level size context only. It does not expose wallet identity, side, or trade timing. |
| CLOB Auth | SIGNED REST | `order placement, order status, balances` | `usable later with auth` | Execution work for Milestones 8-9 only. | `POLYMARKET_PRIVATE_KEY`, `CLOB_API_KEY`, `CLOB_API_SECRET`, `CLOB_API_PASSPHRASE` | `status`, `order id`, `balances` | Not part of the Milestone 1 public notebook validation path. scripts/create_clob_api_credentials.py documents the credential flow and proxy-wallet signature caveats. |

## Confirmed Join Keys

- Gamma.conditionId == CLOB /book market for the sampled market.
- Gamma.clobTokenIds[0] == CLOB /book asset_id for the sampled market.
- Data API trade endpoints expose both conditionId and asset alongside proxyWallet.
- WebSocket market messages expose asset_id and market for the same CLOB token universe.

## Rate Limits And Caveats

- No explicit rate-limit headers were observed on 2026-03-08 for sampled Gamma /markets, CLOB /book, or Data API /v1/leaderboard responses.
- Absolute request ceilings, pagination limits, and burst tolerance remain undocumented in the repository and should be treated as unresolved.
- The public CLOB market channel does not currently document an application-level heartbeat.
- Data API /positions coverage is wallet-dependent; a sampled leaderboard wallet returned an empty snapshot.

## Unresolved Questions

- What are the real public rate limits and pagination ceilings for Gamma, CLOB, and the Data API under sustained collection?
- Can Data API market and wallet trade endpoints be backfilled far enough for later research windows without silent gaps?
- Are there any public endpoints with stronger historical depth or order-book sequencing than the currently verified /book, /price, /prices-history, and market-channel feed?
- Which authenticated CLOB execution endpoints should be treated as the canonical later source for order status and balances once Milestone 8 starts?
