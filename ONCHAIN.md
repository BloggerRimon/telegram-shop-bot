# Self-custody on-chain payments (admin-only)

Status: **admin testing only**. Customers keep using Cryptomus. No customer button,
address or checkout is wired to this module. `customer_new_payments_enabled`
(DB table `onchain_settings`) defaults to `false`, every `network_enabled:<net>` defaults
to `false`, and even with all flags on, `create_customer_invoice` raises `NotReleased`.

Admin entry: `/onchain` in the bot (server-side `is_admin` check on every callback).
Admin tx hint (asks the engine to read a tx from chain; never credits by itself):
`/oc_hint <network> <txid>`.

## Environment variables (names only)
| Name | Purpose |
|---|---|
| `ALCHEMY_API_KEY` | Alchemy RPC key (ETH, BSC, Solana, Bitcoin, Litecoin, TRON reads) |
| `ALCHEMY_SIGNING_KEY_ETHEREUM` / `_BSC` / `_SOLANA` / `_BITCOIN` | Per-webhook signing keys |
| `ALCHEMY_WEBHOOK_ID_ETHEREUM` / `_BSC` / `_SOLANA` / `_BITCOIN` | Optional: pin the expected webhook id |
| `TRONGRID_API_KEY` | Optional: TronGrid key for TRON discovery poller |
| `ONCHAIN_EMERGENCY_DISABLE` | `true` blocks all new invoices (tracking continues) |
| `ONCHAIN_RPC_URL_<NETWORK>` | Optional RPC override |

No webhook-management auth token is needed by the bot.

## Webhook receiver URLs
`https://<railway-domain>/alchemy/webhook/ethereum` (also `bsc`, `solana`, `bitcoin`).
Health: `GET /onchain/health`.

## Safety model
- Webhooks are hints only; every transfer is re-read from the chain (sample events never credit).
- Attribution = unique exact amount per active invoice (partial unique index), invoice must exist
  before the transfer's block time and be unexpired. Everything else goes to the review queue.
- Transfers before monitoring start are never credited. Pollers start at chain head.
- Admin test credit goes to `onchain_test_ledger` only; customer balances are never touched.
