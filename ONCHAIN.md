# Self-custody payments and reversible customer gateway

The existing customer menus, Binance ID option, network grid, payment request
layout and Verify/Change Network/Back buttons remain in place. The new gateway
receives funds at the configured public wallet addresses; no wallet secrets are used.

## Release and switch

- PAYMENT_GATEWAY retains the original gateway fallback (Cryptomus in production).
- ONCHAIN_CUSTOMER_CHECKOUT_ENABLED=true releases customer invoice creation.
- /onchain -> Payment gateway chooses Trust Wallet or Cryptomus for NEW invoices.
- Trust Wallet accepts assets with a successful admin test OR an explicit
  ONCHAIN_OWNER_APPROVED_ASSETS deployment allowlist, plus enabled network,
  available RPC/discovery and enabled master/release gates. The allowlist contains
  exact comma-separated asset IDs, never a wildcard. It does not create ledger
  entries or mark an asset as tested. Admin status distinguishes real-test passes
  from owner-approved, untested assets.
- Optional ONCHAIN_CUSTOMER_LAUNCH=trust_wallet_bsc_v1 performs the initial release
  after the BEP20 admin test. A persistent checkpoint ensures restarts never
  override a subsequent admin rollback.
- ONCHAIN_CUSTOMER_LAUNCH=trust_wallet_all_v1 performs a one-time all-network
  launch only when every asset is explicitly owner-approved and has configured
  infrastructure. Approval and actual tested assets are audited separately.
  The saved launch checkpoint preserves later gateway/network rollback choices.
- Switching back to Cryptomus never deletes code, keys, pending invoices or history.
  All previous invoices continue verification on their original gateway.
- ONCHAIN_AUTO_ENABLE_AFTER_TEST=true optionally enables a network immediately
  after a new real admin test is confirmed, only while Trust Wallet, the master
  and the customer release are on and neither pause nor emergency is active.
  The per-asset paid-test gate remains mandatory; a token test never releases the
  native coin (or another token) on the same network.

## Settlement

- Raw webhook HMAC is checked, then transactions are re-read from the blockchain.
- Exact amount, approved token/chain, invoice time window and confirmations are required.
- Customer Verify buttons bind an invoice ID and enforce invoice ownership.
- The engine atomically records a paid customer invoice in onchain_customer_settlements.
  The bot drains this durable outbox using its existing persistent top-up idempotency
  keys and a strict state save.
- Duplicate deliveries, restarts and notification retries cannot issue a second
  balance credit. Direct order stock is reserved and saved before delivery;
  retries reuse the same saved order/items. Unavailable stock leaves funds in
  the customer's bot balance. Manual products reuse the existing paid manual orders.
- Admin test payments stay in onchain_test_ledger and never become customer funds.
- Customer credits identify the invoice and its unique chain/output/log transfer,
  rather than treating a bare TXID as one payment. Separate outputs/logs in a
  batched transaction can pay separate invoices, while replays remain idempotent.

## Administration

/onchain is admin-only: gateway switch, master/network controls, test invoices,
health, test ledger, review and audit. /oc_hint <network> <txid> only requests a
chain check; it never directly credits a payment.

ONCHAIN_EMERGENCY_DISABLE=true blocks new invoices while tracking continues.
Existing customer settlements keep draining even when the gateway or master is off.

## Infrastructure

- ALCHEMY_API_KEY: RPC key; required and must contain its actual nonempty value.
- ALCHEMY_SIGNING_KEY_ETHEREUM, _BSC, _BITCOIN: webhook signing keys.
- ALCHEMY_WEBHOOK_ID_*: pin each expected webhook ID.
- TRONGRID_API_KEY: optional TRON discovery key (public polling may rate-limit).
- ONCHAIN_RPC_URL_<NETWORK>: optional RPC override.

Receiver: https://<domain>/alchemy/webhook/<ethereum|bsc|bitcoin>.
Health: GET /onchain/health. SOL/TRON use paginated pollers. BTC/LTC also use a
public BlockCypher address index every three minutes; the index supplies hashes
only and every transaction is independently checked through Alchemy. Poller
failures back off without blocking the other networks. Public TronGrid access can
still rate-limit; configure a free TRONGRID_API_KEY for reliable TRON discovery.
Owner-approved untested assets have not completed a real-funds end-to-end test.
Emergency, pause, master, network, chain verification, finality and settlement
idempotency protections apply identically to tested and owner-approved assets.
