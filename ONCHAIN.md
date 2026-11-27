# Self-custody payments and reversible customer gateway

The existing customer menus, Binance ID option, network grid, payment request
layout and Verify/Change Network/Back buttons remain in place. The new gateway
receives funds at the configured public wallet addresses; no wallet secrets are used.

## Release and switch

- PAYMENT_GATEWAY retains the original gateway fallback (Cryptomus in production).
- ONCHAIN_CUSTOMER_CHECKOUT_ENABLED=true releases customer invoice creation.
- /onchain -> Payment gateway chooses Trust Wallet or Cryptomus for NEW invoices.
- Trust Wallet only accepts assets with a successful admin test, enabled network,
  available RPC/discovery and enabled master gate. Other network buttons retain
  their layout but refuse invoice creation until ready. LTC currently has no
  automatic discovery and cannot be enabled for customer checkout.
- Optional ONCHAIN_CUSTOMER_LAUNCH=trust_wallet_bsc_v1 performs the initial release
  after the BEP20 admin test. A persistent checkpoint ensures restarts never
  override a subsequent admin rollback.
- Switching back to Cryptomus never deletes code, keys, pending invoices or history.
  All previous invoices continue verification on their original gateway.

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
Health: GET /onchain/health. SOL/TRON use pollers, LTC uses admin TX hints.
Only independently tested and ready assets should be exposed for payment.
