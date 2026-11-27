# USDT conversion preparation

All nine existing payment options and receiving addresses remain active. Automatic conversion is not enabled.

The admin `/onchain` panel contains **USDT conversion preview**. Select one of the nine source assets, then USDT BEP20 / TRC20 / ERC20 as the receiving asset and an example source amount. The destination is always the owner's existing wallet address for that network. An identical USDT source/destination needs no swap.

Live routes require a dedicated `SWAPKIT_API_KEY` in the bot service. This change does not create a SwapKit account, create a credential, change a plan, or copy a key from Trust Wallet. When the key is absent, the panel states that a live quote is not configured. It does not display invented fees or fake rates.

The adapter calls only `https://api.swapkit.dev/v3/quote`, with zero affiliate fee, source/destination public addresses and gas estimation requested. It has no signing, swap-building, wallet-draining, payout or broadcast operation. Route output amounts, minimum receive, fee components, route providers, expiry and provider warning codes are shown without subtracting fees twice. Separate source gas may still apply. If the provider supplies source/target USD prices, the panel displays the estimated value difference and flags a reduction above 2%. This is a review flag; it does not authorize execution or guarantee an all-in loss limit.

Unsupported pairs, no-route amounts, malformed numbers, wrong assets, mismatched amounts, expired quotes, authorization/rate limits and missing minimum/fee information are handled explicitly. No available route is not a guarantee that a future swap can execute. No customer funds or balances change during a preview.

## Before enabling customer conversion

A production conversion gateway still needs a selected provider account/key, confirmed supported source/destination routes and minimums, a fee policy, quote expiry/requote rules and a reliable pay-in deposit/settlement API. The customer must see the exact required source amount and expected/minimum USDT settlement before sending. Reserve and persist a unique quote/order/invoice relationship, verify the received USDT settlement and account for partial/refunded/expired swaps. Reuse the existing credit outbox and invoice idempotency; a conversion must never create a second customer top-up. Preserve a reversible gateway selector and support for pre-existing direct invoices.

A source payment sent directly to the owner's existing wallet address is still that original asset. Automatic spending from it would require an owner-controlled signer and funded gas wallets; those capabilities are deliberately outside this preview. A provider-managed pay-in flow would instead issue provider deposit addresses per invoice and settle USDT to the owner's chosen wallet. That changes routing and requires a separate rollout with live quotes and settlement tests.

Official schema: https://swapkit.github.io/SwapKit/api/operations/v3quote/
Official fees and setup: https://www.swapkit.dev/swap-api/ and https://www.swapkit.dev/pricing/
