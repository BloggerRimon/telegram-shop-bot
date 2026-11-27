# Delayed customer payment review

A customer's first **I Have Paid (Verify)** tap starts a durable 10-minute timer. Repeated taps do not reset it. Unpaid invoices without a Paid report do not send alerts. This applies to all nine Trust Wallet assets. Confirmed but uncredited settlement outbox entries also qualify.

The settlement worker checks every 10 seconds. Each admin receives a review message with buyer name, username, numeric ID, invoice reference, USD credit, exact crypto amount, network, receiving address, time window, detected TXID/sender/confirmations when known, and an explicitly unverified buyer-supplied hash when available. Unknown data is labelled unknown. Delivery leases and per-admin acknowledgements survive restarts and retry failures; an ambiguous Telegram delivery timeout can cause a repeated notification, never a repeated credit. Old acknowledged unresolved alerts cannot starve new ones.

Buyer receipt submission: `/payment_tx OC_REFERENCE TXID`. Ownership and network-specific hash format are checked, the first hash is retained, and the hash is queued only as a normal verification hint. It never authorizes credit. First-time hash submission refreshes an already-sent admin alert once.

Admin workflow:

1. Open the alert's **Review / refresh payment**, or `/onchain` → **Delayed customer payments**.
2. Check the actual receipt in Trust Wallet: asset/contract, network, receiving address, exact amount, receipt time and confirmations. A receiving address alone does not prove a deposit belongs to a buyer.
3. With a known TXID, press **Prepare manual approval**. If unknown, get the actual receipt TXID from Trust Wallet or the buyer, then send `/oc_manual OC_REFERENCE TXID`.
4. Review the exact user, invoice, USD amount and hash in the preview. Press **Receipt checked — approve once**. The existing balance/order settlement worker applies the payment.

**Use this invoice approval flow, not the unrelated general Add Balance feature.** An unrelated wallet adjustment cannot identify which blockchain payment it replaces.

Every command/callback rechecks admin authorization; approval previews are tied to their creating admin and expire in 15 minutes. Final approval rechecks customer invoice eligibility and the 10-minute report age. Cancelled/test invoices and known contradictory chain data are refused. Approval stores actor, invoice, user, amount, chain and TXID as an admin wallet attestation, without claiming automatic verification or a real admin test.

Manual and automatic finalization acquire the same chain/TX advisory lock and invoice row lock, then share the same unique settlement outbox and persistent invoice credit key. The invoice is marked paid atomically with its manual receipt and outbox. Concurrent approvals, automatic finalization races, retries and restarts cannot create another credit for that invoice. The receipt TXID is reserved on its chain: later automatic outputs of that manually allocated TX require review, including additional outputs in a batched transaction. This conservative reservation does not affect normal automatic multi-output settlement. A transaction already allocated to another invoice is refused.

Database changes are additive. Existing gateway selection, Cryptomus history, addresses, network settings and customer checkout layout are preserved.
