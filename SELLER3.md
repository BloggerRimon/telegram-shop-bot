# Seller 3 inventory and order reporting

Seller 3 (`jero_reseller`) starts with inventory refresh enabled every 30 seconds.
The one-time initialization is saved in the provider's operational status, so a
later admin change survives restarts. Seller API → Auto Refresh shows the actual
interval and offers 30 seconds or the existing minute intervals.

Refresh updates mapped stock, including zero. Missing stock becomes unknown and
cannot authorize an ordinary purchase. Product details reuse a successful refresh
for at most 15 seconds. Order confirmation forces another refresh before any
wallet debit; stale stock, changed price or a failed refresh blocks the purchase.

Supplier order notifications identify the provider, customer, product, quantity,
internal/supplier order IDs, total, state and refund amount. Credentials are not
included. Completed, processing, uncertain, rejected, partial and refunded outcomes
are reported. Failed Telegram recipients are retried individually, with successful
recipients saved to prevent repeat notices on normal reconciliation.

Confirmed rejection or supplier refund retains the existing once-only refund flow.
Timeouts and uncertain outcomes retain the reserved amount, show the order/support
contact and notify admins immediately while supplier lookup continues. They must
be checked before any manual refund, because the supplier may still fulfill them.

Existing seller providers and the payment gateway are unchanged. The inventory
endpoint is polled; already-sent Telegram messages show the refreshed data when
the shop/product view is opened again.
