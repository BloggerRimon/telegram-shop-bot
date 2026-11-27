"""Customer adapter: reversible gateway selection and durable payment settlement.

Public wallet addresses only. This module never signs or broadcasts transactions.
Every confirmed payment comes from the engine's chain-verified settlement outbox.
"""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

from telegram import InlineKeyboardMarkup

from . import engine, networks as nw, store
from . import payment_review

LABEL_ASSETS = {
    "USDT (TRC20)": "usdt_trc20", "USDT (ERC20)": "usdt_erc20",
    "USDT (BEP20)": "usdt_bep20", "TRX (TRC20)": "trx", "BTC": "btc", "LTC": "ltc",
    "ETH (ERC20)": "eth", "BNB (BEP20)": "bnb", "SOL": "sol",
}
_settlement_lock = None


def gateway(default="cryptomus"):
    value = store.get_setting("customer_payment_gateway", default)
    return value if value in ("trust_wallet", "cryptomus", "nowpayments") else default


def switch_gateway(value: str, actor: int):
    if value not in ("trust_wallet", "cryptomus"):
        raise engine.GateClosed("invalid_gateway")
    if value == "trust_wallet":
        if nw.env("ONCHAIN_CUSTOMER_CHECKOUT_ENABLED").lower() != "true":
            raise engine.GateClosed("customer checkout release is not enabled")
        ready = [aid for aid in nw.ASSET_ORDER if engine.asset_available(aid)]
        if not ready:
            raise engine.GateClosed("a successful admin test or explicit owner approval is required")
        for aid in ready:
            store.set_setting("network_enabled:" + nw.ASSETS[aid].network, True, actor)
        store.set_setting("customer_new_payments_enabled", True, actor)
    store.set_setting("customer_payment_gateway", value, actor)
    store.audit(actor, "customer_gateway_changed", {"gateway": value})


def bootstrap():
    """One explicit release, once. Restarts never override an owner's rollback."""
    launch = nw.env("ONCHAIN_CUSTOMER_LAUNCH")
    if launch not in ("trust_wallet_bsc_v1", "trust_wallet_all_v1"):
        return
    checkpoint = "customer_launch:" + launch
    if store.get_checkpoint(checkpoint):
        return
    if launch == "trust_wallet_all_v1":
        if not all(aid in engine.owner_approved_assets() and engine.asset_available(aid)
                   for aid in nw.ASSET_ORDER):
            raise engine.GateClosed("all assets must have explicit owner approval and infrastructure")
    elif not engine.asset_ready("usdt_bep20"):
        raise engine.GateClosed("BEP20 admin test is required before launch")
    switch_gateway("trust_wallet", 0)
    store.audit(0, "customer_launch", {"launch": launch,
                                      "owner_approved_assets": sorted(engine.owner_approved_assets()),
                                      "real_tested_assets": sorted(store.paid_test_assets())})
    store.set_checkpoint(checkpoint, True)


def invoice_record(inv):
    meta = json.loads(inv.get("note") or "{}")
    asset = nw.ASSETS[inv["asset_id"]]
    label = next(label for label, aid in LABEL_ASSETS.items() if aid == asset.id)
    return {
        "provider": "Trust Wallet", "payment_id": inv["public_ref"], "invoice_id": inv["id"],
        "kind": meta.get("kind", "deposit"), "user_id": inv["user_id"],
        "usd_amount": float(inv["usd_amount"]), "network": label,
        "pay_currency": asset.label.split()[0],
        "pay_amount": engine.format_units(inv["expected_units"], asset.decimals, asset.display_decimals),
        "pay_address": inv["destination"], "payment_status": inv["status"],
        "product_id": meta.get("product_id"), "qty": meta.get("qty"),
    }


def create_payment(user_id, kind, usd_amount, network_label, product_id=None, qty=None):
    aid = LABEL_ASSETS.get(network_label)
    if not aid:
        raise engine.GateClosed("unknown_asset")
    inv = engine.create_customer_invoice(user_id, aid, Decimal(str(usd_amount)),
                                         kind=kind, product_id=product_id, qty=qty)
    return invoice_record(inv)


def bind_verify_keyboard(keyboard, invoice_id):
    """Keep the existing labels, layout and styles; bind Verify to this invoice."""
    rows = []
    for row in keyboard.inline_keyboard:
        result = []
        for button in row:
            if button.callback_data in ("deppay_verify", "buypay_verify"):
                data = button.to_dict()
                data["callback_data"] = f"ocpay:verify:{invoice_id}"
                button = type(button).de_json(data, None)
            result.append(button)
        rows.append(result)
    return InlineKeyboardMarkup(rows)


def _credit_and_reserve(bot, inv):
    """Single bot process, existing persistent wallet idempotency, strict save.

    Delivery is reserved and persisted before sending. A retry reuses the saved
    order and its saved credentials; it never consumes another inventory item.
    """
    uid = int(inv["user_id"])
    amount = Decimal(str(inv["usd_amount"]))
    ref = inv["public_ref"]
    meta = json.loads(inv.get("note") or "{}")
    txid = inv["transfer_uid"].split(":")[1]
    with bot.manual_order_lock, bot.topup_credit_lock:
        bot.user_wallet.setdefault(uid, 0.0)
        bot.user_transactions.setdefault(uid, [])
        bot.user_orders.setdefault(uid, [])
        result = bot.apply_confirmed_topup_credit(
            uid, amount, "Trust Wallet", "onchain_manual" if inv.get("manual_actor") else "onchain_verified", payment_id=ref,
            network=inv["network"],
            transaction_meta={"onchain_invoice_id": inv["id"], "onchain_transfer_uid": inv["transfer_uid"],
                              "blockchain_txid": txid, "manual_approval_by": inv.get("manual_actor")})
        credit = result["record"]
        # One blockchain transaction can contain several independently payable
        # outputs/logs. The invoice/outbox identifies a payment; the bare TXID
        # must not act as a global credit alias across outputs or chains.
        credit["tx_hash"] = txid
        credit["onchain_transfer_uid"] = inv["transfer_uid"]
        if result.get("transaction"):
            result["transaction"].setdefault("meta", {})["txid"] = txid
        # Existing legacy credit helper swallows some persistence errors. Never
        # acknowledge the outbox or deliver anything until a strict save succeeds.
        bot.save_bot_state_strict()
        if meta.get("kind") != "order":
            return {"kind": "deposit", "user_id": uid, "amount": amount}
        outcome = credit.get("onchain_order")
        if outcome:
            return outcome
        pid, qty = meta.get("product_id"), int(meta.get("qty") or 0)
        product = bot.PRODUCTS.get(pid)
        existing = bot._manual_order_by_purchase_reference(ref)
        if existing:
            kind = "manual_order" if existing.get("delivery_type") == bot.MANUAL_DELIVERY else "order"
            outcome = {"kind": kind, "user_id": uid, "order_id": existing["id"]}
        elif not product or qty <= 0:
            outcome = {"kind": "wallet_fallback", "user_id": uid, "amount": str(amount)}
        elif bot.is_manual_delivery_product(product):
            reserved = bot.create_paid_manual_order(uid, pid, qty, ref, expected_total=amount)
            if not reserved.get("ok"):
                if reserved.get("order"):
                    raise RuntimeError("manual_order_persistence_unconfirmed")
                outcome = {"kind": "wallet_fallback", "user_id": uid, "amount": str(amount)}
            else:
                outcome = {"kind": "manual_order", "user_id": uid, "order_id": reserved["order"]["id"]}
        elif len(product.get("accounts") or []) < qty or Decimal(str(bot.user_wallet[uid])) < amount:
            outcome = {"kind": "wallet_fallback", "user_id": uid, "amount": str(amount)}
        else:
            display_before = bot.get_display_stock(pid)
            delivered = product["accounts"][:qty]
            del product["accounts"][:qty]
            product["display_stock"] = max(0, display_before - qty)
            bot.user_wallet[uid] = float(Decimal(str(bot.user_wallet[uid])) - amount)
            order = bot.add_order_record(uid, pid, qty, float(amount), "Completed", "Trust Wallet",
                                         delivered_items=delivered)
            order["purchase_reference"] = ref
            bot.add_transaction_record(uid, "Order Payment", float(amount), "Completed",
                                       {"provider": "Trust Wallet", "payment_id": ref, "product_id": pid, "qty": qty})
            outcome = {"kind": "order", "user_id": uid, "order_id": order["id"]}
        credit["onchain_order"] = outcome
        bot.save_bot_state_strict()
        return outcome


async def settle_pending(bot):
    global _settlement_lock
    if _settlement_lock is None:
        _settlement_lock = asyncio.Lock()
    async with _settlement_lock:
        for inv in store.pending_customer_settlements():
            try:
                outcome = _credit_and_reserve(bot, inv)
                uid = int(inv["user_id"])
                kind = outcome["kind"]
                if kind == "order":
                    order = bot.find_order_by_id(outcome["order_id"])
                    if not order:
                        raise RuntimeError("reserved_order_missing")
                    lines = [f"✅ <b>{bot.t(uid, 'order_completed')}</b>", ""]
                    lines += [f"<code>{bot.escape_html(item)}</code>" for item in order.get("delivered_items", [])]
                    await bot.send_html_lines(bot.app_instance.bot, uid, lines)
                    guide = bot.get_delivery_guide(order["product_id"])
                    if guide:
                        await bot.app_instance.bot.send_message(uid, guide)
                elif kind == "manual_order":
                    await bot.app_instance.bot.send_message(uid,
                        f"✅ Payment confirmed. Order #{outcome['order_id']} is awaiting delivery.")
                else:
                    text = (f"🎉 <b>{bot.t(uid, 'payment_verified')}</b>\n\n"
                            f"{bot.t(uid, 'amount_added', amount=bot.format_money(inv['usd_amount']))}\n"
                            f"{bot.get_wallet_balance_text(uid)}")
                    if kind == "wallet_fallback":
                        text += "\n\nThe product is unavailable. Your payment is available in your bot balance."
                    await bot.app_instance.bot.send_message(uid, text, parse_mode="HTML")
                store.finish_customer_settlement(inv["id"])
            except Exception as exc:
                print(f"[onchain] customer settlement {inv['id']} retry: {type(exc).__name__}")


async def settlement_loop(bot):
    while True:
        try:
            await settle_pending(bot)
            await payment_review.notify_delays(bot)
        except Exception as exc:
            print(f"[onchain] customer outbox retry: {type(exc).__name__}")
        await asyncio.sleep(10)


async def verify(bot, query, uid, invoice_id):
    inv = await asyncio.to_thread(store.get_invoice, invoice_id)
    if not inv or inv["purpose"] != "customer" or int(inv["user_id"]) != int(uid):
        await query.message.reply_text("This payment request is not available for your account.")
        return
    if inv["status"] != "cancelled":
        profile = getattr(bot, "user_profiles", {}).get(int(uid), {})
        person = getattr(query, "from_user", None)
        name = getattr(person, "full_name", None) or " ".join(filter(None, [profile.get("first_name"), profile.get("last_name")]))
        username = getattr(person, "username", None) or profile.get("username") or ""
        await asyncio.to_thread(payment_review.report_payment, invoice_id, int(uid), name, username)
    await settle_pending(bot)
    inv = await asyncio.to_thread(store.get_invoice, invoice_id)
    if inv["status"] == "paid":
        if not await asyncio.to_thread(store.customer_settlement_complete, invoice_id):
            text = "Payment confirmed. Your balance or order is being updated. Please check again shortly."
        else:
            text = "✅ Payment already confirmed. Your balance or order was updated."
    elif inv["status"] == "expired":
        text = "This payment request expired. If you already sent funds, contact support with the transaction ID."
    else:
        text = "⌛ Payment not confirmed yet. Please wait 1–2 minutes and tap Verify again."
    if inv["status"] not in ("paid", "cancelled"):
        text += ("\nIf it is still uncredited 10 minutes after your first Verify, an admin will be notified."
                 f"\nFor faster investigation, submit your receipt TXID:\n/payment_tx {inv['public_ref']} TXID")
    await query.message.reply_text(text)
