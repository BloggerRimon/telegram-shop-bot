"""Telegram admin panel for self-custody payments (/onchain).

Every entry point re-checks is_admin() on the server side, so forged callback
data from a normal user is refused even if they guess the payload.
"""

from __future__ import annotations

import asyncio
import html
from decimal import Decimal
from typing import Callable

from telegram import InlineKeyboardButton as B
from telegram import InlineKeyboardMarkup as KB

from . import engine as eng
from . import networks as nw
from . import store
from . import customer
from . import payment_review

PREFIX = "oc:"
TEST_AMOUNTS = ("1", "2", "5")
_engine_ref = {"engine": None}


def set_engine(e):
    _engine_ref["engine"] = e


def esc(v) -> str:
    return html.escape(str(v if v is not None else "—"))


def _fmt(units, asset_id):
    a = nw.ASSETS[asset_id]
    return eng.format_units(units, a.decimals, a.display_decimals)


def _home_kb():
    return KB([
        [B("🔄 Payment gateway", callback_data="oc:gateway")],
        [B("⏱ Delayed customer payments", callback_data="oc:delays")],
        [B("🧪 New admin test invoice", callback_data="oc:new")],
        [B("📋 Invoices", callback_data="oc:invs"), B("📡 Events/health", callback_data="oc:events")],
        [B("🧾 Review queue", callback_data="oc:review"), B("💼 Test ledger", callback_data="oc:ledger")],
        [B("🌐 Network controls", callback_data="oc:nets"), B("🔐 Customer activation", callback_data="oc:master")],
        [B("🛑 Pause NEW customer invoices", callback_data="oc:pause")],
        [B("📜 Audit log", callback_data="oc:audit")],
    ])


def _back():
    return [B("⬅️ Back", callback_data="oc:home")]


def home_text() -> str:
    store.init_schema()
    paid = store.paid_test_assets()
    master = eng.master_enabled()
    paused = store.get_setting("new_invoices_paused", False) is True
    lines = [
        "🔗 <b>SELF-CUSTODY PAYMENTS — ADMIN ONLY</b>",
        "",
        f"Customer activation (master): <b>{'ON' if master else 'OFF'}</b>",
        f"Emergency env ({nw.ENV_EMERGENCY}): <b>{'ACTIVE' if nw.emergency_disabled() else 'off'}</b>",
        f"New customer invoices paused: <b>{'yes' if paused else 'no'}</b>",
        f"New customer crypto payments: <b>{esc({'trust_wallet': 'Trust Wallet', 'cryptomus': 'Cryptomus'}.get(customer.gateway(), customer.gateway()))}</b>.",
        "Previous invoices keep their original gateway and continue verification.",
        "",
        "<b>Availability / real payment tests</b>:",
    ]
    for aid in nw.ASSET_ORDER:
        a = nw.ASSETS[aid]
        ready = eng.asset_ready(aid, paid)
        status = "✅ real test passed" if ready else (
            "⚠️ owner approved; real test pending" if aid in eng.owner_approved_assets() else "⏳ not released")
        lines.append(f"{esc(a.label)} — net {'ON' if eng.network_enabled(a.network) else 'off'} · {status}")
    return "\n".join(lines)


def networks_text() -> str:
    lines = ["🌐 <b>NETWORKS</b>", ""]
    for key, net in nw.NETWORKS.items():
        inf = eng.network_infra(key)
        lines += [
            f"<b>{esc(net.label)}</b> [{key}]",
            f"Address: <code>{esc(net.address)}</code> ({'valid format' if inf['address_valid'] else 'INVALID'})",
            f"RPC ({nw.ENV_ALCHEMY_API_KEY}): {'set' if inf['rpc'] else 'not set'}"
            + (f" · webhook key ({nw.signing_key_env(key)}): {'set' if inf['webhook_key'] else 'not set'}"
               if inf["webhook_key"] is not None else ""),
            f"Discovery: {esc(eng.DISCOVERY[key])} — {'OK' if inf['discovery_ok'] else 'NOT AVAILABLE'}",
            f"Finality: {esc(net.finality_note)} · customer enable: {'ON' if eng.network_enabled(key) else 'off'}",
            "",
        ]
    lines.append("ETH and BSC share the same address text but are tracked as separate chains.")
    return "\n".join(lines)


def invoice_text(inv: dict) -> str:
    a = nw.ASSETS[inv["asset_id"]]
    net = nw.NETWORKS[inv["network"]]
    t = store.get_transfer(inv["matched_transfer_uid"]) if inv.get("matched_transfer_uid") else None
    lines = [
        f"🧪 <b>ADMIN TEST INVOICE {esc(inv['public_ref'])}</b>",
        "",
        f"Asset: <b>{esc(a.label)}</b>" + (f" (contract <code>{esc(a.contract)}</code>)" if a.contract else ""),
        f"Network: <b>{esc(net.label)}</b>",
        f"Send to: <code>{esc(inv['destination'])}</code>",
        f"EXACT amount: <code>{esc(_fmt(inv['expected_units'], a.id))}</code> {esc(a.label.split()[0])}",
        f"USD value: ${esc(inv['usd_amount'])} · rate {esc(inv['rate_usd'])} ({esc(inv['rate_source'])})",
        f"Created: {esc(inv['created_at'])} · expires: {esc(inv['expires_at'])}",
        f"Status: <b>{esc(inv['status'])}</b>",
    ]
    if t:
        lines += [
            "",
            "<b>Detected transfer</b>",
            f"TX: <code>{esc(t['txid'])}</code> [{esc(t['locator'])}]",
            f"Received: {esc(_fmt(t['amount_units'], t['asset_id']))} · block time {esc(t['block_time'])}",
            f"Confirmations: {esc(t['confirmations'])} / required {esc(net.min_confirmations)} ({esc(net.finality_note)})",
            f"Match: {'exact amount, invoice eligible' if t['invoice_id'] == inv['id'] else 'no'} · transfer status {esc(t['status'])}",
        ]
    if inv["status"] == "paid":
        lines.append("\n✅ Credited to the ADMIN TEST LEDGER only. No customer balance changed.")
    lines.append("\n⚠️ Send exactly this amount on exactly this network. Exchange withdrawals that deduct "
                 "fees from the amount will not match and go to review.")
    return "\n".join(lines)


async def _send(query_or_msg, text, kb, edit=True):
    if edit and hasattr(query_or_msg, "edit_message_text"):
        try:
            await query_or_msg.edit_message_text(text, parse_mode="HTML", reply_markup=kb,
                                                 disable_web_page_preview=True)
            return
        except Exception:
            pass
    msg = getattr(query_or_msg, "message", query_or_msg)
    await msg.reply_text(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)


async def command(update, context, is_admin: Callable[[int], bool]):
    uid = update.effective_user.id
    if not is_admin(uid):
        return False  # behave like an unknown command for normal users
    text = await asyncio.to_thread(home_text)
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=_home_kb())
    return True


async def hint_command(update, context, is_admin: Callable[[int], bool]):
    uid = update.effective_user.id
    if not is_admin(uid):
        return False
    args = context.args or []
    if len(args) != 2 or args[0] not in nw.NETWORKS:
        await update.message.reply_text("Usage: /oc_hint <tron|ethereum|bsc|bitcoin|litecoin|solana> <txid>\n"
                                        "A hint only asks the engine to read that tx from the chain; it never "
                                        "credits by itself.")
        return True
    e = _engine_ref["engine"]
    ok = await asyncio.to_thread(e.add_admin_hint, uid, args[0], args[1], is_admin) if e else False
    await update.message.reply_text("✅ Hint queued." if ok else "ℹ️ Already queued or invalid.")
    return True


async def callback(query, data: str, is_admin: Callable[[int], bool]) -> bool:
    if not data.startswith(PREFIX):
        return False
    uid = query.from_user.id
    if not is_admin(uid):
        await _send(query, "❌ <b>You are not allowed.</b>", None)
        return True
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "home"
    try:
        await _dispatch(query, uid, action, parts[2:], is_admin)
    except eng.GateClosed as e:
        await _send(query, f"❌ Refused: {esc(e)}", KB([_back()]))
    except store.AmountSpaceExhausted:
        await _send(query, "❌ No free unique amount for this asset right now.", KB([_back()]))
    except Exception as e:
        print(f"[onchain] admin ui error: {type(e).__name__}")
        await _send(query, f"❌ Error: {esc(type(e).__name__)}", KB([_back()]))
    return True


async def _dispatch(query, uid, action, args, is_admin):
    run = asyncio.to_thread
    if action == "home":
        await _send(query, await run(home_text), _home_kb())
    elif action == "delays":
        rows = await run(payment_review.delayed_invoices)
        kb = [[B(f"Review invoice #{r['id']}", callback_data=f"oc:payment:{r['id']}")] for r in rows[:20]]
        await _send(query, "⏱ Customer payments reported over 10 minutes ago and still uncredited.", KB(kb + [_back()]))
    elif action == "payment" and len(args) == 1:
        r = await run(payment_review.review_record, int(args[0]))
        if not r or r['purpose'] != 'customer':
            raise eng.GateClosed('customer_invoice_not_found')
        await _send(query, payment_review.review_text(r), payment_review.review_keyboard(r))
    elif action == "manual" and len(args) == 1:
        try:
            result = await run(payment_review.approve_manual, args[0], uid)
        except ValueError as exc:
            raise eng.GateClosed(str(exc))
        await _send(query, "✅ Receipt approval recorded. Balance/order update is queued; the same invoice cannot credit twice."
                    if result['result'] == 'queued' else "ℹ️ Already paid. No additional credit was created.", KB([_back()]))
    elif action == "mpreview" and len(args) == 1:
        r = await run(payment_review.review_record, int(args[0]))
        txid = (r.get('detected_txid') or r.get('reported_txid')) if r else None
        if not txid:
            raise eng.GateClosed('Obtain the wallet receipt TXID and use /oc_manual <invoice> <TXID>.')
        token, inv, txid = await run(payment_review.prepare_manual, r['public_ref'], txid, uid)
        await _send(query, payment_review.approval_text(r, txid), payment_review.approval_keyboard(token, inv['id']))
    elif action == "gateway" and not args:
        active = await run(customer.gateway)
        await _send(query, f"🔄 <b>New customer crypto payments: {esc(active)}</b>\n\n"
                           "Switching does not delete Cryptomus or stop previous invoices.\n"
                           "Trust Wallet accepts tested assets or assets explicitly approved by the owner. "
                           "The home panel shows real test status separately.",
                    KB([[B("Use Trust Wallet", callback_data="oc:gateway:trust_wallet")],
                        [B("Use Cryptomus", callback_data="oc:gateway:cryptomus")], _back()]))
    elif action == "gateway" and len(args) == 1:
        await run(customer.switch_gateway, args[0], uid)
        await _send(query, f"✅ New crypto payments use {esc(args[0])}. Previous invoices keep working.",
                    KB([_back()]))
    elif action == "new" and not args:
        rows = [[B(nw.ASSETS[a].label, callback_data=f"oc:new:{a}")] for a in nw.ASSET_ORDER]
        await _send(query, "🧪 Choose asset for an admin test invoice:", KB(rows + [_back()]))
    elif action == "new" and len(args) == 1:
        a = args[0]
        if a not in nw.ASSETS:
            raise eng.GateClosed("unknown_asset")
        net = nw.NETWORKS[nw.ASSETS[a].network]
        rows = [[B(f"${x}", callback_data=f"oc:new:{a}:{x}") for x in TEST_AMOUNTS]]
        await _send(query, f"<b>{esc(nw.ASSETS[a].label)}</b> on {esc(net.label)}\nAddress: "
                           f"<code>{esc(net.address)}</code>\n\nChoose test value (USD):", KB(rows + [_back()]))
    elif action == "new" and len(args) == 2:
        if args[1] not in TEST_AMOUNTS:
            raise eng.GateClosed("bad_amount")
        inv = await run(eng.create_admin_test_invoice, uid, args[0], Decimal(args[1]), is_admin)
        await _send(query, await run(invoice_text, inv), _inv_kb(inv))
    elif action == "inv":
        inv = await run(store.get_invoice, int(args[0]))
        if not inv:
            raise eng.GateClosed("not_found")
        if inv['purpose'] == 'customer':
            r = await run(payment_review.review_record, inv['id'])
            await _send(query, payment_review.review_text(r), payment_review.review_keyboard(r))
        else:
            await _send(query, await run(invoice_text, inv), _inv_kb(inv))
    elif action == "cancel":
        ok = await run(store.cancel_invoice, int(args[0]), uid)
        await _send(query, "✅ Cancelled." if ok else "ℹ️ Only an awaiting invoice can be cancelled.", KB([_back()]))
    elif action == "invs":
        rows = await run(store.list_invoices, 10)
        kb = [[B(f"{r['public_ref']} {nw.ASSETS[r['asset_id']].label} {r['status']}",
                 callback_data=f"oc:inv:{r['id']}")] for r in rows]
        await _send(query, "📋 <b>Recent invoices</b>" if rows else "No invoices yet.", KB(kb + [_back()]))
    elif action == "events":
        await _send(query, await run(_events_text), KB([_back()]))
    elif action == "review":
        rows = await run(store.list_review, "open", 10)
        text = ["🧾 <b>Open review items</b>", ""] + [
            f"#{r['id']} {esc(r['reason'])} · {esc(r['transfer_uid'])}" for r in rows] if rows else ["No open items."]
        kb = [[B(f"Resolve #{r['id']} (no credit)", callback_data=f"oc:rres:{r['id']}")] for r in rows]
        await _send(query, "\n".join(text), KB(kb + [_back()]))
    elif action == "rres":
        ok = await run(store.resolve_review, int(args[0]), uid, "resolved_no_credit")
        await _send(query, "✅ Marked resolved (no credit applied)." if ok else "ℹ️ Already resolved.", KB([_back()]))
    elif action == "ledger":
        rows = await run(store.test_ledger, 15)
        text = ["💼 <b>Admin test ledger</b> (separate from customer balances)", ""] + [
            f"#{r['id']} inv {r['invoice_id']} · {esc(nw.ASSETS[r['asset_id']].label)} "
            f"{esc(_fmt(r['amount_units'], r['asset_id']))} (${esc(r['usd_amount'])})" for r in rows]
        await _send(query, "\n".join(text) if rows else "Test ledger is empty.", KB([_back()]))
    elif action == "nets" and not args:
        kb = []
        for key in nw.NETWORKS:
            on = await run(eng.network_enabled, key)
            kb.append([B(f"{'🟢' if on else '⚪'} {key}: turn {'OFF' if on else 'ON'}",
                         callback_data=f"oc:nets:{key}:{'off' if on else 'on'}")])
        await _send(query, await run(networks_text), KB(kb + [_back()]))
    elif action == "nets" and len(args) == 2:
        key, want = args
        if key not in nw.NETWORKS or want not in ("on", "off"):
            raise eng.GateClosed("bad_request")
        if want == "on":
            ok, why = await run(eng.can_enable_network, key)
            if not ok:
                raise eng.GateClosed(why)
        await run(store.set_setting, f"network_enabled:{key}", want == "on", uid)
        await _send(query, f"✅ {esc(key)} customer enable = {want.upper()}", KB([_back()]))
    elif action == "master" and not args:
        on = await run(eng.master_enabled)
        kb = [[B("Turn OFF", callback_data="oc:master:off")]] if on else \
             [[B("I reviewed the tests — turn ON", callback_data="oc:master:on")]]
        await _send(query, f"🔐 Master customer activation is <b>{'ON' if on else 'OFF'}</b>.\n\n"
                           "New Trust Wallet invoices also require the gateway switch, a tested or owner-approved asset, "
                           "and its network to be enabled. Previous invoices keep tracking.", KB(kb + [_back()]))
    elif action == "master" and args[0] == "on":
        kb = [[B("✅ Yes, activate", callback_data="oc:master:confirm")], _back()]
        await _send(query, "Confirm: activate customer gate? (Only networks you enabled AND assets with a "
                           "successful admin test or explicit owner approval pass the gate.)", KB(kb))
    elif action == "master" and args[0] == "confirm":
        ready_any = await run(lambda: any(eng.asset_available(a) for a in nw.ASSET_ORDER))
        if not ready_any:
            raise eng.GateClosed("no tested or owner-approved asset has available infrastructure")
        await run(store.set_setting, "customer_new_payments_enabled", True, uid)
        await _send(query, "✅ Master activation ON.", KB([_back()]))
    elif action == "master" and args[0] == "off":
        await run(store.set_setting, "customer_new_payments_enabled", False, uid)
        await _send(query, "✅ Master activation OFF. Existing invoices keep being tracked.", KB([_back()]))
    elif action == "pause":
        cur = await run(store.get_setting, "new_invoices_paused", False)
        await run(store.set_setting, "new_invoices_paused", not (cur is True), uid)
        await _send(query, f"✅ New customer invoices paused = {not (cur is True)}. Ingestion and tracking of "
                           "already-issued invoices continue.", KB([_back()]))
    elif action == "audit":
        rows = await run(store.recent_audit, 12)
        text = ["📜 <b>Audit log</b>", ""] + [f"{esc(r['created_at'])} · {esc(r['actor'])} · {esc(r['action'])}"
                                             for r in rows]
        await _send(query, "\n".join(text), KB([_back()]))
    else:
        await _send(query, await run(home_text), _home_kb())


def _inv_kb(inv):
    rows = [[B("🔄 Refresh status", callback_data=f"oc:inv:{inv['id']}")]]
    if inv["status"] == "awaiting":
        rows.append([B("❌ Cancel invoice", callback_data=f"oc:cancel:{inv['id']}")])
    return KB(rows + [_back()])


def _events_text() -> str:
    e = _engine_ref["engine"]
    last = store.last_event_times()
    lines = ["📡 <b>Monitoring health</b>", "",
             f"Worker last tick: {esc(getattr(e, 'last_tick', None))} · last error: {esc(getattr(e, 'last_error', None))}"]
    for k, v in (getattr(e, "poller_errors", {}) or {}).items():
        lines.append(f"Poller {esc(k)} error: {esc(v)}")
    for k in nw.NETWORKS:
        lines.append(f"{k}: last webhook {esc(last.get(k))}")
        probe = store.get_checkpoint("rpc_probe:" + k, {}) or {}
        if probe:
            lines.append(f"  RPC check: {'OK' if probe.get('ok') else esc(probe.get('error'))} · {esc(probe.get('checked_at'))}")
    lines += ["", "<b>Recent tx hints</b>"]
    for h in store.recent_hints(8):
        lines.append(f"{esc(h['network'])} {esc(h['txid'][:18])}… {esc(h['source'])} → {esc(h['status'])}"
                     + (f" ({esc(h['last_error'])})" if h.get("last_error") else ""))
    lines += ["", "<b>Recent incoming transfers</b>"]
    for t in store.recent_transfers(8):
        lines.append(f"{esc(t['asset_id'])} {esc(_fmt(t['amount_units'], t['asset_id']))} · {esc(t['status'])}"
                     + (f" ({esc(t['reason'])})" if t.get("reason") else ""))
    return "\n".join(lines)
