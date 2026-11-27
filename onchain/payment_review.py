"""Delayed customer payment reports and audited, idempotent admin settlement.

Manual approval attests a receipt the admin checked in their wallet. It does not
claim RPC verification, change real-test readiness, or credit a wallet directly.
"""
from __future__ import annotations

import asyncio
import html
import re
import secrets

from telegram import InlineKeyboardButton as B, InlineKeyboardMarkup as KB

from . import engine, networks as nw, store


def normalize_txid(network, value):
    value = value.strip()
    if network == 'solana':
        valid = re.fullmatch(r'[1-9A-HJ-NP-Za-km-z]{64,88}', value)
    elif nw.NETWORKS[network].family == 'evm':
        valid = re.fullmatch(r'0x[0-9a-fA-F]{64}', value)
    else:
        valid = re.fullmatch(r'[0-9a-fA-F]{64}', value)
    if not valid:
        raise ValueError('Invalid transaction hash for this network.')
    return value if network == 'solana' else value.lower()


def report_payment(invoice_id, user_id, name='', username='', txid=None):
    """First Verify starts the clock; repeated taps never move that timestamp."""
    with store.conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_invoices WHERE id=%s FOR UPDATE", (invoice_id,))
        inv = cur.fetchone()
        if not inv or inv['purpose'] != 'customer' or inv['user_id'] != user_id:
            raise ValueError('This invoice does not belong to your account.')
        if inv['status'] == 'cancelled':
            raise ValueError('This invoice is cancelled. Contact support if you sent funds.')
        txid = normalize_txid(inv['network'], txid) if txid else None
        cur.execute("INSERT INTO onchain_payment_reports (invoice_id,buyer_name,username) VALUES (%s,%s,%s) "
                    "ON CONFLICT (invoice_id) DO NOTHING", (invoice_id, str(name)[:200], str(username)[:100]))
        if txid:
            cur.execute("SELECT txid FROM onchain_payment_reports WHERE invoice_id=%s", (invoice_id,))
            old = cur.fetchone()['txid']
            if old and old != txid:
                raise ValueError('A transaction hash was already submitted. Contact support to correct it.')
            if not old:
                cur.execute("UPDATE onchain_payment_reports SET txid=%s WHERE invoice_id=%s", (txid, invoice_id))
                cur.execute("UPDATE onchain_delay_alerts SET sent_at=NULL WHERE invoice_id=%s", (invoice_id,))
            # User-supplied hashes are hints only, never proof or a credit instruction.
            cur.execute("INSERT INTO onchain_tx_hints (network,txid,source) VALUES (%s,%s,'customer_report') "
                        "ON CONFLICT DO NOTHING", (inv['network'], txid))
        return inv


def delayed_invoices(admin_id=None):
    with store.conn() as c:
        return c.execute("SELECT i.id FROM onchain_payment_reports r JOIN onchain_invoices i ON i.id=r.invoice_id "
                         "LEFT JOIN onchain_customer_settlements s ON s.invoice_id=i.id "
                         "WHERE r.reported_at <= NOW()-interval '10 minutes' AND i.purpose='customer' "
                         "AND i.status <> 'cancelled' AND (i.status <> 'paid' OR s.status IS DISTINCT FROM 'completed') "
                         "AND (%s::bigint IS NULL OR NOT EXISTS (SELECT 1 FROM onchain_delay_alerts a "
                         "WHERE a.invoice_id=i.id AND a.admin_id=%s AND a.sent_at IS NOT NULL)) "
                         "ORDER BY r.reported_at LIMIT 100", (admin_id, admin_id)).fetchall()


def review_record(invoice_id):
    with store.conn() as c:
        return c.execute("SELECT i.*,r.reported_at,r.buyer_name,r.username,r.txid AS reported_txid, "
                         "t.txid AS detected_txid,t.from_address,t.confirmations,t.block_time, "
                         "s.status AS settlement_status,m.actor AS manual_actor "
                         "FROM onchain_invoices i LEFT JOIN onchain_payment_reports r ON r.invoice_id=i.id "
                         "LEFT JOIN onchain_transfers t ON t.uid=i.matched_transfer_uid "
                         "LEFT JOIN onchain_customer_settlements s ON s.invoice_id=i.id "
                         "LEFT JOIN onchain_manual_receipts m ON m.invoice_id=i.id WHERE i.id=%s", (invoice_id,)).fetchone()


def claim_alert(invoice_id, admin_id):
    with store.conn() as c:
        row = c.execute("INSERT INTO onchain_delay_alerts (invoice_id,admin_id,lease_until) "
                        "VALUES (%s,%s,NOW()+interval '2 minutes') "
                        "ON CONFLICT (invoice_id,admin_id) DO UPDATE SET lease_until=EXCLUDED.lease_until "
                        "WHERE onchain_delay_alerts.sent_at IS NULL AND "
                        "(onchain_delay_alerts.lease_until IS NULL OR onchain_delay_alerts.lease_until < NOW()) "
                        "RETURNING invoice_id", (invoice_id, admin_id)).fetchone()
        return bool(row)


def finish_alert(invoice_id, admin_id, sent):
    with store.conn() as c:
        c.execute("UPDATE onchain_delay_alerts SET lease_until=NULL, sent_at=CASE WHEN %s THEN NOW() ELSE sent_at END "
                  "WHERE invoice_id=%s AND admin_id=%s", (sent, invoice_id, admin_id))


def review_text(r):
    e = lambda v: html.escape(str(v if v is not None else 'unknown'))
    a = nw.ASSETS[r['asset_id']]
    amount = engine.format_units(r['expected_units'], a.decimals, a.display_decimals)
    txid = r.get('detected_txid') or r.get('reported_txid')
    lines = ['⏱ <b>Payment needs admin review</b>',
             f"Buyer: {e(r.get('buyer_name') or 'unknown')} (@{e(r.get('username') or 'unavailable')})",
             f"User ID: <code>{r['user_id']}</code>",
             f"Invoice: <code>{e(r['public_ref'])}</code> · status: {e(r['status'])}",
             f"Credit: <b>${e(r['usd_amount'])}</b>",
             f"Expected receipt: <code>{e(amount)}</code> {e(a.label)}",
             f"Network: {e(nw.NETWORKS[r['network']].label)}",
             f"Receiving address: <code>{e(r['destination'])}</code>",
             f"Created (UTC): {e(r['created_at'])}",
             f"Buyer tapped Paid (UTC): {e(r.get('reported_at'))}",
             f"Expires (UTC): {e(r['expires_at'])}",
             f"TXID ({'chain detected' if r.get('detected_txid') else 'buyer supplied; UNVERIFIED'}): <code>{e(txid)}</code>",
             f"Sending address: <code>{e(r.get('from_address'))}</code>",
             f"Confirmations: {e(r.get('confirmations'))} · settlement: {e(r.get('settlement_status') or 'not queued')}"]
    if r.get('manual_actor'):
        lines.append(f"Manually approved by admin {r['manual_actor']}.")
    if not txid:
        lines.append('No transaction hash is known yet. Identify the receipt by exact amount, network, address and time; obtain its TXID from Trust Wallet or the buyer.')
    lines += ['', 'Check the actual received asset, network, destination, amount and time in Trust Wallet before approving.',
              'Use this invoice approval flow; the general Add Balance button does not link a payment.',
              f"<code>/oc_manual {e(r['public_ref'])} {e(txid or 'TXID')}</code>"]
    return '\n'.join(lines)


def review_keyboard(r):
    rows = [[B('🔎 Review / refresh payment', callback_data=f"oc:payment:{r['id']}")]]
    if r['status'] in ('awaiting', 'detected', 'expired') and (r.get('detected_txid') or r.get('reported_txid')):
        rows.append([B('✅ Prepare manual approval', callback_data=f"oc:mpreview:{r['id']}")])
    return KB(rows)


def approval_text(r, txid):
    return (review_text(r) + '\n\n<b>FINAL CHECK:</b> I checked this exact receipt in Trust Wallet and approve '
            f"${r['usd_amount']} for user {r['user_id']}.\nTXID: <code>{html.escape(txid)}</code>\n"
            'This records a manual wallet attestation, not an automatic chain verification.')


def approval_keyboard(token, invoice_id):
    return KB([[B('✅ Receipt checked — approve once', callback_data=f'oc:manual:{token}')],
               [B('Cancel', callback_data=f'oc:payment:{invoice_id}')]])


async def notify_delays(bot):
    for admin_id in sorted(bot.ADMIN_IDS):
        for row in await asyncio.to_thread(delayed_invoices, admin_id):
            if not await asyncio.to_thread(claim_alert, row['id'], admin_id):
                continue
            sent = False
            try:
                r = await asyncio.to_thread(review_record, row['id'])
                # A settlement may have completed between selection and claim.
                if r and r['status'] != 'cancelled' and r.get('settlement_status') != 'completed':
                    await bot.app_instance.bot.send_message(admin_id, review_text(r), parse_mode='HTML',
                                                           reply_markup=review_keyboard(r))
                sent = True
            except Exception as exc:
                print(f"[onchain] delayed payment alert retry: {type(exc).__name__}")
            finally:
                await asyncio.to_thread(finish_alert, row['id'], admin_id, sent)


def prepare_manual(public_ref, txid, actor):
    with store.conn() as c:
        inv = c.execute("SELECT * FROM onchain_invoices WHERE public_ref=%s", (public_ref.upper(),)).fetchone()
        if not inv or inv['purpose'] != 'customer':
            raise ValueError('Customer invoice not found.')
        txid = normalize_txid(inv['network'], txid)
        token = secrets.token_hex(12)
        c.execute("DELETE FROM onchain_manual_drafts WHERE expires_at < NOW()")
        c.execute("INSERT INTO onchain_manual_drafts (token,invoice_id,txid,actor) VALUES (%s,%s,%s,%s)",
                  (token, inv['id'], txid, actor))
        return token, inv, txid


def approve_manual(token, actor):
    """Serialize with automatic finalization, then enqueue the SAME wallet outbox.

    A TXID used by a manual attestation is reserved on its chain. Further outputs
    of that TX go to review, rather than guessing how an admin allocated them.
    """
    with store.conn() as c, c.cursor() as cur:
        cur.execute("SELECT d.*,i.network FROM onchain_manual_drafts d JOIN onchain_invoices i ON i.id=d.invoice_id "
                    "WHERE d.token=%s AND d.actor=%s AND d.expires_at>NOW()", (token, actor))
        d = cur.fetchone()
        if not d:
            raise ValueError('Approval expired or belongs to another admin. Create a fresh preview.')
        store.lock_payment_tx(cur, d['network'], d['txid'])
        cur.execute("SELECT * FROM onchain_invoices WHERE id=%s FOR UPDATE", (d['invoice_id'],))
        inv = cur.fetchone()
        if inv['status'] == 'paid':
            return {'result': 'already_paid', 'invoice_id': inv['id']}
        if inv['purpose'] != 'customer' or inv['status'] not in ('awaiting', 'expired', 'detected'):
            raise ValueError('This invoice cannot be manually approved.')
        cur.execute("SELECT 1 FROM onchain_payment_reports WHERE invoice_id=%s "
                    "AND reported_at<=NOW()-interval '10 minutes'", (inv['id'],))
        if not cur.fetchone():
            raise ValueError('Manual review is available 10 minutes after the buyer taps Paid.')
        cur.execute("SELECT 1 FROM onchain_manual_receipts WHERE network=%s AND txid=%s", (d['network'], d['txid']))
        if cur.fetchone():
            raise ValueError('This transaction was already manually allocated to an invoice.')
        cur.execute("SELECT * FROM onchain_transfers WHERE network=%s AND txid=%s", (d['network'], d['txid']))
        transfers = cur.fetchall()
        if any(t['status'] == 'final' or (t['invoice_id'] and t['invoice_id'] != inv['id']) for t in transfers):
            raise ValueError('This transaction is already allocated. Review its existing invoice.')
        if transfers:
            def matches(t):
                dest, expected = t['to_address'], inv['destination']
                if nw.NETWORKS[inv['network']].family == 'evm':
                    dest, expected = dest.lower(), expected.lower()
                return (t['asset_id'] == inv['asset_id'] and dest == expected and
                        t['amount_units'] == inv['expected_units'] and t['block_time'] is not None and
                        inv['created_at'] <= t['block_time'] <= inv['expires_at'] and t['status'] != 'dropped')
            if not any(matches(t) for t in transfers):
                raise ValueError('Known chain data does not match this invoice. Review instead of crediting it.')
        if inv.get('matched_transfer_uid'):
            cur.execute("SELECT txid FROM onchain_transfers WHERE uid=%s", (inv['matched_transfer_uid'],))
            matched = cur.fetchone()
            if matched and matched['txid'] != d['txid']:
                raise ValueError('A different transaction is already matched to this invoice.')
        transfer_uid = inv.get('matched_transfer_uid') or f"{d['network']}:{d['txid']}:manual:{inv['public_ref']}"
        cur.execute("INSERT INTO onchain_manual_receipts (invoice_id,network,txid,actor) VALUES (%s,%s,%s,%s)",
                    (inv['id'], d['network'], d['txid'], actor))
        cur.execute("INSERT INTO onchain_customer_settlements (invoice_id,transfer_uid) VALUES (%s,%s)",
                    (inv['id'], transfer_uid))
        cur.execute("UPDATE onchain_invoices SET status='paid',paid_at=NOW(),updated_at=NOW(),matched_transfer_uid=%s "
                    "WHERE id=%s", (transfer_uid, inv['id']))
        cur.execute("UPDATE onchain_transfers SET status='manual_settled',reason='admin_wallet_attestation',updated_at=NOW() "
                    "WHERE uid=%s", (transfer_uid,))
        cur.execute("INSERT INTO onchain_audit (actor,action,detail) VALUES (%s,'customer_payment_manual_approval',%s)",
                    (actor, store._j({'invoice': inv['id'], 'user_id': inv['user_id'], 'usd_amount': str(inv['usd_amount']),
                                     'network': inv['network'], 'txid': d['txid'], 'proof': 'admin_wallet_attestation'})))
        return {'result': 'queued', 'invoice_id': inv['id']}


async def tx_command(bot, update, context):
    args = context.args or []
    if len(args) != 2:
        await update.message.reply_text('Usage: /payment_tx <invoice reference> <TXID>')
        return
    try:
        def find():
            with store.conn() as c:
                return c.execute("SELECT id FROM onchain_invoices WHERE public_ref=%s", (args[0].upper(),)).fetchone()
        row = await asyncio.to_thread(find)
        if not row:
            raise ValueError('Invoice not found.')
        u = update.effective_user
        await asyncio.to_thread(report_payment, row['id'], u.id, u.full_name, u.username or '', args[1])
        await update.message.reply_text('Transaction hash saved for verification and admin review. It is not proof of payment by itself.')
    except ValueError as exc:
        await update.message.reply_text(str(exc))


async def manual_command(update, context, is_admin):
    actor = update.effective_user.id
    if not is_admin(actor):
        return
    args = context.args or []
    if len(args) != 2:
        await update.message.reply_text('Usage: /oc_manual <invoice reference> <wallet receipt TXID>')
        return
    try:
        token, inv, txid = await asyncio.to_thread(prepare_manual, args[0], args[1], actor)
        r = await asyncio.to_thread(review_record, inv['id'])
        await update.message.reply_text(approval_text(r, txid), parse_mode='HTML',
                                        reply_markup=approval_keyboard(token, inv['id']))
    except ValueError as exc:
        await update.message.reply_text(str(exc))
