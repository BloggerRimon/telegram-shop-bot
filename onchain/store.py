"""PostgreSQL persistence for self-custody payment monitoring.

All tables are additive (CREATE ... IF NOT EXISTS). Nothing here touches the
existing bot_state / users / transactions / orders tables, and admin test credit
is kept in onchain_test_ledger only -- it is never spendable customer balance.
"""

from __future__ import annotations

import json
import os
import secrets
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from . import networks as nw

SCHEMA_VERSION = 1


def _db_url() -> str:
    return (os.getenv("ONCHAIN_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _j(v):
    return Jsonb(v, dumps=lambda x: json.dumps(x, default=str))


@contextmanager
def conn():
    url = _db_url()
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    c = psycopg.connect(url, row_factory=dict_row)
    try:
        yield c
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


DDL = [
    """CREATE TABLE IF NOT EXISTS onchain_customer_settlements (
        invoice_id BIGINT PRIMARY KEY,
        transfer_uid TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL DEFAULT 'pending',
        completed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS onchain_settings (
        key TEXT PRIMARY KEY,
        value JSONB NOT NULL,
        updated_by BIGINT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS onchain_audit (
        id BIGSERIAL PRIMARY KEY,
        actor BIGINT,
        action TEXT NOT NULL,
        detail JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS onchain_invoices (
        id BIGSERIAL PRIMARY KEY,
        public_ref TEXT NOT NULL UNIQUE,
        purpose TEXT NOT NULL CHECK (purpose IN ('admin_test', 'customer')),
        provider TEXT NOT NULL DEFAULT 'self_custody_alchemy',
        created_by BIGINT,
        user_id BIGINT,
        network TEXT NOT NULL,
        asset_id TEXT NOT NULL,
        destination TEXT NOT NULL,
        expected_units NUMERIC(78,0) NOT NULL CHECK (expected_units > 0),
        decimals INTEGER NOT NULL,
        usd_amount NUMERIC(18,2) NOT NULL,
        rate_usd NUMERIC(38,18),
        rate_source TEXT,
        valuation_rule TEXT,
        quoted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL,
        matched_transfer_uid TEXT UNIQUE,
        detected_at TIMESTAMPTZ,
        paid_at TIMESTAMPTZ,
        note TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""",
    # One active invoice per exact (asset, amount): this is the attribution key.
    """CREATE UNIQUE INDEX IF NOT EXISTS uq_onchain_invoice_active_amount
        ON onchain_invoices (asset_id, expected_units)
        WHERE status IN ('awaiting', 'detected')""",
    """CREATE TABLE IF NOT EXISTS onchain_raw_events (
        id BIGSERIAL PRIMARY KEY,
        source TEXT NOT NULL,
        network TEXT NOT NULL,
        webhook_id TEXT,
        provider_event_id TEXT NOT NULL,
        body JSONB NOT NULL,
        received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        processed_at TIMESTAMPTZ,
        process_status TEXT NOT NULL DEFAULT 'new',
        error TEXT,
        UNIQUE (source, provider_event_id))""",
    """CREATE TABLE IF NOT EXISTS onchain_tx_hints (
        network TEXT NOT NULL,
        txid TEXT NOT NULL,
        source TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'new',
        attempts INTEGER NOT NULL DEFAULT 0,
        next_check_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (network, txid))""",
    """CREATE TABLE IF NOT EXISTS onchain_transfers (
        uid TEXT PRIMARY KEY,
        network TEXT NOT NULL,
        asset_id TEXT NOT NULL,
        txid TEXT NOT NULL,
        locator TEXT NOT NULL,
        to_address TEXT NOT NULL,
        from_address TEXT,
        amount_units NUMERIC(78,0) NOT NULL,
        block_number BIGINT,
        block_time TIMESTAMPTZ,
        confirmations INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL,
        reason TEXT,
        invoice_id BIGINT,
        source TEXT,
        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (network, txid, locator))""",
    """CREATE TABLE IF NOT EXISTS onchain_test_ledger (
        id BIGSERIAL PRIMARY KEY,
        invoice_id BIGINT NOT NULL UNIQUE,
        transfer_uid TEXT NOT NULL UNIQUE,
        admin_id BIGINT,
        asset_id TEXT NOT NULL,
        amount_units NUMERIC(78,0) NOT NULL,
        usd_amount NUMERIC(18,2) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""",
    """CREATE TABLE IF NOT EXISTS onchain_review (
        id BIGSERIAL PRIMARY KEY,
        transfer_uid TEXT UNIQUE,
        invoice_id BIGINT,
        reason TEXT NOT NULL,
        detail JSONB NOT NULL DEFAULT '{}'::jsonb,
        status TEXT NOT NULL DEFAULT 'open',
        resolution TEXT,
        resolved_by BIGINT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        resolved_at TIMESTAMPTZ)""",
    """CREATE TABLE IF NOT EXISTS onchain_checkpoints (
        name TEXT PRIMARY KEY,
        value JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""",
]

_initialized = False


def init_schema(force: bool = False) -> None:
    global _initialized
    if _initialized and not force:
        return
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(74810021)")
        for stmt in DDL:
            cur.execute(stmt)
        # Defaults: customer activation OFF, every network OFF.
        cur.execute(
            "INSERT INTO onchain_settings (key, value) VALUES ('customer_new_payments_enabled', 'false'::jsonb) "
            "ON CONFLICT (key) DO NOTHING")
        for key in nw.NETWORKS:
            cur.execute(
                "INSERT INTO onchain_settings (key, value) VALUES (%s, 'false'::jsonb) ON CONFLICT (key) DO NOTHING",
                (f"network_enabled:{key}",))
            # Monitoring start time: anything on-chain before this is historical and never credited.
            cur.execute(
                "INSERT INTO onchain_checkpoints (name, value) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING",
                (f"monitoring_started_at:{key}", _j(utcnow().isoformat())))
    _initialized = True


# ---------------------------------------------------------------- settings
def get_setting(key: str, default=None):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT value FROM onchain_settings WHERE key=%s", (key,))
        row = cur.fetchone()
        return row["value"] if row else default


def set_setting(key: str, value, actor: Optional[int]) -> None:
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT value FROM onchain_settings WHERE key=%s FOR UPDATE", (key,))
        old = cur.fetchone()
        cur.execute(
            "INSERT INTO onchain_settings (key, value, updated_by, updated_at) VALUES (%s,%s,%s,NOW()) "
            "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_by=EXCLUDED.updated_by, updated_at=NOW()",
            (key, _j(value), actor))
        cur.execute("INSERT INTO onchain_audit (actor, action, detail) VALUES (%s,'setting_changed',%s)",
                    (actor, _j({"key": key, "old": old["value"] if old else None, "new": value})))


def audit(actor: Optional[int], action: str, detail: Dict[str, Any]) -> None:
    with conn() as c, c.cursor() as cur:
        cur.execute("INSERT INTO onchain_audit (actor, action, detail) VALUES (%s,%s,%s)", (actor, action, _j(detail)))


def recent_audit(limit: int = 10):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_audit ORDER BY id DESC LIMIT %s", (limit,))
        return cur.fetchall()


def get_checkpoint(name: str, default=None):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT value FROM onchain_checkpoints WHERE name=%s", (name,))
        row = cur.fetchone()
        return row["value"] if row else default


def set_checkpoint(name: str, value) -> None:
    with conn() as c, c.cursor() as cur:
        cur.execute(
            "INSERT INTO onchain_checkpoints (name, value, updated_at) VALUES (%s,%s,NOW()) "
            "ON CONFLICT (name) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()", (name, _j(value)))


def monitoring_started_at(network: str) -> datetime:
    v = get_checkpoint(f"monitoring_started_at:{network}")
    if not v:
        return utcnow()
    return datetime.fromisoformat(str(v))


# ---------------------------------------------------------------- invoices
class AmountSpaceExhausted(Exception):
    pass


def create_invoice_reserved(*, purpose: str, created_by: Optional[int], user_id: Optional[int], asset: nw.Asset,
                            base_units: int, usd_amount: Decimal, rate_usd: Optional[Decimal], rate_source: str,
                            valuation_rule: str, ttl_minutes: int, max_offsets: int = 999, note: Optional[str] = None) -> dict:
    """Atomically reserve a unique exact amount (base + k*quantum) for this asset.

    Uniqueness is enforced by the partial unique index on active invoices, so two
    concurrent creators can never hold the same (asset, amount).
    """
    quantum = 10 ** (asset.decimals - asset.display_decimals)
    base_units = -(-int(base_units) // quantum) * quantum  # round up to display precision
    start = secrets.randbelow(max_offsets)
    network = nw.NETWORKS[asset.network]
    for i in range(max_offsets):
        k = (start + i) % max_offsets + 1
        units = base_units + k * quantum
        ref = "OC" + secrets.token_hex(5).upper()
        try:
            with conn() as c, c.cursor() as cur:
                cur.execute(
                    """INSERT INTO onchain_invoices (public_ref, purpose, created_by, user_id, network, asset_id,
                        destination, expected_units, decimals, usd_amount, rate_usd, rate_source, valuation_rule,
                        quoted_at, expires_at, status)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(), NOW() + make_interval(mins => %s),
                               'awaiting') RETURNING *""",
                    (ref, purpose, created_by, user_id, asset.network, asset.id, network.address, units,
                     asset.decimals, usd_amount, rate_usd, rate_source, valuation_rule, int(ttl_minutes)))
                row = cur.fetchone()
                if note is not None:
                    cur.execute("UPDATE onchain_invoices SET note=%s WHERE id=%s", (note, row["id"]))
                    row["note"] = note
                cur.execute("INSERT INTO onchain_audit (actor, action, detail) VALUES (%s,'invoice_created',%s)",
                            (created_by, _j({"invoice": row["id"], "ref": ref, "asset": asset.id, "purpose": purpose})))
                return row
        except psycopg.errors.UniqueViolation:
            continue
    raise AmountSpaceExhausted(asset.id)


def get_invoice(invoice_id: int) -> Optional[dict]:
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_invoices WHERE id=%s", (invoice_id,))
        return cur.fetchone()


def list_invoices(limit: int = 10, purpose: Optional[str] = None):
    with conn() as c, c.cursor() as cur:
        if purpose:
            cur.execute("SELECT * FROM onchain_invoices WHERE purpose=%s ORDER BY id DESC LIMIT %s", (purpose, limit))
        else:
            cur.execute("SELECT * FROM onchain_invoices ORDER BY id DESC LIMIT %s", (limit,))
        return cur.fetchall()


def cancel_invoice(invoice_id: int, actor: int) -> bool:
    with conn() as c, c.cursor() as cur:
        cur.execute("UPDATE onchain_invoices SET status='cancelled', updated_at=NOW() "
                    "WHERE id=%s AND status='awaiting' RETURNING id", (invoice_id,))
        ok = cur.fetchone() is not None
        if ok:
            cur.execute("INSERT INTO onchain_audit (actor, action, detail) VALUES (%s,'invoice_cancelled',%s)",
                        (actor, _j({"invoice": invoice_id})))
        return ok


def expire_invoices() -> int:
    """Expire unpaid invoices. Detected-but-not-final invoices are NOT expired:
    a payment that arrived in time keeps tracking until finality."""
    with conn() as c, c.cursor() as cur:
        cur.execute("UPDATE onchain_invoices SET status='expired', updated_at=NOW() "
                    "WHERE status='awaiting' AND expires_at < NOW() RETURNING id")
        return len(cur.fetchall())


# ---------------------------------------------------------------- raw events / hints
def store_raw_event(source: str, network: str, webhook_id: Optional[str], provider_event_id: str, body: dict) -> bool:
    """Persist a verified webhook delivery. Returns False for a duplicate delivery."""
    with conn() as c, c.cursor() as cur:
        cur.execute(
            "INSERT INTO onchain_raw_events (source, network, webhook_id, provider_event_id, body) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (source, provider_event_id) DO NOTHING RETURNING id",
            (source, network, webhook_id, provider_event_id, _j(body)))
        return cur.fetchone() is not None


def claim_raw_events(limit: int = 50):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_raw_events WHERE process_status='new' ORDER BY id "
                    "LIMIT %s FOR UPDATE SKIP LOCKED", (limit,))
        rows = cur.fetchall()
        for r in rows:
            cur.execute("UPDATE onchain_raw_events SET process_status='processing' WHERE id=%s", (r["id"],))
        return rows


def finish_raw_event(event_id: int, status: str, error: Optional[str] = None) -> None:
    with conn() as c, c.cursor() as cur:
        cur.execute("UPDATE onchain_raw_events SET process_status=%s, error=%s, processed_at=NOW() WHERE id=%s",
                    (status, (error or "")[:500] or None, event_id))


def requeue_stuck_raw_events(minutes: int = 10) -> None:
    with conn() as c, c.cursor() as cur:
        cur.execute("UPDATE onchain_raw_events SET process_status='new' WHERE process_status='processing' "
                    "AND received_at < NOW() - make_interval(mins => %s) AND processed_at IS NULL", (minutes,))


def add_hint(network: str, txid: str, source: str) -> bool:
    with conn() as c, c.cursor() as cur:
        cur.execute("INSERT INTO onchain_tx_hints (network, txid, source) VALUES (%s,%s,%s) "
                    "ON CONFLICT (network, txid) DO NOTHING RETURNING txid", (network, txid, source))
        return cur.fetchone() is not None


def due_hints(limit: int = 25):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_tx_hints WHERE status IN ('new','retry') AND next_check_at <= NOW() "
                    "ORDER BY next_check_at LIMIT %s FOR UPDATE SKIP LOCKED", (limit,))
        rows = cur.fetchall()
        for r in rows:
            cur.execute("UPDATE onchain_tx_hints SET next_check_at = NOW() + interval '5 minutes' "
                        "WHERE network=%s AND txid=%s", (r["network"], r["txid"]))
        return rows


def update_hint(network: str, txid: str, status: str, error: Optional[str] = None, delay_seconds: int = 0) -> None:
    with conn() as c, c.cursor() as cur:
        cur.execute("UPDATE onchain_tx_hints SET status=%s, last_error=%s, attempts=attempts+1, "
                    "next_check_at=NOW() + make_interval(secs => %s), updated_at=NOW() WHERE network=%s AND txid=%s",
                    (status, (error or "")[:300] or None, int(delay_seconds), network, txid))


def recent_hints(limit: int = 10):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_tx_hints ORDER BY created_at DESC LIMIT %s", (limit,))
        return cur.fetchall()


# ---------------------------------------------------------------- transfers
def upsert_transfer(t: dict) -> dict:
    """Insert a newly observed incoming transfer; existing rows only get fresher chain data."""
    with conn() as c, c.cursor() as cur:
        cur.execute(
            """INSERT INTO onchain_transfers (uid, network, asset_id, txid, locator, to_address, from_address,
                   amount_units, block_number, block_time, confirmations, status, reason, source)
               VALUES (%(uid)s,%(network)s,%(asset_id)s,%(txid)s,%(locator)s,%(to_address)s,%(from_address)s,
                   %(amount_units)s,%(block_number)s,%(block_time)s,%(confirmations)s,'detected',NULL,%(source)s)
               ON CONFLICT (uid) DO UPDATE SET
                   block_number=COALESCE(EXCLUDED.block_number, onchain_transfers.block_number),
                   block_time=COALESCE(EXCLUDED.block_time, onchain_transfers.block_time),
                   confirmations=GREATEST(EXCLUDED.confirmations, onchain_transfers.confirmations),
                   updated_at=NOW()
               RETURNING *""", t)
        return cur.fetchone()


def get_transfer(uid: str) -> Optional[dict]:
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_transfers WHERE uid=%s", (uid,))
        return cur.fetchone()


def set_transfer_status(uid: str, status: str, reason: Optional[str] = None, confirmations: Optional[int] = None):
    with conn() as c, c.cursor() as cur:
        cur.execute("UPDATE onchain_transfers SET status=%s, reason=COALESCE(%s, reason), "
                    "confirmations=COALESCE(%s, confirmations), updated_at=NOW() WHERE uid=%s",
                    (status, reason, confirmations, uid))


def pending_transfers(limit: int = 50):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_transfers WHERE status IN ('detected','confirming') "
                    "ORDER BY first_seen_at LIMIT %s", (limit,))
        return cur.fetchall()


def recent_transfers(limit: int = 10):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_transfers ORDER BY first_seen_at DESC LIMIT %s", (limit,))
        return cur.fetchall()


def open_review(transfer_uid: Optional[str], invoice_id: Optional[int], reason: str, detail: dict) -> None:
    with conn() as c, c.cursor() as cur:
        cur.execute("INSERT INTO onchain_review (transfer_uid, invoice_id, reason, detail) VALUES (%s,%s,%s,%s) "
                    "ON CONFLICT (transfer_uid) DO NOTHING", (transfer_uid, invoice_id, reason, _j(detail)))


def list_review(status: str = "open", limit: int = 10):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_review WHERE status=%s ORDER BY id DESC LIMIT %s", (status, limit))
        return cur.fetchall()


def resolve_review(review_id: int, actor: int, resolution: str) -> bool:
    with conn() as c, c.cursor() as cur:
        cur.execute("UPDATE onchain_review SET status='resolved', resolution=%s, resolved_by=%s, resolved_at=NOW() "
                    "WHERE id=%s AND status='open' RETURNING id", (resolution, actor, review_id))
        ok = cur.fetchone() is not None
        if ok:
            cur.execute("INSERT INTO onchain_audit (actor, action, detail) VALUES (%s,'review_resolved',%s)",
                        (actor, _j({"review": review_id, "resolution": resolution})))
        return ok


# ---------------------------------------------------------------- matching & credit
def match_transfer(uid: str) -> dict:
    """Attach a detected transfer to exactly one eligible invoice, or route it to review.

    Eligible = same asset, exact amount, invoice was awaiting, created at or before the
    transfer's block time, and not expired at block time. Returns {'result': ...}.
    """
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_transfers WHERE uid=%s FOR UPDATE", (uid,))
        t = cur.fetchone()
        if not t or t["invoice_id"] is not None or t["status"] not in ("detected", "confirming"):
            return {"result": "skip"}
        if t["block_time"] is None:
            return {"result": "wait_block_time"}
        cur.execute("SELECT value FROM onchain_checkpoints WHERE name=%s", (f"monitoring_started_at:{t['network']}",))
        ms = cur.fetchone()
        started = datetime.fromisoformat(str(ms["value"])) if ms else None
        if started and t["block_time"] < started:
            cur.execute("UPDATE onchain_transfers SET status='rejected', reason='historical_before_monitoring', "
                        "updated_at=NOW() WHERE uid=%s", (uid,))
            return {"result": "historical"}
        cur.execute(
            """SELECT * FROM onchain_invoices WHERE asset_id=%s AND expected_units=%s
               AND status='awaiting' AND created_at <= %s AND expires_at >= %s
               ORDER BY id FOR UPDATE""",
            (t["asset_id"], t["amount_units"], t["block_time"], t["block_time"]))
        cands = cur.fetchall()
        if len(cands) == 1:
            inv = cands[0]
            cur.execute("UPDATE onchain_invoices SET status='detected', matched_transfer_uid=%s, detected_at=NOW(), "
                        "updated_at=NOW() WHERE id=%s", (uid, inv["id"]))
            cur.execute("UPDATE onchain_transfers SET invoice_id=%s, updated_at=NOW() WHERE uid=%s", (inv["id"], uid))
            return {"result": "matched", "invoice_id": inv["id"]}
        if len(cands) > 1:
            reason = "ambiguous_match"
        else:
            cur.execute("SELECT id, status, created_at, expires_at FROM onchain_invoices WHERE asset_id=%s "
                        "AND expected_units=%s ORDER BY id DESC LIMIT 1", (t["asset_id"], t["amount_units"]))
            same = cur.fetchone()
            if same and same["status"] in ("expired", "awaiting") and same["created_at"] <= t["block_time"]:
                reason = "late_payment"
            elif same and same["created_at"] > t["block_time"]:
                reason = "transfer_before_invoice"
            else:
                cur.execute("SELECT 1 FROM onchain_invoices WHERE asset_id=%s AND status='awaiting' LIMIT 1",
                            (t["asset_id"],))
                reason = "amount_mismatch" if cur.fetchone() else "unmatched"
        cur.execute("UPDATE onchain_transfers SET status='review', reason=%s, updated_at=NOW() WHERE uid=%s",
                    (reason, uid))
        cur.execute("INSERT INTO onchain_review (transfer_uid, invoice_id, reason, detail) VALUES (%s,%s,%s,%s) "
                    "ON CONFLICT (transfer_uid) DO NOTHING",
                    (uid, cands[0]["id"] if cands else None, reason,
                     _j({"asset": t["asset_id"], "amount_units": str(t["amount_units"]), "txid": t["txid"]})))
        return {"result": "review", "reason": reason}


def credit_final_transfer(uid: str, confirmations: int) -> dict:
    """Atomically finalize: transfer -> final, invoice -> paid, admin test ledger row.

    Unique constraints on onchain_test_ledger (invoice_id, transfer_uid) make a double
    credit impossible even with concurrent workers.
    """
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_transfers WHERE uid=%s FOR UPDATE", (uid,))
        t = cur.fetchone()
        if not t or t["status"] not in ("detected", "confirming") or t["invoice_id"] is None:
            return {"result": "skip"}
        cur.execute("SELECT * FROM onchain_invoices WHERE id=%s FOR UPDATE", (t["invoice_id"],))
        inv = cur.fetchone()
        if not inv or inv["status"] != "detected" or inv["matched_transfer_uid"] != uid:
            return {"result": "skip"}
        if inv["purpose"] == "customer":
            # Durable outbox. The bot applies the normal wallet credit using its
            # existing persistent idempotency keys before acknowledging this row.
            cur.execute("INSERT INTO onchain_customer_settlements (invoice_id, transfer_uid) "
                        "VALUES (%s,%s) ON CONFLICT DO NOTHING", (inv["id"], uid))
            cur.execute("UPDATE onchain_transfers SET status='final', confirmations=%s, updated_at=NOW() WHERE uid=%s",
                        (confirmations, uid))
            cur.execute("UPDATE onchain_invoices SET status='paid', paid_at=NOW(), updated_at=NOW() WHERE id=%s",
                        (inv["id"],))
            return {"result": "customer_pending", "invoice_id": inv["id"]}
        cur.execute("INSERT INTO onchain_test_ledger (invoice_id, transfer_uid, admin_id, asset_id, amount_units, "
                    "usd_amount) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id",
                    (inv["id"], uid, inv["created_by"], inv["asset_id"], t["amount_units"], inv["usd_amount"]))
        if cur.fetchone() is None:
            return {"result": "duplicate"}
        cur.execute("UPDATE onchain_transfers SET status='final', confirmations=%s, updated_at=NOW() WHERE uid=%s",
                    (confirmations, uid))
        cur.execute("UPDATE onchain_invoices SET status='paid', paid_at=NOW(), updated_at=NOW() WHERE id=%s",
                    (inv["id"],))
        cur.execute("INSERT INTO onchain_audit (actor, action, detail) VALUES (NULL,'admin_test_credited',%s)",
                    (_j({"invoice": inv["id"], "transfer": uid}),))
        return {"result": "credited", "invoice_id": inv["id"], "admin_id": inv["created_by"]}


def unmatch_dropped_transfer(uid: str, reason: str) -> None:
    """Transfer disappeared/failed before finality (e.g. reorg): release its invoice."""
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_transfers WHERE uid=%s FOR UPDATE", (uid,))
        t = cur.fetchone()
        if not t or t["status"] == "final":
            return
        cur.execute("UPDATE onchain_transfers SET status='dropped', reason=%s, updated_at=NOW() WHERE uid=%s",
                    (reason, uid))
        if t["invoice_id"]:
            cur.execute("UPDATE onchain_invoices SET status=CASE WHEN expires_at < NOW() THEN 'expired' "
                        "ELSE 'awaiting' END, matched_transfer_uid=NULL, detected_at=NULL, updated_at=NOW() "
                        "WHERE id=%s AND status='detected' AND matched_transfer_uid=%s", (t["invoice_id"], uid))


def test_ledger(limit: int = 10):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_test_ledger ORDER BY id DESC LIMIT %s", (limit,))
        return cur.fetchall()


def paid_test_assets() -> set:
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT DISTINCT asset_id FROM onchain_test_ledger")
        return {r["asset_id"] for r in cur.fetchall()}


def earliest_open_invoice_time(network: str):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT MIN(created_at) AS started FROM onchain_invoices WHERE network=%s "
                    "AND status IN ('awaiting','detected') AND (expires_at > NOW() OR status='detected')",
                    (network,))
        return cur.fetchone()["started"]


def pending_customer_settlements(limit: int = 25):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT i.*, s.transfer_uid FROM onchain_customer_settlements s "
                    "JOIN onchain_invoices i ON i.id=s.invoice_id "
                    "WHERE s.status='pending' AND i.purpose='customer' AND i.status='paid' "
                    "ORDER BY i.id LIMIT %s", (limit,))
        return cur.fetchall()


def finish_customer_settlement(invoice_id: int):
    with conn() as c, c.cursor() as cur:
        cur.execute("UPDATE onchain_customer_settlements SET status='completed', completed_at=NOW() "
                    "WHERE invoice_id=%s", (invoice_id,))


def customer_settlement_complete(invoice_id: int) -> bool:
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT status FROM onchain_customer_settlements WHERE invoice_id=%s", (invoice_id,))
        row = cur.fetchone()
        return bool(row and row["status"] == "completed")


def latest_customer_invoice(user_id: int, kind: str):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM onchain_invoices WHERE purpose='customer' AND user_id=%s "
                    "AND note::jsonb->>'kind'=%s ORDER BY id DESC LIMIT 1", (user_id, kind))
        return cur.fetchone()


def last_event_times() -> Dict[str, Any]:
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT network, MAX(received_at) AS t FROM onchain_raw_events GROUP BY network")
        return {r["network"]: r["t"] for r in cur.fetchall()}
