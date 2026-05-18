import os
import json
from datetime import datetime
from decimal import Decimal
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


def _json_default(value):
    if isinstance(value, datetime):
        return {"__datetime__": value.isoformat()}
    if isinstance(value, set):
        return list(value)
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _restore_datetime(value):
    if isinstance(value, dict) and "__datetime__" in value:
        try:
            return datetime.fromisoformat(value["__datetime__"])
        except Exception:
            return value.get("__datetime__")
    if isinstance(value, dict):
        return {k: _restore_datetime(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_restore_datetime(v) for v in value]
    return value


def _to_jsonb(value):
    return Jsonb(value, dumps=lambda x: json.dumps(x, ensure_ascii=False, default=_json_default))


@contextmanager
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    conn = psycopg.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create all required tables if they do not exist."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_state (
                    key TEXT PRIMARY KEY,
                    value JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    is_bot BOOLEAN NOT NULL DEFAULT FALSE,
                    wallet_balance NUMERIC(14, 2) NOT NULL DEFAULT 0,
                    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_bot BOOLEAN NOT NULL DEFAULT FALSE;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS transactions (
                    id BIGINT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    tx_type TEXT NOT NULL,
                    amount NUMERIC(14, 2) NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    meta JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    id BIGINT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    product_id TEXT,
                    product TEXT,
                    qty INTEGER NOT NULL DEFAULT 1,
                    total NUMERIC(14, 2) NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    payment_type TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);")


def save_state(state: dict):
    """Save the full bot state as JSONB. This is safest for your current code structure."""
    init_db()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_state (key, value, updated_at)
                VALUES ('main', %s, NOW())
                ON CONFLICT (key)
                DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
                """,
                (_to_jsonb(state),),
            )


def load_state():
    """Load full bot state. Returns None if database is empty."""
    init_db()
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT value FROM bot_state WHERE key = 'main';")
            row = cur.fetchone()
            if not row:
                return None
            return _restore_datetime(row["value"])


def upsert_user(user_id: int, wallet_balance=0):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (user_id, wallet_balance, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (user_id)
                DO UPDATE SET wallet_balance = EXCLUDED.wallet_balance, updated_at = NOW();
                """,
                (int(user_id), Decimal(str(wallet_balance))),
            )


def upsert_transaction(tx: dict):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO transactions (id, user_id, tx_type, amount, status, meta, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id)
                DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    tx_type = EXCLUDED.tx_type,
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status,
                    meta = EXCLUDED.meta,
                    updated_at = EXCLUDED.updated_at;
                """,
                (
                    int(tx["id"]),
                    int(tx["user_id"]),
                    str(tx.get("type", "")),
                    Decimal(str(tx.get("amount", 0))),
                    str(tx.get("status", "")),
                    _to_jsonb(tx.get("meta", {}) or {}),
                    tx.get("created_at") or datetime.now(),
                    tx.get("updated_at") or datetime.now(),
                ),
            )


def upsert_order(order: dict):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (id, user_id, product_id, product, qty, total, status, payment_type, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id)
                DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    product_id = EXCLUDED.product_id,
                    product = EXCLUDED.product,
                    qty = EXCLUDED.qty,
                    total = EXCLUDED.total,
                    status = EXCLUDED.status,
                    payment_type = EXCLUDED.payment_type,
                    updated_at = EXCLUDED.updated_at;
                """,
                (
                    int(order["id"]),
                    int(order["user_id"]),
                    order.get("product_id"),
                    order.get("product"),
                    int(order.get("qty", 1)),
                    Decimal(str(order.get("total", 0))),
                    str(order.get("status", "")),
                    order.get("payment_type"),
                    order.get("created_at") or datetime.now(),
                    order.get("updated_at") or datetime.now(),
                ),
            )


def upsert_user_profile(profile: dict):
    """Store Telegram profile details for admin User Details page."""
    init_db()
    user_id = int(profile.get("user_id"))
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (user_id, username, first_name, last_name, is_bot, wallet_balance, first_seen_at, last_seen_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id)
                DO UPDATE SET
                    username = COALESCE(EXCLUDED.username, users.username),
                    first_name = COALESCE(EXCLUDED.first_name, users.first_name),
                    last_name = COALESCE(EXCLUDED.last_name, users.last_name),
                    is_bot = EXCLUDED.is_bot,
                    wallet_balance = EXCLUDED.wallet_balance,
                    last_seen_at = EXCLUDED.last_seen_at,
                    updated_at = NOW();
                """,
                (
                    user_id,
                    profile.get("username"),
                    profile.get("first_name"),
                    profile.get("last_name"),
                    bool(profile.get("is_bot", False)),
                    Decimal(str(profile.get("wallet_balance", 0))),
                    profile.get("first_seen_at") or datetime.now(),
                    profile.get("last_seen_at") or datetime.now(),
                ),
            )


def list_user_profiles(limit: int = 1000, offset: int = 0):
    init_db()
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT user_id, username, first_name, last_name, is_bot, wallet_balance, first_seen_at, last_seen_at
                FROM users
                ORDER BY last_seen_at DESC, user_id ASC
                LIMIT %s OFFSET %s;
                """,
                (int(limit), int(offset)),
            )
            return list(cur.fetchall())
