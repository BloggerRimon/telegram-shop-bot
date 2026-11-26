"""Alchemy webhook receiver logic (framework-agnostic, called from the bot's HTTP server).

Flow: bounded body -> per-network signing key -> HMAC-SHA256(raw body) check ->
network/webhook identity check -> persist raw event (idempotent) -> 200.
Processing happens later in the worker; payload contents are only used to find
transaction ids, which are then re-read from the chain.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Iterable, List, Optional, Tuple

from . import networks as nw
from . import store

MAX_BODY_BYTES = 256 * 1024
PATH_PREFIX = "/alchemy/webhook/"
WEBHOOK_NETWORKS = ("ethereum", "bsc", "solana", "bitcoin")  # networks with an Alchemy webhook product


def verify_signature(raw_body: bytes, signature: str, signing_key: str) -> bool:
    if not signing_key or not signature:
        return False
    expected = hmac.new(signing_key.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    sig = signature.strip().lower()
    if sig.startswith("sha256="):
        sig = sig[7:]
    return hmac.compare_digest(expected, sig)


def network_from_path(path: str) -> Optional[str]:
    if not path.startswith(PATH_PREFIX):
        return None
    key = path[len(PATH_PREFIX):].strip("/")
    return key if key in WEBHOOK_NETWORKS else None


def handle(path: str, headers, raw_body: bytes) -> Tuple[int, dict]:
    network = network_from_path(path)
    if network is None:
        return 404, {"ok": False, "error": "not found"}
    if len(raw_body) > MAX_BODY_BYTES:
        return 413, {"ok": False, "error": "too large"}
    key = nw.env(nw.signing_key_env(network))
    if not key:
        return 503, {"ok": False, "error": "receiver not configured"}
    if not verify_signature(raw_body, headers.get("X-Alchemy-Signature", "") or "", key):
        return 401, {"ok": False, "error": "invalid signature"}
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except Exception:
        return 400, {"ok": False, "error": "invalid json"}
    if not isinstance(payload, dict) or not isinstance(payload.get("event"), dict):
        return 400, {"ok": False, "error": "bad payload"}
    event_network = str(payload["event"].get("network") or "")
    if event_network and event_network not in nw.NETWORKS[network].alchemy_networks:
        return 400, {"ok": False, "error": "wrong network"}
    expected_id = nw.env(nw.webhook_id_env(network))
    webhook_id = str(payload.get("webhookId") or "")
    if expected_id and webhook_id != expected_id:
        return 403, {"ok": False, "error": "unknown webhook"}
    provider_event_id = str(payload.get("id") or hashlib.sha256(raw_body).hexdigest())
    try:
        store.init_schema()
        store.store_raw_event("alchemy", network, webhook_id or None, provider_event_id, payload)
    except Exception as e:
        print(f"[onchain] webhook persist failed: {type(e).__name__}")
        return 500, {"ok": False, "error": "persist failed"}  # Alchemy retries
    return 200, {"ok": True}


def _walk(obj) -> Iterable:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v)


def extract_txids(network: str, payload: dict) -> List[str]:
    """Collect candidate transaction ids from a webhook body (hints only)."""
    ev = payload.get("event") or {}
    found = []
    fam = nw.NETWORKS[network].family
    for d in _walk(ev):
        if fam == "evm":
            h = d.get("hash")
            if isinstance(h, str) and len(h) == 66 and h.startswith("0x"):
                found.append(h.lower())
        elif fam == "solana":
            for k in ("signature", "signatures"):
                v = d.get(k)
                for s in (v if isinstance(v, list) else [v]):
                    if isinstance(s, str) and 64 <= len(s) <= 90:
                        found.append(s)
        elif fam == "utxo":
            for k in ("txid", "hash", "transactionHash"):
                v = d.get(k)
                if isinstance(v, str) and len(v) == 64:
                    found.append(v.lower())
    return list(dict.fromkeys(found))[:200]
