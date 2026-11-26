"""Payment engine: gates, invoice creation, worker pipeline, readiness.

Ingestion and reconciliation always run, independent of whether new customer
invoices are enabled, so already-issued invoices keep tracking after a rollback.
"""

from __future__ import annotations

import threading
import time
import traceback
from datetime import datetime, timezone
from decimal import ROUND_UP, Decimal
from typing import Callable, Dict, Optional, Tuple

import requests

from . import chains, store
from . import networks as nw
from . import webhook as wh

INVOICE_TTL_MINUTES = 60
MAX_HINT_ATTEMPTS = 120
NOT_FOUND_GIVE_UP = 40

# Networks and how they discover transactions.
DISCOVERY = {
    "ethereum": "alchemy webhook + alchemy_getAssetTransfers reconciliation",
    "bsc": "alchemy webhook + alchemy_getAssetTransfers reconciliation (support to be confirmed)",
    "solana": "getSignaturesForAddress poller (+ optional webhook)",
    "bitcoin": "alchemy Bitcoin Address Activity webhook (beta) + admin tx hint",
    "litecoin": "admin tx hint only (no free address index confirmed)",
    "tron": "TronGrid account history poller",
}


class GateClosed(Exception):
    pass


class NotReleased(Exception):
    pass


# ------------------------------------------------------------------ gates
def master_enabled() -> bool:
    return store.get_setting("customer_new_payments_enabled", False) is True


def network_enabled(network: str) -> bool:
    return store.get_setting(f"network_enabled:{network}", False) is True


def network_infra(network: str) -> Dict[str, object]:
    net = nw.NETWORKS[network]
    rpc = bool(nw.alchemy_url(network)) or network == "tron"
    hook = bool(nw.env(nw.signing_key_env(network))) if network in wh.WEBHOOK_NETWORKS else None
    if network in ("ethereum", "bsc", "solana"):
        discovery_ok = rpc
    elif network == "bitcoin":
        discovery_ok = bool(hook)
    elif network == "tron":
        discovery_ok = True
    else:
        discovery_ok = False
    return {"address_valid": nw.validate_address(network, net.address), "rpc": rpc, "webhook_key": hook,
            "discovery_ok": discovery_ok}


def asset_ready(asset_id: str, paid_assets: Optional[set] = None) -> bool:
    a = nw.ASSETS[asset_id]
    infra = network_infra(a.network)
    paid = paid_assets if paid_assets is not None else store.paid_test_assets()
    return bool(infra["address_valid"] and infra["rpc"] and infra["discovery_ok"] and asset_id in paid)


def customer_gate(asset_id: str) -> Tuple[bool, str]:
    """Server-side gate for ANY customer-facing invoice. Hiding buttons is not enough."""
    if asset_id not in nw.ASSETS:
        return False, "unknown_asset"
    if nw.emergency_disabled():
        return False, "emergency_disabled"
    if store.get_setting("new_invoices_paused", False) is True:
        return False, "new_invoices_paused"
    if not master_enabled():
        return False, "master_off"
    if not network_enabled(nw.ASSETS[asset_id].network):
        return False, "network_off"
    if not asset_ready(asset_id):
        return False, "asset_not_ready"
    return True, "ok"


def can_enable_network(network: str) -> Tuple[bool, str]:
    ready = [a.id for a in nw.assets_for_network(network) if asset_ready(a.id)]
    if not ready:
        return False, "no asset on this network has a successful admin test yet"
    return True, "ok"


# ------------------------------------------------------------------ quotes
def fetch_rate_usd(coingecko_id: str) -> Decimal:
    r = requests.get("https://api.coingecko.com/api/v3/simple/price",
                     params={"ids": coingecko_id, "vs_currencies": "usd"}, timeout=15)
    r.raise_for_status()
    v = (r.json().get(coingecko_id) or {}).get("usd")
    if v is None:
        raise ValueError("rate unavailable")
    rate = Decimal(str(v))
    if rate <= 0:
        raise ValueError("bad rate")
    return rate


def quote_units(asset: nw.Asset, usd: Decimal, rate_fn: Callable[[str], Decimal] = fetch_rate_usd):
    if asset.stablecoin:
        return int(usd * (Decimal(10) ** asset.decimals)), Decimal(1), "fixed", "stablecoin face value 1 USDT = 1 USD"
    rate = rate_fn(asset.coingecko_id)
    units = (usd / rate * (Decimal(10) ** asset.decimals)).to_integral_value(rounding=ROUND_UP)
    return int(units), rate, "coingecko simple/price", "USD amount / spot rate at quote time, rounded up"


def create_admin_test_invoice(admin_id: int, asset_id: str, usd: Decimal, is_admin: Callable[[int], bool],
                              rate_fn: Callable[[str], Decimal] = fetch_rate_usd) -> dict:
    if not is_admin(admin_id):
        raise GateClosed("not_admin")
    if nw.emergency_disabled():
        raise GateClosed("emergency_disabled")
    if asset_id not in nw.ASSETS:
        raise GateClosed("unknown_asset")
    asset = nw.ASSETS[asset_id]
    if not nw.validate_address(asset.network, nw.NETWORKS[asset.network].address):
        raise GateClosed("address_invalid")
    usd = Decimal(str(usd)).quantize(Decimal("0.01"))
    if usd <= 0 or usd > Decimal("100"):
        raise GateClosed("test amount must be between 0.01 and 100 USD")
    store.init_schema()
    units, rate, source, rule = quote_units(asset, usd, rate_fn)
    return store.create_invoice_reserved(purpose="admin_test", created_by=admin_id, user_id=admin_id, asset=asset,
                                         base_units=units, usd_amount=usd, rate_usd=rate, rate_source=source,
                                         valuation_rule=rule, ttl_minutes=INVOICE_TTL_MINUTES)


def create_customer_invoice(user_id: int, asset_id: str, usd: Decimal) -> dict:
    ok, reason = customer_gate(asset_id)
    if not ok:
        raise GateClosed(reason)
    # Deliberately not released: the customer checkout must first be designed to
    # comply with Telegram's digital-goods rules (Stars) and reviewed by the owner.
    raise NotReleased("customer self-custody checkout is not released in this version")


def format_units(units, decimals: int, display: int) -> str:
    q = Decimal(int(units)) / (Decimal(10) ** decimals)
    return f"{q:.{display}f}"


# ------------------------------------------------------------------ worker pipeline
class Engine:
    def __init__(self, notify: Optional[Callable[[str], None]] = None, client: Optional[chains.RpcClient] = None,
                 interval: int = 30):
        self.notify = notify or (lambda text: None)
        self.client = client or chains.RpcClient()
        self.interval = interval
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.last_tick: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.poller_errors: Dict[str, str] = {}

    # -- lifecycle
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="onchain-engine", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.is_set():
            try:
                self.tick()
                self.last_error = None
            except Exception as e:
                self.last_error = type(e).__name__
                print(f"[onchain] tick error: {type(e).__name__}")
                traceback.print_exc(limit=2)
            self._stop.wait(self.interval)

    # -- one pass
    def tick(self):
        store.init_schema()
        self.ingest_raw_events()
        self.run_pollers()
        self.process_hints()
        store.expire_invoices()
        self.last_tick = datetime.now(timezone.utc)

    def ingest_raw_events(self):
        store.requeue_stuck_raw_events()
        for ev in store.claim_raw_events():
            try:
                txids = wh.extract_txids(ev["network"], ev["body"])
                for txid in txids:
                    store.add_hint(ev["network"], txid, "webhook")
                store.finish_raw_event(ev["id"], "done" if txids else "no_txids")
            except Exception as e:
                store.finish_raw_event(ev["id"], "error", type(e).__name__)

    def _due(self, name: str, every_seconds: int) -> bool:
        last = store.get_checkpoint(f"poll_last_run:{name}")
        now = time.time()
        if last and now - float(last) < every_seconds:
            return False
        store.set_checkpoint(f"poll_last_run:{name}", now)
        return True

    def run_pollers(self):
        for name, fn, every in (("solana", self.poll_solana, 60), ("tron", self.poll_tron, 60),
                                ("ethereum", lambda: self.poll_evm("ethereum"), 120),
                                ("bsc", lambda: self.poll_evm("bsc"), 120)):
            try:
                if self._due(name, every):
                    fn()
                    self.poller_errors.pop(name, None)
            except Exception as e:
                self.poller_errors[name] = str(e)[:120] or type(e).__name__

    def poll_solana(self):
        if not nw.alchemy_url("solana"):
            return
        cp = store.get_checkpoint("solana_last_sig")
        if not cp:
            latest = chains.discover_solana(self.client, None, limit=1)
            store.set_checkpoint("solana_last_sig", latest[0] if latest else "")
            return  # never backfill history
        sigs = chains.discover_solana(self.client, cp or None, limit=50)
        for s in sigs:
            store.add_hint("solana", s, "poller")
        if sigs:
            store.set_checkpoint("solana_last_sig", sigs[0])  # newest first

    def poll_tron(self):
        started_ms = int(store.monitoring_started_at("tron").timestamp() * 1000)
        cp = int(store.get_checkpoint("tron_min_ts", started_ms) or started_ms)
        rows = chains.discover_tron(self.client, max(cp, started_ms))
        newest = cp
        for r in rows:
            store.add_hint("tron", r["txid"], "poller")
            newest = max(newest, r["ts_ms"])
        store.set_checkpoint("tron_min_ts", newest)

    def poll_evm(self, network: str):
        if not nw.alchemy_url(network):
            return
        head = chains.evm_head(network, self.client)
        cp = store.get_checkpoint(f"evm_block:{network}")
        if cp is None:
            store.set_checkpoint(f"evm_block:{network}", head)  # start at "now": no history
            return
        start = int(cp) + 1
        if start > head:
            return
        end = min(head, start + 1999)
        for h in chains.discover_evm(network, self.client, start, end):
            store.add_hint(network, h, "reconcile")
        store.set_checkpoint(f"evm_block:{network}", end)

    def process_hints(self):
        for h in store.due_hints():
            self.process_hint(h["network"], h["txid"], int(h["attempts"]))

    def process_hint(self, network: str, txid: str, attempts: int):
        res = chains.inspect_tx(network, txid, self.client)
        st = res["status"]
        if st == "error":
            if attempts >= MAX_HINT_ATTEMPTS:
                store.update_hint(network, txid, "gave_up", res.get("error"))
            else:
                store.update_hint(network, txid, "retry", res.get("error"), min(600, 30 * (attempts + 1)))
            return
        if st == "not_found":
            self._drop_known(network, txid, "not_found_after_detection")
            if attempts >= NOT_FOUND_GIVE_UP:
                store.update_hint(network, txid, "not_found", res.get("error"))
            else:
                store.update_hint(network, txid, "retry", "not_found", 60)
            return
        if st == "failed":
            self._drop_known(network, txid, res.get("error") or "failed")
            store.update_hint(network, txid, "failed", res.get("error"))
            return
        # found
        uids = []
        for tr in res["transfers"]:
            uid = f"{network}:{txid}:{tr['locator']}"
            store.upsert_transfer({
                "uid": uid, "network": network, "asset_id": tr["asset_id"], "txid": txid, "locator": tr["locator"],
                "to_address": tr["to_address"], "from_address": tr.get("from_address"),
                "amount_units": int(tr["amount_units"]), "block_number": res.get("block_number"),
                "block_time": res.get("block_time"), "confirmations": int(res.get("confirmations") or 0),
                "source": "chain"})
            uids.append(uid)
            m = store.match_transfer(uid)
            if m["result"] == "review":
                self.notify(f"🧾 On-chain review needed: {network} {tr['asset_id']} — {m.get('reason')}")
            if res["final"]:
                c = store.credit_final_transfer(uid, int(res.get("confirmations") or 0))
                if c["result"] == "credited":
                    inv = store.get_invoice(c["invoice_id"])
                    self.notify(f"✅ Admin test payment confirmed: {inv['public_ref']} ({inv['asset_id']}) — "
                                f"test ledger only, no customer balance changed.")
            else:
                cur_t = store.get_transfer(uid)
                if cur_t and cur_t["status"] in ("detected", "confirming"):
                    store.set_transfer_status(uid, "confirming" if cur_t["invoice_id"] else cur_t["status"],
                                              None, int(res.get("confirmations") or 0))
        if res["final"]:
            store.update_hint(network, txid, "done" if uids else "no_incoming", res.get("error"))
        else:
            store.update_hint(network, txid, "retry", res.get("error"), 45)

    def _drop_known(self, network: str, txid: str, reason: str):
        with store.conn() as c, c.cursor() as cur:
            cur.execute("SELECT uid FROM onchain_transfers WHERE network=%s AND txid=%s "
                        "AND status IN ('detected','confirming')", (network, txid))
            uids = [r["uid"] for r in cur.fetchall()]
        for uid in uids:
            store.unmatch_dropped_transfer(uid, reason)

    # -- admin helpers
    def add_admin_hint(self, admin_id: int, network: str, txid: str, is_admin: Callable[[int], bool]) -> bool:
        if not is_admin(admin_id) or network not in nw.NETWORKS:
            return False
        txid = txid.strip()
        if not (8 <= len(txid) <= 100) or not txid.replace("0x", "").isalnum():
            return False
        store.init_schema()
        store.audit(admin_id, "admin_tx_hint", {"network": network, "txid": txid})
        return store.add_hint(network, txid if network == "solana" else txid.lower(),
                              "admin_hint")
