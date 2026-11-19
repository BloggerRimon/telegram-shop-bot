"""Jero Accounts reseller API adapter (provider type ``jero_reseller``).

The adapter is deliberately free of bot globals so it can be tested in
isolation.  It owns transport concerns only:

* one complete endpoint URL (``?action=`` is the only thing added to it),
* ``Authorization: Bearer`` authentication (never query/body),
* TLS verification on and redirects refused (so the header never follows a
  redirect to another host),
* a provider-scoped rate limiter shared by refresh, balance, polling and
  ordering (120 requests/minute, 30 orders/minute),
* bounded exponential backoff with jitter that honours ``Retry-After``,
* the documented error envelope, HTTP 200 ``success:false``, malformed JSON,
  non-JSON Cloudflare pages and upstream 5xx/52x statuses,
* secret/credential redaction for anything that may reach logs or admins,
* normalisation of products, balance and order responses.

Order creation is never retried here with a *new* identity.  The caller keeps
one durable ``external_order_id`` per logical order and recovers through
``lookup_order`` (or a resend of the identical payload) on transient failures.
"""

from __future__ import annotations

import random
import re
import threading
import time
from collections import deque
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from urllib.parse import urlsplit, urlunsplit, urlencode, parse_qsl

import requests

PROVIDER_TYPE = "jero_reseller"

# Special Jero products that need customer input. They are only special for
# the Jero provider; another seller's product 900001 is an ordinary product.
WORDPRESS_PRODUCT_IDS = {"900001": "Elementor Pro", "900003": "Rank Math PRO"}
UI8_PRODUCT_ID = "900002"
SPECIAL_PRODUCT_IDS = set(WORDPRESS_PRODUCT_IDS) | {UI8_PRODUCT_ID}

MAX_ITEMS_PER_ORDER = 100
REQUESTS_PER_MINUTE = 120
ORDERS_PER_MINUTE = 30

DOCUMENTED_ERROR_CODES = {
    "unauthorized",
    "invalid_request",
    "product_not_found",
    "out_of_stock",
    "insufficient_balance",
    "rate_limited",
    "api_paused",
}
# Errors after which the supplier did not create an order. Safe to settle the
# customer side, because the request was rejected as a whole.
ORDER_REJECTED_CODES = {
    "invalid_request",
    "product_not_found",
    "out_of_stock",
    "insufficient_balance",
    "unauthorized",
    "api_paused",
    "rate_limited",
}
# Errors that must stop automatic retries (retry storm protection).
NON_RETRYABLE_CODES = {
    "unauthorized",
    "invalid_request",
    "product_not_found",
    "out_of_stock",
    "insufficient_balance",
    "config",
    "redirect_blocked",
}
AUTH_CODES = {"unauthorized", "config", "redirect_blocked"}

ORDER_STATUS_COMPLETED = "completed"
ORDER_STATUS_PROCESSING = "processing"
ORDER_STATUS_REFUNDED = "refunded"
ORDER_STATUS_PARTIAL = "partial"
ORDER_STATUS_UNKNOWN = "unknown"

_COMPLETED_WORDS = {"completed", "complete", "success", "succeeded", "delivered", "done", "fulfilled"}
_PROCESSING_WORDS = {"processing", "pending", "queued", "in_progress", "inprogress", "running", "waiting"}
_REFUNDED_WORDS = {"refunded", "refund", "cancelled", "canceled", "failed", "failure", "rejected"}
_PARTIAL_WORDS = {"partial", "partially_completed", "partially_refunded", "partial_refund"}

# Documented/obvious delivery fields for object items. Raw dictionaries are
# never stringified for customers; an unrecognised object item is reported.
_ITEM_DELIVERY_FIELDS = (
    "account", "credentials", "credential", "content", "data", "value",
    "details", "delivery", "item", "text", "code", "key", "license",
    "download_url", "download_link", "download", "link", "url",
)
_ITEM_STATUS_FIELDS = ("status", "state", "result")
_ITEM_MESSAGE_FIELDS = ("message", "reason", "error", "note")


class JeroAPIError(Exception):
    """Transport or documented API error. ``str()`` is always secret-free."""

    def __init__(
        self,
        message: str,
        code: str = "unknown",
        retryable: bool = False,
        http_status: int = None,
        retry_after: float = None,
        order_may_exist: bool = False,
    ):
        super().__init__(message)
        self.code = str(code or "unknown")
        self.retryable = bool(retryable)
        self.http_status = http_status
        self.retry_after = retry_after
        # True when the request may have reached the supplier (timeouts,
        # dropped connections, 5xx). The caller must reconcile, not refund.
        self.order_may_exist = bool(order_may_exist)

    @property
    def rejected_order(self) -> bool:
        return self.code in ORDER_REJECTED_CODES and not self.order_may_exist


# ---------------------------------------------------------------------------
# Money helpers
# ---------------------------------------------------------------------------
def to_decimal(value):
    """Parse a supplier money value. Returns None for missing/invalid values."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, Decimal):
        parsed = value
    else:
        text = str(value).strip().replace(",", "").replace("$", "")
        for suffix in ("USDT", "USD"):
            if text.upper().endswith(suffix):
                text = text[: -len(suffix)].strip()
        if not text:
            return None
        try:
            parsed = Decimal(text)
        except (InvalidOperation, ValueError):
            return None
    if not parsed.is_finite():
        return None
    return parsed


def money(value) -> Decimal:
    parsed = to_decimal(value)
    if parsed is None:
        raise ValueError("invalid money value")
    return parsed.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _to_int(value):
    if isinstance(value, bool) or value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite() or parsed != parsed.to_integral_value():
        return None
    return int(parsed)


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------
_SECRETISH_PATTERNS = (
    re.compile(r"(?i)bearer\s+[A-Za-z0-9._~+/=-]+"),
    re.compile(r"\bjak_[A-Za-z0-9]+"),
    re.compile(r"(?i)(password|passwd|pwd|secret|token|api[_-]?key)\s*[\"']?\s*[:=]\s*[\"']?[^\s\"',}]+"),
    re.compile(r"\b[^\s@|:]+@[^\s@|:]+\.[^\s@|:]+\b"),
    re.compile(r"\b[^\s|]+\|[^\s|]+"),
    re.compile(r"\b[A-Za-z0-9_-]{32,}\b"),
)


def redact(text, secrets=()) -> str:
    """Remove API keys, passwords and account-looking data from free text."""
    text = " ".join(str(text or "").split())
    for secret in secrets or ():
        secret = str(secret or "")
        if len(secret) >= 4:
            text = re.sub(re.escape(secret), "[redacted]", text, flags=re.IGNORECASE)
    for pattern in _SECRETISH_PATTERNS:
        text = pattern.sub("[redacted]", text)
    return text[:300]


def describe_shape(value, depth: int = 0, max_depth: int = 3) -> object:
    """Return a value-free description (keys and types only) for diagnostics."""
    if isinstance(value, dict):
        if depth >= max_depth:
            return "object"
        return {str(key)[:40]: describe_shape(item, depth + 1, max_depth) for key, item in list(value.items())[:25]}
    if isinstance(value, list):
        if not value:
            return "list[0]"
        return {"list": len(value), "first": describe_shape(value[0], depth + 1, max_depth)}
    if value is None:
        return "null"
    return type(value).__name__


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------
class RateLimiter:
    """Sliding-window limiter shared by every call for one provider."""

    def __init__(
        self,
        requests_per_minute: int = REQUESTS_PER_MINUTE,
        orders_per_minute: int = ORDERS_PER_MINUTE,
        clock=time.monotonic,
        sleep=time.sleep,
    ):
        self.limits = {"request": int(requests_per_minute), "order": int(orders_per_minute)}
        self.windows = {"request": deque(), "order": deque()}
        self.blocked_until = 0.0
        self.clock = clock
        self.sleep = sleep
        self.lock = threading.Lock()

    def _wait_needed(self, kinds, now: float) -> float:
        wait = max(0.0, self.blocked_until - now)
        for kind in kinds:
            window = self.windows[kind]
            while window and now - window[0] >= 60.0:
                window.popleft()
            if len(window) >= self.limits[kind]:
                wait = max(wait, 60.0 - (now - window[0]))
        return wait

    def acquire(self, is_order: bool = False, max_wait: float = 30.0) -> None:
        kinds = ("request", "order") if is_order else ("request",)
        deadline = self.clock() + max(0.0, float(max_wait))
        while True:
            with self.lock:
                now = self.clock()
                wait = self._wait_needed(kinds, now)
                if wait <= 0:
                    for kind in kinds:
                        self.windows[kind].append(now)
                    return
            if now + wait > deadline:
                raise JeroAPIError(
                    "Local Jero rate limit reached; request was not sent.",
                    "rate_limited",
                    retryable=True,
                    retry_after=wait,
                )
            self.sleep(min(wait, 5.0))

    def pause(self, seconds: float) -> None:
        """Respect a supplier Retry-After for every caller of this provider."""
        with self.lock:
            self.blocked_until = max(self.blocked_until, self.clock() + max(0.0, float(seconds)))


_LIMITERS = {}
_LIMITERS_LOCK = threading.Lock()


def limiter_for(provider_id: str) -> RateLimiter:
    with _LIMITERS_LOCK:
        limiter = _LIMITERS.get(provider_id)
        if limiter is None:
            limiter = RateLimiter()
            _LIMITERS[provider_id] = limiter
        return limiter


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
def _parse_retry_after(value):
    if value in (None, ""):
        return None
    try:
        seconds = float(str(value).strip())
    except ValueError:
        return None
    if seconds < 0:
        return None
    return min(seconds, 300.0)


def build_action_url(endpoint: str, action: str, params: dict = None) -> str:
    """Add ``action`` (and lookup params) to the complete endpoint URL."""
    parts = urlsplit(str(endpoint or "").strip())
    query = [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key != "action"]
    query.append(("action", action))
    for key, value in (params or {}).items():
        query.append((str(key), str(value)))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))


def validate_endpoint(endpoint: str) -> str:
    parts = urlsplit(str(endpoint or "").strip())
    if parts.scheme != "https" or not parts.netloc:
        raise JeroAPIError("Jero endpoint must be a complete https URL.", "config")
    if parts.username or parts.password:
        raise JeroAPIError("Jero endpoint must not contain credentials.", "config")
    return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))


class JeroClient:
    def __init__(
        self,
        endpoint: str,
        api_key: str,
        provider_id: str = "seller_3",
        session=None,
        limiter: RateLimiter = None,
        timeout: float = 20.0,
        order_timeout: float = 45.0,
        sleep=time.sleep,
        jitter=random.random,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 20.0,
    ):
        self.endpoint = validate_endpoint(endpoint)
        self.api_key = str(api_key or "").strip()
        if not self.api_key:
            raise JeroAPIError("Jero API key is not configured.", "config")
        self.provider_id = provider_id
        self.session = session or requests
        self.limiter = limiter or limiter_for(provider_id)
        self.timeout = timeout
        self.order_timeout = order_timeout
        self.sleep = sleep
        self.jitter = jitter
        self.max_attempts = max(1, int(max_attempts))
        self.base_delay = base_delay
        self.max_delay = max_delay

    # -- low level -----------------------------------------------------------
    def _headers(self, json_body: bool) -> dict:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }
        if json_body:
            headers["Content-Type"] = "application/json"
        return headers

    def safe(self, text) -> str:
        return redact(text, (self.api_key,))

    def _request_once(self, method: str, action: str, params: dict = None, json_body: dict = None):
        is_order = method == "POST" and action == "order"
        self.limiter.acquire(is_order=is_order, max_wait=60.0 if is_order else 30.0)
        url = build_action_url(self.endpoint, action, params)
        try:
            response = self.session.request(
                method,
                url,
                json=json_body,
                headers=self._headers(json_body is not None),
                timeout=self.order_timeout if is_order else self.timeout,
                allow_redirects=False,
                verify=True,
            )
        except requests.Timeout as exc:
            raise JeroAPIError(
                "Jero request timed out.", "timeout", retryable=True, order_may_exist=True,
            ) from exc
        except requests.ConnectionError as exc:
            raise JeroAPIError(
                "Could not connect to Jero.", "network", retryable=True, order_may_exist=True,
            ) from exc
        except requests.RequestException as exc:
            raise JeroAPIError(
                "Jero request failed.", "network", retryable=True, order_may_exist=True,
            ) from exc
        return self._parse_response(response, is_order)

    def _parse_response(self, response, is_order: bool):
        status = int(getattr(response, "status_code", 0) or 0)
        headers = getattr(response, "headers", {}) or {}
        retry_after = _parse_retry_after(headers.get("Retry-After") if hasattr(headers, "get") else None)
        if 300 <= status < 400:
            raise JeroAPIError(
                f"Jero answered with a redirect (HTTP {status}); it was not followed.",
                "redirect_blocked",
                http_status=status,
            )
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if not isinstance(payload, (dict, list)):
            cloudflare = 520 <= status <= 530
            if status >= 500 or cloudflare or status == 0:
                raise JeroAPIError(
                    f"Jero is unavailable (HTTP {status}, non-JSON response).",
                    "upstream_unavailable",
                    retryable=True,
                    http_status=status,
                    retry_after=retry_after,
                    order_may_exist=is_order,
                )
            if status == 429:
                raise JeroAPIError(
                    "Jero rate limit reached.", "rate_limited", retryable=True,
                    http_status=status, retry_after=retry_after,
                )
            if status in (401, 403):
                raise JeroAPIError(
                    f"Jero rejected the API key (HTTP {status}).", "unauthorized", http_status=status,
                )
            raise JeroAPIError(
                f"Jero returned a non-JSON response (HTTP {status}).",
                "malformed_response",
                retryable=status >= 500,
                http_status=status,
                order_may_exist=is_order and (status >= 500 or 200 <= status < 300),
            )
        error = extract_error(payload)
        if error is not None or status >= 400:
            code, message = error or ("", "")
            code = code if code in DOCUMENTED_ERROR_CODES else ""
            if not code:
                if status == 429:
                    code = "rate_limited"
                elif status in (401, 403):
                    code = "unauthorized"
                elif status >= 500:
                    code = "upstream_unavailable"
                else:
                    code = "invalid_request" if status >= 400 else "api_error"
            retryable = code in {"rate_limited", "api_paused", "upstream_unavailable"}
            may_exist = is_order and code in {"upstream_unavailable", "api_error"}
            text = f"Jero error: {code}"
            if message:
                text += f" — {self.safe(message)}"
            raise JeroAPIError(
                text, code, retryable=retryable, http_status=status,
                retry_after=retry_after, order_may_exist=may_exist,
            )
        return payload

    def _with_retry(self, method: str, action: str, params: dict = None, json_body: dict = None):
        attempt = 0
        while True:
            attempt += 1
            try:
                return self._request_once(method, action, params, json_body)
            except JeroAPIError as exc:
                if exc.retry_after and exc.code == "rate_limited" and exc.http_status:
                    self.limiter.pause(exc.retry_after)
                if not exc.retryable or exc.code in NON_RETRYABLE_CODES or attempt >= self.max_attempts:
                    raise
                delay = min(self.max_delay, self.base_delay * (2 ** (attempt - 1)))
                delay = delay * (0.5 + self.jitter())
                if exc.retry_after:
                    delay = max(delay, float(exc.retry_after))
                self.sleep(min(delay, self.max_delay if not exc.retry_after else max(self.max_delay, exc.retry_after)))

    # -- public operations ---------------------------------------------------
    def products_raw(self):
        return self._with_retry("GET", "products")

    def balance_raw(self):
        return self._with_retry("GET", "balance")

    def products(self) -> list:
        return normalize_products(self.products_raw())

    def balance(self) -> dict:
        return normalize_balance(self.balance_raw())

    def create_order(self, payload: dict) -> dict:
        """Send one order attempt. Never retried here with a different ID."""
        validate_order_payload(payload)
        raw = self._request_once("POST", "order", json_body=dict(payload))
        return normalize_order(raw, secrets=_payload_secrets(payload) + [self.api_key])

    def resend_order(self, payload: dict) -> dict:
        """Recovery resend of the *identical* payload (same external_order_id)."""
        return self.create_order(payload)

    def lookup_order(self, external_order_id: str) -> dict:
        external_order_id = str(external_order_id or "").strip()
        if not external_order_id:
            raise JeroAPIError("external_order_id is required for lookup.", "invalid_request")
        raw = self._with_retry("GET", "order", params={"external_order_id": external_order_id})
        return normalize_order(raw, secrets=[self.api_key])


def _payload_secrets(payload: dict) -> list:
    return [str(payload.get(key)) for key in ("password",) if payload.get(key)]


def validate_order_payload(payload: dict) -> None:
    if not isinstance(payload, dict):
        raise JeroAPIError("Order payload must be an object.", "invalid_request")
    product_id = _to_int(payload.get("product_id"))
    quantity = _to_int(payload.get("quantity"))
    external_order_id = str(payload.get("external_order_id") or "").strip()
    if product_id is None or product_id <= 0:
        raise JeroAPIError("Order payload has an invalid product_id.", "invalid_request")
    if quantity is None or quantity <= 0 or quantity > MAX_ITEMS_PER_ORDER:
        raise JeroAPIError("Order quantity must be 1-100.", "invalid_request")
    if not external_order_id or len(external_order_id) > 64:
        raise JeroAPIError("Order payload needs a stable external_order_id.", "invalid_request")
    pid = str(product_id)
    if pid in WORDPRESS_PRODUCT_IDS:
        if quantity != 1 or not all(str(payload.get(key) or "").strip() for key in ("site", "username", "password")):
            raise JeroAPIError("WordPress install needs quantity 1, site, username and password.", "invalid_request")
    if pid == UI8_PRODUCT_ID:
        links = payload.get("links")
        if not isinstance(links, list) or len(links) != quantity:
            raise JeroAPIError("UI8 order needs one link per item.", "invalid_request")


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------
def extract_error(payload):
    """Return (code, message) for the documented error envelope, else None."""
    if not isinstance(payload, dict):
        return None
    if payload.get("success") is not False and not isinstance(payload.get("error"), dict):
        return None
    error = payload.get("error")
    if isinstance(error, dict):
        code = str(error.get("code") or "").strip().lower()
        message = str(error.get("message") or "").strip()
        return code, message
    if isinstance(error, str):
        return error.strip().lower(), ""
    message = payload.get("message")
    return "", str(message or "").strip()


def _unwrap(payload):
    """Follow a single ``data``/``result`` wrapper if present."""
    if isinstance(payload, dict):
        for key in ("data", "result"):
            inner = payload.get(key)
            if isinstance(inner, (dict, list)):
                return inner
    return payload


def _product_list(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return None
    for candidate in (payload, _unwrap(payload)):
        if isinstance(candidate, list):
            return candidate
        if isinstance(candidate, dict):
            for key in ("products", "items", "list"):
                if isinstance(candidate.get(key), list):
                    return candidate[key]
    return None


def _sanitize_tiers(tiers):
    """Keep tiers as metadata only (no discount formula is derived here)."""
    if not isinstance(tiers, list):
        return [], tiers is not None
    clean = []
    unclear = False
    for tier in tiers[:20]:
        if not isinstance(tier, dict):
            unclear = True
            continue
        entry = {}
        for key, value in tier.items():
            key = str(key)[:32]
            if isinstance(value, (int, float, str)) and not isinstance(value, bool):
                entry[key] = value if not isinstance(value, str) else value[:40]
        min_qty = _to_int(tier.get("min_quantity", tier.get("min_qty", tier.get("quantity", tier.get("min")))))
        price = to_decimal(tier.get("price", tier.get("unit_price")))
        if min_qty is None or price is None:
            unclear = True
        clean.append(entry)
    return clean, unclear


def normalize_product(product: dict) -> dict:
    if not isinstance(product, dict):
        return {}
    product_id = product.get("id")
    product_id = str(product_id).strip() if product_id not in (None, "") else ""
    title = str(product.get("title") or "").strip()
    price = to_decimal(product.get("price"))
    price_valid = price is not None and price > 0
    stock = _to_int(product.get("stock"))
    stock_known = stock is not None and stock >= 0
    tiers, tiers_unclear = _sanitize_tiers(product.get("tiers"))
    special = product_id in SPECIAL_PRODUCT_IDS
    return {
        "id": product_id,
        "name": title or (f"Jero product {product_id}" if product_id else "N/A"),
        "title": title,
        # Floats keep compatibility with the existing mapping fields; every
        # accounting calculation re-reads these through Decimal.
        "price": float(price) if price_valid else None,
        "price_text": str(price) if price_valid else None,
        "stock": max(0, stock) if stock_known else None,
        "tiers": tiers,
        "_jero_price_valid": price_valid,
        "_jero_stock_known": stock_known,
        "_jero_tiers_unclear": tiers_unclear,
        "_jero_special": special,
        "_jero_special_kind": (
            "wordpress" if product_id in WORDPRESS_PRODUCT_IDS
            else "ui8" if product_id == UI8_PRODUCT_ID else ""
        ),
        "_source_field_names": [str(key)[:64] for key in list(product.keys())[:30]],
    }


def normalize_products(payload) -> list:
    products = _product_list(payload)
    if products is None:
        raise JeroAPIError(
            "Jero product list not found. Response shape: " + str(describe_shape(payload))[:400],
            "products_unrecognized",
        )
    normalized = [normalize_product(item) for item in products if isinstance(item, dict)]
    normalized = [item for item in normalized if item.get("id")]
    if products and not normalized:
        first = products[0]
        raise JeroAPIError(
            "Jero product fields not recognized. First item shape: " + str(describe_shape(first))[:400],
            "product_fields_unrecognized",
        )
    return normalized


def normalize_balance(payload) -> dict:
    source = _unwrap(payload)
    value = None
    for candidate in (source, payload):
        if isinstance(candidate, dict):
            for key in ("balance", "available_balance", "wallet_balance", "amount"):
                if key in candidate:
                    value = candidate.get(key)
                    break
            if isinstance(value, dict):
                value = value.get("amount", value.get("balance", value.get("value")))
        elif isinstance(candidate, (int, float, str)) and not isinstance(candidate, bool):
            value = candidate
        if value is not None:
            break
    balance = to_decimal(value)
    if balance is None:
        raise JeroAPIError(
            "Jero balance field not recognized. Response shape: " + str(describe_shape(payload))[:400],
            "balance_unrecognized",
        )
    currency = "USD"
    if isinstance(source, dict) and isinstance(source.get("currency"), str) and source["currency"].strip():
        currency = source["currency"].strip()[:8]
    return {
        "balance": float(balance),
        "balance_decimal": str(balance),
        "balanceText": f"{balance} {currency}",
        "walletCurrency": currency,
    }


def _status_word(value) -> str:
    text = str(value or "").strip().lower().replace(" ", "_").replace("-", "_")
    if text in _PARTIAL_WORDS:
        return ORDER_STATUS_PARTIAL
    if text in _COMPLETED_WORDS:
        return ORDER_STATUS_COMPLETED
    if text in _PROCESSING_WORDS:
        return ORDER_STATUS_PROCESSING
    if text in _REFUNDED_WORDS:
        return ORDER_STATUS_REFUNDED
    return ""


def normalize_item(item, secrets=()) -> dict:
    """Return {status, value, message, recognized} for one order item."""
    if isinstance(item, (str, int, float)) and not isinstance(item, bool):
        value = str(item).strip()
        return {"status": "", "value": value, "message": "", "recognized": bool(value)}
    if not isinstance(item, dict):
        return {"status": "", "value": "", "message": "", "recognized": False}
    status = ""
    for key in _ITEM_STATUS_FIELDS:
        if item.get(key) not in (None, ""):
            status = _status_word(item.get(key))
            break
    value = ""
    for key in _ITEM_DELIVERY_FIELDS:
        candidate = item.get(key)
        if isinstance(candidate, (str, int, float)) and not isinstance(candidate, bool) and str(candidate).strip():
            value = str(candidate).strip()
            break
        if isinstance(candidate, list) and candidate and all(
            isinstance(entry, (str, int, float)) and not isinstance(entry, bool) for entry in candidate
        ):
            value = "\n".join(str(entry).strip() for entry in candidate if str(entry).strip())
            break
    message = ""
    for key in _ITEM_MESSAGE_FIELDS:
        candidate = item.get(key)
        if isinstance(candidate, str) and candidate.strip():
            message = redact(candidate, secrets)[:200]
            break
    source_link = ""
    for key in ("source", "source_link", "input", "requested_link"):
        candidate = item.get(key)
        if isinstance(candidate, str) and candidate.strip():
            source_link = candidate.strip()[:300]
            break
    recognized = bool(value) or status in {ORDER_STATUS_REFUNDED, ORDER_STATUS_PROCESSING}
    return {
        "status": status,
        "value": value,
        "message": message,
        "source": source_link,
        "recognized": recognized,
    }


def normalize_order(payload, secrets=()) -> dict:
    """Normalise create/lookup responses into one secret-free structure."""
    error = extract_error(payload)
    if error is not None:
        code, message = error
        raise JeroAPIError(
            f"Jero error: {code or 'api_error'}" + (f" — {redact(message, secrets)}" if message else ""),
            code if code in DOCUMENTED_ERROR_CODES else "api_error",
        )
    source = _unwrap(payload)
    if isinstance(source, dict) and isinstance(source.get("order"), dict):
        source = source["order"]
    if not isinstance(source, dict):
        raise JeroAPIError(
            "Jero order response not recognized. Shape: " + str(describe_shape(payload))[:300],
            "order_unrecognized",
            order_may_exist=True,
        )
    order_id = source.get("order_id", source.get("id"))
    order_id = str(order_id).strip() if order_id not in (None, "") else ""
    external_order_id = str(source.get("external_order_id") or "").strip()
    raw_items = source.get("items")
    if raw_items is None and isinstance(payload, dict):
        raw_items = payload.get("items")
    items = [normalize_item(item, secrets) for item in raw_items] if isinstance(raw_items, list) else []
    explicit_status = ""
    for key in ("status", "state", "order_status"):
        if source.get(key) not in (None, ""):
            explicit_status = _status_word(source.get(key))
            if not explicit_status:
                explicit_status = ORDER_STATUS_UNKNOWN
            break
    total = to_decimal(source.get("total"))
    balance = to_decimal(source.get("balance"))
    refund_amount = to_decimal(source.get("refund", source.get("refunded", source.get("refund_amount"))))
    message = ""
    for key in ("message", "result", "note", "reason", "error_message"):
        candidate = source.get(key)
        if isinstance(candidate, str) and candidate.strip():
            message = redact(candidate, secrets)[:300]
            break

    item_statuses = {item["status"] for item in items if item["status"]}
    unrecognized_items = [item for item in items if not item["recognized"]]
    delivered = [item for item in items if item["value"] and item["status"] in ("", ORDER_STATUS_COMPLETED)]
    refunded_items = [item for item in items if item["status"] == ORDER_STATUS_REFUNDED]
    pending_items = [item for item in items if item["status"] == ORDER_STATUS_PROCESSING]

    if explicit_status and explicit_status != ORDER_STATUS_UNKNOWN:
        status = explicit_status
        if status == ORDER_STATUS_COMPLETED and refunded_items and delivered:
            status = ORDER_STATUS_PARTIAL
    elif pending_items:
        status = ORDER_STATUS_PROCESSING
    elif delivered and refunded_items:
        status = ORDER_STATUS_PARTIAL
    elif items and len(refunded_items) == len(items):
        status = ORDER_STATUS_REFUNDED
    elif delivered and not unrecognized_items and len(delivered) == len(items):
        status = ORDER_STATUS_COMPLETED
    else:
        status = ORDER_STATUS_UNKNOWN
    if status == ORDER_STATUS_COMPLETED and (unrecognized_items or (items and not delivered)):
        # Completed without a usable delivery is never shown as completed.
        status = ORDER_STATUS_UNKNOWN

    return {
        "order_id": order_id,
        "external_order_id": external_order_id,
        "status": status,
        "supplier_status_word": explicit_status,
        "items": items,
        "delivered": [item["value"] for item in delivered],
        "delivered_count": len(delivered),
        "refunded_count": len(refunded_items),
        "pending_count": len(pending_items),
        "unrecognized_count": len(unrecognized_items),
        "item_count": len(items),
        "item_statuses": sorted(item_statuses),
        "total": str(total) if total is not None else None,
        "balance": str(balance) if balance is not None else None,
        "refund_amount": str(refund_amount) if refund_amount is not None else None,
        "message": message,
    }


# ---------------------------------------------------------------------------
# Customer input validation for special products
# ---------------------------------------------------------------------------
_UI8_HOSTS = {"ui8.net", "www.ui8.net"}


def validate_ui8_links(text: str, max_links: int = MAX_ITEMS_PER_ORDER) -> dict:
    """Split customer text into accepted/invalid/duplicate UI8 product links.

    Purely syntactic: nothing is fetched from the server.
    """
    accepted, invalid, duplicates = [], [], []
    seen = set()
    for raw in re.split(r"[\s,]+", str(text or "")):
        candidate = raw.strip().strip("<>")
        if not candidate:
            continue
        parts = urlsplit(candidate)
        host = (parts.hostname or "").lower()
        path = parts.path or ""
        valid = (
            parts.scheme == "https"
            and host in _UI8_HOSTS
            and not parts.username
            and not parts.password
            and "/products/" in path
            and len(candidate) <= 300
        )
        if not valid:
            invalid.append(candidate[:120])
            continue
        normalized = urlunsplit(("https", "ui8.net", path.rstrip("/"), "", ""))
        if normalized in seen:
            duplicates.append(candidate[:120])
            continue
        seen.add(normalized)
        accepted.append(normalized)
    over_limit = accepted[max_links:]
    return {
        "accepted": accepted[:max_links],
        "invalid": invalid,
        "duplicates": duplicates,
        "over_limit": over_limit,
    }


def validate_wordpress_site(value: str) -> str:
    """Return a normalised https/http site URL or '' if unusable."""
    text = str(value or "").strip()
    if not text or len(text) > 200 or any(ch.isspace() for ch in text):
        return ""
    if "://" not in text:
        text = "https://" + text
    parts = urlsplit(text)
    host = (parts.hostname or "").lower()
    if parts.scheme not in {"https", "http"} or not host or "." not in host:
        return ""
    if parts.username or parts.password:
        return ""
    if host in {"localhost"} or host.endswith(".local") or re.fullmatch(r"[\d.]+", host):
        return ""
    return urlunsplit((parts.scheme, parts.netloc.lower(), parts.path.rstrip("/"), "", ""))
