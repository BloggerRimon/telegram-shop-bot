import base64
import hashlib
import hmac
import json
from typing import Any, Dict, Optional

import requests


API_BASE = "https://api.cryptomus.com"


def _compact_json(data: Dict[str, Any]) -> str:
    return json.dumps(data or {}, ensure_ascii=False, separators=(",", ":"))


def make_sign(data: Dict[str, Any], api_key: str) -> str:
    body = _compact_json(data)
    encoded = base64.b64encode(body.encode("utf-8")).decode("utf-8")
    return hashlib.md5((encoded + api_key).encode("utf-8")).hexdigest()


def headers(data: Dict[str, Any], merchant_id: str, api_key: str) -> Dict[str, str]:
    return {
        "merchant": merchant_id,
        "sign": make_sign(data, api_key),
        "Content-Type": "application/json",
    }


def post(path: str, data: Dict[str, Any], merchant_id: str, api_key: str, timeout: int = 30) -> Dict[str, Any]:
    url = API_BASE.rstrip("/") + path
    body = _compact_json(data)
    res = requests.post(url, data=body.encode("utf-8"), headers=headers(data, merchant_id, api_key), timeout=timeout)
    try:
        payload = res.json()
    except Exception:
        payload = {"raw": res.text[:1000]}
    if not res.ok:
        raise RuntimeError(f"Cryptomus API HTTP {res.status_code}: {payload}")
    if isinstance(payload, dict) and payload.get("state") not in (0, "0", None):
        raise RuntimeError(f"Cryptomus API error: {payload}")
    return payload


def create_invoice(data: Dict[str, Any], merchant_id: str, api_key: str) -> Dict[str, Any]:
    return post("/v1/payment", data, merchant_id, api_key)


def payment_info(uuid: Optional[str], order_id: Optional[str], merchant_id: str, api_key: str) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    if uuid:
        data["uuid"] = uuid
    elif order_id:
        data["order_id"] = order_id
    else:
        raise ValueError("uuid or order_id required")
    return post("/v1/payment/info", data, merchant_id, api_key)


def verify_webhook_signature(payload: Dict[str, Any], api_key: str) -> bool:
    if not isinstance(payload, dict) or not api_key:
        return False
    given = str(payload.get("sign") or "")
    if not given:
        return False
    data = dict(payload)
    data.pop("sign", None)

    # Cryptomus signs JSON body base64 + Payment API key.
    # Try normal compact JSON and slash-escaped JSON because some Cryptomus examples mention escaped slashes.
    candidates = []
    normal = _compact_json(data)
    candidates.append(normal)
    escaped_slash = normal.replace("/", "\\/")
    if escaped_slash != normal:
        candidates.append(escaped_slash)

    for body in candidates:
        encoded = base64.b64encode(body.encode("utf-8")).decode("utf-8")
        expected = hashlib.md5((encoded + api_key).encode("utf-8")).hexdigest()
        if hmac.compare_digest(expected, given):
            return True
    return False
