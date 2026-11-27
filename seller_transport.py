"""Bounded supplier reads and clear transport diagnostics; purchases are never retried."""
import time
import requests


def error_kind(exc):
    if isinstance(exc, requests.ConnectTimeout):
        return 'connect_timeout'
    if isinstance(exc, requests.ReadTimeout):
        return 'read_timeout'
    if isinstance(exc, requests.Timeout):
        return 'timeout'
    if isinstance(exc, requests.ConnectionError):
        return 'connection_error'
    return type(exc).__name__


def read_get(url, *, provider_id, params=None, headers=None):
    for attempt in range(2):
        start = time.monotonic()
        try:
            response = requests.get(url, params=params, headers=headers, timeout=(8, 25))
            if response.status_code in (429, 500, 502, 503, 504) and attempt == 0:
                time.sleep(0.5)
                continue
            return response
        except (requests.Timeout, requests.ConnectionError) as exc:
            print(f"Seller read [{provider_id}]: {error_kind(exc)} seconds={time.monotonic()-start:.1f} attempt={attempt+1}")
            if attempt:
                raise
            time.sleep(0.5)
