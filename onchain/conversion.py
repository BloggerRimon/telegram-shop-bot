"""Admin-only read-only USDT conversion preparation. No signer or swap endpoint.

SwapKit v3 quote schema: https://swapkit.github.io/SwapKit/api/operations/v3quote/
Receiving addresses stay the owner's existing public addresses. A quote does not
change customer invoices, reserve liquidity, create a swap or transfer funds.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
import html
import os
import requests

from . import networks as nw

QUOTE_URL = 'https://api.swapkit.dev/v3/quote'
TARGETS = ('usdt_bep20', 'usdt_trc20', 'usdt_erc20')
NATIVE_IDS = {'trx':'TRON.TRX','btc':'BTC.BTC','ltc':'LTC.LTC','eth':'ETH.ETH','bnb':'BSC.BNB','sol':'SOL.SOL'}
TOKEN_CHAINS = {'tron':'TRON','ethereum':'ETH','bsc':'BSC'}
PRESETS = {'trx':('20','100','500'), 'btc':('0.0001','0.001','0.01'),
           'ltc':('0.1','1','10'), 'eth':('0.002','0.01','0.1'),
           'bnb':('0.01','0.05','0.1'), 'sol':('0.05','0.1','1')}


class QuoteUnavailable(ValueError):
    pass


def configured():
    return bool(os.getenv('SWAPKIT_API_KEY','').strip())


def asset_identifier(asset_id):
    a = nw.ASSETS[asset_id]
    return NATIVE_IDS[asset_id] if a.kind == 'native' else f'{TOKEN_CHAINS[a.network]}.USDT-{a.contract}'


def _number(value, positive=False):
    try:
        n = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise QuoteUnavailable('The provider returned an invalid number.') from None
    if not n.is_finite() or n < 0 or (positive and n <= 0):
        raise QuoteUnavailable('The provider returned an invalid amount.')
    return n


def _same_asset(a,b):
    if not isinstance(a,str) or not isinstance(b,str):
        return False
    def canonical(s):
        prefix, sep, address = s.partition('-')
        return prefix.upper() + (sep + (address.lower() if address.lower().startswith('0x') else address) if sep else '')
    return canonical(a) == canonical(b)


def _expiry(value, now):
    try:
        stamp = datetime.fromisoformat(str(value).replace('Z','+00:00')) if value else now + timedelta(minutes=5)
        if stamp.tzinfo is None:
            raise ValueError()
        stamp = min(stamp, now + timedelta(minutes=5))
        if stamp <= now:
            raise ValueError()
        return stamp
    except (TypeError, ValueError):
        raise QuoteUnavailable('The quote expired or has an invalid expiry.') from None


def quote(sell_id, target_id, amount, *, session=None, now=None):
    if sell_id not in nw.ASSETS or target_id not in TARGETS:
        raise QuoteUnavailable('Unsupported source or USDT destination.')
    amount = _number(amount, positive=True)
    if amount > Decimal('1000000000') or amount * (10 ** nw.ASSETS[sell_id].decimals) % 1:
        raise QuoteUnavailable('Invalid source amount or precision.')
    now = now or datetime.now(timezone.utc)
    common = {'mode':'preview_only','sell_id':sell_id,'target_id':target_id,'sell_amount':str(amount),
              'destination':nw.NETWORKS[nw.ASSETS[target_id].network].address,'quoted_at':now.isoformat()}
    if sell_id == target_id:
        return dict(common, direct=True, expected=str(amount), minimum=str(amount), fees=[], providers=[],
                    warnings=[], expires_at=None, estimated_delta_usd='0')
    key = os.getenv('SWAPKIT_API_KEY','').strip()
    if not key:
        raise QuoteUnavailable('SwapKit quote API is not configured yet (SWAPKIT_API_KEY). Conversion remains preview-only; all payment coins stay enabled.')
    sell_asset, buy_asset = asset_identifier(sell_id), asset_identifier(target_id)
    payload = {'sellAsset':sell_asset,'buyAsset':buy_asset,'sellAmount':str(amount),
               'sourceAddress':nw.NETWORKS[nw.ASSETS[sell_id].network].address,
               'destinationAddress':common['destination'],'slippage':0.5,'affiliateFee':0,'disableEstimate':False}
    try:
        response = (session or requests).post(QUOTE_URL, json=payload,
                     headers={'x-api-key':key,'Content-Type':'application/json'}, timeout=(5,20))
        if response.status_code in (401,403):
            raise QuoteUnavailable('The SwapKit quote key is not authorized.')
        if response.status_code == 429:
            raise QuoteUnavailable('Quote rate limit reached. Try again shortly.')
        if response.status_code != 200:
            raise QuoteUnavailable(f'Quote service unavailable (HTTP {response.status_code}).')
        data = response.json()
    except (requests.RequestException, ValueError) as exc:
        if isinstance(exc,QuoteUnavailable):
            raise
        raise QuoteUnavailable('Could not obtain a live conversion quote. No swap was created.') from None
    if not isinstance(data,dict) or not isinstance(data.get('routes'),list):
        raise QuoteUnavailable('Invalid quote response. No swap was created.')
    valid = []
    for r in data['routes'][:20]:
        try:
            if (not isinstance(r,dict) or not _same_asset(r.get('sellAsset'),sell_asset)
                    or not _same_asset(r.get('buyAsset'),buy_asset) or _number(r.get('sellAmount')) != amount):
                continue
            expected = _number(r.get('expectedBuyAmount') or r.get('buyAmount'), positive=True)
            minimum_raw = r.get('expectedBuyAmountMaxSlippage') or r.get('buyAmountMaxSlippage')
            minimum = _number(minimum_raw, positive=True) if minimum_raw is not None else None
            if minimum is not None and minimum > expected:
                continue
            expiry = _expiry(r.get('expiration'),now)
            fees = [{'type':str(f['type'])[:30], 'asset':str(f['asset'])[:140], 'amount':str(_number(f['amount']))}
                    for f in r.get('fees',[])[:8]]
            # Keep the provider's outputs intact; never subtract displayed fees twice.
            prices = r.get('meta',{}).get('assets',[]) if isinstance(r.get('meta'),dict) else []
            source_price = next((_number(a['price'],positive=True) for a in prices if _same_asset(a.get('asset'),sell_asset)),None)
            target_price = next((_number(a['price'],positive=True) for a in prices if _same_asset(a.get('asset'),buy_asset)),None)
            delta = amount * source_price - expected * target_price if source_price and target_price else None
            warnings = [str(w.get('code') or 'provider_warning')[:80] for w in r.get('warnings',[])[:8] if isinstance(w,dict)]
            if minimum is None:
                warnings.append('minimum_receive_not_provided')
            if delta is not None and delta > amount * source_price * Decimal('0.02'):
                warnings.append('estimated_value_reduction_over_2_percent')
            valid.append(dict(common,direct=False,expected=str(expected),minimum=str(minimum) if minimum is not None else None,
                         fees=fees,providers=[str(p)[:60] for p in r.get('providers',[])[:3]],warnings=warnings,
                         expires_at=expiry.isoformat(),estimated_delta_usd=str(delta) if delta is not None else None))
        except (QuoteUnavailable,KeyError,TypeError,AttributeError):
            continue
    if not valid:
        raise QuoteUnavailable('No valid live route for this amount and destination. All payment coins remain available; no conversion was started.')
    return max(valid,key=lambda r:Decimal(r['expected']))


def render(q):
    e = lambda x:html.escape(str(x))
    lines = ['💱 <b>USDT CONVERSION — PREVIEW ONLY</b>',
             f"Source: <code>{e(q['sell_amount'])}</code> {e(nw.ASSETS[q['sell_id']].label)}",
             f"Receive network: <b>{e(nw.ASSETS[q['target_id']].label)}</b>",
             f"Receiving wallet: <code>{e(q['destination'])}</code>",
             f"Provider quoted output: <b>{e(q['expected'])} USDT</b>",
             f"Minimum receive: {e(q.get('minimum') or 'not supplied')} USDT"]
    if q['direct']:
        lines += ['', 'Already the same USDT asset/network. No conversion is needed. Sending/withdrawal network fees can still apply.']
    else:
        lines += [f"Route: {e(', '.join(q['providers']) or 'provider not supplied')}",
                  f"Expires (UTC): {e(q['expires_at'])}", '', '<b>Provider fee breakdown</b>']
        lines += [f"• {e(f['type'])}: {e(f['amount'])} {e(f['asset'])}" for f in q['fees']]
        if not q['fees']:
            lines.append('Fee components were not provided; this does not mean zero fees.')
        if q.get('estimated_delta_usd') is not None:
            lines.append(f"Estimated value difference (provider prices): ${e(Decimal(q['estimated_delta_usd']).quantize(Decimal('0.0001')))}")
        lines += ['Output is shown as quoted by the provider; listed fees are not deducted again. Source-chain gas may be additional.']
        if q['warnings']:
            lines += ['', '⚠️ Review: '+e(', '.join(q['warnings']))]
    lines += ['', 'No funds were sent, no transaction was signed, and customer payment addresses/balances were not changed.',
              'Automatic USDT conversion is not active. A live quote is an estimate, not a guaranteed final execution price.']
    return '\n'.join(lines)
