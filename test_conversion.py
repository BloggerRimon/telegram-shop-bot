"""Conversion preparation uses only mocked quote traffic, never a signed swap."""
import asyncio
import copy
import os
import unittest
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from onchain import admin_ui, conversion as cv

NOW = datetime(2026,11,27,12,tzinfo=timezone.utc)


def route(expected='2.35', minimum='2.34'):
    return {'sellAsset':'TRON.TRX','sellAmount':'20','buyAsset':cv.asset_identifier('usdt_bep20'),
            'expectedBuyAmount':expected,'expectedBuyAmountMaxSlippage':minimum,
            'expiration':(NOW+timedelta(minutes=5)).isoformat(), 'providers':['TEST_PROVIDER'],
            'fees':[{'type':'network','amount':'1','asset':'TRON.TRX'}], 'warnings':[],
            'meta':{'assets':[{'asset':'TRON.TRX','price':0.3},
                             {'asset':cv.asset_identifier('usdt_bep20'),'price':1}]}}


class ConversionTests(unittest.TestCase):
    def request(self, routes=None):
        s=Mock();s.post.return_value=Mock(status_code=200)
        s.post.return_value.json.return_value={'quoteId':'test','routes':routes if routes is not None else [route()]}
        return s

    def test_same_usdt_needs_no_conversion_key_or_network(self):
        s=self.request()
        with patch.dict(os.environ,{'SWAPKIT_API_KEY':''}):
            q=cv.quote('usdt_bep20','usdt_bep20','5',session=s,now=NOW)
        s.post.assert_not_called();self.assertTrue(q['direct']);self.assertEqual(q['expected'],'5')

    def test_missing_key_reports_unconfigured_without_sending_funds(self):
        s=self.request()
        with patch.dict(os.environ,{'SWAPKIT_API_KEY':''}):
            with self.assertRaisesRegex(cv.QuoteUnavailable,'not configured'):
                cv.quote('trx','usdt_bep20','20',session=s,now=NOW)
        s.post.assert_not_called()

    def test_quote_only_endpoint_zero_affiliate_and_existing_owner_addresses(self):
        s=self.request()
        with patch.dict(os.environ,{'SWAPKIT_API_KEY':'test-key'}):
            q=cv.quote('trx','usdt_bep20','20',session=s,now=NOW)
        s.post.assert_called_once()
        args=s.post.call_args
        self.assertEqual(args.args,(cv.QUOTE_URL,))
        self.assertNotIn('/swap',args.args[0].split('api.swapkit.dev')[-1])
        self.assertEqual(args.kwargs['json']['affiliateFee'],0)
        self.assertFalse(args.kwargs['json']['disableEstimate'])
        self.assertEqual(args.kwargs['json']['destinationAddress'],cv.nw.BSC_ADDRESS)
        self.assertEqual(args.kwargs['json']['sourceAddress'],cv.nw.TRON_ADDRESS)
        self.assertEqual(q['mode'],'preview_only')

    def test_provider_output_preserved_and_high_cost_flagged(self):
        with patch.dict(os.environ,{'SWAPKIT_API_KEY':'test-key'}):
            q=cv.quote('trx','usdt_bep20','20',session=self.request(),now=NOW)
        self.assertEqual(q['expected'],'2.35');self.assertEqual(q['minimum'],'2.34')
        self.assertEqual(q['estimated_delta_usd'],'3.65')
        self.assertIn('estimated_value_reduction_over_2_percent',q['warnings'])
        text=cv.render(q)
        for value in ['2.35','2.34','network','1','not deducted again','Source-chain gas']:
            self.assertIn(value,text)

    def test_invalid_wrong_asset_expired_or_nonfinite_routes_rejected(self):
        variants=[]
        for k,v in [('sellAsset','BTC.BTC'),('sellAmount','30'),('expectedBuyAmount','NaN'),
                    ('expectedBuyAmountMaxSlippage','100'),('expiration',(NOW-timedelta(seconds=1)).isoformat())]:
            r=route();r[k]=v;variants.append(r)
        with patch.dict(os.environ,{'SWAPKIT_API_KEY':'test-key'}):
            for r in variants:
                with self.subTest(r=r):
                    with self.assertRaises(cv.QuoteUnavailable):
                        cv.quote('trx','usdt_bep20','20',session=self.request([r]),now=NOW)

    def test_empty_route_and_unknown_fee_do_not_claim_zero_cost(self):
        with patch.dict(os.environ,{'SWAPKIT_API_KEY':'test-key'}):
            with self.assertRaises(cv.QuoteUnavailable):
                cv.quote('trx','usdt_bep20','20',session=self.request([]),now=NOW)
            r=route();r['fees']=[];r.pop('expectedBuyAmountMaxSlippage');r.pop('meta')
            q=cv.quote('trx','usdt_bep20','20',session=self.request([r]),now=NOW)
        self.assertIsNone(q['minimum']);self.assertIsNone(q['estimated_delta_usd'])
        self.assertIn('does not mean zero fees',cv.render(q))

    def test_amount_validation_all_assets_and_targets(self):
        self.assertEqual(len([cv.asset_identifier(a) for a in cv.nw.ASSET_ORDER]),9)
        for amount in ('NaN','-1','0','Infinity','0.0000001'):
            with self.assertRaises(cv.QuoteUnavailable):cv.quote('trx','usdt_bep20',amount)
        with self.assertRaises(cv.QuoteUnavailable):cv.quote('trx','btc','20')

    def test_choose_best_valid_route_and_preserve_minimum(self):
        with patch.dict(os.environ,{'SWAPKIT_API_KEY':'test-key'}):
            q=cv.quote('trx','usdt_bep20','20',session=self.request([route('4.9','4.8'),route('5.8','5.7')]),now=NOW)
        self.assertEqual(q['expected'],'5.8');self.assertEqual(q['minimum'],'5.7')

    def test_error_response_does_not_expose_provider_body_or_api_key(self):
        for status in (401,403,429,500):
            s=self.request();s.post.return_value.status_code=status
            s.post.return_value.text='test-key secret-data'
            with patch.dict(os.environ,{'SWAPKIT_API_KEY':'test-key'}):
                with self.assertRaises(cv.QuoteUnavailable) as error:
                    cv.quote('trx','usdt_bep20','20',session=s,now=NOW)
            self.assertNotIn('test-key',str(error.exception));self.assertNotIn('secret-data',str(error.exception))

    def test_non_admin_cannot_trigger_quote_callback(self):
        q=SimpleNamespace(from_user=SimpleNamespace(id=42),message=SimpleNamespace(reply_text=AsyncMock()))
        with patch.object(cv,'quote') as quote:
            asyncio.run(admin_ui.callback(q,'oc:cquote:trx:usdt_bep20:20',lambda u:u==1))
            quote.assert_not_called()
