"""Supplier timeout regression tests: all HTTP and Telegram calls are mocked."""
import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import requests
import bot
import seller_transport as transport
from test_phase15 import Phase15StateMixin

PROVIDER = {'type':'warzone_v1','enabled':True,'configured':True,'api_key':'dummy-key',
            'base_url':'https://seller.test','product_id_field':'service_id'}


class TransportTests(unittest.TestCase):
    def test_read_retries_transient_timeout_without_leaking_secrets(self):
        response = Mock(status_code=200)
        with patch.object(transport.requests,'get',side_effect=[requests.ConnectTimeout('secret'),response]) as get, \
             patch.object(transport.time,'sleep'), patch('builtins.print') as output:
            self.assertIs(transport.read_get('https://seller.test/api/v1/me',provider_id='seller_2'),response)
        self.assertEqual(get.call_count,2)
        self.assertNotIn('secret',str(output.call_args_list))
        self.assertEqual(get.call_args.kwargs['timeout'],(8,25))

    def test_read_retry_is_bounded(self):
        with patch.object(transport.requests,'get',side_effect=requests.ReadTimeout()) as get, \
             patch.object(transport.time,'sleep'):
            with self.assertRaises(requests.ReadTimeout):
                transport.read_get('https://seller.test',provider_id='seller_2')
        self.assertEqual(get.call_count,2)

    def test_retry_temporary_server_status_but_not_auth_failure(self):
        with patch.object(transport.requests,'get',side_effect=[Mock(status_code=503),Mock(status_code=200)]) as get, \
             patch.object(transport.time,'sleep'):
            self.assertEqual(transport.read_get('https://seller.test',provider_id='seller_2').status_code,200)
            self.assertEqual(get.call_count,2)
        with patch.object(transport.requests,'get',return_value=Mock(status_code=401)) as get:
            self.assertEqual(transport.read_get('https://seller.test',provider_id='seller_2').status_code,401)
            get.assert_called_once()

    def purchase(self,post_error=None):
        with patch.object(bot,'_validate_buyer_api_config',return_value=PROVIDER), \
             patch.object(bot,'_buyer_api_get',return_value={'balance':100}), \
             patch.object(bot,'buyer_api_endpoint',return_value='https://seller.test/api/v1/order'), \
             patch.object(bot.requests,'post',side_effect=post_error) as post:
            with self.assertRaises(bot.BuyerAPIError) as error:
                bot.purchase_buyer_api_product('S_20',1,None,'seller_2')
        return error.exception,post

    def test_preflight_outage_never_posts_purchase(self):
        with patch.object(bot,'_validate_buyer_api_config',return_value=PROVIDER), \
             patch.object(bot,'_buyer_api_get',side_effect=bot.BuyerAPIError('unavailable')), \
             patch.object(bot.requests,'post') as post:
            with self.assertRaises(bot.BuyerAPIError) as error:
                bot.purchase_buyer_api_product('S_20',1,None,'seller_2')
        self.assertEqual(error.exception.category,'not_sent'); post.assert_not_called()

    def test_connect_timeout_classified_not_sent_and_never_replayed(self):
        error,post=self.purchase(requests.ConnectTimeout())
        self.assertEqual(error.category,'not_sent'); post.assert_called_once()
        self.assertEqual(post.call_args.kwargs['timeout'],(8,60))

    def test_read_timeout_stays_ambiguous_and_never_replays_order(self):
        error,post=self.purchase(requests.ReadTimeout())
        self.assertEqual(error.category,'timeout'); post.assert_called_once()


class CheckoutTests(Phase15StateMixin,unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        bot.user_wallet[self.user_id]=10
        bot.user_orders[self.user_id]=[]
        bot.user_transactions[self.user_id]=[]
        self.mapping=self.add_api('S_20',provider='seller_2',selling_price=3.6,last_stock=20)
        self.token=bot.api_shop_callback_token('S_20','seller_2')
        self.context=SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock()))

    async def test_preflight_failure_keeps_wallet_and_order_history_unchanged(self):
        with patch.object(bot,'get_seller_api_provider',return_value=PROVIDER), \
             patch.object(bot,'purchase_buyer_api_product',side_effect=bot.BuyerAPIError('not sent','not_sent')) as post:
            ok=await bot.process_api_shop_purchase(self.context,self.user_id,self.token,1)
        self.assertFalse(ok);post.assert_called_once()
        self.assertEqual(bot.user_wallet[self.user_id],10)
        self.assertEqual(bot.all_orders,[])

    async def test_existing_unknown_order_blocks_second_charge_including_legacy_order(self):
        bot.all_orders.append({'id':3046,'user_id':self.user_id,'api_product_id':'S_20',
                               'product_type':'api','status':'pending_support','failure_category':'timeout'})
        with patch.object(bot,'get_seller_api_provider',return_value=PROVIDER), \
             patch.object(bot,'purchase_buyer_api_product') as post:
            ok=await bot.process_api_shop_purchase(self.context,self.user_id,self.token,1)
        self.assertFalse(ok);post.assert_not_called()
        self.assertEqual(bot.user_wallet[self.user_id],10)
        self.assertIn('#3046',self.context.bot.send_message.await_args.args[1])

    def test_new_support_order_records_provider(self):
        order=bot.add_api_pending_support_order_record(self.user_id,self.mapping,1,3.6,3.6,'timeout')
        self.assertEqual(order['provider_id'],'seller_2')
