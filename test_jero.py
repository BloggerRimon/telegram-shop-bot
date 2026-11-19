"""Tests for the Jero Accounts reseller provider (jero_reseller).

All supplier traffic is mocked: a fake HTTP session for the adapter and a fake
client for bot-level flows. No network, no production database.
"""
import asyncio
import copy
import json
import os
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import requests

import bot
import jero_api
from test_phase15 import Phase15StateMixin

TEST_KEY = "jak_test_dummy_key_0123456789abcdef0123456789"
ENDPOINT = "https://api.jeroaccounts.test/api/reseller"
JERO_ENV = {
    "BUYER_API_3_ENABLED": "true",
    "BUYER_API_3_NAME": "Jero Accounts",
    "BUYER_API_3_TYPE": "jero_reseller",
    "BUYER_API_3_URL": ENDPOINT,
    "BUYER_API_3_KEY": TEST_KEY,
    "BUYER_API_2_ENABLED": "true",
    "BUYER_API_2_NAME": "Seller Two",
    "BUYER_API_2_TYPE": "warzone_v1",
    "BUYER_API_2_URL": "https://seller-two.test",
    "BUYER_API_2_KEY": "seller-two-key-0000",
}


class FakeResponse:
    def __init__(self, status=200, payload=None, text=None, headers=None):
        self.status_code = status
        self._payload = payload
        self.text = text if text is not None else (json.dumps(payload) if payload is not None else "")
        self.headers = headers or {}

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def make_client(responses, **kwargs):
    session = FakeSession(responses)
    limiter = jero_api.RateLimiter(sleep=lambda _s: None)
    client = jero_api.JeroClient(
        ENDPOINT, TEST_KEY, provider_id="seller_3", session=session, limiter=limiter,
        sleep=lambda _s: None, jitter=lambda: 0.5, **kwargs,
    )
    return client, session


# ---------------------------------------------------------------------------
# Adapter tests
# ---------------------------------------------------------------------------
class JeroAdapterTests(unittest.TestCase):
    def test_bearer_header_action_endpoint_no_key_in_query_or_body(self):
        client, session = make_client([FakeResponse(200, {"success": True, "data": [
            {"id": 12, "title": "Netflix", "price": "1.25", "stock": 4, "tiers": []},
        ]})])
        products = client.products()
        call = session.calls[0]
        self.assertEqual(call["method"], "GET")
        self.assertEqual(call["url"], ENDPOINT + "?action=products")
        self.assertEqual(call["headers"]["Authorization"], f"Bearer {TEST_KEY}")
        self.assertNotIn(TEST_KEY, call["url"])
        self.assertIsNone(call["json"])
        self.assertFalse(call["allow_redirects"])
        self.assertTrue(call["verify"])
        self.assertEqual(products[0]["id"], "12")

    def test_order_post_uses_json_body_without_key(self):
        client, session = make_client([FakeResponse(200, {
            "success": True, "order_id": 77, "status": "completed",
            "items": ["user1|pass1"], "total": "1.25", "balance": "98.75",
        })])
        payload = {"product_id": 12, "quantity": 1, "external_order_id": "TSB3-1-aa"}
        result = client.create_order(payload)
        call = session.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], ENDPOINT + "?action=order")
        self.assertEqual(call["json"], payload)
        self.assertNotIn(TEST_KEY, json.dumps(call["json"]))
        self.assertEqual(call["headers"]["Content-Type"], "application/json")
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["delivered"], ["user1|pass1"])
        self.assertEqual(result["total"], "1.25")

    def test_lookup_uses_external_order_id(self):
        client, session = make_client([FakeResponse(200, {"success": True, "data": {
            "order_id": 5, "status": "processing", "items": [],
        }})])
        result = client.lookup_order("TSB3-9-ab")
        self.assertEqual(session.calls[0]["url"], ENDPOINT + "?action=order&external_order_id=TSB3-9-ab")
        self.assertEqual(result["status"], "processing")

    def test_redirect_is_not_followed(self):
        client, session = make_client([FakeResponse(302, None, headers={"Location": "https://evil.test"})])
        with self.assertRaises(jero_api.JeroAPIError) as ctx:
            client.balance()
        self.assertEqual(ctx.exception.code, "redirect_blocked")
        self.assertEqual(len(session.calls), 1)

    def test_product_normalization_price_stock_title_tiers(self):
        products = jero_api.normalize_products({"products": [
            {"id": 1, "title": "A", "price": "2.50", "stock": 3,
             "tiers": [{"min_quantity": 10, "price": "2.00"}]},
            {"id": 2, "title": "B", "price": None, "stock": None},
            {"id": 3, "title": "C", "price": "abc", "stock": "7"},
            {"id": 900001, "title": "Elementor", "price": 9, "stock": 0},
        ]})
        first, missing, invalid, special = products
        self.assertEqual(first["price"], 2.5)
        self.assertEqual(first["price_text"], "2.50")
        self.assertEqual(first["stock"], 3)
        self.assertEqual(first["tiers"], [{"min_quantity": 10, "price": "2.00"}])
        self.assertFalse(first["_jero_tiers_unclear"])
        self.assertIsNone(missing["price"])          # never a zero-price product
        self.assertFalse(missing["_jero_price_valid"])
        self.assertIsNone(missing["stock"])          # never treated as unlimited
        self.assertFalse(missing["_jero_stock_known"])
        self.assertIsNone(invalid["price"])
        self.assertEqual(invalid["stock"], 7)
        self.assertEqual(special["_jero_special_kind"], "wordpress")

    def test_unrecognized_product_shape_reports_shape_without_values(self):
        with self.assertRaises(jero_api.JeroAPIError) as ctx:
            jero_api.normalize_products({"weird": {"secret_value": "x"}})
        self.assertEqual(ctx.exception.code, "products_unrecognized")
        self.assertNotIn("secret_value\": \"x", str(ctx.exception))

    def test_balance_normalization(self):
        self.assertEqual(jero_api.normalize_balance({"success": True, "balance": "12.40"})["balance_decimal"], "12.40")
        self.assertEqual(jero_api.normalize_balance({"data": {"balance": 3, "currency": "USD"}})["balanceText"], "3 USD")
        with self.assertRaises(jero_api.JeroAPIError):
            jero_api.normalize_balance({"data": {"nothing": 1}})

    def test_documented_error_envelope_on_http_200(self):
        client, _ = make_client([FakeResponse(200, {
            "success": False, "error": {"code": "insufficient_balance", "message": "Top up"},
        })])
        with self.assertRaises(jero_api.JeroAPIError) as ctx:
            client.create_order({"product_id": 1, "quantity": 1, "external_order_id": "X1"})
        self.assertEqual(ctx.exception.code, "insufficient_balance")
        self.assertTrue(ctx.exception.rejected_order)

    def test_every_documented_error_code_is_recognized(self):
        for code in jero_api.DOCUMENTED_ERROR_CODES:
            with self.subTest(code=code):
                client, _ = make_client([FakeResponse(400, {"success": False, "error": {"code": code, "message": "m"}})] * 3)
                with self.assertRaises(jero_api.JeroAPIError) as ctx:
                    client.create_order({"product_id": 1, "quantity": 1, "external_order_id": "X1"})
                self.assertEqual(ctx.exception.code, code)

    def test_cloudflare_html_and_malformed_json_are_retryable_and_may_exist(self):
        client, _ = make_client([FakeResponse(522, None, text="<html>cloudflare</html>")])
        with self.assertRaises(jero_api.JeroAPIError) as ctx:
            client.create_order({"product_id": 1, "quantity": 1, "external_order_id": "X1"})
        self.assertEqual(ctx.exception.code, "upstream_unavailable")
        self.assertTrue(ctx.exception.order_may_exist)
        self.assertFalse(ctx.exception.rejected_order)
        client, _ = make_client([FakeResponse(200, None, text="{broken")])
        with self.assertRaises(jero_api.JeroAPIError) as ctx:
            client.create_order({"product_id": 1, "quantity": 1, "external_order_id": "X1"})
        self.assertTrue(ctx.exception.order_may_exist)

    def test_timeout_on_order_is_not_a_rejection_and_is_not_retried_with_new_id(self):
        client, session = make_client([requests.Timeout()])
        with self.assertRaises(jero_api.JeroAPIError) as ctx:
            client.create_order({"product_id": 1, "quantity": 1, "external_order_id": "SAME"})
        self.assertEqual(ctx.exception.code, "timeout")
        self.assertTrue(ctx.exception.order_may_exist)
        self.assertEqual(len(session.calls), 1)

    def test_get_retries_with_backoff_and_retry_after(self):
        sleeps = []
        session = FakeSession([
            FakeResponse(503, None, text="down"),
            FakeResponse(429, {"success": False, "error": {"code": "rate_limited", "message": "slow"}}, headers={"Retry-After": "7"}),
            FakeResponse(200, {"balance": "5"}),
        ])
        clock = {"now": 1000.0}

        def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        limiter = jero_api.RateLimiter(clock=lambda: clock["now"], sleep=lambda s: clock.__setitem__("now", clock["now"] + s))
        client = jero_api.JeroClient(
            ENDPOINT, TEST_KEY, session=session, limiter=limiter, sleep=fake_sleep,
            jitter=lambda: 0.0, max_attempts=3,
        )
        self.assertEqual(client.balance()["balance_decimal"], "5")
        self.assertEqual(len(session.calls), 3)
        self.assertEqual(sleeps[0], 0.5)          # base 1s * (0.5 + jitter 0)
        self.assertGreaterEqual(sleeps[1], 7.0)   # Retry-After honoured

    def test_auth_failure_is_not_retried(self):
        client, session = make_client([FakeResponse(401, {"success": False, "error": {"code": "unauthorized", "message": "bad"}})] * 3)
        with self.assertRaises(jero_api.JeroAPIError) as ctx:
            client.balance()
        self.assertEqual(ctx.exception.code, "unauthorized")
        self.assertEqual(len(session.calls), 1)

    def test_rate_limiter_windows(self):
        clock = {"now": 0.0}
        limiter = jero_api.RateLimiter(requests_per_minute=3, orders_per_minute=1,
                                       clock=lambda: clock["now"], sleep=lambda _s: None)
        for _ in range(3):
            limiter.acquire()
        with self.assertRaises(jero_api.JeroAPIError) as ctx:
            limiter.acquire(max_wait=0)
        self.assertEqual(ctx.exception.code, "rate_limited")
        clock["now"] = 61.0
        limiter.acquire(is_order=True)
        with self.assertRaises(jero_api.JeroAPIError):
            limiter.acquire(is_order=True, max_wait=0)
        limiter.pause(30)
        clock["now"] = 62.0
        with self.assertRaises(jero_api.JeroAPIError):
            limiter.acquire(max_wait=0)

    def test_default_limits_match_documentation(self):
        limiter = jero_api.RateLimiter()
        self.assertEqual(limiter.limits, {"request": 120, "order": 30})
        with self.assertRaises(jero_api.JeroAPIError):
            jero_api.validate_order_payload({"product_id": 1, "quantity": 101, "external_order_id": "X"})

    def test_redaction(self):
        text = jero_api.redact(
            f"Bearer {TEST_KEY} password=hunter2 user@example.com acct|pw {TEST_KEY}", (TEST_KEY,)
        )
        for secret in (TEST_KEY, "hunter2", "user@example.com", "acct|pw"):
            self.assertNotIn(secret, text)
        client, _ = make_client([FakeResponse(400, {"success": False, "error": {
            "code": "invalid_request", "message": f"key {TEST_KEY} rejected"}})])
        with self.assertRaises(jero_api.JeroAPIError) as ctx:
            client.balance()
        self.assertNotIn(TEST_KEY, str(ctx.exception))

    def test_object_items_are_never_stringified(self):
        result = jero_api.normalize_order({"order_id": 1, "status": "completed", "items": [{"foo": {"bar": 1}}]})
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(result["delivered"], [])
        result = jero_api.normalize_order({"order_id": 1, "items": [{"account": "a|b", "status": "completed"}]})
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["delivered"], ["a|b"])

    def test_ui8_link_validation(self):
        result = jero_api.validate_ui8_links(
            "https://ui8.net/a/products/x\nhttps://ui8.net/a/products/x/\nhttp://ui8.net/a/products/y\n"
            "https://evil.test/products/z\nhttps://www.ui8.net/b/products/q"
        )
        self.assertEqual(result["accepted"], ["https://ui8.net/a/products/x", "https://ui8.net/b/products/q"])
        self.assertEqual(len(result["duplicates"]), 1)
        self.assertEqual(len(result["invalid"]), 2)

    def test_special_payload_requirements(self):
        with self.assertRaises(jero_api.JeroAPIError):
            jero_api.validate_order_payload({"product_id": 900001, "quantity": 1, "external_order_id": "E1", "site": "x"})
        with self.assertRaises(jero_api.JeroAPIError):
            jero_api.validate_order_payload({
                "product_id": 900003, "quantity": 2, "external_order_id": "E1",
                "site": "https://a.b", "username": "u", "password": "p",
            })
        jero_api.validate_order_payload({
            "product_id": 900001, "quantity": 1, "external_order_id": "E1",
            "site": "https://a.b", "username": "u", "password": "p",
        })
        with self.assertRaises(jero_api.JeroAPIError):
            jero_api.validate_order_payload({"product_id": 900002, "quantity": 2, "external_order_id": "U1", "links": ["x"]})


# ---------------------------------------------------------------------------
# Bot-level tests
# ---------------------------------------------------------------------------
class FakeJeroClient:
    def __init__(self):
        self.created = []
        self.resent = []
        self.lookups = []
        self.create_results = []
        self.lookup_results = []

    def _next(self, queue):
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def create_order(self, payload):
        self.created.append(dict(payload))
        return self._next(self.create_results)

    def resend_order(self, payload):
        self.resent.append(dict(payload))
        return self._next(self.create_results)

    def lookup_order(self, external_order_id):
        self.lookups.append(external_order_id)
        return self._next(self.lookup_results)


def completed(items, order_id="J1"):
    return jero_api.normalize_order({"order_id": order_id, "status": "completed", "items": items, "total": "1", "balance": "9"})


class JeroBotBase(Phase15StateMixin):
    def setUp(self):
        super().setUp()
        self.env = patch.dict(os.environ, JERO_ENV)
        self.env.start()
        self.extra_saved = {
            "user_orders": copy.deepcopy(bot.user_orders),
            "user_transactions": copy.deepcopy(bot.user_transactions),
            "global_order_id": bot.global_order_id,
            "global_tx_id": bot.global_tx_id,
            "provider_status": copy.deepcopy(bot.seller_api_provider_status),
        }
        bot.jero_orders.clear()
        bot.jero_checkout_drafts.clear()
        bot.jero_draft_results.clear()
        bot.jero_secret_inputs.clear()
        bot.jero_local_claims.clear()
        bot.jero_fresh_reservations.clear()
        bot.user_wallet[self.user_id] = 20.0
        bot.get_seller_api_provider_status("seller_3")["jero_products_verified_at"] = "2026-11-20T00:00:00"
        bot.get_seller_api_provider_status("seller_3").pop("jero_auth_blocked_until", None)
        self.saves = [
            patch.object(bot, "save_bot_state_strict"),
            patch.object(bot, "save_bot_state"),
        ]
        for item in self.saves:
            item.start()
        self.client = FakeJeroClient()
        self.client_patch = patch.object(bot, "jero_client", return_value=self.client)
        self.client_patch.start()
        self.sent = AsyncMock()
        self.context = SimpleNamespace(bot=SimpleNamespace(send_message=self.sent))

    def tearDown(self):
        self.client_patch.stop()
        for item in self.saves:
            item.stop()
        self.env.stop()
        bot.jero_orders.clear()
        bot.jero_checkout_drafts.clear()
        bot.jero_draft_results.clear()
        bot.jero_secret_inputs.clear()
        bot.jero_local_claims.clear()
        bot.jero_fresh_reservations.clear()
        bot.user_orders.clear()
        bot.user_orders.update(self.extra_saved["user_orders"])
        bot.user_transactions.clear()
        bot.user_transactions.update(self.extra_saved["user_transactions"])
        bot.global_order_id = self.extra_saved["global_order_id"]
        bot.global_tx_id = self.extra_saved["global_tx_id"]
        bot.seller_api_provider_status.clear()
        bot.seller_api_provider_status.update(self.extra_saved["provider_status"])
        super().tearDown()

    def add_jero(self, product_id="12", title="Jero Netflix", price="1.00", stock=10, selling=2.5, provider="seller_3"):
        product = jero_api.normalize_product({"id": product_id, "title": title, "price": price, "stock": stock, "tiers": []})
        product["_seller_provider_id"] = provider
        return bot.update_api_product_mapping(
            product, provider_id=provider, enabled=True,
            category_id=bot.DEFAULT_CATEGORY_ID, selling_price=selling,
        )

    def token(self, mapping):
        return bot.api_shop_callback_token(mapping["api_product_id"], bot.mapping_provider_id(mapping))

    def texts(self):
        return [str(call.args[1]) if len(call.args) > 1 else str(call.kwargs.get("text")) for call in self.sent.await_args_list]

    async def confirm(self, mapping, quantity=1, **inputs):
        draft = await bot.jero_send_confirmation(self.context, self.user_id, mapping, quantity, **inputs)
        self.assertTrue(draft)
        return draft


class JeroProviderConfigTests(JeroBotBase, unittest.TestCase):
    def test_provider_loaded_in_slot_3_with_complete_url(self):
        providers = bot.load_seller_api_providers()
        jero = providers["seller_3"]
        self.assertEqual(jero["type"], "jero_reseller")
        self.assertEqual(jero["name"], "Jero Accounts")
        self.assertEqual(jero["base_url"], ENDPOINT)
        self.assertFalse(jero["url_had_api_suffix"])
        self.assertEqual(providers["seller_2"]["type"], "warzone_v1")
        self.assertEqual(bot._validate_buyer_api_config("seller_3")["id"], "seller_3")
        self.assertIn("Jero Accounts / seller_3", str([
            button.text for row in bot.seller_api_keyboard().inline_keyboard for button in row
        ]))

    def test_old_transport_and_purchase_paths_refuse_jero(self):
        with self.assertRaises(bot.BuyerAPIError):
            bot._buyer_api_get("/x", provider_id="seller_3")
        with self.assertRaises(bot.BuyerAPIError):
            bot.purchase_buyer_api_product("12", 1, None, "seller_3")

    def test_debug_and_panel_do_not_apply_api_prefix_check_or_leak_key(self):
        panel = bot.render_seller_api_provider_panel("seller_3")
        debug = bot.render_seller_api_debug_config("seller_3")
        for text in (panel, debug):
            self.assertNotIn("Starts with api_", text)
            self.assertNotIn(TEST_KEY, text)
        self.assertIn("Bearer", debug)
        self.assertIn("?action=products", debug)

    def test_fetch_uses_adapter_and_marks_parser_verified(self):
        bot.get_seller_api_provider_status("seller_3").pop("jero_products_verified_at", None)
        session = FakeSession([FakeResponse(200, {"success": True, "products": [
            {"id": 12, "title": "A", "price": "1.5", "stock": 2, "tiers": []}]})])
        real = jero_api.JeroClient(ENDPOINT, TEST_KEY, provider_id="seller_3", session=session,
                                   limiter=jero_api.RateLimiter(sleep=lambda _s: None))
        with patch.object(bot, "jero_client", return_value=real):
            products, total = bot.fetch_buyer_api_products("seller_3")
        self.assertEqual((total, products[0]["id"], products[0]["_seller_provider_id"]), (1, "12", "seller_3"))
        self.assertTrue(bot.get_seller_api_provider_status("seller_3")["jero_products_verified_at"])

    def test_auth_failure_pauses_requests(self):
        session = FakeSession([FakeResponse(401, {"success": False, "error": {"code": "unauthorized", "message": "x"}})])
        real = jero_api.JeroClient(ENDPOINT, TEST_KEY, provider_id="seller_3", session=session,
                                   limiter=jero_api.RateLimiter(sleep=lambda _s: None))
        with patch.object(bot, "jero_client", return_value=real):
            with self.assertRaises(bot.BuyerAPIError):
                bot.fetch_buyer_api_balance("seller_3")
            with self.assertRaises(bot.BuyerAPIError):
                bot.fetch_buyer_api_balance("seller_3")
        self.assertEqual(len(session.calls), 1)   # no retry storm
        self.assertTrue(bot.jero_auth_blocked("seller_3"))


class JeroCatalogTests(JeroBotBase, unittest.TestCase):
    def test_provider_identity_isolation(self):
        other = {"id": "12", "name": "Other seller product", "price": 3, "stock": 5, "_seller_provider_id": "seller_2"}
        other_mapping = bot.update_api_product_mapping(other, provider_id="seller_2", enabled=True,
                                                       category_id=bot.DEFAULT_CATEGORY_ID, selling_price=4)
        jero_mapping = self.add_jero("12")
        self.assertIsNot(other_mapping, jero_mapping)
        self.assertNotEqual(self.token(other_mapping), self.token(jero_mapping))
        self.assertIs(bot.find_api_shop_mapping(self.token(jero_mapping)), jero_mapping)
        self.assertFalse(bot.is_jero_mapping(other_mapping))
        self.assertEqual(bot.jero_special_kind("900001"), "wordpress")
        # Special IDs only matter for Jero.
        other_special = bot.update_api_product_mapping(
            {"id": "900001", "name": "Other", "price": 1, "stock": 1, "_seller_provider_id": "seller_2"},
            provider_id="seller_2", enabled=True, category_id=bot.DEFAULT_CATEGORY_ID, selling_price=2,
        )
        self.assertFalse(bot.is_jero_mapping(other_special))

    def test_refresh_keeps_custom_price_name_category_and_tiers_are_metadata(self):
        mapping = self.add_jero("12", price="1.00", selling=2.5)
        mapping["custom_name"] = "My Name"
        position_before = list(bot.shop_order)
        product = jero_api.normalize_product({"id": 12, "title": "Renamed", "price": "1.40", "stock": 3,
                                              "tiers": [{"min_quantity": 5, "price": "1.20"}]})
        bot.sync_api_product_mapping_validity([product], provider_id="seller_3")
        self.assertEqual(mapping["selling_price"], 2.5)
        self.assertEqual(mapping["custom_name"], "My Name")
        self.assertEqual(mapping["category_id"], bot.DEFAULT_CATEGORY_ID)
        self.assertEqual(mapping["api_cost"], 1.4)
        self.assertEqual(mapping["last_stock"], 3)
        self.assertEqual(mapping["api_tiers"], [{"min_quantity": 5, "price": "1.20"}])
        self.assertEqual(bot.shop_order, position_before)

    def test_fetched_products_are_not_auto_published(self):
        product = jero_api.normalize_product({"id": 55, "title": "New", "price": "1", "stock": 1})
        product["_seller_provider_id"] = "seller_3"
        mapping = bot.update_api_product_mapping(product, provider_id="seller_3")
        self.assertFalse(mapping["enabled"])
        self.assertFalse(bot.is_api_shop_mapping_visible(mapping))

    def test_missing_stock_price_and_unverified_parser_block_checkout(self):
        mapping = self.add_jero("12", stock=None)
        self.assertTrue(bot.validate_api_shop_purchase(mapping, 1, self.user_id))
        mapping = self.add_jero("13", price=None)
        self.assertTrue(bot.validate_api_shop_purchase(mapping, 1, self.user_id))
        mapping = self.add_jero("14")
        self.assertEqual(bot.validate_api_shop_purchase(mapping, 1, self.user_id), "")
        self.assertTrue(bot.validate_api_shop_purchase(mapping, 101, self.user_id))
        bot.get_seller_api_provider_status("seller_3").pop("jero_products_verified_at")
        self.assertTrue(bot.validate_api_shop_purchase(mapping, 1, self.user_id))

    def test_customer_pagination_stays_fifteen(self):
        self.assertEqual(bot.SHOP_PRODUCT_PAGE_SIZE, 15)
        for index in range(16):
            self.add_jero(str(100 + index), title=f"P{index:02d}")
        first = bot.shop_categories_keyboard(self.user_id, styled=False, page=0)
        second = bot.shop_categories_keyboard(self.user_id, styled=False, page=1)
        count = lambda kb: len([b for r in kb.inline_keyboard for b in r if (b.callback_data or "").startswith("api_shop_view_")])
        self.assertEqual((count(first), count(second)), (15, 1))


class JeroPurchaseTests(JeroBotBase, unittest.IsolatedAsyncioTestCase):
    async def test_no_supplier_call_before_confirmation(self):
        mapping = self.add_jero()
        await bot.continue_api_shop_purchase(self.context, self.user_id, self.token(mapping), 2)
        self.assertEqual(self.client.created, [])
        self.assertEqual(bot.user_wallet[self.user_id], 20.0)
        self.assertEqual(len(bot.jero_checkout_drafts), 1)
        self.assertIn("5.00", " ".join(self.texts()))
        # The legacy direct path is routed to confirmation as well.
        await bot.process_api_shop_purchase(self.context, self.user_id, self.token(mapping), 1)
        self.assertEqual(self.client.created, [])

    async def test_ordinary_purchase_delivery_and_wallet(self):
        mapping = self.add_jero(selling=2.5)
        draft = await self.confirm(mapping, 2)
        self.client.create_results.append(completed(["acc1|pw1", "acc2|pw2"]))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        self.assertEqual(record["state"], "completed")
        self.assertTrue(record["delivery_sent"])
        self.assertEqual(bot.user_wallet[self.user_id], 15.0)
        self.assertEqual(mapping["last_stock"], 8)
        payload = self.client.created[0]
        self.assertEqual(payload["product_id"], 12)
        self.assertEqual(payload["quantity"], 2)
        self.assertEqual(payload["external_order_id"], record["external_order_id"])
        joined = " ".join(self.texts())
        self.assertIn("acc1|pw1", joined)
        order = bot._jero_find_order(record["order_id"])
        self.assertEqual(order["status"], "Completed")
        self.assertEqual(bot._jero_find_tx(record["tx_id"])["status"], "Completed")
        for call in self.sent.await_args_list:
            self.assertEqual(call.args[0] if call.args else call.kwargs.get("chat_id"), self.user_id)

    async def test_duplicate_confirmation_charges_once(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping, 1)
        self.client.create_results.append(completed(["a|b"]))
        await asyncio.gather(
            bot.jero_confirm_draft(self.context, self.user_id, draft["id"]),
            bot.jero_confirm_draft(self.context, self.user_id, draft["id"]),
        )
        await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        self.assertEqual(len(self.client.created), 1)
        self.assertEqual(bot.user_wallet[self.user_id], 17.5)
        self.assertEqual(len(bot.jero_orders), 1)

    async def test_concurrent_orders_cannot_overspend_wallet(self):
        bot.user_wallet[self.user_id] = 3.0
        mapping = self.add_jero(selling=2.5)
        first = await self.confirm(mapping, 1)
        second = await self.confirm(mapping, 1)
        self.client.create_results.extend([completed(["a|b"]), completed(["c|d"])])
        await asyncio.gather(
            bot.jero_confirm_draft(self.context, self.user_id, first["id"]),
            bot.jero_confirm_draft(self.context, self.user_id, second["id"]),
        )
        self.assertEqual(len(self.client.created), 1)
        self.assertAlmostEqual(bot.user_wallet[self.user_id], 0.5)
        self.assertGreaterEqual(bot.user_wallet[self.user_id], 0)

    async def test_supplier_insufficient_balance_is_not_customer_wallet(self):
        mapping = self.add_jero(selling=2.5)
        draft = await self.confirm(mapping, 1)
        self.client.create_results.append(jero_api.JeroAPIError("Jero error: insufficient_balance", "insufficient_balance"))
        with patch.object(bot, "ADMIN_IDS", {999}):
            record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        self.assertEqual(record["state"], "failed")
        self.assertEqual(bot.user_wallet[self.user_id], 20.0)
        texts = " ".join(self.texts())
        self.assertNotIn(bot.t(self.user_id, "wallet_not_enough"), texts)
        self.assertIn("supplier", texts.lower())
        self.assertIn(999, [call.args[0] for call in self.sent.await_args_list if call.args])

    async def test_timeout_then_lookup_recovers_same_order(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping, 1)
        self.client.create_results.append(jero_api.JeroAPIError("timeout", "timeout", retryable=True, order_may_exist=True))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        self.assertEqual(record["state"], "unknown")      # timeout is not failed/refunded
        self.assertEqual(bot.user_wallet[self.user_id], 17.5)
        self.client.lookup_results.append(completed(["x|y"]))
        record = await bot.jero_reconcile_one(self.context.bot, record["external_order_id"])
        self.assertEqual(record["state"], "completed")
        self.assertEqual(self.client.lookups, [record["external_order_id"]])
        self.assertEqual(len(self.client.created), 1)
        self.assertEqual(bot.user_wallet[self.user_id], 17.5)

    async def test_same_id_resend_after_lookup_not_found(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping, 1)
        self.client.create_results.append(jero_api.JeroAPIError("net", "network", retryable=True, order_may_exist=True))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        ext = record["external_order_id"]
        self.client.lookup_results.append(jero_api.JeroAPIError("not found", "invalid_request"))
        self.client.create_results.append(completed(["r|s"]))
        record = await bot.jero_reconcile_one(self.context.bot, ext)
        self.assertEqual(record["state"], "completed")
        self.assertEqual(self.client.resent[0], self.client.created[0])   # identical payload and ID
        self.assertEqual(self.client.resent[0]["external_order_id"], ext)

    async def test_processing_then_completed_and_refunded(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping, 1)
        self.client.create_results.append(jero_api.normalize_order({"order_id": 9, "status": "processing", "items": []}))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        self.assertEqual(record["state"], "processing")
        self.assertEqual(bot._jero_find_order(record["order_id"])["status"], "Processing")
        self.assertEqual(mapping["last_stock"], 10)  # stock reduced only after completion
        self.assertIn(bot.t(self.user_id, "jero_order_pending"), " ".join(self.texts()))
        self.client.lookup_results.append(completed(["done|1"], order_id=9))
        record = await bot.jero_reconcile_one(self.context.bot, record["external_order_id"])
        self.assertEqual(record["state"], "completed")
        self.assertEqual(mapping["last_stock"], 9)

        draft = await self.confirm(mapping, 1)
        self.client.create_results.append(jero_api.normalize_order({"order_id": 10, "status": "processing", "items": []}))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        before = bot.user_wallet[self.user_id]
        self.client.lookup_results.append(jero_api.normalize_order({"order_id": 10, "status": "refunded", "message": "Install failed"}))
        record = await bot.jero_reconcile_one(self.context.bot, record["external_order_id"])
        self.assertEqual(record["state"], "refunded")
        self.assertEqual(bot.user_wallet[self.user_id], before + 2.5)

    async def test_double_refund_prevention(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping, 1)
        self.client.create_results.append(jero_api.normalize_order({"order_id": 3, "status": "processing", "items": []}))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        ext = record["external_order_id"]
        refunded = jero_api.normalize_order({"order_id": 3, "status": "refunded"})
        bot.jero_apply_result_sync(ext, refunded)
        bot.jero_apply_result_sync(ext, refunded)
        bot.jero_settle_rejected_sync(ext, jero_api.JeroAPIError("x", "out_of_stock"))
        record["state"] = "processing"           # simulate a stale replay
        bot.jero_apply_result_sync(ext, refunded)
        self.assertEqual(bot.user_wallet[self.user_id], 20.0)
        refunds = [tx for tx in bot.all_transactions if tx.get("type") == "Supplier Order Refund"]
        self.assertEqual(len(refunds), 1)

    async def test_completed_with_missing_items_goes_to_review(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping, 2)
        self.client.create_results.append(jero_api.normalize_order({"order_id": 4, "status": "completed", "items": ["only|one"]}))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        self.assertEqual(record["state"], "needs_review")
        self.assertEqual(bot.user_wallet[self.user_id], 15.0)   # no guessed refund
        self.assertFalse(record["delivery_sent"])

    async def test_delivery_failure_retries_without_new_purchase(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping, 1)
        self.client.create_results.append(completed(["k|v"]))
        calls = {"n": 0}

        async def flaky(chat_id, text=None, **kwargs):
            calls["n"] += 1
            if "k|v" in str(text) and calls.setdefault("failed", 0) == 0:
                calls["failed"] = 1
                raise RuntimeError("telegram down")

        self.context.bot.send_message = flaky
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        self.assertEqual(record["state"], "completed")
        self.assertFalse(record["delivery_sent"])
        record["next_check_at"] = 0
        await bot.jero_reconcile_once(self.context.bot)
        self.assertTrue(record["delivery_sent"])
        self.assertEqual(len(self.client.created), 1)

    async def test_restart_recovery_from_snapshot(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping, 1)
        self.client.create_results.append(jero_api.JeroAPIError("t", "timeout", retryable=True, order_may_exist=True))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        ext = record["external_order_id"]
        snapshot = copy.deepcopy(bot._json_safe(bot.build_state_snapshot()))
        bot.jero_orders.clear()
        bot.jero_fresh_reservations.clear()
        bot.apply_loaded_state(snapshot)
        self.assertIn(ext, bot.jero_orders)
        bot.jero_orders[ext]["next_check_at"] = 0
        self.client.lookup_results.append(completed(["z|z"]))
        await bot.jero_reconcile_once(self.context.bot)
        self.assertEqual(bot.jero_orders[ext]["state"], "completed")
        self.assertEqual(len(self.client.created), 1)

    async def test_wordpress_flow_password_handling_and_restart(self):
        mapping = self.add_jero("900001", title="Elementor Pro", stock=None, selling=10)
        token = self.token(mapping)
        await bot.continue_api_shop_purchase(self.context, self.user_id, token, 1)
        self.assertEqual(bot.user_state[self.user_id]["step"], "jero_wp_site")
        self.assertIn("Jero Accounts", " ".join(self.texts()))

        async def send_text(text):
            message = SimpleNamespace(text=text, reply_text=AsyncMock(), delete=AsyncMock())
            update = SimpleNamespace(message=message)
            await bot.jero_handle_text_step(update, self.context, self.user_id, bot.user_state[self.user_id], text.strip())
            return message

        await send_text("example.com")
        await send_text("admin")
        password_message = await send_text("S3cret pass!")
        password_message.delete.assert_awaited()
        draft = next(iter(bot.jero_checkout_drafts.values()))
        confirmation = self.texts()[-1]
        self.assertNotIn("S3cret", confirmation)
        self.assertIn("https://example.com", confirmation)

        self.client.create_results.append(jero_api.JeroAPIError("t", "timeout", retryable=True, order_may_exist=True))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        sent_payload = self.client.created[0]
        self.assertEqual(sent_payload["password"], "S3cret pass!")
        self.assertEqual((sent_payload["product_id"], sent_payload["quantity"]), (900001, 1))
        snapshot_text = json.dumps(bot._json_safe(bot.build_state_snapshot()), default=str)
        self.assertNotIn("S3cret", snapshot_text)
        self.assertNotIn("password", json.dumps(record["payload"]))

        # Restart: the password is gone; only lookups happen, never a new order.
        bot.jero_secret_inputs.clear()
        self.client.lookup_results.append(jero_api.JeroAPIError("not found", "invalid_request"))
        record = await bot.jero_reconcile_one(self.context.bot, record["external_order_id"])
        self.assertEqual(len(self.client.created), 1)
        self.assertEqual(self.client.resent, [])
        self.client.lookup_results.append(jero_api.normalize_order({"order_id": 8, "status": "completed", "message": "Installed"}))
        record = await bot.jero_reconcile_one(self.context.bot, record["external_order_id"])
        self.assertEqual(record["state"], "completed")
        self.assertIn(bot.t(self.user_id, "jero_wp_completed"), " ".join(self.texts()))
        self.assertEqual(bot.jero_secret_inputs, {})

    async def test_wordpress_cancel_clears_password(self):
        mapping = self.add_jero("900003", title="Rank Math PRO", stock=None, selling=8)
        bot.user_state[self.user_id] = {
            "step": "jero_wp_password", "api_callback_token": self.token(mapping),
            "site": "https://a.example", "username": "admin",
        }
        message = SimpleNamespace(text="pw", reply_text=AsyncMock(), delete=AsyncMock())
        await bot.jero_handle_text_step(SimpleNamespace(message=message), self.context, self.user_id,
                                        bot.user_state[self.user_id], "pw")
        draft_id = next(iter(bot.jero_checkout_drafts))
        self.assertIn(f"draft:{draft_id}", bot.jero_secret_inputs)
        bot.jero_cancel_draft(self.user_id, draft_id)
        self.assertEqual(bot.jero_secret_inputs, {})
        self.assertEqual(self.client.created, [])

    async def test_ui8_partial_fulfillment(self):
        mapping = self.add_jero("900002", title="UI8 download", stock=None, selling=3)
        bot.user_state[self.user_id] = {"step": "jero_ui8_links", "api_callback_token": self.token(mapping)}
        text = "https://ui8.net/a/products/one\nhttps://ui8.net/a/products/two\nhttps://ui8.net/a/products/two\nbad"
        message = SimpleNamespace(text=text, reply_text=AsyncMock())
        await bot.jero_handle_text_step(SimpleNamespace(message=message), self.context, self.user_id,
                                        bot.user_state[self.user_id], text)
        draft = next(iter(bot.jero_checkout_drafts.values()))
        self.assertEqual((draft["quantity"], draft["total"]), (2, "6.00"))
        confirmation = self.texts()[-1]
        self.assertIn("Duplicate", confirmation)
        self.assertIn("Invalid", confirmation)
        self.client.create_results.append(jero_api.normalize_order({"order_id": 6, "status": "processing", "items": []}))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        self.assertEqual(self.client.created[0]["links"], ["https://ui8.net/a/products/one", "https://ui8.net/a/products/two"])
        self.assertEqual(bot.user_wallet[self.user_id], 14.0)
        partial = jero_api.normalize_order({"order_id": 6, "status": "partial", "items": [
            {"status": "completed", "download_url": "https://sendgb.test/abc"},
            {"status": "refunded", "message": "Not available"},
        ]})
        self.client.lookup_results.append(partial)
        record = await bot.jero_reconcile_one(self.context.bot, record["external_order_id"])
        self.assertEqual(record["state"], "partial")
        self.assertEqual(bot.user_wallet[self.user_id], 17.0)    # one item refunded at selling price
        joined = " ".join(self.texts())
        self.assertIn("sendgb.test/abc", joined)
        self.assertIn(bot.t(self.user_id, "jero_ui8_valid_days"), joined)

    async def test_ui8_unclear_partial_goes_to_review(self):
        mapping = self.add_jero("900002", title="UI8", stock=None, selling=3)
        draft = await self.confirm(mapping, 2, links=["https://ui8.net/a/products/1", "https://ui8.net/a/products/2"])
        self.client.create_results.append(jero_api.normalize_order({
            "order_id": 7, "status": "partial", "refund": "1.10",
            "items": [{"status": "completed", "download_url": "https://sendgb.test/a"}],
        }))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        self.assertEqual(record["state"], "needs_review")
        self.assertEqual(bot.user_wallet[self.user_id], 14.0)   # unknown refund not guessed

    async def test_rejected_codes_refund_and_rate_limited_message(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping, 1)
        self.client.create_results.append(jero_api.JeroAPIError("busy", "rate_limited", retryable=True))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        self.assertEqual(record["state"], "failed")
        self.assertEqual(bot.user_wallet[self.user_id], 20.0)
        self.assertIn(bot.t(self.user_id, "jero_supplier_busy"), " ".join(self.texts()))

    async def test_persistence_failure_before_send_rolls_back(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping, 1)
        with patch.object(bot, "save_bot_state_strict", side_effect=RuntimeError("db down")):
            record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        self.assertEqual(record, {})
        self.assertEqual(self.client.created, [])
        self.assertEqual(bot.user_wallet[self.user_id], 20.0)
        self.assertEqual(bot.jero_orders, {})

    async def test_cross_worker_claim_prevents_second_finalize(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping, 1)
        self.client.create_results.append(jero_api.normalize_order({"order_id": 3, "status": "processing", "items": []}))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        key = ("seller_3", record["external_order_id"], "final")
        bot.jero_local_claims[key] = "other-instance"
        bot.jero_apply_result_sync(record["external_order_id"], jero_api.normalize_order({"order_id": 3, "status": "refunded"}))
        self.assertEqual(record["state"], "needs_review")
        self.assertEqual(bot.user_wallet[self.user_id], 17.5)

    async def test_unknown_outcome_becomes_review_after_24h_without_refund(self):
        mapping = self.add_jero()
        draft = await self.confirm(mapping, 1)
        self.client.create_results.append(jero_api.JeroAPIError("t", "timeout", retryable=True, order_may_exist=True))
        record = await bot.jero_confirm_draft(self.context, self.user_id, draft["id"])
        record["created_ts"] -= bot.JERO_REVIEW_AFTER_SECONDS + 5
        self.client.lookup_results.append(jero_api.JeroAPIError("down", "upstream_unavailable", retryable=True))
        record = await bot.jero_reconcile_one(self.context.bot, record["external_order_id"])
        self.assertEqual(record["state"], "needs_review")
        self.assertEqual(bot.user_wallet[self.user_id], 17.5)

    async def test_existing_providers_still_use_their_own_purchase_path(self):
        bot.ensure_user(self.user_id, SimpleNamespace(
            id=self.user_id, username="u", first_name="U", last_name="", is_bot=False,
        ))
        other = {"id": "77", "name": "Seller two product", "price": 1, "stock": 5, "_seller_provider_id": "seller_2"}
        mapping = bot.update_api_product_mapping(other, provider_id="seller_2", enabled=True,
                                                 category_id=bot.DEFAULT_CATEGORY_ID, selling_price=4)
        with patch.object(bot, "purchase_buyer_api_product", return_value={
            "order_code": "W1", "accounts": ["w|1"], "remaining_stock": 4,
        }) as legacy:
            await bot.continue_api_shop_purchase(self.context, self.user_id, self.token(mapping), 1)
        legacy.assert_called_once()
        self.assertEqual(legacy.call_args.args[3], "seller_2")
        self.assertEqual(self.client.created, [])
        self.assertEqual(bot.user_wallet[self.user_id], 16.0)


if __name__ == "__main__":
    unittest.main()
