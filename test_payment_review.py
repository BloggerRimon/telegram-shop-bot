"""Real disposable PostgreSQL tests; no real transfers or Telegram messages."""
import asyncio
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from decimal import Decimal

import bot
import test_onchain_customer as cases
from onchain import admin_ui, customer, engine, payment_review as review, store

TX = '0x' + 'c'*64


@unittest.skipUnless(os.getenv('ONCHAIN_TEST_DATABASE_URL'), 'disposable PostgreSQL required')
class PaymentReviewTests(unittest.TestCase):
    setUp = cases.CustomerGatewayTests.setUp
    tearDown = cases.CustomerGatewayTests.tearDown
    patch = cases.CustomerGatewayTests.patch
    ready = cases.CustomerGatewayTests.ready
    found = cases.CustomerGatewayTests.found
    confirm = cases.CustomerGatewayTests.confirm

    def pending(self):
        self.ready()
        inv = engine.create_customer_invoice(42, 'usdt_bep20', Decimal('5'))
        review.report_payment(inv['id'], 42, '<Buyer>', 'buyer')
        return inv

    def age(self, inv, minutes=11):
        with store.conn() as c:
            c.execute("UPDATE onchain_payment_reports SET reported_at=NOW()-make_interval(mins=>%s) WHERE invoice_id=%s",
                      (minutes, inv['id']))

    def draft(self, inv, tx=TX, actor=1):
        return review.prepare_manual(inv['public_ref'], tx, actor)[0]

    def test_clock_starts_only_once_and_not_for_abandoned_invoice(self):
        inv = self.pending()
        initial = review.review_record(inv['id'])['reported_at']
        self.assertEqual(review.delayed_invoices(), [])
        self.age(inv)
        aged = review.review_record(inv['id'])['reported_at']
        review.report_payment(inv['id'], 42)
        self.assertEqual(review.review_record(inv['id'])['reported_at'], aged)
        self.assertLess(aged, initial)
        self.assertEqual(review.delayed_invoices(), [{'id': inv['id']}])
        another = engine.create_customer_invoice(43, 'usdt_bep20', Decimal('6'))
        self.assertNotIn(another['id'], [r['id'] for r in review.delayed_invoices()])

    def test_admin_alert_has_identity_amount_network_and_unverified_hash(self):
        inv = self.pending(); self.age(inv)
        review.report_payment(inv['id'], 42, txid=TX)
        with patch.object(bot, 'ADMIN_IDS', {1, 2}):
            asyncio.run(review.notify_delays(bot)); asyncio.run(review.notify_delays(bot))
        self.assertEqual(self.sender.await_count, 2)
        text = self.sender.await_args_list[0].args[1]
        for value in ['&lt;Buyer&gt;', '42', inv['public_ref'], inv['destination'], TX, 'UNVERIFIED', '5.00']:
            self.assertIn(value, text)

    def test_failed_notification_retries_per_admin_without_resending_success(self):
        inv = self.pending(); self.age(inv)
        self.sender.side_effect = [RuntimeError('Telegram unavailable'), None, None]
        with patch.object(bot, 'ADMIN_IDS', {1, 2}):
            asyncio.run(review.notify_delays(bot)); asyncio.run(review.notify_delays(bot))
        self.assertEqual([x.args[0] for x in self.sender.await_args_list], [1, 2, 1])

    def test_notification_lease_prevents_parallel_claims_and_recovers_after_restart(self):
        inv = self.pending(); self.age(inv)
        with ThreadPoolExecutor(6) as pool:
            results = list(pool.map(lambda _: review.claim_alert(inv['id'], 1), range(6)))
        self.assertEqual(sum(results), 1)
        with store.conn() as c:
            c.execute("UPDATE onchain_delay_alerts SET lease_until=NOW()-interval '1 second'")
        self.assertTrue(review.claim_alert(inv['id'], 1))
        review.finish_alert(inv['id'], 1, True)
        self.assertFalse(review.claim_alert(inv['id'], 1))

    def test_unknown_hash_alert_and_later_hash_update(self):
        inv = self.pending(); self.age(inv)
        with patch.object(bot, 'ADMIN_IDS', {1}):
            asyncio.run(review.notify_delays(bot))
            self.assertIn('No transaction hash is known', self.sender.await_args.args[1])
            review.report_payment(inv['id'], 42, txid=TX)
            asyncio.run(review.notify_delays(bot)); asyncio.run(review.notify_delays(bot))
        self.assertEqual(self.sender.await_count, 2)
        self.assertIn(TX, self.sender.await_args.args[1])
        with self.assertRaises(ValueError):
            review.report_payment(inv['id'], 42, txid='0x'+'d'*64)

    def test_manual_credit_uses_existing_outbox_and_retries_once_only(self):
        inv = self.pending(); self.age(inv)
        token = self.draft(inv)
        self.assertEqual(review.approve_manual(token, 1)['result'], 'queued')
        self.assertEqual(review.approve_manual(token, 1)['result'], 'already_paid')
        self.assertEqual(bot.user_wallet, {})
        with patch.object(bot, 'save_bot_state_strict', side_effect=RuntimeError('database unavailable')):
            asyncio.run(customer.settle_pending(bot))
        asyncio.run(customer.settle_pending(bot)); asyncio.run(customer.settle_pending(bot))
        self.assertEqual(bot.user_wallet[42], 5)
        self.assertEqual(len(bot.credited_topups), 1)
        self.assertEqual(len(store.test_ledger()), 1)
        self.assertTrue(store.customer_settlement_complete(inv['id']))
        self.assertEqual(review.delayed_invoices(), [])
        self.assertEqual(store.recent_audit(1)[0]['action'], 'customer_payment_manual_approval')

    def test_two_admins_only_one_receipt_outbox(self):
        inv = self.pending(); self.age(inv)
        tokens = [self.draft(inv, actor=i) for i in (1, 2)]
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda x: review.approve_manual(x[0], x[1]), zip(tokens, (1, 2))))
        self.assertEqual(sorted(r['result'] for r in results), ['already_paid', 'queued'])
        self.assertEqual(len(store.pending_customer_settlements()), 1)

    def test_automatic_and_manual_race_produces_one_credit(self):
        inv = self.pending(); self.age(inv)
        token = self.draft(inv)
        with ThreadPoolExecutor(2) as pool:
            jobs = [pool.submit(review.approve_manual, token, 1), pool.submit(self.confirm, inv, TX)]
            for job in jobs:
                job.result()
        asyncio.run(customer.settle_pending(bot)); asyncio.run(customer.settle_pending(bot))
        self.assertEqual(bot.user_wallet[42], 5)
        self.assertEqual(len(bot.credited_topups), 1)
        self.assertTrue(store.customer_settlement_complete(inv['id']))

    def test_late_automatic_discovery_cannot_recredit_manual_receipt(self):
        inv = self.pending(); self.age(inv)
        review.approve_manual(self.draft(inv), 1)
        asyncio.run(customer.settle_pending(bot))
        self.confirm(inv, TX)
        asyncio.run(customer.settle_pending(bot))
        self.assertEqual(bot.user_wallet[42], 5)
        self.assertEqual(len(bot.credited_topups), 1)
        self.assertIn('manual_receipt_consumed', [x['reason'] for x in store.list_review()])

    def test_already_automatically_paid_cannot_be_approved_again(self):
        inv = self.pending(); self.age(inv)
        token = self.draft(inv)
        self.confirm(inv, TX)
        self.assertEqual(review.approve_manual(token, 1)['result'], 'already_paid')
        asyncio.run(customer.settle_pending(bot))
        self.assertEqual(bot.user_wallet[42], 5)
        self.assertIsNone(review.review_record(inv['id'])['manual_actor'])

    def test_same_receipt_cannot_credit_another_invoice(self):
        inv = self.pending(); self.age(inv)
        review.approve_manual(self.draft(inv), 1)
        second = engine.create_customer_invoice(42, 'usdt_bep20', Decimal('5'))
        review.report_payment(second['id'], 42); self.age(second)
        with self.assertRaisesRegex(ValueError, 'already manually'):
            review.approve_manual(self.draft(second), 1)
        self.assertEqual(store.get_invoice(second['id'])['status'], 'awaiting')

    def test_already_credited_transfer_cannot_be_manually_reused(self):
        inv = self.pending(); self.age(inv); self.confirm(inv, TX)
        second = engine.create_customer_invoice(42, 'usdt_bep20', Decimal('5'))
        review.report_payment(second['id'], 42); self.age(second)
        with self.assertRaisesRegex(ValueError, 'already allocated'):
            review.approve_manual(self.draft(second), 1)

    def test_cancelled_early_wrong_actor_and_expired_draft_are_rejected(self):
        inv = self.pending(); token = self.draft(inv)
        with self.assertRaisesRegex(ValueError, '10 minutes'):
            review.approve_manual(token, 1)
        with self.assertRaises(ValueError):
            review.approve_manual(token, 2)
        self.age(inv)
        with store.conn() as c:
            c.execute("UPDATE onchain_manual_drafts SET expires_at=NOW()-interval '1 second'")
        with self.assertRaises(ValueError):
            review.approve_manual(token, 1)
        token = self.draft(inv)
        store.cancel_invoice(inv['id'], 1)
        with self.assertRaises(ValueError):
            review.approve_manual(token, 1)
        self.assertEqual(review.delayed_invoices(), [])

    def test_customer_cannot_forge_admin_callback_or_claim_another_invoice(self):
        inv = self.pending(); self.age(inv)
        query = SimpleNamespace(from_user=SimpleNamespace(id=42), message=SimpleNamespace(reply_text=AsyncMock()))
        with patch.object(review, 'approve_manual') as approve:
            asyncio.run(admin_ui.callback(query, 'oc:manual:' + self.draft(inv), lambda x: x == 1))
            approve.assert_not_called()
        with self.assertRaises(ValueError):
            review.report_payment(inv['id'], 43, txid=TX)

    def test_paid_but_unsettled_still_alerts_without_creating_another_credit(self):
        inv = self.pending(); self.age(inv); self.confirm(inv, TX)
        self.assertEqual(review.delayed_invoices(), [{'id': inv['id']}])
        asyncio.run(customer.settle_pending(bot))
        self.assertEqual(review.delayed_invoices(), [])

    def test_network_hash_formats_and_sol_case(self):
        for network in ('tron', 'bitcoin', 'litecoin'):
            self.assertEqual(review.normalize_txid(network, 'A'*64), 'a'*64)
        for network in ('ethereum', 'bsc'):
            self.assertEqual(review.normalize_txid(network, '0x'+'A'*64), '0x'+'a'*64)
        sig = 'Ab'*44
        self.assertEqual(review.normalize_txid('solana', sig), sig)
        for network in ('tron', 'ethereum', 'bsc', 'bitcoin', 'litecoin', 'solana'):
            with self.assertRaises(ValueError):
                review.normalize_txid(network, 'not-a-transaction')

    def test_verify_button_persists_report_identity(self):
        self.ready()
        inv = engine.create_customer_invoice(42, 'usdt_bep20', Decimal('5'))
        query = SimpleNamespace(from_user=SimpleNamespace(full_name='Buyer Name', username='buyer'),
                                message=SimpleNamespace(reply_text=AsyncMock()))
        asyncio.run(customer.verify(bot, query, 42, inv['id']))
        r = review.review_record(inv['id'])
        self.assertEqual(r['buyer_name'], 'Buyer Name')
        self.assertIsNotNone(r['reported_at'])
        self.assertIn('/payment_tx ' + inv['public_ref'], query.message.reply_text.await_args.args[0])

    def test_new_alerts_not_starved_by_already_notified_unresolved_payments(self):
        inv = self.pending(); self.age(inv)
        review.claim_alert(inv['id'], 1); review.finish_alert(inv['id'], 1, True)
        self.assertEqual(review.delayed_invoices(1), [])
        self.assertEqual(review.delayed_invoices(2), [{'id': inv['id']}])

    def test_known_wrong_amount_and_different_matched_tx_cannot_be_approved(self):
        inv = self.pending(); self.age(inv)
        found = self.found(inv); found['final'] = False
        found['transfers'][0]['amount_units'] += 1
        from onchain import chains
        with patch.object(chains, 'inspect_tx', return_value=found):
            self.eng.process_hint('bsc', TX, 0)
        with self.assertRaisesRegex(ValueError, 'does not match'):
            review.approve_manual(self.draft(inv), 1)
        found = self.found(inv); found['final'] = False
        with patch.object(chains, 'inspect_tx', return_value=found):
            self.eng.process_hint('bsc', '0x'+'d'*64, 0)
        with self.assertRaisesRegex(ValueError, 'different transaction'):
            review.approve_manual(self.draft(inv, '0x'+'e'*64), 1)
