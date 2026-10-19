import json
import threading
import time
import unittest
import uuid

from cdms import db
from cdms.connectors import Batch
from cdms.connectors.shopify import ShopifyConnector
from cdms.mocks.shopify_server import MockShopify
from cdms.connectors.vietful import VietfulClient
from cdms.mocks.data import inv_changed_event
from cdms.mocks.vietful_api import MockVietful
from cdms.webhook import serve as serve_webhooks, sign
from cdms.stats import active_workers
from cdms.worker import run_worker

try:
    import redis
    from cdms import config

    _conn = db.connect()
    db.init_db(_conn)
    _conn.close()
    redis.Redis.from_url(config.REDIS_URL).ping()
    AVAILABLE = True
except Exception as e:
    AVAILABLE, REASON = False, str(e)


def wait_until(cond, timeout=20.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.2)
    return False


@unittest.skipUnless(AVAILABLE, "PostgreSQL/Redis not available")
class IntegrationTest(unittest.TestCase):
    def setUp(self):
        self.conn = db.connect()
        self.tid = "it_" + uuid.uuid4().hex[:8]
        db.create_tenant(self.conn, self.tid)

    def tearDown(self):
        self.conn.execute(f"DROP SCHEMA IF EXISTS t_{self.tid} CASCADE")
        self.conn.execute("DELETE FROM cdms.workers WHERE tenant_id = %s", (self.tid,))
        self.conn.execute("DELETE FROM cdms.rejected_records WHERE tenant_id = %s", (self.tid,))
        self.conn.close()

    def add_worker(self, kind, cfg, interval=0):
        wid = f"{self.tid}-{kind}"
        self.conn.execute(
            "INSERT INTO cdms.workers (id, tenant_id, kind, interval_seconds, config, desired_state)"
            " VALUES (%s, %s, %s, %s, %s, 'running')",
            (wid, self.tid, kind, interval, json.dumps(cfg)))
        return wid

    def count(self, sql, *params):
        return self.conn.execute(sql, params).fetchone()["n"]

    def start(self, wid):
        ev = threading.Event()
        th = threading.Thread(target=run_worker, args=(wid, ev, "test-node"), daemon=True)
        th.start()
        return th, ev

    def stop(self, wid, th, ev):
        self.conn.execute("UPDATE cdms.workers SET desired_state='stopped' WHERE id=%s", (wid,))
        ev.set()
        th.join(10)

    def resume(self, wid):
        self.conn.execute("UPDATE cdms.workers SET desired_state='running' WHERE id=%s", (wid,))
        return self.start(wid)

    def checkpoint(self, wid):
        return self.conn.execute("SELECT checkpoint FROM cdms.workers WHERE id=%s", (wid,)).fetchone()["checkpoint"]

    def http(self, server):
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_address[1]}"

    def vietful_setup(self, cfg_extra=None, **mock_kw):
        mock = MockVietful(retry_delay_s=0.2, **mock_kw)
        api = self.http(mock.serve("127.0.0.1", 0))
        hooks = self.http(serve_webhooks("127.0.0.1", 0))
        stream = f"vietful:{self.tid}"
        cfg = {"stream": stream, "api_base": api, "client_id": self.tid, "client_secret": f"{self.tid}-secret",
               "block_ms": 200, "rate_per_s": 500, **(cfg_extra or {})}
        wid = self.add_worker("vietful", cfg)
        VietfulClient(api, self.tid, f"{self.tid}-secret").subscribe(f"{hooks}/webhooks/vietful/{self.tid}")
        r = redis.Redis.from_url(config.REDIS_URL, decode_responses=True)
        self.addCleanup(r.delete, stream)
        return mock, wid, stream, r, hooks

    def stream_tail(self, r, stream):
        last = r.xrevrange(stream, count=1)
        return last[0][0] if last else "0-0"

    def test_vietful_backfill_webhooks_stop_resume(self):
        mock, wid, stream, r, _ = self.vietful_setup(n_products=40, bad_ratio=0.1, error_rate=0.1,
                                                     event_fault_ratio=0.3, event_faults=("duplicate",))
        th, ev = self.start(wid)
        self.assertTrue(wait_until(lambda: self.checkpoint(wid).get("backfill_done")))
        n_rows = self.count(f"SELECT count(*) n FROM t_{self.tid}.products")
        self.assertGreater(n_rows, 0)
        self.assertGreater(self.count("SELECT count(*) n FROM cdms.rejected_records WHERE tenant_id=%s", self.tid), 0)
        self.assertGreaterEqual(active_workers(self.conn)["active"], 1)
        self.stop(wid, th, ev)
        cp = self.checkpoint(wid)["last_id"]

        mock.change_inventory(self.tid, 15)
        self.assertTrue(wait_until(lambda: r.xlen(stream) >= 15))
        time.sleep(1)
        self.assertEqual(self.checkpoint(wid)["last_id"], cp)

        th, ev = self.resume(wid)
        tail = self.stream_tail(r, stream)
        self.assertTrue(wait_until(lambda: self.checkpoint(wid)["last_id"] == tail))
        self.stop(wid, th, ev)

        self.assertEqual(self.count(f"SELECT count(*) n FROM t_{self.tid}.products"), n_rows)
        self.assertTrue(self.assert_stock_matches_vietful(mock))
        self.assertGreater(self.count(
            "SELECT coalesce(sum(updated),0) n FROM cdms.sync_runs WHERE worker_id=%s", wid), 0)

        acc = mock.account(self.tid)
        mock.push(acc, inv_changed_event("e-unknown", "KQ7", "QUICK_STORE", [
            {"unitCode": "CAI", "sku": "x", "partnerSKU": "NO-SUCH-SKU", "conditionTypeCode": "NEW", "changeQty": 1}],
            int(time.time())))
        th, ev = self.resume(wid)
        self.assertTrue(wait_until(lambda: self.count(
            "SELECT count(*) n FROM cdms.rejected_records WHERE tenant_id=%s AND errors LIKE %s",
            self.tid, "%NO-SUCH-SKU%unknown%") == 1))
        self.stop(wid, th, ev)

    def assert_stock_matches_vietful(self, mock):
        acc = mock.account(self.tid)
        for row in self.conn.execute(f"SELECT sku, quantity FROM t_{self.tid}.products").fetchall():
            expected = sum(i["availableQty"] for i in acc.inventory
                           if i["partnerSKU"] == row["sku"] and i["conditionTypeCode"] == "NEW")
            if row["quantity"] != expected:
                return False
        return True

    def test_vietful_reconcile_repairs_lost_webhooks(self):
        mock, wid, stream, r, _ = self.vietful_setup({"reconcile_every_s": 1}, n_products=20, bad_ratio=0,
                                                     event_fault_ratio=1.0, event_faults=("bad_signature",))
        th, ev = self.start(wid)
        self.assertTrue(wait_until(lambda: self.checkpoint(wid).get("backfill_done")))
        mock.change_inventory(self.tid, 10)
        time.sleep(0.5)
        self.assertEqual(r.xlen(stream), 0)
        self.assertTrue(wait_until(lambda: self.assert_stock_matches_vietful(mock), timeout=20))
        self.stop(wid, th, ev)

    def test_webhook_receiver_verifies_hmac(self):
        import urllib.error
        import urllib.request
        _, _, stream, r, hooks = self.vietful_setup(n_products=5)
        url = f"{hooks}/webhooks/vietful/{self.tid}"
        body = json.dumps({"event": "INV_CHANGED", "id": "x", "timestamp": 1, "items": []}).encode()

        def post(sig):
            req = urllib.request.Request(url, data=body, method="POST", headers={"x-vf-hmacsha256": sig})
            try:
                return urllib.request.urlopen(req, timeout=5).status
            except urllib.error.HTTPError as e:
                return e.code

        self.assertEqual(post(sign(body, f"{self.tid}-secret")), 200)
        self.assertEqual(post(sign(body, "attacker")), 401)
        self.assertEqual(post(""), 401)
        self.assertEqual(r.xlen(stream), 1)
        self.assertEqual(self.count("SELECT count(*) n FROM cdms.rejected_records WHERE tenant_id=%s"
                                    " AND errors LIKE %s", self.tid, "%HMAC%"), 2)

    def test_shopify_pull_idempotent_and_master_shared(self):
        mock = MockShopify(n_products=60, bad_ratio=0.1, error_rate=0.2)
        srv = mock.serve("127.0.0.1", 0)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        url = f"http://127.0.0.1:{srv.server_address[1]}/{self.tid}"
        try:
            conn_cfg = {"shop_url": url, "access_token": f"{self.tid}-token"}
            wid = self.add_worker("shopify", conn_cfg, interval=1)
            th, ev = self.start(wid)
            self.assertTrue(wait_until(lambda: (self.checkpoint(wid) or {}).get("since") is not None))
            n1 = self.count(f"SELECT count(*) n FROM t_{self.tid}.products")
            self.assertGreater(n1, 0)

            mock.shop(self.tid).mutate(5)
            self.assertTrue(wait_until(lambda: self.count(
                "SELECT coalesce(sum(updated),0) n FROM cdms.sync_runs WHERE worker_id=%s", wid) >= 1))
            self.stop(wid, th, ev)
            self.assertEqual(self.count(f"SELECT count(*) n FROM t_{self.tid}.products"), n1)

            self.assertEqual(
                self.count("SELECT count(*) n FROM master.products"),
                self.count("SELECT count(DISTINCT gtin) n FROM master.products"))
        finally:
            srv.shutdown()
            srv.server_close()

    def test_master_same_product_upc_and_ean_is_one_row(self):
        from datetime import datetime, timezone
        from cdms.normalize import Item
        from cdms.store import write_items
        now = datetime.now(timezone.utc)
        upc, ean = "036000291452", "0036000291452"
        items = [Item("vietful", f"{self.tid}-1", "A1", "Tissue", now, barcode=upc, brand="X"),
                 Item("shopify", f"{self.tid}-2", "B1", "Tissue", now, barcode=ean, unit="Box")]
        with self.conn.transaction():
            write_items(self.conn, self.tid, items)
        row = self.conn.execute("SELECT * FROM master.products WHERE gtin = %s", ("00036000291452",)).fetchone()
        self.assertEqual((row["brand"], row["unit"]), ("X", "Box"))
        self.assertEqual(self.count("SELECT count(*) n FROM master.products WHERE gtin LIKE %s", "%36000291452"), 1)
        self.conn.execute("DELETE FROM master.products WHERE gtin = %s", ("00036000291452",))

    def test_lease_prevents_double_run(self):
        mock = MockShopify(n_products=5, bad_ratio=0)
        url = self.http(mock.serve("127.0.0.1", 0))
        wid = self.add_worker("shopify", {"shop_url": f"{url}/{self.tid}", "access_token": f"{self.tid}-token"}, 30)
        ev_a, ev_b = threading.Event(), threading.Event()
        a = threading.Thread(target=run_worker, args=(wid, ev_a, "node-a"), daemon=True)
        a.start()
        self.assertTrue(wait_until(lambda: self.conn.execute(
            "SELECT owner FROM cdms.workers WHERE id=%s", (wid,)).fetchone()["owner"] == "node-a"))
        b = threading.Thread(target=run_worker, args=(wid, ev_b, "node-b"), daemon=True)
        b.start()
        b.join(5)
        self.assertFalse(b.is_alive())
        self.assertTrue(a.is_alive())
        self.stop(wid, a, ev_a)

    def test_shopify_connector_resumes_mid_pagination(self):
        mock = MockShopify(n_products=30, bad_ratio=0)
        srv = mock.serve("127.0.0.1", 0)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            c = ShopifyConnector(f"http://127.0.0.1:{srv.server_address[1]}/s1", "s1-token")
            b1: Batch = c.fetch({}, 10)
            self.assertTrue(b1.more)
            b2 = c.fetch(b1.checkpoint, 10)
            ids1 = {p["id"] for p in b1.records}
            ids2 = {p["id"] for p in b2.records}
            self.assertFalse(ids1 & ids2)
        finally:
            srv.shutdown()
            srv.server_close()


if __name__ == "__main__":
    unittest.main()
