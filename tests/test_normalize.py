import json
import random
import unittest
from datetime import datetime, timezone

from cdms.connectors.vietful import VietfulConnector
from cdms.mocks.data import (base_catalog, corrupt_shopify, corrupt_vietful_product, shopify_product,
                             to_product_dto, vietful_inventory, vietful_product)
from cdms.normalize import Item, Rejected, from_shopify, from_vietful, gtin_is_valid
from cdms.webhook import sign


def vietful_record(product, inventory, event_time=None):
    return {"kind": "product", "product": product, "inventory": inventory,
            "event_time": event_time, "fetched_at": datetime.now(timezone.utc)}


class GtinTest(unittest.TestCase):
    def test_check_digit(self):
        self.assertTrue(gtin_is_valid("8934563138165"))
        self.assertFalse(gtin_is_valid("8934563138166"))
        self.assertFalse(gtin_is_valid("12345"))
        self.assertTrue(all(gtin_is_valid(p["gtin"]) for p in base_catalog()))


class VietfulTest(unittest.TestCase):
    def setUp(self):
        self.rng = random.Random(1)
        self.now = datetime.now(timezone.utc)
        self.full = vietful_product(base_catalog()[0], 0, "ACM")

    def test_product_dto_and_inventory(self):
        dto = {**to_product_dto(self.full), "hsCode": "0401.20", "countryOfOrigin": "VN"}
        inv = vietful_inventory(self.full, self.rng, self.now)
        item = from_vietful(dto, inv, None, self.now)
        self.assertEqual(item.sku, self.full["partnerSKU"])
        self.assertEqual(item.barcode, self.full["sku"])
        self.assertEqual(item.quantity, sum(r["availableQty"] for r in inv))
        self.assertEqual(item.hs_code, "040120")
        self.assertEqual(item.unit, self.full["units"][0]["unitCode"])

    def test_non_gtin_sku_is_not_an_error(self):
        dto = {**to_product_dto(self.full), "sku": "IPHONE001"}
        item = from_vietful(dto, [], None, self.now)
        self.assertIsNone(item.barcode)

    def test_every_product_fault_is_rejected(self):
        for _ in range(40):
            p = vietful_product(base_catalog()[1], 1, "ACM")
            inv = vietful_inventory(p, self.rng, self.now)
            fault = corrupt_vietful_product(p, inv, self.rng)
            res = VietfulConnector.parse(vietful_record(to_product_dto(p), inv))
            self.assertIsInstance(res[0], Rejected, fault)

    def test_hash_stable_for_same_state(self):
        inv = vietful_inventory(self.full, self.rng, self.now)
        a = from_vietful(self.full, inv, datetime(2026, 1, 1, tzinfo=timezone.utc))
        b = from_vietful(self.full, inv, datetime(2026, 1, 2, tzinfo=timezone.utc))
        self.assertEqual(a.content_hash(), b.content_hash())

    def test_hmac_matches_vietful_definition(self):
        body = json.dumps({"event": "INV_CHANGED"}).encode()
        self.assertEqual(sign(body, "s3cret"), sign(body, "s3cret"))
        self.assertNotEqual(sign(body, "s3cret"), sign(body, "other"))
        self.assertEqual(len(sign(body, "s3cret")), 44)


class ShopifyTest(unittest.TestCase):
    def setUp(self):
        self.rng = random.Random(2)
        self.now = datetime.now(timezone.utc)

    def test_one_item_per_variant(self):
        p = shopify_product(base_catalog()[3], 3, self.rng, self.now)
        res = from_shopify(p)
        self.assertEqual(len(res), len(p["variants"]["edges"]))
        self.assertTrue(all(isinstance(r, Item) for r in res))
        self.assertEqual(res[0].hs_code, base_catalog()[3]["hs_code"])

    def test_faults_rejected(self):
        for _ in range(60):
            p = shopify_product(base_catalog()[5], 5, self.rng, self.now)
            fault = corrupt_shopify(p, self.rng)
            self.assertTrue(any(isinstance(r, Rejected) for r in from_shopify(p)), fault)


if __name__ == "__main__":
    unittest.main()
