from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

BRANDS = [
    ("Vinamilk", "Dairy", "040120", "VN"),
    ("TH true MILK", "Dairy", "040120", "VN"),
    ("Acecook", "Instant food", "190230", "VN"),
    ("Masan", "Sauces", "210390", "VN"),
    ("Trung Nguyen", "Coffee", "090121", "VN"),
    ("Bitis", "Footwear", "640299", "VN"),
    ("Unilever", "Personal care", "330510", "TH"),
]
NOUNS = ["Fresh Milk", "Yogurt", "Noodles", "Fish Sauce", "Ground Coffee", "Sandals", "Shampoo", "Soy Sauce"]
UNITS = [("CAI", "Cái"), ("HOP", "Hộp"), ("CHAI", "Chai"), ("GOI", "Gói"), ("DOI", "Đôi")]
WAREHOUSES = ["KQ7", "TNA", "HN1"]
CHANGED_REASONS = ["PACKING_COMPLETED", "RECEIVING_COMPLETED", "ADJUSTMENT_COMPLETED",
                   "OR_CANCELLATION_COMPLETED", "FREEZE_COMPLETED", "QUICK_STORE"]


def make_gtin13(body12: str) -> str:
    digits = [int(c) for c in body12]
    total = sum(d * (3 if i % 2 == 0 else 1) for i, d in enumerate(reversed(digits)))
    return body12 + str((10 - total % 10) % 10)


def break_checksum(gtin: str) -> str:
    return gtin[:-1] + str((int(gtin[-1]) + 1) % 10)


def base_catalog(n: int = 300, seed: int = 42) -> list[dict]:
    rng = random.Random(seed)
    out = []
    for i in range(n):
        brand, cat, hs, country = rng.choice(BRANDS)
        unit = rng.choice(UNITS)
        out.append({
            "gtin": make_gtin13(f"893{i:09d}"),
            "name": f"{brand} {rng.choice(NOUNS)} {rng.choice([100, 180, 250, 500, 1000])}",
            "brand": brand, "category": cat, "hs_code": hs, "country": country,
            "unit_code": unit[0], "unit_name": unit[1],
            "price": rng.randrange(10, 500) * 1000,
        })
    return out


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def dotnet_ts(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def vietful_product(base: dict, idx: int, prefix: str) -> dict:
    return {
        "productId": 100000 + idx,
        "sku": base["gtin"],
        "partnerSKU": f"{prefix}-{base['gtin'][-6:]}",
        "productName": base["name"],
        "categories": [{"categoryCode": base["category"].upper().replace(" ", "_"),
                        "categoryName": base["category"]}],
        "units": [{"unitCode": base["unit_code"], "unitName": base["unit_name"],
                   "weight": 250.0, "length": 10.0, "width": 5.0, "height": 15.0}],
        "color": None, "size": None,
        "description": f"{base['name']} by {base['brand']}",
        "isActive": True,
        "assetType": "Single",
        "hasSerial": False, "hasExpiration": base["category"] in ("Dairy", "Instant food", "Sauces"),
        "hsCode": base["hs_code"],
        "countryOfOrigin": base["country"],
        "refCode": None, "avatarURL": None, "packingNote": None,
        "isPhysical": True, "isExpiryDate": False, "isLotNumber": False, "isTrackingSerial": False,
        "outboundType": "NotDefined", "productBarcodes": [], "productBundles": [],
    }


PRODUCT_DTO_FIELDS = ["productId", "sku", "partnerSKU", "productName", "categories", "units", "color",
                      "size", "description", "isActive", "assetType", "hasSerial", "hasExpiration"]


def to_product_dto(full: dict) -> dict:
    dto = {k: full.get(k) for k in PRODUCT_DTO_FIELDS}
    dto["units"] = [u["unitCode"] for u in full.get("units") or []]
    return dto


def vietful_inventory(product: dict, rng: random.Random, now: datetime) -> list[dict]:
    rows = []
    for wh in rng.sample(WAREHOUSES, rng.choice([1, 1, 2])):
        physical = rng.randrange(0, 500)
        freeze = rng.choice([0, 0, 0, 5])
        pending_out = rng.randrange(0, 20)
        rows.append({
            "warehouseCode": wh,
            "sku": product["sku"], "partnerSKU": product["partnerSKU"],
            "unitCode": product["units"][0]["unitCode"],
            "conditionTypeCode": "NEW",
            "categoryCode": product["categories"][0]["categoryCode"],
            "categoryName": product["categories"][0]["categoryName"],
            "physicalQty": physical, "freezeQty": freeze,
            "pendingOutQty": pending_out, "pendingInQty": 0, "inTransitQty": 0,
            "availableQty": max(0, physical - freeze - pending_out),
            "lastUpdatedDate": dotnet_ts(now),
        })
    return rows


VIETFUL_PRODUCT_FAULTS = ["missing_name", "bad_barcode", "missing_partner_sku", "negative_qty"]


def corrupt_vietful_product(product: dict, inventory: list[dict], rng: random.Random) -> str:
    fault = rng.choice(VIETFUL_PRODUCT_FAULTS)
    if fault == "missing_name":
        product["productName"] = ""
    elif fault == "bad_barcode":
        product["sku"] = break_checksum(product["sku"])
        for r in inventory:
            r["sku"] = product["sku"]
    elif fault == "missing_partner_sku":
        product["partnerSKU"] = None
        for r in inventory:
            r["partnerSKU"] = None
    elif fault == "negative_qty":
        inventory[0]["availableQty"] = -rng.randrange(1, 50)
    return fault


def inv_changed_event(event_id: str, warehouse: str, reason: str, items: list[dict], ts: int) -> dict:
    return {"event": "INV_CHANGED", "id": event_id, "timestamp": ts,
            "warehouseCode": warehouse, "changedReason": reason, "items": items}


def shopify_product(base: dict, idx: int, rng: random.Random, updated: datetime) -> dict:
    pid = 7000000000 + idx
    n_var = rng.choice([1, 1, 1, 2, 3])
    variants = []
    for v in range(n_var):
        title = "Default Title" if n_var == 1 else ["Small", "Medium", "Large"][v]
        variants.append({"node": {
            "id": f"gid://shopify/ProductVariant/{pid * 10 + v}",
            "title": title,
            "sku": f"SH-{base['gtin'][-6:]}-{v}",
            "barcode": base["gtin"] if v == 0 else None,
            "price": f"{base['price'] * (1 + v * 0.2):.2f}",
            "inventoryQuantity": rng.randrange(0, 300),
            "inventoryItem": {"harmonizedSystemCode": base["hs_code"], "countryCodeOfOrigin": base["country"]},
        }})
    created = updated - timedelta(days=rng.randrange(1, 300))
    return {
        "id": f"gid://shopify/Product/{pid}",
        "title": base["name"],
        "handle": base["name"].lower().replace(" ", "-"),
        "vendor": base["brand"],
        "productType": base["category"],
        "status": "ACTIVE",
        "createdAt": iso(created),
        "updatedAt": iso(updated),
        "variants": {"edges": variants},
    }


SHOPIFY_FAULTS = ["missing_title", "missing_sku", "negative_qty", "bad_price", "bad_barcode", "no_variants"]


def corrupt_shopify(p: dict, rng: random.Random) -> str:
    fault = rng.choice(SHOPIFY_FAULTS)
    v = p["variants"]["edges"][0]["node"]
    if fault == "missing_title":
        p["title"] = ""
    elif fault == "missing_sku":
        v["sku"] = None
    elif fault == "negative_qty":
        v["inventoryQuantity"] = -5
    elif fault == "bad_price":
        v["price"] = "free"
    elif fault == "bad_barcode":
        v["barcode"] = break_checksum(v["barcode"]) if v.get("barcode") else "8930000000000"
    elif fault == "no_variants":
        p["variants"]["edges"] = []
    return fault
