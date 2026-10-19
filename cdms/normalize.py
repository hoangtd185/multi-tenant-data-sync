from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation


class InvalidRecord(Exception):
    def __init__(self, errors: list[str]):
        super().__init__("; ".join(errors))
        self.errors = errors


@dataclass
class Item:
    source: str
    source_id: str
    sku: str
    name: str
    source_updated_at: datetime
    barcode: str | None = None
    brand: str | None = None
    category: str | None = None
    unit: str | None = None
    status: str | None = None
    price: Decimal | None = None
    quantity: int | None = None
    hs_code: str | None = None
    country_of_origin: str | None = None
    event_time: datetime | None = None
    raw: dict = field(default_factory=dict)

    def content_hash(self) -> str:
        payload = [
            self.sku, self.name, self.barcode, self.brand, self.category, self.unit, self.status,
            str(self.price) if self.price is not None else None, self.quantity,
            self.hs_code, self.country_of_origin,
        ]
        return hashlib.sha256(json.dumps(payload, ensure_ascii=False).encode()).hexdigest()


@dataclass
class Rejected:
    payload: str
    errors: list[str]


def gtin_is_valid(code: str) -> bool:
    if not code.isdigit() or len(code) not in (8, 12, 13, 14):
        return False
    digits = [int(c) for c in code]
    body, check = digits[:-1], digits[-1]
    total = sum(d * (3 if i % 2 == 0 else 1) for i, d in enumerate(reversed(body)))
    return (10 - total % 10) % 10 == check


def _gtin(value, errors: list[str], field_name: str = "barcode") -> str | None:
    if value in (None, ""):
        return None
    value = str(value).strip()
    if value.isdigit() and len(value) in (8, 12, 13, 14):
        if not gtin_is_valid(value):
            errors.append(f"{field_name}: invalid GTIN check digit ({value!r})")
        return value
    return None


def _parse_ts(value, field_name: str, errors: list[str]) -> datetime | None:
    if not value or not isinstance(value, str):
        errors.append(f"{field_name}: missing")
        return None
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        errors.append(f"{field_name}: not an ISO-8601 timestamp ({value!r})")
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def _parse_price(value, errors: list[str]) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        price = Decimal(str(value))
    except InvalidOperation:
        errors.append(f"price: not a number ({value!r})")
        return None
    if price < 0:
        errors.append(f"price: negative ({value})")
    return price


def _parse_qty(value, name: str, errors: list[str]) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        errors.append(f"{name}: not an integer ({value!r})")
        return None
    if value < 0:
        errors.append(f"{name}: negative ({value})")
    return value


def _req_str(d: dict, key: str, errors: list[str]) -> str:
    v = d.get(key)
    if not isinstance(v, str) or not v.strip():
        errors.append(f"{key}: missing")
        return ""
    return v.strip()


def _hs(value) -> str | None:
    if not value:
        return None
    digits = "".join(c for c in str(value) if c.isdigit())
    return digits or None


def from_vietful(product: dict, inventory: list[dict], event_time: datetime | None = None,
                 fetched_at: datetime | None = None) -> Item:
    errors: list[str] = []
    if not isinstance(product.get("productId"), int):
        errors.append("productId: missing")
    sku = _req_str(product, "partnerSKU", errors)
    name = _req_str(product, "productName", errors)

    qty, latest = 0, None
    for row in inventory:
        q = _parse_qty(row.get("availableQty"), f"availableQty[{row.get('warehouseCode')}]", errors)
        if q is not None and row.get("conditionTypeCode", "NEW") == "NEW":
            qty += q
        ts = _parse_ts(row.get("lastUpdatedDate"), "lastUpdatedDate", errors)
        if ts and (latest is None or ts > latest):
            latest = ts
    updated = latest or event_time or fetched_at or datetime.now(timezone.utc)

    categories = product.get("categories") or []
    units = product.get("units") or []
    unit = units[0] if units else None
    if isinstance(unit, dict):
        unit = unit.get("unitCode")
    item = Item(
        source="vietful",
        source_id=str(product.get("productId")),
        sku=sku,
        name=name,
        barcode=_gtin(product.get("sku"), errors, "sku"),
        category=categories[0].get("categoryName") if categories else None,
        unit=unit,
        status="ACTIVE" if product.get("isActive") else "INACTIVE",
        quantity=qty if inventory else None,
        hs_code=_hs(product.get("hsCode")),
        country_of_origin=product.get("countryOfOrigin"),
        source_updated_at=updated,
        event_time=event_time,
        raw={"product": product, "inventory": inventory},
    )
    if errors:
        raise InvalidRecord(errors)
    return item


def from_shopify(product: dict) -> list[Item | Rejected]:
    out: list[Item | Rejected] = []
    perrors: list[str] = []
    title = _req_str(product, "title", perrors)
    updated = _parse_ts(product.get("updatedAt"), "updatedAt", perrors)
    variants = [e["node"] for e in (product.get("variants") or {}).get("edges", [])]
    if not variants:
        perrors.append("variants: none")
    if perrors:
        return [Rejected(json.dumps(product, ensure_ascii=False), perrors)]

    for v in variants:
        errors: list[str] = []
        vtitle = v.get("title")
        name = title if not vtitle or vtitle == "Default Title" else f"{title} - {vtitle}"
        inv_item = v.get("inventoryItem") or {}
        item = Item(
            source="shopify",
            source_id=str(v.get("id")),
            sku=_req_str(v, "sku", errors),
            name=name,
            barcode=_gtin(v.get("barcode"), errors),
            brand=product.get("vendor"),
            category=product.get("productType"),
            status=product.get("status"),
            price=_parse_price(v.get("price"), errors),
            quantity=_parse_qty(v.get("inventoryQuantity"), "inventoryQuantity", errors),
            hs_code=_hs(inv_item.get("harmonizedSystemCode")),
            country_of_origin=inv_item.get("countryCodeOfOrigin"),
            source_updated_at=updated,
            raw={"product_id": product.get("id"), "variant": v},
        )
        if not v.get("id"):
            errors.append("variant.id: missing")
        if errors:
            out.append(Rejected(json.dumps(v, ensure_ascii=False), errors))
        else:
            out.append(item)
    return out
