from __future__ import annotations

import json
from dataclasses import dataclass, field

import psycopg
from psycopg import sql

from .db import tenant_schema
from .normalize import Item, Rejected


@dataclass
class WriteCounts:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    master_inserted: int = 0
    changed: list = field(default_factory=list)


UPSERT_TENANT = """
INSERT INTO {s}.products AS p
    (source, source_id, sku, barcode, name, brand, category, unit, status,
     price, quantity, hs_code, country_of_origin, source_updated_at, content_hash, raw)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (source, source_id) DO UPDATE SET
    sku = EXCLUDED.sku, barcode = EXCLUDED.barcode, name = EXCLUDED.name,
    brand = EXCLUDED.brand, category = EXCLUDED.category, unit = EXCLUDED.unit,
    status = EXCLUDED.status, price = EXCLUDED.price, quantity = EXCLUDED.quantity,
    hs_code = EXCLUDED.hs_code, country_of_origin = EXCLUDED.country_of_origin,
    source_updated_at = EXCLUDED.source_updated_at, content_hash = EXCLUDED.content_hash,
    raw = EXCLUDED.raw, updated_at = now()
WHERE p.content_hash <> EXCLUDED.content_hash
  AND p.source_updated_at <= EXCLUDED.source_updated_at
RETURNING (p.created_at = p.updated_at) AS inserted
"""

UPSERT_MASTER = """
INSERT INTO master.products AS m (gtin, name, brand, category, unit, hs_code, country_of_origin, first_source)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (gtin) DO UPDATE SET
    name = COALESCE(m.name, EXCLUDED.name),
    brand = COALESCE(m.brand, EXCLUDED.brand),
    category = COALESCE(m.category, EXCLUDED.category),
    unit = COALESCE(m.unit, EXCLUDED.unit),
    hs_code = COALESCE(m.hs_code, EXCLUDED.hs_code),
    country_of_origin = COALESCE(m.country_of_origin, EXCLUDED.country_of_origin),
    updated_at = now()
WHERE (m.brand IS NULL AND EXCLUDED.brand IS NOT NULL)
   OR (m.category IS NULL AND EXCLUDED.category IS NOT NULL)
   OR (m.unit IS NULL AND EXCLUDED.unit IS NOT NULL)
   OR (m.hs_code IS NULL AND EXCLUDED.hs_code IS NOT NULL)
   OR (m.country_of_origin IS NULL AND EXCLUDED.country_of_origin IS NOT NULL)
RETURNING (m.created_at = m.updated_at) AS inserted
"""


def normalize_gtin(code: str) -> str:
    return code.zfill(14)


def dedupe_batch(items: list[Item]) -> list[Item]:
    latest: dict[tuple[str, str], Item] = {}
    for it in items:
        key = (it.source, it.source_id)
        cur = latest.get(key)
        if cur is None or it.source_updated_at >= cur.source_updated_at:
            latest[key] = it
    return list(latest.values())


def write_items(conn: psycopg.Connection, tenant_id: str, items: list[Item]) -> WriteCounts:
    counts = WriteCounts()
    q = sql.SQL(UPSERT_TENANT).format(s=tenant_schema(tenant_id))
    deduped = dedupe_batch(items)
    counts.unchanged += len(items) - len(deduped)
    items = deduped
    with conn.cursor() as cur:
        for it in sorted(items, key=lambda i: (i.source, i.source_id)):
            cur.execute(q, (
                it.source, it.source_id, it.sku, it.barcode, it.name, it.brand, it.category,
                it.unit, it.status, it.price, it.quantity, it.hs_code, it.country_of_origin,
                it.source_updated_at,
                it.content_hash(), json.dumps(it.raw, ensure_ascii=False, default=str),
            ))
            row = cur.fetchone()
            if row is None:
                counts.unchanged += 1
            elif row["inserted"]:
                counts.inserted += 1
                counts.changed.append(it)
            else:
                counts.updated += 1
                counts.changed.append(it)

        new_gtins: set[str] = set()
        for it in sorted((i for i in items if i.barcode), key=lambda i: normalize_gtin(i.barcode)):
            gtin = normalize_gtin(it.barcode)
            cur.execute(UPSERT_MASTER, (
                gtin, it.name, it.brand, it.category, it.unit, it.hs_code, it.country_of_origin, it.source,
            ))
            row = cur.fetchone()
            if row is not None and row["inserted"]:
                new_gtins.add(gtin)
        counts.master_inserted = len(new_gtins)
    return counts


def write_rejected(conn, worker_id: str, tenant_id: str, source: str, rejects: list[Rejected]) -> None:
    if not rejects:
        return
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO cdms.rejected_records (worker_id, tenant_id, source, payload, errors)"
            " VALUES (%s, %s, %s, %s, %s)",
            [(worker_id, tenant_id, source, r.payload[:10000], "; ".join(r.errors)) for r in rejects],
        )
