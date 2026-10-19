import re

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from . import config

TENANT_ID_RE = re.compile(r"^[a-z][a-z0-9_]{1,30}$")

CONTROL_DDL = """
CREATE SCHEMA IF NOT EXISTS cdms;
CREATE SCHEMA IF NOT EXISTS master;

CREATE TABLE IF NOT EXISTS cdms.tenants (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cdms.workers (
    id               TEXT PRIMARY KEY,
    tenant_id        TEXT NOT NULL REFERENCES cdms.tenants(id),
    kind             TEXT NOT NULL CHECK (kind IN ('vietful', 'shopify')),
    config           JSONB NOT NULL DEFAULT '{}',
    interval_seconds INT NOT NULL DEFAULT 30,
    desired_state    TEXT NOT NULL DEFAULT 'stopped' CHECK (desired_state IN ('running', 'stopped')),
    status           TEXT NOT NULL DEFAULT 'stopped',
    checkpoint       JSONB NOT NULL DEFAULT '{}',
    owner            TEXT,
    lease_until      TIMESTAMPTZ,
    last_heartbeat   TIMESTAMPTZ,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS cdms.sync_runs (
    id              BIGSERIAL PRIMARY KEY,
    worker_id       TEXT NOT NULL,
    tenant_id       TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ NOT NULL,
    received        INT NOT NULL,
    inserted        INT NOT NULL,
    updated         INT NOT NULL,
    unchanged       INT NOT NULL,
    rejected        INT NOT NULL,
    master_inserted INT NOT NULL,
    latency_avg_ms  DOUBLE PRECISION,
    latency_p95_ms  DOUBLE PRECISION,
    latency_max_ms  DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS sync_runs_worker_idx ON cdms.sync_runs (worker_id, started_at);

CREATE TABLE IF NOT EXISTS cdms.rejected_records (
    id          BIGSERIAL PRIMARY KEY,
    worker_id   TEXT NOT NULL,
    tenant_id   TEXT NOT NULL,
    source      TEXT NOT NULL,
    payload     TEXT NOT NULL,
    errors      TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS master.products (
    gtin              TEXT PRIMARY KEY,
    name              TEXT,
    brand             TEXT,
    category          TEXT,
    unit              TEXT,
    hs_code           TEXT,
    country_of_origin TEXT,
    first_source  TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

TENANT_DDL = """
CREATE SCHEMA IF NOT EXISTS {schema};
CREATE TABLE IF NOT EXISTS {schema}.products (
    id                BIGSERIAL PRIMARY KEY,
    source            TEXT NOT NULL,
    source_id         TEXT NOT NULL,
    sku               TEXT NOT NULL,
    barcode           TEXT,
    name              TEXT NOT NULL,
    brand             TEXT,
    category          TEXT,
    unit              TEXT,
    status            TEXT,
    price             NUMERIC(18, 2),
    quantity          INT,
    hs_code           TEXT,
    country_of_origin TEXT,
    source_updated_at TIMESTAMPTZ NOT NULL,
    content_hash      TEXT NOT NULL,
    raw               JSONB NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source, source_id)
);
CREATE INDEX IF NOT EXISTS products_sku_idx ON {schema}.products (sku);
"""


def connect(dsn: str | None = None) -> psycopg.Connection:
    return psycopg.connect(dsn or config.DSN, row_factory=dict_row, autocommit=True)


def tenant_schema(tenant_id: str) -> sql.Identifier:
    if not TENANT_ID_RE.match(tenant_id):
        raise ValueError(f"invalid tenant id {tenant_id!r} (expected {TENANT_ID_RE.pattern})")
    return sql.Identifier(f"t_{tenant_id}")


def init_db(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute(CONTROL_DDL)


def create_tenant(conn: psycopg.Connection, tenant_id: str, name: str | None = None) -> None:
    schema = tenant_schema(tenant_id)
    with conn.transaction():
        conn.execute(
            "INSERT INTO cdms.tenants (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
            (tenant_id, name or tenant_id),
        )
        conn.execute(sql.SQL(TENANT_DDL).format(schema=schema))
