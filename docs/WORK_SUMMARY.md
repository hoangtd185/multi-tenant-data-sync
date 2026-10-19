# CDMS: summary of the work

This document maps every point of the job test to what was built, where it lives in the code, and how it
was verified. The design reasoning is in [adr/0001-cdms-architecture.md](adr/0001-cdms-architecture.md);
how to run everything is in the [README](../README.md).

## 1. Requirements and what was delivered

| Requirement (job test) | What was built | Where | Verified by |
|---|---|---|---|
| Two data sources: Vietful inventory products, Shopify products | Two connectors that map both sources into one canonical product (SKU) model | `cdms/connectors/`, `cdms/normalize.py` | unit + integration tests |
| Vietful data is **pushed** near real-time, with a tenant-specific messaging configuration | Webhook receiver that verifies HMAC with the tenant's secret and buffers into the tenant's own Redis Stream; the endpoint is registered with Vietful's `POST /api/v1/WebhookSubscribers` | `cdms/webhook.py`, `cdms/connectors/vietful.py`, `cdms vietful subscribe` | `test_webhook_receiver_verifies_hmac`, `test_vietful_backfill_webhooks_stop_resume` |
| Shopify data can only be **queried** | Incremental GraphQL poller (`updated_at >= checkpoint`, cursor pagination, retries) | `cdms/connectors/shopify.py` | `test_shopify_pull_idempotent_and_master_shared`, `test_shopify_connector_resumes_mid_pagination` |
| Workers use connectors, can be configured and scheduled; tenants schedule differently | `cdms.workers` table: kind, config, `interval_seconds` per worker and tenant; `agent` runs them | `cdms/worker.py`, `cdms/cli.py` (`worker add`) | demo: 4 workers, intervals 0/15/60 s |
| One database stores tenant data; new and changed data from both sources gives a single view per tenant | `t_<tenant>.products` holds both sources' rows with the same columns | `cdms/db.py`, `cdms/store.py` | integration tests |
| Multi-node CockroachDB or PostgreSQL cluster | SQL limited to features both support (no advisory locks, `xmax` or PL/pgSQL); stateless app nodes | `cdms/db.py`, ADR D10 | **only run on single-node PostgreSQL 16** (see §4) |
| Tenant data not stored in the same table | One schema per tenant (`t_<tenant>`) | `cdms/db.py` | `tenant add` creates the schema |
| Master product DB with basic info (manufacturer/brand) and standard regulated info, regardless of tenant | `master.products` keyed by GTIN-14: name, brand, category, unit, `hs_code`, `country_of_origin` (from Vietful `ProductFullDetailDto` and Shopify `inventoryItem`) | `cdms/store.py` | `test_master_same_product_upc_and_ean_is_one_row` |
| No duplicated data (tenant sync and master) | Natural-key upserts + content hash + stale-update guard; GTIN normalized to 14 digits; batch-level de-duplication | `cdms/store.py` | integration tests assert row count = distinct keys; master GTINs unique |
| CDMS knows how many workers are active | Heartbeat + `cdms active` (configured vs. actually running), `worker list` marks dead workers | `cdms/stats.py` | demo output, `active_workers` asserted in tests |
| Mock services for data sources | Mock Vietful External API (OAuth, Products, inventories, WebhookSubscribers, signed webhooks) and mock Shopify Admin GraphQL | `cdms/mocks/vietful_api.py`, `cdms/mocks/shopify_server.py` | used by all integration tests |
| Mock data based on real schemas | Vietful: `ProductDto`, `ProductFullDetailDto`, `InvProductBriefDto`, `ExtPagedList`, `INV_CHANGED` body and `x-vf-hmacsha256` taken from the official OpenAPI spec. Shopify: `Product` / `ProductVariant` / `InventoryItem` | `cdms/mocks/data.py` | §2 |
| Mock situations where source data is incorrect | Vietful: missing `productName`/`partnerSKU`, bad GTIN check digit, negative `availableQty`, bad signature, malformed body, unknown SKU, duplicate delivery, HTTP 429/500. Shopify: missing title/SKU, negative stock, non-numeric price, bad barcode, no variants, HTTP 429/500 | `cdms/mocks/*` | rejected records in demo; `test_every_product_fault_is_rejected`, `test_faults_rejected` |
| Designed and operated for cloud | Stateless agents and receivers, horizontal scaling, DB leases for failover, 12-factor config, Docker image, compose with 2 agent replicas and restart policy | `Dockerfile`, `docker-compose.yml`, ADR D7 | `test_lease_prevents_double_run`; demo with 2 agents |
| Start and stop workers; they resume from the last checkpoint | `worker start/stop` flips `desired_state`; checkpoint committed in the same transaction as the data | `cdms/worker.py` | `test_vietful_backfill_webhooks_stop_resume` (webhooks arrive while stopped and are processed after restart) |
| Performance measurements: throughput, latency, wrong records | Per-batch stats in `cdms.sync_runs`; `cdms stats` (throughput, latency avg/p95/max, rejected %); `cdms rejected` lists reasons | `cdms/stats.py`, `cdms/worker.py` | README "Statistics" |
| Delivery: GitHub repo with license | MIT license | `LICENSE` | author name to be filled in |
| Delivery: architecture decision record | ADR with 10 decisions, alternatives and trade-offs | `docs/adr/0001-cdms-architecture.md` | |
| Delivery: Python/Go/Java | Python 3.10+ | | |
| Delivery: README in Markdown, English | | `README.md` | |

## 2. Conformance to the Vietful External API

Source: `https://ext.stg.vnfai.com/swagger/v1/swagger.json` (OpenAPI 3.0.4, "VietFul API" v1.0).

| Vietful API element | Used by CDMS for | Implemented in mock | Used by connector |
|---|---|---|---|
| OAuth2 `client_credentials` at `/auth/realms/{realm}/protocol/openid-connect/token` | authentication; token cached, refreshed on expiry or 401 | yes | yes |
| Rate limit 15 req/s | client-side limiter (`rate_per_s`) | – | yes |
| `GET /api/v1/Products` (`PartnerSKUs`, `PageIndex`, `PageSize`) -> `ProductDto[]` | back-fill / reconciliation; product data for changed SKUs | yes | yes |
| `GET /api/v1/Products/{partnerSKU}` -> `ProductFullDetailDto` | `hsCode`, `countryOfOrigin` for the master DB | yes (400 if unknown) | yes (cached per process) |
| `GET /api/v1/Products/inventories` -> `ExtPagedList<InvProductBriefDto>` | `availableQty` per warehouse, `lastUpdatedDate` | yes (paged) | yes |
| `POST /api/v1/WebhookSubscribers {endpoint}` | registering the tenant's webhook endpoint | yes (204) | `cdms vietful subscribe` |
| Webhook `x-vf-hmacsha256` (Base64 HMAC-SHA256 of body with `client_secret`) | authenticating pushes | yes | receiver verifies |
| Webhook retry on non-2xx (15 min) | receiver answers fast; reconciliation as a safety net | yes (shortened delay) | – |
| Event `INV_CHANGED` `{id, timestamp, event, warehouseCode, changedReason, items[{sku, partnerSKU, unitCode, conditionTypeCode, changeQty}]}` | trigger to refresh the listed SKUs | yes | yes |

Field mapping (Vietful to CDMS): `productId` -> `source_id`, `partnerSKU` -> `sku`, `sku` (Vietful's SKU/barcode) -> `barcode` when it
is a GTIN, `productName` -> `name`, `categories[0].categoryName` -> `category`, `units[0]` -> `unit`,
`isActive` -> `status`, sum of `availableQty` (condition `NEW`) over warehouses -> `quantity`,
`hsCode` -> `hs_code`, `countryOfOrigin` -> `country_of_origin`.

## 3. How it was verified

* 15 automated tests (`python -m unittest discover -s tests`), run repeatedly against PostgreSQL 16 and Redis 7.
  All pass.
* End-to-end demo: 2 tenants, 4 workers, 2 agents, both mocks with 5% bad data and 5% HTTP errors.
  Results: all 4 workers active and spread across agents; stop/start resumes from the checkpoint; 235 master
  products with 235 distinct GTINs; no duplicate tenant rows; every injected fault type appears in
  `cdms rejected` with a clear reason; webhook-to-DB latency about 0.6–0.8 s.
* Bugs found by tests and fixed during development: lease expiry for workers with long schedules (heartbeat
  now continues while idle); duplicate master rows for UPC-12 vs. EAN-13 forms of one GTIN; lost fields when
  two records with the same GTIN arrive in one batch; and stale stock when a webhook is lost, which led to the
  reconciliation design.

## 4. Known limitations and next steps

* **Database cluster**: everything was run on a single PostgreSQL 16 node. A 3-node CockroachDB compose setup
  is the next step to prove the multi-node claim.
* **Docker**: `docker-compose.yml` is validated (`docker compose config`) but the stack was exercised with
  local processes rather than the containers.
* Vietful `lastUpdatedDate` has no time zone in the spec; it is treated as UTC (to confirm with Vietful).
* Only `INV_CHANGED` is consumed; order/inbound events are ignored. Bundles and multi-barcode children
  are not split into separate products.
* Schedules are fixed intervals (no cron windows). No Prometheus endpoint or admin UI. Secrets are kept in
  the worker config instead of a secret manager.
