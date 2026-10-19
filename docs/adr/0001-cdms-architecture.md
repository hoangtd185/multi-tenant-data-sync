# ADR 0001: CDMS architecture

* Status: accepted
* Date: 2026-10-17 (revised 2026-10-19: Vietful flow aligned with the real Vietful External API spec)

## Context

We need a multi-tenant Change Data Management System that:

1. Receives Vietful inventory product data that Vietful **pushes** in near real-time, with a messaging setup configured per tenant. Per the Vietful External API spec, push means HMAC-signed webhooks (`INV_CHANGED` carries stock deltas only), and product and stock state is available through an OAuth2 REST API limited to 15 requests/second.
2. **Pulls** Shopify product data per tenant.
3. Lets each tenant configure and schedule its own workers. Workers can be started and stopped, and they resume from their last checkpoint.
4. Stores each tenant's data separately (never in the same table as another tenant), on a multi-node CockroachDB or PostgreSQL cluster.
5. Maintains one master product database shared by all tenants.
6. Allows no duplicates, in tenant data or in the master DB.
7. Knows how many workers are active at any time.
8. Runs in the cloud, handles bad source data, and reports throughput, latency and error statistics.

## Decisions

### D1. Python, one codebase, one container image

Python 3 with only two runtime libraries: `psycopg` 3 for PostgreSQL/CockroachDB and `redis`. HTTP is stdlib. The same image runs the agent, the webhook receiver, the CLI and the mocks.
*Why*: the problem is I/O-bound (network and DB), so Python's throughput is enough, and a small dependency surface keeps it easy to review.
*Alternative*: Go would give more throughput per core. It is not needed at this scale.

### D2. Vietful push: webhook receiver -> Redis Stream per tenant -> worker fetches current state

* **Receiver.** A stateless HTTP service, `POST /webhooks/vietful/<tenant>`. It verifies `x-vf-hmacsha256` with the tenant's `client_secret`, appends the raw body to `vietful:<tenant>` and answers 200. Requests with a bad signature get 401 and are recorded as rejected.
* **Tenant-specific messaging configuration.** Each tenant's Vietful worker config holds its stream, API credentials (the same secret signs its webhooks) and endpoint, which is registered with `POST /api/v1/WebhookSubscribers`.
* **Worker.** It reads events after its checkpoint (the stream entry id), collects the changed `partnerSKU`s and fetches their **current** state: `GET /Products` (`ProductDto`), `GET /Products/inventories` (`InvProductBriefDto`) and, once per product per process, `GET /Products/{partnerSKU}` for `hsCode`/`countryOfOrigin`. A client-side limiter keeps calls under 15 req/s; 401 refreshes the token, and 429/5xx retry with backoff.
* **Back-fill and reconciliation.** The first run loads the whole catalogue page by page; the stream position is saved before it starts, so nothing that arrives during the back-fill is missed. The back-fill repeats every `reconcile_every_s` (default 1 h).

*Why buffer first*: Vietful retries non-2xx only after 15 minutes. Answering fast and never depending on the DB in the request path keeps that from happening. The stream gives ordering, durability and a replayable offset for checkpoints. A receiver outage still loses nothing for long, because Vietful retries and reconciliation catches up.
*Why fetch state instead of applying `changeQty`*: deltas are not idempotent. A duplicate delivery or a replay after a crash would double-count stock, and a lost event would leave a permanent error. Reading the current state makes every event a harmless "refresh SKU X" signal. The cost is extra API calls, which are batched (50 SKUs per call) and rate-limited.
*Why reconcile*: webhooks can be lost (bad signature, Vietful gives up, misconfiguration). A periodic full read is the safety net, and idempotent writes mean it only writes real drift.
*Alternatives*: Kafka instead of Redis Streams at high volume or when long retention is needed; the connector interface (`fetch(checkpoint) -> Batch`) maps onto Kafka offsets. SQS/PubSub have no replayable offset, so resuming from a checkpoint is weaker.

### D3. Shopify is pulled incrementally by `updatedAt`, with a cursor

The query is `products(sortKey: UPDATED_AT, query: "updated_at:>='<since>'")`, paginated. The checkpoint is `{since, cursor, max_seen}`.
*Why*: `>=` re-reads boundary records instead of risking a gap; idempotent writes (D5) make the re-read harmless. Keeping the cursor allows a resume in the middle of a window. Retries with exponential backoff and `Retry-After` handle 429/5xx.
*Alternative*: Shopify webhooks plus a periodic reconciliation pull. That is the recommended production evolution, but the problem statement says Shopify data "can only be queried".

### D4. Schema per tenant, plus shared control and master schemas

`t_<tenant>.products`, `master.products`, and `cdms.*` for control data.
*Why*: it meets "data of different tenants is not stored in the same table", it isolates tenants without the cost of one database per tenant, and a tenant can be dropped or exported per schema.
*Trade-offs*: schema count grows with tenants (fine for hundreds to low thousands), and migrations must be applied to every tenant schema. A database per tenant is the next step for very large or regulated tenants.

### D5. Idempotent upserts based on a natural key and a content hash

* Tenant: `UNIQUE(source, source_id)`. `ON CONFLICT DO UPDATE ... WHERE content_hash differs AND incoming.updated_at >= stored.updated_at`.
* Within a batch, only the newest version of each key is kept.
* Master: `PRIMARY KEY (gtin)`, holding basic info (name, brand, category, unit) and regulated info (`hs_code`, `country_of_origin`), with the GTIN left-padded to 14 digits (GTIN-8/12/13 are GTIN-14 with leading zeros, so one product has one key). The first writer creates the row, and later writers only fill NULL fields. GTIN check digits are validated, so a bad barcode cannot create a fake master product.

*Why*: no duplicates under re-delivery, retries, overlapping polls, replays from an old checkpoint, or concurrent workers. Out-of-order (older) updates are ignored. Unchanged data causes no write, which saves I/O on CockroachDB.
*Trade-off*: the master is "first good value wins". A real MDM would add source priority and survivorship rules, plus a link table from tenant SKU to GTIN.

### D6. The checkpoint is committed in the same transaction as the data

*Why*: this gives effectively-once processing. On a crash, either both the data and the checkpoint are committed, or neither is, and the batch is re-read and re-applied idempotently.

### D7. Control plane in the database: desired state, lease, heartbeat

`cdms.workers` holds the config, the schedule (`interval_seconds`), `desired_state`, `status`, `checkpoint`, `owner`, `lease_until` and `last_heartbeat`.

* Stateless `agent` processes (one per pod or VM) run every worker whose `desired_state = running`.
* A worker is claimed with a conditional `UPDATE` (owner is NULL, or is me, or the lease has expired). The lease is renewed while working and while waiting for the next scheduled poll.
* Active workers = `status = 'running'` and a heartbeat within the timeout. Configured workers = `desired_state = 'running'`.

*Why*: start/stop works from any node through one source of truth. Agents scale horizontally, and a dead node's workers fail over after `LEASE_S`. No extra coordinator (ZooKeeper, etcd) is needed.
*Alternative*: a Kubernetes Deployment or Job per worker. That is heavier, and a lease is still needed to avoid double runs during rollouts.

### D8. Validate at the edge and dead-letter bad records

Invalid records go to `cdms.rejected_records` with the reason and the payload, and they are counted in the stats. They never stop the worker or the stream.

### D9. Statistics stored per batch in `cdms.sync_runs`

Counts (received, inserted, updated, unchanged, rejected, master_inserted), duration, and latency (avg, p95, max) are written in the same transaction as the data and aggregated with SQL (`cdms stats`).
*Why*: no extra infrastructure, and the numbers are consistent with the data. In production the same values would also be exported as Prometheus metrics.

### D10. Portable SQL (PostgreSQL and CockroachDB)

Only `ON CONFLICT`, `JSONB`, `TIMESTAMPTZ`, `FILTER` and `RETURNING` are used. Advisory locks, `xmax` tricks and PL/pgSQL are avoided. Inserted versus updated is detected with `created_at = updated_at`, because `now()` is the transaction timestamp on both engines.

## Consequences

* Positive: simple to deploy (one image, Postgres, Redis), safe under retries and failover, tenants are isolated, and the metrics are built in.
* Negative or open: Shopify freshness is bounded by the poll interval. Vietful sync costs REST calls per changed SKU and is bounded by the 15 req/s limit per account. Each worker is single-threaded, so a very large tenant needs partitioning (for example several streams or shops per worker). The tenant schema count and per-tenant migrations need tooling as the system grows. Secrets must move to a secret manager.

## Testing approach

* Unit tests: mapping and validation for both sources, including every injected fault type.
* Integration tests against real Postgres and Redis. The mock Shopify server injects 429/500 errors and invalid products. The mock Vietful API serves the real endpoints and schemas, injects invalid products and inventory rows plus 429/500 errors, and sends signed webhooks with injected faults (bad signature, malformed body, unknown SKU, duplicate delivery). The tests cover: back-fill; stop and resume from checkpoint while webhooks keep arriving; stock in CDMS equal to Vietful's state despite duplicates; reconciliation repairing lost webhooks; HMAC verification; no duplicate rows; master uniqueness (including UPC/EAN forms); lease exclusivity; and pagination resume.
