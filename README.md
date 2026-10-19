# CDMS: Change Data Management System (multi-tenant)

CDMS syncs product and inventory data from two sources into PostgreSQL, for many tenants:

- **Vietful** (warehouse) pushes changes: it sends a signed webhook whenever stock changes.
- **Shopify** (online store) can only be queried, so CDMS polls it on a schedule.

Each tenant gets its own database schema (`t_<tenant>`). A shared `master.products` table keeps basic product
info (name, brand, category, unit, HS code, country of origin) once per barcode (GTIN), for all tenants.

Design decisions: [docs/adr/0001-cdms-architecture.md](docs/adr/0001-cdms-architecture.md).
Summary of the work: [docs/WORK_SUMMARY.md](docs/WORK_SUMMARY.md).

## How it works

1. **Setup.** You create tenants and workers with the CLI. A worker belongs to one tenant and one source,
   and has its own config (URL, credentials) and schedule. `worker start` / `worker stop` only change the
   worker's desired state in the database.
2. **Agent.** `cdms agent` runs on every node. Every 2 seconds it reads the workers table, starts a thread for
   each worker that should be running, and stops the ones that were switched off. A worker must hold a lease
   in the database before it runs, so it never runs on two nodes at once. If a node dies, its lease expires
   and another node takes over.
3. **Worker loop.** The worker reads its checkpoint, asks the connector for the next batch, then validates
   the records and writes them. Valid records go to the tenant table. Invalid ones go to `cdms.rejected_records`
   with the reason. Products with a barcode also go to the master table. The data, the new checkpoint and the
   stats are saved in one transaction. A stopped or crashed worker therefore resumes exactly where it left off,
   with nothing lost or duplicated. When there is nothing new, the worker waits for its interval. When something
   fails, it retries with a growing delay (up to 60 s).
4. **No duplicates.** A row is updated only when its content changed and the incoming data is not older than
   what is stored. Barcodes are normalized to 14 digits, so the UPC and EAN forms of one product become one
   master row. The master table only fills in empty fields and never overwrites them.

**Vietful.** Webhooks arrive at `webhook-server`. It checks the HMAC signature with the tenant's secret and
puts the event into the tenant's Redis Stream; bad signatures are rejected. The Vietful worker reads the stream.
An event only says which SKUs changed, so the worker calls the Vietful API for their current product and stock
data. On the first run it loads the full catalogue, and it reloads it every hour to recover lost webhooks.
Events sent while a worker is stopped wait in Redis.

**Shopify.** The worker queries products updated since its last run, page by page. The checkpoint keeps the
timestamp and the page cursor, so it can resume mid-pagination. Each variant is stored as one row.

## Project layout

```
cdms/
  cli.py            command line (python -m cdms ...)
  config.py         settings from environment variables
  db.py             tables and schemas
  worker.py         agent, worker loop, lease, checkpoint
  connectors/       Vietful and Shopify API clients
  normalize.py      validation and mapping to one product format
  store.py          writes to tenant and master tables
  webhook.py        Vietful webhook receiver
  stats.py          statistics
  mocks/            fake Vietful and Shopify servers (with bad data and random errors)
tests/              unit and integration tests
scripts/            demo seed script
docs/               ADR and work summary
```

## Run with Docker

```bash
docker compose up --build -d
```

This starts PostgreSQL, Redis, both mocks, the webhook server and 2 agents. It also runs `scripts/seed_demo.sh`,
which creates 2 tenants (`acme`, `globex`) with a Vietful and a Shopify worker each.

```bash
docker compose run --rm cli worker list        # workers and their state
docker compose run --rm cli active             # how many workers are running
docker compose run --rm cli mock vietful-change --client-id acme --count 50   # simulate stock changes
curl -X POST "http://localhost:8081/acme-shop/_mock/mutate?n=20"              # simulate Shopify changes
docker compose run --rm cli stats              # sync statistics
docker compose run --rm cli rejected           # rejected records and why
docker compose run --rm cli worker stop acme-vietful
docker compose run --rm cli worker start acme-vietful
docker compose down -v                         # stop and delete data
```

## Run locally

Needs Python 3.10+, PostgreSQL and Redis.

```bash
pip install -r requirements.txt
export CDMS_DSN=postgresql://postgres:postgres@localhost:5432/cdms
export CDMS_REDIS_URL=redis://localhost:6379/0
```

Run each of these in its own terminal, in this order:

```bash
python -m cdms mock shopify-server --port 8081
python -m cdms mock vietful-api --port 8082
sh scripts/seed_demo.sh
python -m cdms webhook-server --port 8090
python -m cdms agent
```

Other commands: `tenant add|list`, `worker add|start|stop|run|reset|list`, `vietful subscribe`, `active`, `stats`,
`rejected`. Run `python -m cdms --help` for details.

## Tests

```bash
python -m unittest discover -s tests -v
```

`test_normalize.py` needs nothing. `test_integration.py` needs PostgreSQL and Redis and is skipped without them.

## Limitations

- Only Vietful `INV_CHANGED` events are used.
- Secrets are stored in the worker config for the demo; production should use a secret manager.
- Tested on a single PostgreSQL 16 node only.
- No admin UI or metrics endpoint.

## License

MIT, see [LICENSE](LICENSE).
