from __future__ import annotations

import argparse
import json
import logging
import sys

from . import db
from .stats import active_workers, list_workers, sync_stats


def _table(rows: list[dict], cols: list[str] | None = None) -> None:
    if not rows:
        print("(none)")
        return
    cols = cols or list(rows[0].keys())
    cells = [[("" if r[c] is None else json.dumps(r[c]) if isinstance(r[c], dict) else str(r[c])) for c in cols]
             for r in rows]
    widths = [max(len(c), *(len(row[i]) for row in cells)) for i, c in enumerate(cols)]
    print("  ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print("  ".join("-" * w for w in widths))
    for row in cells:
        print("  ".join(v.ljust(w) for v, w in zip(row, widths)))


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(prog="cdms", description="Change Data Management System")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init-db", help="create control + master schemas")

    t = sub.add_parser("tenant", help="manage tenants").add_subparsers(dest="action", required=True)
    ta = t.add_parser("add")
    ta.add_argument("tenant_id")
    ta.add_argument("--name")
    t.add_parser("list")

    w = sub.add_parser("worker", help="manage workers").add_subparsers(dest="action", required=True)
    wa = w.add_parser("add", help="register/configure a worker (upsert)")
    wa.add_argument("worker_id")
    wa.add_argument("--tenant", required=True)
    wa.add_argument("--kind", choices=["vietful", "shopify"], required=True)
    wa.add_argument("--interval", type=int, default=30, help="seconds between polls (schedule)")
    wa.add_argument("--config", required=True, help="connector config as JSON (see README)")
    for name in ("start", "stop", "run", "reset"):
        p = w.add_parser(name)
        p.add_argument("worker_id")
    w.add_parser("list")

    sub.add_parser("agent", help="run all workers whose desired state is 'running' (one per node)")
    sub.add_parser("active", help="how many workers are active")
    st = sub.add_parser("stats", help="sync performance statistics")
    st.add_argument("--tenant")
    st.add_argument("--since-minutes", type=int)
    rj = sub.add_parser("rejected", help="show latest rejected (invalid) records")
    rj.add_argument("--limit", type=int, default=20)

    wh = sub.add_parser("webhook-server", help="receive Vietful webhooks (HMAC-verified) into tenant streams")
    wh.add_argument("--port", type=int, default=8090)

    vf = sub.add_parser("vietful", help="Vietful account operations").add_subparsers(dest="action", required=True)
    vs = vf.add_parser("subscribe", help="register the CDMS webhook endpoint for a Vietful worker's account")
    vs.add_argument("worker_id")
    vs.add_argument("--endpoint", required=True, help="public URL, e.g. https://cdms.example.com/webhooks/vietful/acme")

    m = sub.add_parser("mock", help="mock data sources").add_subparsers(dest="action", required=True)
    ms = m.add_parser("shopify-server")
    ms.add_argument("--port", type=int, default=8081)
    mva = m.add_parser("vietful-api", help="mock Vietful External API + webhook sender")
    mva.add_argument("--port", type=int, default=8082)
    mvc = m.add_parser("vietful-change", help="make the mock Vietful change stock and push INV_CHANGED webhooks")
    mvc.add_argument("--client-id", required=True)
    mvc.add_argument("--count", type=int, default=20)
    mvc.add_argument("--url", default=None, help="mock Vietful base URL (default $VIETFUL_MOCK_URL or http://127.0.0.1:8082)")

    a = ap.parse_args(argv)

    if a.cmd == "mock":
        if a.action == "shopify-server":
            from .mocks.shopify_server import main as serve
            serve(port=a.port)
        elif a.action == "vietful-api":
            from .mocks.vietful_api import main as serve
            serve(port=a.port)
        else:
            import os
            import urllib.request
            base = a.url or os.getenv("VIETFUL_MOCK_URL", "http://127.0.0.1:8082")
            req = urllib.request.Request(f"{base}/_mock/{a.client_id}/inventory-change?n={a.count}", method="POST")
            with urllib.request.urlopen(req, timeout=30) as resp:
                print(resp.read().decode())
        return 0

    if a.cmd == "webhook-server":
        from .webhook import main as serve_webhooks
        serve_webhooks(a.port)
        return 0

    if a.cmd == "agent":
        from .worker import run_agent
        run_agent()
        return 0
    if a.cmd == "worker" and a.action == "run":
        from .worker import run_single
        run_single(a.worker_id)
        return 0

    conn = db.connect()
    if a.cmd == "init-db":
        db.init_db(conn)
        print("ok")
    elif a.cmd == "tenant":
        if a.action == "add":
            db.create_tenant(conn, a.tenant_id, a.name)
            print(f"tenant {a.tenant_id} ready (schema t_{a.tenant_id})")
        else:
            _table(conn.execute("SELECT * FROM cdms.tenants ORDER BY id").fetchall())
    elif a.cmd == "worker":
        if a.action == "add":
            conn.execute(
                """INSERT INTO cdms.workers (id, tenant_id, kind, interval_seconds, config)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id, kind = EXCLUDED.kind,
                       interval_seconds = EXCLUDED.interval_seconds, config = EXCLUDED.config""",
                (a.worker_id, a.tenant, a.kind, a.interval, json.dumps(json.loads(a.config))),
            )
            print(f"worker {a.worker_id} configured")
        elif a.action in ("start", "stop"):
            state = "running" if a.action == "start" else "stopped"
            n = conn.execute("UPDATE cdms.workers SET desired_state = %s WHERE id = %s",
                             (state, a.worker_id)).rowcount
            print(f"worker {a.worker_id} -> {state}" if n else f"worker {a.worker_id} not found")
        elif a.action == "reset":
            n = conn.execute("UPDATE cdms.workers SET checkpoint = '{}' WHERE id = %s"
                             " AND desired_state = 'stopped' AND owner IS NULL", (a.worker_id,)).rowcount
            print("checkpoint cleared" if n else "stop the worker first (and wait until it has stopped)")
        else:
            _table(list_workers(conn))
    elif a.cmd == "vietful":
        row = conn.execute("SELECT config FROM cdms.workers WHERE id = %s AND kind = 'vietful'",
                           (a.worker_id,)).fetchone()
        if row is None:
            print(f"vietful worker {a.worker_id} not found")
            return 1
        from .connectors.vietful import VietfulClient
        c = row["config"]
        VietfulClient(c["api_base"], c["client_id"], c["client_secret"], c.get("auth_url"),
                      c.get("realm", "aad")).subscribe(a.endpoint)
        print(f"webhooks for {a.worker_id} -> {a.endpoint}")
    elif a.cmd == "active":
        print(json.dumps(active_workers(conn)))
    elif a.cmd == "stats":
        _table(sync_stats(conn, a.tenant, a.since_minutes))
    elif a.cmd == "rejected":
        _table(conn.execute(
            "SELECT id, tenant_id, source, errors, created_at FROM cdms.rejected_records ORDER BY id DESC LIMIT %s",
            (a.limit,)).fetchall())
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
