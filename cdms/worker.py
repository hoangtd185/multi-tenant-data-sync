from __future__ import annotations

import json
import logging
import signal
import statistics
import threading
import time
from datetime import datetime, timezone

from . import config, db
from .connectors import build_connector
from .normalize import Item, Rejected
from .store import write_items, write_rejected

log = logging.getLogger("cdms.worker")


def claim(conn, worker_id: str, owner: str) -> dict | None:
    with conn.transaction():
        return conn.execute(
            """
            UPDATE cdms.workers
               SET owner = %s, lease_until = now() + %s * INTERVAL '1 second',
                   last_heartbeat = now(), status = 'running'
             WHERE id = %s AND desired_state = 'running'
               AND (owner IS NULL OR owner = %s OR lease_until < now())
            RETURNING *
            """,
            (owner, config.LEASE_S, worker_id, owner),
        ).fetchone()


def release(conn, worker_id: str, owner: str, status: str = "stopped") -> None:
    with conn.transaction():
        conn.execute(
            "UPDATE cdms.workers SET owner = NULL, lease_until = NULL, status = %s, last_heartbeat = now()"
            " WHERE id = %s AND owner = %s",
            (status, worker_id, owner),
        )


def set_error(conn, worker_id: str, message: str | None) -> None:
    with conn.transaction():
        conn.execute("UPDATE cdms.workers SET last_error = %s WHERE id = %s", (message, worker_id))


def _latency_stats(items: list[Item], now: datetime) -> tuple:
    lat = [(now - (i.event_time or i.source_updated_at)).total_seconds() * 1000 for i in items]
    lat = [x for x in lat if x >= 0]
    if not lat:
        return None, None, None
    p95 = statistics.quantiles(lat, n=20)[-1] if len(lat) >= 2 else lat[0]
    return statistics.fmean(lat), p95, max(lat)


def process_batch(conn, worker: dict, connector, batch, started_at: datetime) -> dict:
    items: list[Item] = []
    rejects: list[Rejected] = []
    for raw in batch.records:
        for r in connector.parse(raw):
            (items if isinstance(r, Item) else rejects).append(r)

    with conn.transaction():
        counts = write_items(conn, worker["tenant_id"], items)
        write_rejected(conn, worker["id"], worker["tenant_id"], worker["kind"], rejects)
        conn.execute(
            "UPDATE cdms.workers SET checkpoint = %s, last_error = NULL WHERE id = %s",
            (json.dumps(batch.checkpoint), worker["id"]),
        )
        finished = datetime.now(timezone.utc)
        avg, p95, mx = _latency_stats(counts.changed, finished) if batch.measure_latency else (None, None, None)
        if batch.records:
            conn.execute(
                """INSERT INTO cdms.sync_runs (worker_id, tenant_id, started_at, finished_at, received,
                       inserted, updated, unchanged, rejected, master_inserted,
                       latency_avg_ms, latency_p95_ms, latency_max_ms)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (worker["id"], worker["tenant_id"], started_at, finished, len(items) + len(rejects),
                 counts.inserted, counts.updated, counts.unchanged, len(rejects), counts.master_inserted,
                 avg, p95, mx),
            )
    return {"received": len(items) + len(rejects), "inserted": counts.inserted,
            "updated": counts.updated, "unchanged": counts.unchanged, "rejected": len(rejects)}


def _idle(conn, worker_id: str, owner: str, stop: threading.Event, seconds: float) -> bool:
    step = max(1.0, config.LEASE_S / 3)
    end = time.monotonic() + seconds
    while (left := end - time.monotonic()) > 0:
        if stop.wait(min(step, left)):
            return False
        if claim(conn, worker_id, owner) is None:
            return False
    return True


def run_worker(worker_id: str, stop: threading.Event, owner: str = config.NODE_ID) -> None:
    conn = db.connect()
    try:
        w = claim(conn, worker_id, owner)
        if w is None:
            log.warning("worker %s: not claimable (stopped, or owned by another node)", worker_id)
            return
        connector = build_connector(w["kind"], w["config"])
        checkpoint = w["checkpoint"] or {}
        log.info("worker %s started (tenant=%s kind=%s checkpoint=%s)", worker_id, w["tenant_id"], w["kind"], checkpoint)
        backoff = 1.0
        while not stop.is_set():
            if claim(conn, worker_id, owner) is None:
                log.info("worker %s: stop requested or lease lost", worker_id)
                break
            started = datetime.now(timezone.utc)
            try:
                batch = connector.fetch(checkpoint, config.BATCH_SIZE)
                res = process_batch(conn, w, connector, batch, started)
                checkpoint = batch.checkpoint
                backoff = 1.0
                if res["received"]:
                    log.info("worker %s: %s", worker_id, res)
            except Exception as e:
                log.exception("worker %s: batch failed", worker_id)
                conn.rollback()
                set_error(conn, worker_id, str(e)[:500])
                stop.wait(backoff)
                backoff = min(backoff * 2, 60)
                continue
            if not batch.more and not _idle(conn, worker_id, owner, stop, w["interval_seconds"]):
                log.info("worker %s: stop requested or lease lost", worker_id)
                break
        release(conn, worker_id, owner)
        log.info("worker %s stopped at checkpoint %s", worker_id, checkpoint)
    finally:
        conn.close()


def run_agent(owner: str = config.NODE_ID, poll_s: float = 2.0) -> None:
    shutdown = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: shutdown.set())

    running: dict[str, tuple[threading.Thread, threading.Event]] = {}
    conn = db.connect()
    log.info("agent %s started", owner)
    try:
        while not shutdown.is_set():
            with conn.transaction():
                rows = conn.execute(
                    "SELECT id, desired_state, owner, lease_until < now() AS expired FROM cdms.workers"
                ).fetchall()
            for r in rows:
                wid = r["id"]
                t = running.get(wid)
                if t and not t[0].is_alive():
                    running.pop(wid)
                    t = None
                free = r["owner"] in (None, owner) or r["expired"]
                if r["desired_state"] == "running" and t is None and free:
                    ev = threading.Event()
                    th = threading.Thread(target=run_worker, args=(wid, ev, owner), name=wid, daemon=True)
                    th.start()
                    running[wid] = (th, ev)
                elif r["desired_state"] == "stopped" and t is not None:
                    t[1].set()
            shutdown.wait(poll_s)
    finally:
        for th, ev in running.values():
            ev.set()
        for th, _ in running.values():
            th.join(timeout=15)
        conn.close()
        log.info("agent %s stopped", owner)


def run_single(worker_id: str) -> None:
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    conn = db.connect()
    with conn.transaction():
        conn.execute("UPDATE cdms.workers SET desired_state = 'running' WHERE id = %s", (worker_id,))
    conn.close()
    run_worker(worker_id, stop)
