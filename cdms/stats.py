from . import config


def active_workers(conn) -> dict:
    row = conn.execute(
        """
        SELECT count(*) FILTER (WHERE desired_state = 'running') AS configured_running,
               count(*) FILTER (WHERE status = 'running'
                                AND last_heartbeat > now() - %s * INTERVAL '1 second') AS active,
               count(*) AS total
          FROM cdms.workers
        """,
        (config.HEARTBEAT_TIMEOUT_S,),
    ).fetchone()
    return dict(row)


def list_workers(conn) -> list[dict]:
    return conn.execute(
        """
        SELECT id, tenant_id, kind, interval_seconds, desired_state,
               CASE WHEN status = 'running' AND last_heartbeat < now() - %s * INTERVAL '1 second'
                    THEN 'dead' ELSE status END AS status,
               owner, checkpoint, last_heartbeat, last_error
          FROM cdms.workers ORDER BY tenant_id, id
        """,
        (config.HEARTBEAT_TIMEOUT_S,),
    ).fetchall()


def sync_stats(conn, tenant_id: str | None = None, since_minutes: int | None = None) -> list[dict]:
    where, params = ["true"], []
    if tenant_id:
        where.append("tenant_id = %s")
        params.append(tenant_id)
    if since_minutes:
        where.append("started_at > now() - %s * INTERVAL '1 minute'")
        params.append(since_minutes)
    return conn.execute(
        f"""
        SELECT worker_id, tenant_id,
               count(*) AS batches,
               sum(received) AS received, sum(inserted) AS inserted, sum(updated) AS updated,
               sum(unchanged) AS unchanged, sum(rejected) AS rejected,
               sum(master_inserted) AS master_inserted,
               round((100.0 * sum(rejected) / NULLIF(sum(received), 0))::numeric, 2) AS rejected_pct,
               round((sum(received) / NULLIF(sum(EXTRACT(EPOCH FROM finished_at - started_at)), 0))::numeric, 1)
                   AS throughput_rps,
               round((sum(latency_avg_ms * (received - rejected)) / NULLIF(sum(received - rejected)
                     FILTER (WHERE latency_avg_ms IS NOT NULL), 0))::numeric, 1)
                   AS latency_avg_ms,
               round(max(latency_p95_ms)::numeric, 1) AS latency_p95_ms_worst_batch,
               round(max(latency_max_ms)::numeric, 1) AS latency_max_ms
          FROM cdms.sync_runs
         WHERE {' AND '.join(where)}
         GROUP BY worker_id, tenant_id
         ORDER BY tenant_id, worker_id
        """,
        params,
    ).fetchall()
