from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import redis

from . import config, db

log = logging.getLogger("cdms.webhook")
SIGNATURE_HEADER = "x-vf-hmacsha256"


def sign(body: bytes, secret: str) -> str:
    return base64.b64encode(hmac.new(secret.encode(), body, hashlib.sha256).digest()).decode()


class TenantRoutes:
    def __init__(self, ttl_s: float = 30):
        self.ttl_s = ttl_s
        self.lock = threading.Lock()
        self.conn = db.connect()
        self.routes: dict[str, list[tuple[str, str]]] = {}
        self.loaded = 0.0

    def _load(self) -> None:
        rows = self.conn.execute("SELECT tenant_id, config FROM cdms.workers WHERE kind = 'vietful'").fetchall()
        routes: dict[str, list] = {}
        for r in rows:
            c = r["config"] or {}
            if c.get("client_secret") and c.get("stream"):
                routes.setdefault(r["tenant_id"], []).append((c["client_secret"], c["stream"]))
        self.routes, self.loaded = routes, time.monotonic()

    def get(self, tenant_id: str) -> list[tuple[str, str]]:
        with self.lock:
            if time.monotonic() - self.loaded > self.ttl_s or tenant_id not in self.routes:
                self._load()
            return self.routes.get(tenant_id, [])

    def reject(self, tenant_id: str, payload: str, error: str) -> None:
        with self.lock:
            self.conn.execute(
                "INSERT INTO cdms.rejected_records (worker_id, tenant_id, source, payload, errors)"
                " VALUES ('webhook-receiver', %s, 'vietful', %s, %s)", (tenant_id, payload[:10000], error))


def make_handler(routes: TenantRoutes, r: redis.Redis):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _reply(self, code: int, obj: dict) -> None:
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/healthz":
                return self._reply(200, {"ok": True})
            return self._reply(404, {"error": "not found"})

        def do_POST(self):
            parts = self.path.strip("/").split("/")
            if len(parts) != 3 or parts[:2] != ["webhooks", "vietful"]:
                return self._reply(404, {"error": "not found"})
            tenant = parts[2]
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            candidates = routes.get(tenant)
            if not candidates:
                return self._reply(404, {"error": "unknown tenant"})
            signature = self.headers.get(SIGNATURE_HEADER, "")
            stream = next((s for secret, s in candidates if hmac.compare_digest(sign(body, secret), signature)), None)
            if stream is None:
                routes.reject(tenant, body.decode("utf-8", "replace"), "webhook: invalid HMAC signature")
                log.warning("tenant %s: rejected webhook with invalid signature", tenant)
                return self._reply(401, {"error": "invalid signature"})
            r.xadd(stream, {"payload": body.decode("utf-8", "replace")})
            return self._reply(200, {"ok": True})

    return H


def serve(host: str = "0.0.0.0", port: int = 8090) -> ThreadingHTTPServer:
    routes = TenantRoutes()
    r = redis.Redis.from_url(config.REDIS_URL, decode_responses=True)
    return ThreadingHTTPServer((host, port), make_handler(routes, r))


def main(port: int = 8090) -> None:
    srv = serve(port=port)
    log.info("webhook receiver listening on :%s", port)
    srv.serve_forever()
