from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import random
import secrets
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .data import (CHANGED_REASONS, base_catalog, corrupt_vietful_product, dotnet_ts, inv_changed_event,
                   to_product_dto, vietful_inventory, vietful_product)


def sign(body: bytes, secret: str) -> str:
    return base64.b64encode(hmac.new(secret.encode(), body, hashlib.sha256).digest()).decode()


class Account:
    def __init__(self, client_id: str, n_products: int, bad_ratio: float):
        self.client_id = client_id
        self.secret = f"{client_id}-secret"
        self.rng = random.Random(client_id)
        self.lock = threading.Lock()
        self.subscriber: str | None = None
        catalog = base_catalog()
        now = datetime.now(timezone.utc)
        self.products: list[dict] = []
        self.inventory: list[dict] = []
        for i in self.rng.sample(range(len(catalog)), min(n_products, len(catalog))):
            p = vietful_product(catalog[i], i, client_id.upper()[:3])
            inv = vietful_inventory(p, self.rng, now)
            if self.rng.random() < bad_ratio:
                corrupt_vietful_product(p, inv, self.rng)
            self.products.append(p)
            self.inventory.extend(inv)

    def find(self, partner_skus: set[str] | None, skus: set[str] | None = None) -> list[dict]:
        return [p for p in self.products
                if (not partner_skus or p["partnerSKU"] in partner_skus) and (not skus or p["sku"] in skus)]


class MockVietful:
    def __init__(self, n_products: int = 120, bad_ratio: float = 0.05, error_rate: float = 0.0,
                 event_fault_ratio: float = 0.05, retry_delay_s: float = 2.0,
                 event_faults: tuple = ("bad_signature", "malformed_body", "unknown_sku", "duplicate")):
        self.n_products, self.bad_ratio, self.error_rate = n_products, bad_ratio, error_rate
        self.event_fault_ratio, self.retry_delay_s = event_fault_ratio, retry_delay_s
        self.event_faults = event_faults
        self.accounts: dict[str, Account] = {}
        self.tokens: dict[str, str] = {}
        self.lock = threading.Lock()
        self.rng = random.Random()
        self.stats = {"webhooks_sent": 0, "webhooks_failed": 0, "faults": 0}

    def account(self, client_id: str) -> Account:
        with self.lock:
            if client_id not in self.accounts:
                self.accounts[client_id] = Account(client_id, self.n_products, self.bad_ratio)
            return self.accounts[client_id]

    def _deliver(self, acc: Account, body: bytes, signature: str, attempts: int = 4) -> None:
        for attempt in range(attempts):
            if acc.subscriber is None:
                return
            req = urllib.request.Request(acc.subscriber, data=body, method="POST", headers={
                "Content-Type": "application/json", "x-vf-hmacsha256": signature})
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    if 200 <= resp.status < 300:
                        self.stats["webhooks_sent"] += 1
                        return
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                pass
            self.stats["webhooks_failed"] += 1
            time.sleep(self.retry_delay_s * (attempt + 1))

    def push(self, acc: Account, event: dict, fault: str | None = None) -> None:
        body = json.dumps(event, ensure_ascii=False).encode()
        signature = sign(body, acc.secret)
        if fault == "bad_signature":
            signature = sign(body, "wrong-secret")
        elif fault == "malformed_body":
            body = body[: len(body) // 2]
            signature = sign(body, acc.secret)
        threading.Thread(target=self._deliver, args=(acc, body, signature), daemon=True).start()
        if fault == "duplicate":
            threading.Thread(target=self._deliver, args=(acc, body, signature), daemon=True).start()

    def change_inventory(self, client_id: str, n: int) -> int:
        acc = self.account(client_id)
        sent = 0
        with acc.lock:
            picks = acc.rng.sample(acc.products, min(n, len(acc.products)))
            events = []
            now = datetime.now(timezone.utc)
            for p in picks:
                rows = [r for r in acc.inventory if r["partnerSKU"] == p["partnerSKU"] and r["sku"] == p["sku"]]
                if not rows:
                    continue
                row = acc.rng.choice(rows)
                delta = acc.rng.choice([-1, 1]) * acc.rng.randrange(1, 30)
                delta = max(delta, -row["physicalQty"]) if row["availableQty"] >= 0 else delta
                row["physicalQty"] += delta
                if row["availableQty"] >= 0:
                    row["availableQty"] = max(0, row["availableQty"] + delta)
                row["lastUpdatedDate"] = dotnet_ts(now)
                items = [{"unitCode": row["unitCode"], "sku": row["sku"], "partnerSKU": row["partnerSKU"],
                          "conditionTypeCode": row["conditionTypeCode"], "changeQty": delta}]
                events.append(inv_changed_event(str(uuid.uuid4()), row["warehouseCode"],
                                                acc.rng.choice(CHANGED_REASONS), items, int(time.time())))
        for ev in events:
            fault = None
            if self.rng.random() < self.event_fault_ratio:
                fault = self.rng.choice(self.event_faults)
                self.stats["faults"] += 1
                if fault == "unknown_sku":
                    ev["items"][0]["partnerSKU"] = "DOES-NOT-EXIST"
            self.push(acc, ev, fault)
            sent += 1
        return sent

    def handler(self):
        mock = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _json(self, code: int, obj=None, headers=None):
                body = b"" if obj is None else json.dumps(obj, ensure_ascii=False).encode()
                self.send_response(code)
                if obj is not None:
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("X-TransactionId", str(uuid.uuid4()))
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _body(self) -> bytes:
                n = int(self.headers.get("Content-Length") or 0)
                return self.rfile.read(n) if n else b""

            def _auth(self) -> Account | None:
                h = self.headers.get("Authorization", "")
                cid = mock.tokens.get(h[7:]) if h.startswith("Bearer ") else None
                return mock.account(cid) if cid else None

            def _faulty(self) -> bool:
                r = mock.rng.random()
                if r < mock.error_rate / 2:
                    self._json(429, {"message": "Too Many Requests"}, {"Retry-After": "1"})
                    return True
                if r < mock.error_rate:
                    self._json(500, {"message": "Internal Server Error"})
                    return True
                return False

            def do_POST(self):
                url = urlparse(self.path)
                parts = url.path.strip("/").split("/")
                body = self._body()
                if parts[:2] == ["auth", "realms"] and parts[-1] == "token":
                    form = {k: v[0] for k, v in parse_qs(body.decode()).items()}
                    cid = form.get("client_id", "")
                    if form.get("grant_type") != "client_credentials" or form.get("client_secret") != f"{cid}-secret":
                        return self._json(400, {"error": "invalid_client"})
                    tok = secrets.token_urlsafe(24)
                    mock.tokens[tok] = cid
                    return self._json(200, {"access_token": tok, "expires_in": 86400, "refresh_token": "n/a",
                                            "refresh_expires_in": 604800, "token_type": "bearer"})
                if parts[0] == "_mock" and len(parts) == 3 and parts[2] == "inventory-change":
                    n = int(parse_qs(url.query).get("n", ["10"])[0])
                    return self._json(200, {"pushed": mock.change_inventory(parts[1], n)})
                acc = self._auth()
                if acc is None:
                    return self._json(401, {"message": "Unauthorized"})
                if url.path == "/api/v1/WebhookSubscribers":
                    try:
                        endpoint = json.loads(body)["endpoint"]
                    except (ValueError, KeyError, TypeError):
                        return self._json(400, {"message": "endpoint is required"})
                    acc.subscriber = endpoint
                    return self._json(204)
                return self._json(404, {"message": "Not Found"})

            def do_GET(self):
                url = urlparse(self.path)
                q = {k: v[0] for k, v in parse_qs(url.query).items()}
                acc = self._auth()
                if acc is None:
                    return self._json(401, {"message": "Unauthorized"})
                if self._faulty():
                    return
                csv = lambda k: {s.strip() for s in q[k].split(",") if s.strip()} if q.get(k) else None
                page, size = int(q.get("PageIndex", 0)), int(q.get("PageSize", 50))
                with acc.lock:
                    if url.path == "/api/v1/Products":
                        rows = acc.find(csv("PartnerSKUs"), csv("SKUs"))
                        return self._json(200, [to_product_dto(p) for p in rows[page * size:(page + 1) * size]])
                    if url.path == "/api/v1/Products/inventories":
                        ps = csv("PartnerSKUs")
                        rows = [dict(r) for r in acc.inventory if not ps or r["partnerSKU"] in ps]
                        items = rows[page * size:(page + 1) * size]
                        total_pages = (len(rows) + size - 1) // size
                        return self._json(200, {"pageIndex": page, "pageSize": size, "indexFrom": 0,
                                                "totalCount": len(rows), "totalPages": total_pages,
                                                "hasPreviousPage": page > 0, "hasNextPage": page + 1 < total_pages,
                                                "items": items})
                    if url.path.startswith("/api/v1/Products/"):
                        psku = url.path.rsplit("/", 1)[1]
                        found = acc.find({psku})
                        if not found:
                            return self._json(400, {"message": f"Product {psku} not found"})
                        return self._json(200, found[0])
                return self._json(404, {"message": "Not Found"})

        return H

    def serve(self, host: str = "0.0.0.0", port: int = 8082) -> ThreadingHTTPServer:
        return ThreadingHTTPServer((host, port), self.handler())


def main(host: str = "0.0.0.0", port: int = 8082) -> None:
    mock = MockVietful(
        n_products=int(os.getenv("VIETFUL_MOCK_PRODUCTS", "120")),
        bad_ratio=float(os.getenv("VIETFUL_MOCK_BAD_RATIO", "0.05")),
        error_rate=float(os.getenv("VIETFUL_MOCK_ERROR_RATE", "0.05")),
        event_fault_ratio=float(os.getenv("VIETFUL_MOCK_EVENT_FAULT_RATIO", "0.05")),
    )
    srv = mock.serve(host, port)
    print(f"mock vietful listening on http://{host}:{port}")
    srv.serve_forever()
