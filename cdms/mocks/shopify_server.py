from __future__ import annotations

import base64
import json
import os
import random
import re
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .data import base_catalog, corrupt_shopify, iso, shopify_product

QUERY_RE = re.compile(r"updated_at:>=?'?([^'\s]+)'?")


class Shop:
    def __init__(self, name: str, n_products: int, bad_ratio: float):
        self.lock = threading.Lock()
        self.rng = random.Random(name)
        catalog = base_catalog()
        picks = self.rng.sample(range(len(catalog)), min(n_products, len(catalog)))
        start = datetime.now(timezone.utc) - timedelta(days=1)
        self.products = []
        for k, i in enumerate(picks):
            p = shopify_product(catalog[i], i, self.rng, start + timedelta(seconds=k))
            if self.rng.random() < bad_ratio:
                corrupt_shopify(p, self.rng)
            self.products.append(p)

    def mutate(self, n: int) -> int:
        with self.lock:
            for p in self.rng.sample(self.products, min(n, len(self.products))):
                for e in p["variants"]["edges"]:
                    if isinstance(e["node"].get("inventoryQuantity"), int):
                        e["node"]["inventoryQuantity"] = self.rng.randrange(0, 300)
                p["updatedAt"] = iso(datetime.now(timezone.utc))
        return n

    def query(self, first: int, after: str | None, q: str | None) -> dict:
        with self.lock:
            rows = sorted(self.products, key=lambda p: (p["updatedAt"], p["id"]))
            if q and (m := QUERY_RE.search(q)):
                rows = [p for p in rows if p["updatedAt"] >= m.group(1)]
            offset = int(base64.b64decode(after).decode()) if after else 0
            page = rows[offset: offset + first]
            edges = [{"cursor": base64.b64encode(str(offset + i + 1).encode()).decode(),
                      "node": json.loads(json.dumps(p))} for i, p in enumerate(page)]
        return {"products": {"edges": edges, "pageInfo": {
            "hasNextPage": offset + first < len(rows),
            "endCursor": edges[-1]["cursor"] if edges else after,
        }}}


class MockShopify:
    def __init__(self, n_products: int = 120, bad_ratio: float = 0.05, error_rate: float = 0.0):
        self.n_products, self.bad_ratio, self.error_rate = n_products, bad_ratio, error_rate
        self.shops: dict[str, Shop] = {}
        self.lock = threading.Lock()
        self.rng = random.Random()

    def shop(self, name: str) -> Shop:
        with self.lock:
            if name not in self.shops:
                self.shops[name] = Shop(name, self.n_products, self.bad_ratio)
            return self.shops[name]

    def handler(self):
        mock = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _json(self, code: int, obj, headers=None):
                body = json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                url = urlparse(self.path)
                parts = url.path.strip("/").split("/")
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                if len(parts) >= 3 and parts[1] == "_mock" and parts[2] == "mutate":
                    n = int(parse_qs(url.query).get("n", ["10"])[0])
                    return self._json(200, {"mutated": mock.shop(parts[0]).mutate(n)})
                if len(parts) != 5 or parts[1:3] != ["admin", "api"] or parts[4] != "graphql.json":
                    return self._json(404, {"errors": "Not Found"})
                shop = parts[0]
                if self.headers.get("X-Shopify-Access-Token") != f"{shop}-token":
                    return self._json(401, {"errors": "[API] Invalid API key or access token"})
                r = mock.rng.random()
                if r < mock.error_rate / 2:
                    return self._json(429, {"errors": "Throttled"}, {"Retry-After": "0.2"})
                if r < mock.error_rate:
                    return self._json(500, {"errors": "Internal Server Error"})
                try:
                    v = json.loads(body).get("variables") or {}
                    data = mock.shop(shop).query(int(v.get("first", 50)), v.get("after"), v.get("query"))
                except Exception as e:
                    return self._json(200, {"errors": [{"message": str(e)}]})
                return self._json(200, {"data": data})

        return H

    def serve(self, host: str = "0.0.0.0", port: int = 8081) -> ThreadingHTTPServer:
        return ThreadingHTTPServer((host, port), self.handler())


def main(host: str = "0.0.0.0", port: int = 8081) -> None:
    mock = MockShopify(
        n_products=int(os.getenv("SHOPIFY_MOCK_PRODUCTS", "120")),
        bad_ratio=float(os.getenv("SHOPIFY_MOCK_BAD_RATIO", "0.05")),
        error_rate=float(os.getenv("SHOPIFY_MOCK_ERROR_RATE", "0.05")),
    )
    srv = mock.serve(host, port)
    print(f"mock shopify listening on http://{host}:{port}")
    srv.serve_forever()
