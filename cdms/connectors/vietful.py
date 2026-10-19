from __future__ import annotations

import json
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import redis

from .. import config
from ..normalize import InvalidRecord, Rejected, from_vietful
from . import Batch

CHUNK = 50


class VietfulError(Exception):
    pass


class VietfulClient:
    def __init__(self, api_base: str, client_id: str, client_secret: str, auth_url: str | None = None,
                 realm: str = "aad", rate_per_s: float = 15, max_retries: int = 5, timeout_s: float = 15):
        self.api_base = api_base.rstrip("/")
        self.token_url = f"{(auth_url or api_base).rstrip('/')}/auth/realms/{realm}/protocol/openid-connect/token"
        self.client_id, self.client_secret = client_id, client_secret
        self.min_interval = 1.0 / rate_per_s
        self.max_retries, self.timeout_s = max_retries, timeout_s
        self._token: str | None = None
        self._token_exp = 0.0
        self._last_call = 0.0
        self._lock = threading.Lock()
        self.calls = 0

    def _get_token(self) -> str:
        if self._token and time.time() < self._token_exp - 60:
            return self._token
        form = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": self.client_id,
                                       "client_secret": self.client_secret}).encode()
        req = urllib.request.Request(self.token_url, data=form, method="POST",
                                     headers={"Content-Type": "application/x-www-form-urlencoded"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            raise VietfulError(f"token request failed: HTTP {e.code}") from e
        self._token = data["access_token"]
        self._token_exp = time.time() + float(data.get("expires_in", 3600))
        return self._token

    def _throttle(self) -> None:
        with self._lock:
            wait = self._last_call + self.min_interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()

    def request(self, method: str, path: str, params: dict | None = None, body: dict | None = None,
                allow_400: bool = False):
        query = "?" + urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v is not None}) if params else ""
        data = json.dumps(body).encode() if body is not None else None
        for attempt in range(self.max_retries + 1):
            self._throttle()
            self.calls += 1
            req = urllib.request.Request(self.api_base + path + query, data=data, method=method, headers={
                "Authorization": f"Bearer {self._get_token()}", "Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                    raw = resp.read()
                    return json.loads(raw) if raw else None
            except urllib.error.HTTPError as e:
                if e.code == 401 and attempt < self.max_retries:
                    self._token = None
                    continue
                if e.code == 400 and allow_400:
                    return None
                if e.code not in (429, 500, 502, 503, 504) or attempt == self.max_retries:
                    raise VietfulError(f"{method} {path}: HTTP {e.code}") from e
                retry_after = float(e.headers.get("Retry-After") or 0)
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                if attempt == self.max_retries:
                    raise VietfulError(f"{method} {path}: {e}") from e
                retry_after = 0
            time.sleep(max(retry_after, min(8.0, 0.2 * 2 ** attempt)) + random.random() * 0.1)
        raise VietfulError("unreachable")

    def list_products(self, partner_skus: list[str] | None = None, page: int = 0, size: int = 50) -> list[dict]:
        return self.request("GET", "/api/v1/Products", {
            "PartnerSKUs": ",".join(partner_skus) if partner_skus else None, "PageIndex": page, "PageSize": size}) or []

    def product_detail(self, partner_sku: str) -> dict | None:
        return self.request("GET", f"/api/v1/Products/{urllib.parse.quote(partner_sku, safe='')}", allow_400=True)

    def inventories(self, partner_skus: list[str]) -> list[dict]:
        rows, page = [], 0
        while True:
            res = self.request("GET", "/api/v1/Products/inventories", {
                "PartnerSKUs": ",".join(partner_skus), "PageIndex": page, "PageSize": 100}) or {}
            rows.extend(res.get("items") or [])
            if not res.get("hasNextPage"):
                return rows
            page += 1

    def subscribe(self, endpoint: str) -> None:
        self.request("POST", "/api/v1/WebhookSubscribers", body={"endpoint": endpoint})


class VietfulConnector:
    kind = "vietful"

    def __init__(self, stream: str, api_base: str, client_id: str, client_secret: str,
                 auth_url: str | None = None, realm: str = "aad", redis_url: str | None = None,
                 block_ms: int = 2000, page_size: int = 50, rate_per_s: float = 15,
                 reconcile_every_s: float = 3600):
        self.stream, self.block_ms, self.page_size = stream, block_ms, page_size
        self.reconcile_every_s = reconcile_every_s
        self.r = redis.Redis.from_url(redis_url or config.REDIS_URL, decode_responses=True)
        self.api = VietfulClient(api_base, client_id, client_secret, auth_url, realm, rate_per_s)
        self._regulated: dict[str, tuple] = {}

    def _stream_tail(self) -> str:
        last = self.r.xrevrange(self.stream, count=1)
        return last[0][0] if last else "0-0"

    def _enrich(self, product: dict) -> dict:
        psku = product.get("partnerSKU")
        if psku and psku not in self._regulated:
            d = self.api.product_detail(psku) or {}
            self._regulated[psku] = (d.get("hsCode"), d.get("countryOfOrigin"))
        hs, country = self._regulated.get(psku, (None, None))
        return {**product, "hsCode": hs, "countryOfOrigin": country}

    def _snapshots(self, products: list[dict], event_times: dict | None = None) -> list[dict]:
        fetched_at = datetime.now(timezone.utc)
        pskus = [p["partnerSKU"] for p in products if p.get("partnerSKU")]
        inv: dict[str, list] = {}
        for i in range(0, len(pskus), CHUNK):
            for row in self.api.inventories(pskus[i:i + CHUNK]):
                inv.setdefault(row.get("partnerSKU"), []).append(row)
        out = []
        for p in products:
            psku = p.get("partnerSKU")
            out.append({"kind": "product", "product": self._enrich(p), "inventory": inv.get(psku, []) if psku else [],
                        "event_time": (event_times or {}).get(psku), "fetched_at": fetched_at})
        return out

    def fetch(self, checkpoint: dict, batch_size: int) -> Batch:
        if not checkpoint.get("backfill_done"):
            return self._backfill(checkpoint)
        if self.reconcile_every_s and time.time() - checkpoint.get("reconciled_at", 0) > self.reconcile_every_s:
            return self._backfill({"last_id": checkpoint.get("last_id"), "backfill_page": 0})

        last_id = checkpoint.get("last_id", "0-0")
        resp = self.r.xread({self.stream: last_id}, count=batch_size, block=self.block_ms)
        entries = [e for _s, es in (resp or []) for e in es]
        rejects: list[dict] = []
        event_times: dict[str, datetime | None] = {}
        for entry_id, fields in entries:
            last_id = entry_id
            raw = fields.get("payload", "")
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError as e:
                rejects.append({"kind": "rejected", "payload": raw, "errors": [f"malformed JSON: {e.msg}"]})
                continue
            if not isinstance(ev, dict) or ev.get("event") != "INV_CHANGED":
                continue
            try:
                ts = datetime.fromtimestamp(int(ev["timestamp"]), tz=timezone.utc)
            except (KeyError, TypeError, ValueError):
                ts = None
            for it in ev.get("items") or []:
                psku = it.get("partnerSKU") if isinstance(it, dict) else None
                if not psku:
                    rejects.append({"kind": "rejected", "payload": json.dumps(it), "errors": ["partnerSKU: missing in event"]})
                    continue
                prev = event_times.get(psku)
                event_times[psku] = ts if prev is None or (ts is not None and ts < prev) else prev

        products: list[dict] = []
        pskus = sorted(event_times)
        for i in range(0, len(pskus), CHUNK):
            products.extend(self.api.list_products(pskus[i:i + CHUNK], 0, CHUNK))
        found = {p.get("partnerSKU") for p in products}
        for psku in pskus:
            if psku not in found:
                rejects.append({"kind": "rejected", "payload": psku, "errors": [f"partnerSKU {psku!r}: unknown to Vietful"]})
        records = rejects + self._snapshots(products, event_times)
        cp = {**checkpoint, "last_id": last_id, "backfill_done": True}
        return Batch(records, cp, more=len(entries) >= batch_size)

    def _backfill(self, checkpoint: dict) -> Batch:
        start_id = checkpoint.get("last_id") or self._stream_tail()
        page = checkpoint.get("backfill_page", 0)
        products = self.api.list_products(None, page, self.page_size)
        done = len(products) < self.page_size
        cp = {"last_id": start_id, "backfill_done": done, "backfill_page": page + 1}
        if done:
            cp["reconciled_at"] = time.time()
        return Batch(self._snapshots(products), cp, more=not done, measure_latency=False)

    @staticmethod
    def parse(record: dict) -> list:
        if record["kind"] == "rejected":
            return [Rejected(record["payload"], record["errors"])]
        try:
            return [from_vietful(record["product"], record["inventory"], record["event_time"], record["fetched_at"])]
        except InvalidRecord as e:
            return [Rejected(json.dumps(record["product"], ensure_ascii=False, default=str), e.errors)]
