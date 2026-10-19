from __future__ import annotations

import json
import random
import time
import urllib.error
import urllib.request

from ..normalize import from_shopify
from . import Batch

API_VERSION = "2025-01"

PRODUCTS_QUERY = """
query Products($first: Int!, $after: String, $query: String) {
  products(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
    edges {
      cursor
      node {
        id title handle vendor productType status createdAt updatedAt
        variants(first: 100) {
          edges { node { id title sku barcode price inventoryQuantity
                          inventoryItem { harmonizedSystemCode countryCodeOfOrigin } } }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


class ShopifyError(Exception):
    pass


class ShopifyConnector:
    kind = "shopify"

    def __init__(self, shop_url: str, access_token: str, max_retries: int = 5, timeout_s: float = 10):
        self.endpoint = f"{shop_url.rstrip('/')}/admin/api/{API_VERSION}/graphql.json"
        self.token = access_token
        self.max_retries = max_retries
        self.timeout_s = timeout_s

    def _post(self, payload: dict) -> dict:
        body = json.dumps(payload).encode()
        for attempt in range(self.max_retries + 1):
            req = urllib.request.Request(self.endpoint, data=body, method="POST", headers={
                "Content-Type": "application/json", "X-Shopify-Access-Token": self.token,
            })
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                    data = json.loads(resp.read())
                if data.get("errors"):
                    raise ShopifyError(f"GraphQL errors: {data['errors']}")
                return data["data"]
            except urllib.error.HTTPError as e:
                if e.code not in (429, 500, 502, 503, 504) or attempt == self.max_retries:
                    raise ShopifyError(f"HTTP {e.code}") from e
                retry_after = float(e.headers.get("Retry-After") or 0)
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
                if attempt == self.max_retries:
                    raise ShopifyError(str(e)) from e
                retry_after = 0
            time.sleep(max(retry_after, min(8.0, 0.2 * 2 ** attempt)) + random.random() * 0.1)
        raise ShopifyError("unreachable")

    def fetch(self, checkpoint: dict, batch_size: int) -> Batch:
        since = checkpoint.get("since")
        variables = {
            "first": min(batch_size, 250),
            "after": checkpoint.get("cursor"),
            "query": f"updated_at:>='{since}'" if since else None,
        }
        data = self._post({"query": PRODUCTS_QUERY, "variables": variables})
        conn = data["products"]
        products = [e["node"] for e in conn["edges"]]

        max_seen = checkpoint.get("max_seen") or since
        for p in products:
            u = p.get("updatedAt")
            if isinstance(u, str) and (max_seen is None or u > max_seen):
                max_seen = u

        if conn["pageInfo"]["hasNextPage"]:
            new_cp = {"since": since, "cursor": conn["pageInfo"]["endCursor"], "max_seen": max_seen}
            return Batch(products, new_cp, more=True, measure_latency=since is not None)
        return Batch(products, {"since": max_seen, "cursor": None, "max_seen": max_seen},
                     more=False, measure_latency=since is not None)

    @staticmethod
    def parse(product: dict) -> list:
        return from_shopify(product)
