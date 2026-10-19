from dataclasses import dataclass, field


@dataclass
class Batch:
    records: list = field(default_factory=list)
    checkpoint: dict = field(default_factory=dict)
    more: bool = False
    measure_latency: bool = True


def build_connector(kind: str, config: dict):
    if kind == "vietful":
        from .vietful import VietfulConnector
        return VietfulConnector(**config)
    if kind == "shopify":
        from .shopify import ShopifyConnector
        return ShopifyConnector(**config)
    raise ValueError(f"unknown connector kind {kind!r}")
