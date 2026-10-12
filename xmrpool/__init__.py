"""Read the Monero transaction pool from any monerod, including restricted public nodes.

monerod 0.18.5.3 stopped serving ``/get_transaction_pool`` in restricted RPC mode (monero-project/monero#10973):
public nodes now answer it with HTTP 404, while ``/get_transaction_pool_hashes`` and ``/get_transactions`` keep
working. Code that called the old endpoint now fails on public nodes, or worse, catches the error and acts on an
empty pool. This module tries the full endpoint first and, when a node refuses it, rebuilds the pool from the hashes
in batches the restricted limit allows. If neither works it raises :class:`PoolUnavailable`, never an empty list.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

__all__ = ["PoolUnavailable", "Pool", "get_pool", "aget_pool", "RESTRICTED_TRANSACTIONS_COUNT"]
__version__ = "0.1.0"

#: monerod's cap on hashes per /get_transactions call in restricted mode (src/rpc/core_rpc_server.cpp).
RESTRICTED_TRANSACTIONS_COUNT = 100

#: HTTP statuses that mean "this node does not serve the endpoint", as opposed to "the node is down".
REFUSED = {403, 404, 405}


class PoolUnavailable(Exception):
    """The node serves neither the full pool nor the pool hashes. Not the same as an empty pool."""


class EndpointRefused(Exception):
    def __init__(self, status: int):
        super().__init__(f"HTTP {status}")
        self.status = status


@dataclass
class Pool:
    """Pool contents in the shape of ``/get_transaction_pool``'s ``transactions`` list.

    ``source`` is ``"full"`` when the node served /get_transaction_pool, ``"hashes"`` when the pool was rebuilt from
    /get_transaction_pool_hashes + /get_transactions. Rebuilt entries carry ``id_hash``, ``tx_blob``, ``tx_json``,
    ``fee`` (atomic units, when the tx has RingCT) and ``double_spend_seen``; fields only the full endpoint knows
    (``receive_time``, ``relayed``, ``kept_by_block``, ...) are absent.
    """

    source: str
    transactions: list[dict[str, Any]] = field(default_factory=list)
    dropped: int = 0  # hashes that left the pool (mined or evicted) between the two calls

    def __len__(self) -> int:
        return len(self.transactions)

    @property
    def hashes(self) -> list[str]:
        return [t["id_hash"] for t in self.transactions]


def _entry(tx: dict[str, Any]) -> dict[str, Any]:
    as_json = tx.get("as_json") or ""
    fee = None
    if as_json:
        try:
            fee = json.loads(as_json).get("rct_signatures", {}).get("txnFee")
        except ValueError:
            pass
    out = {"id_hash": tx["tx_hash"], "tx_blob": tx.get("as_hex", ""), "tx_json": as_json,
           "double_spend_seen": tx.get("double_spend_seen", False)}
    if fee is not None:
        out["fee"] = fee
    return out


def _check(resp: dict[str, Any], what: str) -> dict[str, Any]:
    if not isinstance(resp, dict) or resp.get("status") != "OK":
        raise PoolUnavailable(f"{what}: status {resp.get('status') if isinstance(resp, dict) else resp!r}")
    return resp


def _rebuild_steps(hashes_resp: dict[str, Any]):
    """Generator shared by the sync and async clients: yields /get_transactions batches, returns the Pool."""
    hashes = _check(hashes_resp, "get_transaction_pool_hashes").get("tx_hashes") or []
    entries, dropped = [], 0
    for i in range(0, len(hashes), RESTRICTED_TRANSACTIONS_COUNT):
        batch = hashes[i:i + RESTRICTED_TRANSACTIONS_COUNT]
        resp = _check((yield {"txs_hashes": batch, "decode_as_json": True}), "get_transactions")
        found = {t["tx_hash"]: t for t in resp.get("txs") or []}
        for h in batch:
            t = found.get(h)
            if t is None or not t.get("in_pool"):
                dropped += 1
                continue
            entries.append(_entry(t))
    return Pool("hashes", entries, dropped)


def _http_post(url: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        if e.code in REFUSED:
            raise EndpointRefused(e.code) from None
        raise


def get_pool(node: str, *, timeout: float = 30.0, post: Callable[[str, dict], dict] | None = None) -> Pool:
    """Pool of ``node`` (e.g. ``http://127.0.0.1:18081``). Works on restricted public nodes. Raises PoolUnavailable."""
    base = node.rstrip("/")
    call = post or (lambda path, payload: _http_post(f"{base}/{path}", payload, timeout))
    try:
        resp = call("get_transaction_pool", {})
        if isinstance(resp, dict) and resp.get("status") == "OK":
            return Pool("full", resp.get("transactions") or [])
    except EndpointRefused:
        pass
    try:
        steps = _rebuild_steps(call("get_transaction_pool_hashes", {}))
        payload = next(steps)
        while True:
            payload = steps.send(call("get_transactions", payload))
    except StopIteration as done:
        return done.value
    except EndpointRefused as e:
        raise PoolUnavailable(f"node refuses get_transaction_pool_hashes ({e})") from None


async def aget_pool(post: Callable[[str, dict], Awaitable[dict]]) -> Pool:
    """Async variant. ``post(path, payload)`` must return the decoded JSON body and raise
    :class:`EndpointRefused` (or any exception with ``.status`` in 403/404/405) when the node refuses the path."""
    def refused(e: Exception) -> bool:
        return isinstance(e, EndpointRefused) or getattr(e, "status", None) in REFUSED
    try:
        resp = await post("get_transaction_pool", {})
        if isinstance(resp, dict) and resp.get("status") == "OK":
            return Pool("full", resp.get("transactions") or [])
    except Exception as e:  # noqa: BLE001 - only refusals fall through to the rebuild
        if not refused(e):
            raise
    try:
        steps = _rebuild_steps(await post("get_transaction_pool_hashes", {}))
        payload = next(steps)
        while True:
            payload = steps.send(await post("get_transactions", payload))
    except StopIteration as done:
        return done.value
    except Exception as e:  # noqa: BLE001
        if refused(e):
            raise PoolUnavailable(f"node refuses get_transaction_pool_hashes ({e})") from None
        raise
