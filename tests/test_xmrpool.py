import asyncio
import json
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import xmrpool
from xmrpool import EndpointRefused, PoolUnavailable, aget_pool, get_pool

HASHES = [f"{i:064x}" for i in range(1, 251)]  # 250 txs: three restricted batches (100/100/50)
MINED = HASHES[7]  # leaves the pool between the hashes call and the transactions call
TX_JSON = json.dumps({"version": 2, "rct_signatures": {"type": 6, "txnFee": 30440000}})


class FakeMonerod(BaseHTTPRequestHandler):
    mode = "restricted"
    calls: list = []

    def log_message(self, *a):
        pass

    def _send(self, code, body=None):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if body is not None:
            self.wfile.write(json.dumps(body).encode())

    def do_POST(self):
        req = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        path = self.path.strip("/")
        FakeMonerod.calls.append((path, len(req.get("txs_hashes", []))))
        if self.mode == "dead":
            return self._send(404)
        if path == "get_transaction_pool":
            if self.mode == "restricted":
                return self._send(404)  # monerod >= 0.18.5.3 restricted: route not mapped
            return self._send(200, {"status": "OK", "transactions": [{"id_hash": h, "tx_json": TX_JSON, "tx_blob": "00"} for h in HASHES]})
        if path == "get_transaction_pool_hashes":
            return self._send(200, {"status": "OK", "tx_hashes": HASHES})
        if path == "get_transactions":
            hs = req["txs_hashes"]
            if len(hs) > xmrpool.RESTRICTED_TRANSACTIONS_COUNT:
                return self._send(200, {"status": "Too many transactions requested in restricted mode"})
            txs = [{"tx_hash": h, "in_pool": h != MINED, "as_hex": "0201", "as_json": TX_JSON, "double_spend_seen": False} for h in hs]
            return self._send(200, {"status": "OK", "txs": txs})
        return self._send(404)


class LocalNode:
    def __enter__(self):
        FakeMonerod.calls = []
        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), FakeMonerod)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        return f"http://127.0.0.1:{self.srv.server_address[1]}"

    def __exit__(self, *a):
        self.srv.shutdown()
        self.srv.server_close()


class SyncTests(unittest.TestCase):
    def test_unrestricted_node_uses_full_endpoint(self):
        FakeMonerod.mode = "open"
        with LocalNode() as url:
            pool = get_pool(url)
        self.assertEqual(pool.source, "full")
        self.assertEqual(len(pool), 250)
        self.assertEqual([c[0] for c in FakeMonerod.calls], ["get_transaction_pool"])

    def test_restricted_node_rebuilds_pool_in_capped_batches(self):
        FakeMonerod.mode = "restricted"
        with LocalNode() as url:
            pool = get_pool(url)
        self.assertEqual(pool.source, "hashes")
        self.assertEqual(len(pool), 249)
        self.assertEqual(pool.dropped, 1)
        self.assertNotIn(MINED, pool.hashes)
        batches = [n for p, n in FakeMonerod.calls if p == "get_transactions"]
        self.assertEqual(batches, [100, 100, 50])
        tx = pool.transactions[0]
        self.assertEqual(set(tx), {"id_hash", "tx_blob", "tx_json", "double_spend_seen", "fee"})
        self.assertEqual(tx["fee"], 30440000)

    def test_node_refusing_everything_is_unavailable_not_empty(self):
        FakeMonerod.mode = "dead"
        with LocalNode() as url:
            with self.assertRaises(PoolUnavailable):
                get_pool(url)

    def test_bad_status_on_rebuild_raises(self):
        def post(path, payload):
            if path == "get_transaction_pool":
                raise EndpointRefused(404)
            return {"status": "BUSY"}
        with self.assertRaises(PoolUnavailable):
            get_pool("http://unused", post=post)


class AsyncTests(unittest.TestCase):
    def test_async_rebuild_with_status_style_errors(self):
        class ClientResponseError(Exception):  # mimics aiohttp: refusal carries .status
            status = 404
        seen = []

        async def post(path, payload):
            seen.append(path)
            if path == "get_transaction_pool":
                raise ClientResponseError()
            if path == "get_transaction_pool_hashes":
                return {"status": "OK", "tx_hashes": HASHES[:3]}
            return {"status": "OK", "txs": [{"tx_hash": h, "in_pool": True, "as_hex": "02", "as_json": TX_JSON} for h in payload["txs_hashes"]]}

        pool = asyncio.run(aget_pool(post))
        self.assertEqual((pool.source, len(pool)), ("hashes", 3))
        self.assertEqual(seen, ["get_transaction_pool", "get_transaction_pool_hashes", "get_transactions"])

    def test_async_other_errors_propagate(self):
        async def post(path, payload):
            raise TimeoutError("node down")
        with self.assertRaises(TimeoutError):
            asyncio.run(aget_pool(post))


@unittest.skipUnless(os.environ.get("XMRPOOL_LIVE_NODE"), "set XMRPOOL_LIVE_NODE=https://host:port to test a real node")
class LiveTest(unittest.TestCase):
    def test_live_node(self):
        pool = get_pool(os.environ["XMRPOOL_LIVE_NODE"])
        self.assertIn(pool.source, ("full", "hashes"))
        print(f"\nlive: source={pool.source} txs={len(pool)} dropped={pool.dropped}")


if __name__ == "__main__":
    unittest.main()
