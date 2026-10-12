# xmrpool

Read the Monero transaction pool from **any** `monerod`, including restricted public nodes.

Since **monerod 0.18.5.3** (released 2026-10-06, [monero-project/monero#10973](https://github.com/monero-project/monero/pull/10973)),
restricted RPC no longer serves `/get_transaction_pool`. Public nodes now answer it with **HTTP 404**:

```text
$ curl -s -o /dev/null -w '%{http_code}\n' -X POST https://xmr-node.cakewallet.com:18081/get_transaction_pool -d '{}'
404
$ curl -s -X POST https://xmr-node.cakewallet.com:18081/get_transaction_pool_hashes -d '{}' | jq '.status, (.tx_hashes|length)'
"OK"
52
```

Software that called the old endpoint against a public node now fails every poll. Worse, it may catch the error and
treat the pool as empty. A payment processor watching the mempool for incoming XMR then misses every 0-conf payment
until it confirms.

`xmrpool` asks for the full pool first. When a node refuses it, it rebuilds the pool from
`/get_transaction_pool_hashes` plus `/get_transactions` (in batches of 100, the restricted limit). If neither works,
it raises `PoolUnavailable`, never an empty list.

Pure Python 3.9+, standard library only, MIT.

## Install

```bash
pip install git+https://github.com/apeirs/xmrpool
```

## Use

```python
from xmrpool import get_pool, PoolUnavailable

pool = get_pool("https://xmr-node.cakewallet.com:18081")
print(pool.source)        # "full" on your own node, "hashes" on a restricted public node
print(len(pool), pool.dropped)
for tx in pool.transactions:            # same shape as /get_transaction_pool's "transactions"
    tx["id_hash"], tx["tx_blob"], tx["tx_json"], tx.get("fee")
```

Async code, with any HTTP client: pass a `post(path, payload)` coroutine. It must raise an exception with
`.status` 403/404/405 when the node refuses a path. aiohttp's `ClientResponseError` already does.

```python
import aiohttp
from xmrpool import aget_pool

async def read_pool(session: aiohttp.ClientSession, node: str):
    async def post(path, payload):
        async with session.post(f"{node}/{path}", json=payload) as r:
            r.raise_for_status()
            return await r.json(content_type=None)
    return await aget_pool(post)
```

Command line:

```text
$ python -m xmrpool https://node.monerodevs.org:18089
source=hashes txs=44 dropped=0
dc3da19ac439360386f7a42b4873319b384994aad3e2341aeb3d1519dd69c107 30680000
...
```

## What a rebuilt pool contains

| field | full endpoint | rebuilt from hashes |
|---|---|---|
| `id_hash`, `tx_blob`, `tx_json` | yes | yes (from `tx_hash`, `as_hex`, `as_json`) |
| `fee` | yes | yes, from `rct_signatures.txnFee` (RingCT txs) |
| `double_spend_seen` | yes | yes |
| `receive_time`, `relayed`, `kept_by_block`, `last_relayed_time`, … | yes | **no**; the restricted node doesn't expose them |

`pool.dropped` counts hashes that left the pool between the two calls (mined or evicted).

## Tests

```bash
python -m unittest -v
XMRPOOL_LIVE_NODE=https://xmr-node.cakewallet.com:18081 python -m unittest tests.test_xmrpool.LiveTest
```

The offline tests run a stub monerod in three modes:
- unrestricted;
- restricted: 404 on the pool, the 100-hash cap enforced, and one tx mined mid-read;
- refusing everything.

They also cover the async path with aiohttp-style errors. The live test hits a real restricted node.

## Known affected projects (October 2026)

Code calling `/get_transaction_pool` that breaks on restricted nodes includes:
- the Monero daemon in **bitcart** (`daemons/xmr.py`, polled every 5 s for 0-conf payments);
- `MoneroDaemonRpc.getTxPool` in **monero-ts**;
- **onion-monero-blockchain-explorer**.

Running against your own unrestricted node is unaffected.
