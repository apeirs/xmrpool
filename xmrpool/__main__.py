"""python -m xmrpool http://node:18081  -> prints how the pool was read and its transactions (id, fee)."""
import sys

from . import PoolUnavailable, get_pool


def main(argv=None):
    args = argv if argv is not None else sys.argv[1:]
    if len(args) != 1:
        print("usage: python -m xmrpool <node-url>   e.g. https://xmr-node.cakewallet.com:18081", file=sys.stderr)
        return 2
    try:
        pool = get_pool(args[0])
    except PoolUnavailable as e:
        print(f"pool unavailable: {e}", file=sys.stderr)
        return 1
    print(f"source={pool.source} txs={len(pool)} dropped={pool.dropped}")
    for t in pool.transactions:
        print(t["id_hash"], t.get("fee", ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
