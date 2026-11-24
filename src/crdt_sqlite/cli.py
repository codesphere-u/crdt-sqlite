"""CLI for crdt-sqlite."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from crdt_sqlite.sync import CrdtSqliteStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crdt-sqlite",
        description="HLC + LWW-Element-Set + 16-bucket Prefix-Merkle anti-entropy SQLite sync CLI.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("demo", help="Simulate 2 partitioned offline SQLite nodes converging via Merkle sync")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "demo":
        node_a = CrdtSqliteStore("clinic-gulu")
        node_b = CrdtSqliteStore("clinic-lira")
        node_a.put("stock:ors", {"boxes": 40}, physical_ms=1_000_000)
        node_b.put("stock:ors", {"boxes": 35}, physical_ms=1_005_000)
        node_a.put("stock:zinc", {"boxes": 20}, physical_ms=1_002_000)
        node_b.put("temp:obsolete", {"v": 1}, physical_ms=1_001_000)
        node_b.delete("temp:obsolete", physical_ms=1_006_000)

        s_a, s_b = node_a.sync_bidirectional(node_b)
        converged = node_a.bucket_digests() == node_b.bucket_digests()
        print(
            json.dumps(
                {
                    "divergent_buckets": list(s_a.divergent_buckets),
                    "node_a_applied": s_a.records_applied,
                    "node_b_applied": s_b.records_applied,
                    "merkle_converged": converged,
                    "active_state": node_a.active_items(),
                },
                indent=2,
            )
        )
        node_a.close()
        node_b.close()
        return 0 if converged else 2
    return 1


if __name__ == "__main__":
    sys.exit(main())
