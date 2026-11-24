"""Tests for HLC monotonicity, LWW commutativity/tombstones, Merkle anti-entropy sync & CLI."""

from __future__ import annotations

import json

import pytest

from crdt_sqlite import CrdtSqliteStore, HybridLogicalClock
from crdt_sqlite.cli import main as cli_main


def test_hlc_stays_monotonic_under_backward_clock_drift() -> None:
    clk = HybridLogicalClock("node-1", initial_wall_ms=5_000)
    t1 = clk.tick(5_000)
    t2 = clk.tick(4_200)  # physical clock jumped backward by 800ms
    assert t2 > t1
    assert t2.wall_ms == 5_000 and t2.logical == t1.logical + 1


def test_merkle_sync_converges_independently_of_order_and_preserves_tombstones() -> None:
    a = CrdtSqliteStore("tablet-a")
    b = CrdtSqliteStore("tablet-b")
    c = CrdtSqliteStore("tablet-c")

    a.put("k1", {"v": "from-a"}, physical_ms=10_000)
    b.put("k1", {"v": "from-b-newer"}, physical_ms=11_000)
    b.put("k2", {"v": "only-b"}, physical_ms=10_500)
    c.put("k3", {"v": "to-delete"}, physical_ms=10_100)
    c.delete("k3", physical_ms=12_000)

    # Sync in arbitrary order: A<->B, B<->C, A<->C
    a.sync_bidirectional(b)
    b.sync_bidirectional(c)
    s_a, s_c = a.sync_bidirectional(c)
    assert s_a.divergent_buckets or s_c.divergent_buckets

    # Second sync must detect 0 divergent buckets (idempotent convergence)
    s2_a, s2_b = a.sync_bidirectional(b)
    assert s2_a.divergent_buckets == ()
    assert s2_b.divergent_buckets == ()

    assert a.bucket_digests() == b.bucket_digests() == c.bucket_digests()
    assert a.active_items() == {"k1": {"v": "from-b-newer"}, "k2": {"v": "only-b"}}
    assert a.get("k3") is None

    a.close()
    b.close()
    c.close()


def test_cli_demo(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli_main(["demo"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["merkle_converged"] is True
    assert out["active_state"]["stock:ors"] == {"boxes": 35}
