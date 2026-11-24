"""Hybrid Logical Clock (HLC), LWW-Element-Set & 16-bucket Prefix-Merkle SQLite sync engine."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, order=True)
class HlcTimestamp:
    """Kulkarni et al. (2014) Hybrid Logical Clock tuple providing strict total order."""

    wall_ms: int
    logical: int
    node_id: str


class HybridLogicalClock:
    """Monotonic HLC resilient to backward physical wall-clock drift."""

    def __init__(self, node_id: str, *, initial_wall_ms: int | None = None) -> None:
        if not node_id.strip():
            raise ValueError("node_id cannot be empty")
        self.node_id = node_id
        self._wall_ms = initial_wall_ms if initial_wall_ms is not None else int(time.time() * 1000)
        self._logical = 0

    def tick(self, physical_ms: int | None = None) -> HlcTimestamp:
        pt = physical_ms if physical_ms is not None else int(time.time() * 1000)
        if pt > self._wall_ms:
            self._wall_ms = pt
            self._logical = 0
        else:
            self._logical += 1
        return HlcTimestamp(self._wall_ms, self._logical, self.node_id)

    def receive(self, remote: HlcTimestamp, physical_ms: int | None = None) -> HlcTimestamp:
        pt = physical_ms if physical_ms is not None else int(time.time() * 1000)
        prev_wall = self._wall_ms
        self._wall_ms = max(prev_wall, remote.wall_ms, pt)
        if self._wall_ms == prev_wall == remote.wall_ms:
            self._logical = max(self._logical, remote.logical) + 1
        elif self._wall_ms == prev_wall:
            self._logical += 1
        elif self._wall_ms == remote.wall_ms:
            self._logical = remote.logical + 1
        else:
            self._logical = 0
        return HlcTimestamp(self._wall_ms, self._logical, self.node_id)


@dataclass(frozen=True)
class CrdtRecord:
    key: str
    value: Any
    deleted: bool
    ts: HlcTimestamp
    bucket: str


@dataclass(frozen=True)
class SyncStats:
    divergent_buckets: tuple[str, ...]
    records_received: int
    records_applied: int
    records_ignored_stale: int


def _key_bucket(key: str) -> str:
    """Map key uniformly into 1 of 16 hex prefix buckets ('0'..'f') for Merkle anti-entropy."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[0]


def _canonical(val: Any) -> str:
    return json.dumps(val, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


class CrdtSqliteStore:
    """Offline-first SQLite LWW-Element-Set CRDT store with Prefix-Merkle anti-entropy sync."""

    def __init__(self, node_id: str, db_path: str | Path = ":memory:") -> None:
        self.node_id = node_id
        self.clock = HybridLogicalClock(node_id)
        self.db_path = str(db_path)
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS crdt_kv (
                key TEXT PRIMARY KEY,
                bucket TEXT NOT NULL,
                value_json TEXT NOT NULL,
                deleted INTEGER NOT NULL,
                wall_ms INTEGER NOT NULL,
                logical INTEGER NOT NULL,
                origin_node TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_crdt_bucket ON crdt_kv(bucket);
            """
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def put(self, key: str, value: Any, *, physical_ms: int | None = None) -> CrdtRecord:
        ts = self.clock.tick(physical_ms)
        rec = CrdtRecord(key=key, value=value, deleted=False, ts=ts, bucket=_key_bucket(key))
        self.apply_remote([rec])
        return rec

    def delete(self, key: str, *, physical_ms: int | None = None) -> CrdtRecord:
        ts = self.clock.tick(physical_ms)
        rec = CrdtRecord(key=key, value=None, deleted=True, ts=ts, bucket=_key_bucket(key))
        self.apply_remote([rec])
        return rec

    def get(self, key: str) -> Any | None:
        row = self._conn.execute(
            "SELECT value_json, deleted FROM crdt_kv WHERE key = ?", (key,)
        ).fetchone()
        if row is None or bool(row["deleted"]):
            return None
        return json.loads(str(row["value_json"]))

    def active_items(self) -> dict[str, Any]:
        rows = self._conn.execute(
            "SELECT key, value_json FROM crdt_kv WHERE deleted = 0 ORDER BY key ASC"
        ).fetchall()
        return {str(r["key"]): json.loads(str(r["value_json"])) for r in rows}

    def apply_remote(self, records: list[CrdtRecord]) -> tuple[int, int]:
        """Idempotently merge remote LWW-Element-Set records; returns (applied, stale_ignored)."""
        applied = 0
        ignored = 0
        with self._conn:
            for rec in records:
                self.clock.receive(rec.ts)
                existing = self._conn.execute(
                    "SELECT wall_ms, logical, origin_node FROM crdt_kv WHERE key = ?",
                    (rec.key,),
                ).fetchone()
                if existing is not None:
                    cur_ts = HlcTimestamp(
                        int(existing["wall_ms"]),
                        int(existing["logical"]),
                        str(existing["origin_node"]),
                    )
                    if cur_ts >= rec.ts:
                        ignored += 1
                        continue
                self._conn.execute(
                    """
                    INSERT INTO crdt_kv (key, bucket, value_json, deleted, wall_ms, logical, origin_node)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        bucket=excluded.bucket,
                        value_json=excluded.value_json,
                        deleted=excluded.deleted,
                        wall_ms=excluded.wall_ms,
                        logical=excluded.logical,
                        origin_node=excluded.origin_node
                    """,
                    (
                        rec.key,
                        rec.bucket,
                        _canonical(rec.value),
                        1 if rec.deleted else 0,
                        rec.ts.wall_ms,
                        rec.ts.logical,
                        rec.ts.node_id,
                    ),
                )
                applied += 1
        return applied, ignored

    def bucket_digests(self) -> dict[str, str]:
        """Compute deterministic SHA-256 Merkle digests for each of the 16 hex prefix buckets."""
        digests: dict[str, str] = {}
        for b in "0123456789abcdef":
            rows = self._conn.execute(
                """
                SELECT key, value_json, deleted, wall_ms, logical, origin_node
                FROM crdt_kv WHERE bucket = ? ORDER BY key ASC
                """,
                (b,),
            ).fetchall()
            h = hashlib.sha256()
            for r in rows:
                line = f"{r['key']}|{r['value_json']}|{r['deleted']}|{r['wall_ms']}|{r['logical']}|{r['origin_node']}\n"
                h.update(line.encode("utf-8"))
            digests[b] = h.hexdigest()
        return digests

    def export_buckets(self, buckets: list[str] | tuple[str, ...]) -> list[CrdtRecord]:
        if not buckets:
            return []
        placeholders = ",".join("?" for _ in buckets)
        rows = self._conn.execute(
            f"""
            SELECT key, bucket, value_json, deleted, wall_ms, logical, origin_node
            FROM crdt_kv WHERE bucket IN ({placeholders}) ORDER BY key ASC
            """,
            list(buckets),
        ).fetchall()
        return [
            CrdtRecord(
                key=str(r["key"]),
                value=json.loads(str(r["value_json"])),
                deleted=bool(r["deleted"]),
                ts=HlcTimestamp(int(r["wall_ms"]), int(r["logical"]), str(r["origin_node"])),
                bucket=str(r["bucket"]),
            )
            for r in rows
        ]

    def sync_bidirectional(self, peer: CrdtSqliteStore) -> tuple[SyncStats, SyncStats]:
        """Perform 16-bucket Merkle anti-entropy sync between self and `peer`."""
        mine = self.bucket_digests()
        theirs = peer.bucket_digests()
        divergent = tuple(sorted(b for b in mine if mine[b] != theirs[b]))
        if not divergent:
            empty = SyncStats((), 0, 0, 0)
            return empty, empty

        from_peer = peer.export_buckets(divergent)
        from_self = self.export_buckets(divergent)
        a_self, i_self = self.apply_remote(from_peer)
        a_peer, i_peer = peer.apply_remote(from_self)
        return (
            SyncStats(divergent, len(from_peer), a_self, i_self),
            SyncStats(divergent, len(from_self), a_peer, i_peer),
        )
