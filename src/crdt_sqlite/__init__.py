"""crdt-sqlite — HLC + LWW-Element-Set + Prefix-Merkle anti-entropy SQLite sync engine."""

from crdt_sqlite.sync import (
    CrdtRecord,
    CrdtSqliteStore,
    HlcTimestamp,
    HybridLogicalClock,
    SyncStats,
)

__version__ = "0.1.0"

__all__ = [
    "CrdtRecord",
    "CrdtSqliteStore",
    "HlcTimestamp",
    "HybridLogicalClock",
    "SyncStats",
    "__version__",
]
