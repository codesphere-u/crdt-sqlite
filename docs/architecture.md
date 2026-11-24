# Architecture — `crdt-sqlite`

- `crdt_sqlite.sync`: Kulkarni et al. (2014) Hybrid Logical Clock (`HybridLogicalClock`), SQLite WAL LWW-Element-Set with tombstone preservation, 16-bucket SHA-256 Prefix-Merkle anti-entropy digest exchange, and bidirectional delta sync.
- `crdt_sqlite.cli`: Partitioned offline-node convergence demo CLI.
