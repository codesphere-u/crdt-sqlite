# crdt-sqlite

**Zero-dependency Python Hybrid Logical Clock (HLC), LWW-Element-Set with tombstones, and 16-bucket Prefix-Merkle anti-entropy SQLite sync engine for offline-first distributed apps.**

Pure Python 3.11+ & SQLite WAL. Lets offline field tablets, clinics, cooperatives, and edge servers synchronize diverging state in minimal round-trips with deterministic conflict-free convergence.

## Quick Start

```bash
uv sync --extra dev
crdt-sqlite demo
```

## License

MIT License — see [LICENSE](LICENSE).
