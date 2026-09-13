# 🏎️ Architecture & Engine Internals

This document covers the internal design, concurrency patterns, and data structures powering VortexKV.

---

## 1. 64-Shard Lock Striping

To avoid global thread contention without compromising atomicity, VortexKV divides the entire database keyspace into 64 independent shards:

```go
type Engine struct {
    shards [64]*Shard
}

type Shard struct {
    mu       sync.RWMutex
    entries  map[string]*Entry
    expiries map[string]int64
}
```

- **Hashing Algorithm**: FNV-1a / Murmur3 hash over the key bytes.
- **Complexity**: $O(1)$ shard lookup via bitwise mask: `shardIndex = hash(key) & 63`.
- **Concurrency Benefit**: Up to 64 concurrent writes can proceed simultaneously across parallel CPU cores without waiting for global mutexes.

---

## 2. Native HNSW AI Vector Graph Engine

VortexKV provides native vector search without external plugins:
- **Data Structure**: Hierarchical Navigable Small World (HNSW) skip-graph.
- **Layers**: Multi-layer graph where higher layers contain sparse long-range links and lower layers contain dense local links.
- **Search Complexity**: $O(\log N)$ nearest neighbor search.
- **Supported Distance Metrics**:
  - `cosine`: Normalized similarity between -1 and 1.
  - `euclidean`: Euclidean geometric distance.
  - `dot`: Raw dot-product similarity.

---

## 3. Zero-Alloc Timing Wheel & TTL Expiration

Keys with an active Time-To-Live (TTL) are managed through a dual-mode mechanism:
1. **Passive Eviction**: When a key is accessed (`GET`, `HGET`, etc.), its expiration timestamp is checked against `time.Now().UnixMilli()`. If expired, it is deleted instantly and `(nil)` is returned.
2. **Active Sampling**: A background loop executes every 100ms, randomly sampling 20 keys with TTLs from each shard. If more than 25% are expired, it repeats immediately, preventing dead keys from accumulating in memory.

---

## 4. Dual Persistence Architecture

VortexKV combines snapshot safety with point-in-time recovery:
- **Append-Only File (AOF)**: Logs every write command (`SET`, `HSET`, `XADD`, etc.) with configurable `fsync` policies (`always`, `everysec`, `no`).
- **Binary RDB Snapshots**: Emits standard `REDIS0009` binary snapshots (`SAVE`, `BGSAVE`) verified with 64-bit CRC64 checksums.
