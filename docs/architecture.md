# ⚙️ VortexKV Architecture & Internals

This document details the internal design and concurrency model that allow VortexKV to achieve **sub-millisecond latency** and **over 210,000 operations per second** on modern multi-core machines.

---

## 1. Concurrency Model: Lock-Striped Sharding

Unlike single-threaded in-memory stores that bottleneck on a single CPU core, VortexKV implements a **64-way lock-striped concurrent keyspace**:

```
                       Incoming Commands (Wire Port 7379)
                                       │
                         MurmurHash3 / FNV-1a Hash
                                       │
           ┌───────────┬───────────┬───┴───────┬───────────┐
           ▼           ▼           ▼           ▼           ▼
       [Shard 0]   [Shard 1]   [Shard 2]  ... [Shard 62]  [Shard 63]
       [Mutex 0]   [Mutex 1]   [Mutex 2]  ... [Mutex 62]  [Mutex 63]
```

- Each key is hashed deterministically into one of 64 independent shards:
  $$\text{shardIndex} = \text{hash}(\text{key}) \pmod{64}$$
- Each shard owns an independent `sync.RWMutex` and hash map.
- Read operations (`GET`, `HGET`) acquire read locks on only the designated shard, allowing hundreds of concurrent readers across multiple CPU cores without lock contention.
- Writes (`SET`, `DEL`) only acquire an exclusive lock on the single target shard.

---

## 2. Zero-Allocation Sub-Millisecond TTL Timing Wheel

Managing expirations efficiently is critical to preventing latency spikes. VortexKV pairs two complimentary expiration engines:

1. **Passive Expiration (Lazy Check)**:
   - When any key is accessed (`GET`, `HGET`, `EXISTS`), its TTL timestamp is inspected.
   - If `expiredAt > 0` and `time.Now() >= expiredAt`, the key is immediately purged and `nil` is returned.
2. **Active Expiration (Timing Wheel)**:
   - An asynchronous background routine samples keys from shards every 100 milliseconds.
   - If more than 25% of sampled keys are expired, the routine loops aggressively to reclaim memory before evictions are needed.
   - Expired keys emit delete notifications into the AOF stream to ensure replica parity.

---

## 3. High-Performance Zero-Copy RESP2/RESP3 Wire Parser

VortexKV's network layer in `internal/resp/parser.go` directly parses byte buffers from TCP streams:
- Uses stack-allocated token buffers for common single-digit integers and bulk string lengths.
- Implements strict validation to eliminate allocation bombs (`MaxArrayLength = 1,000,000` elements).
- Eliminates unnecessary string conversions by passing byte slices directly into command handlers whenever possible.

---

## 4. Single-Binary Architecture & Embedded Web Studio

VortexKV ships as a **single, static, zero-dependency executable**:
- The Cyberpunk Web Studio UI (`internal/web/dist/index.html`) is compiled directly into the Go binary using Go 1.16+ `//go:embed`.
- The Web Studio communicates with the engine via:
  - High-frequency bi-directional **WebSockets (`/ws`)** for live 60 FPS telemetry streams (CPU, ops/sec, slowlog events).
  - High-speed REST endpoints (`/api/keys`, `/api/exec`, `/api/acl/users`).
  - Standard Prometheus scrape endpoint (`/metrics`) and health check probe (`/healthz`).
