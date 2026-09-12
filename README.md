# 🌌 VortexKV — Hyper-Performance Redis & Immersive Command Studio

> **New-Generation, Cyberpunk, Blazing-Fast In-Memory Data Engine & Visual Control Deck.**  
> Drop-in compatible with standard Redis clients, running on non-conflicting dedicated ports (**`7379`** for wire protocol & **`7380`** for Web Studio).

---

## ⚡ Highlights & Innovations

- 🚀 **Drop-in Redis Protocol Compatibility**: Fully implements the RESP2/RESP3 wire protocol on dedicated port **`7379`** (avoids any conflict with standard Redis on 6379). Works out-of-the-box with `redis-cli -p 7379`, Python `redis`, Node `ioredis`, Go `go-redis`, Spring Data Redis, etc.
- 🏎️ **Blazing Concurrent Throughput**:
  - **`210,000+ ops/sec`** on local workstations with **`~135µs` p50 latency**.
  - Lock-striped concurrent keyspace with 64 shards to eliminate global mutex bottlenecks.
- 🔒 **Enterprise Production Security**:
  - Full `requirepass` and `AUTH [username] <password>` support.
  - Native **TLS/SSL wire encryption** (`-tls-cert`, `-tls-key`).
  - Web Studio security shield modal protecting HTTP endpoints and WebSockets with Bearer tokens.
- 💾 **Resource Safety & MaxMemory LRU Eviction**:
  - Strict memory cap (`-maxmemory 4gb`) with active `allkeys-lru` eviction to eliminate Out-Of-Memory (OOM) crashes.
  - Configurable `maxclients` connection ceiling (default 10,000).
- 🔄 **ACID Atomic Transactions**: Full support for `MULTI`, `EXEC`, and `DISCARD`.
- 📜 **Embedded Lua 5.1 Scripting**: Sub-millisecond atomic multi-step scripts (`EVAL`, `EVALSHA`, `SCRIPT LOAD`, `SCRIPT EXISTS`, `SCRIPT FLUSH`, `SCRIPT KILL`) with pure-Go runtime, bidirectional RESP conversion, `redis.call`/`redis.pcall` bridge, SHA1 caching, and 5-second runaway timeout protection.
- 📡 **Cluster Gossip Bus & Automatic Failover**: Dedicated binary inter-node bus on `port + 10000` (e.g. `17379`) with continuous heartbeat exchanges, majority `PFAIL`/`FAIL` detection consensus, and fully autonomous replica election and slot takeover without human intervention.
- 📦 **Automated Multi-Architecture Releases**: Official GitHub Actions pipeline releasing pre-compiled binary packages for Linux (`amd64`, `arm64`), macOS (`Apple Silicon`, `Intel`), and Windows (`amd64`) with cryptographic SHA256 checksums.
- 🪐 **2D/3D Force-Directed Keyspace Galaxy**: Interactive canvas visualizer in the browser grouping keys by namespace and data types with neon particle flows.
- 🧠 **Native HNSW AI Vector Graph Indexing**: Million-scale sub-millisecond approximate nearest-neighbor search (`VADD`, `VSEARCH`, `VSIM`, `VINFO`, `VDEL`) with multi-layer skip-graphs ($O(\log N)$) and cosine, euclidean, and dot product metrics.
- ⏱️ **Zero-Alloc Active & Passive TTL Expiration**: Sub-millisecond timing wheel and probabilistic active sampling.
- 📦 **Zero-Concern Production Packaging**: Includes multi-stage `Dockerfile`, `systemd` service unit (`vortexkv.service`), and production template `vortex.conf`.
- 💾 **Dual Persistence (AOF + Binary RDB Snapshots)**: Complete point-in-time snapshotting (`SAVE`, `BGSAVE`, `LASTSAVE`) in standard `REDIS0009` format with 64-bit CRC64 checksum validation, alongside Append-Only File (AOF) durability with configurable fsync policies (`always`, `everysec`, `no`).
- 🔁 **Master-Replica Asynchronous Replication**: Redis 6+ compatible `PSYNC`, `REPLCONF`, and `REPLICAOF` for horizontal read scaling, live command streaming, and instant failover (`REPLICAOF NO ONE`).
- 🌐 **Distributed Multi-Node Cluster & 16,384 Hash Slots**: Linear horizontal scaling with 16,384 CRC16 hash slots, standard `-MOVED <slot> <ip:port>` client redirection, `{...}` hash tags for atomic multi-key co-location, and cluster wire commands (`CLUSTER KEYSLOT`, `CLUSTER NODES`, `CLUSTER SLOTS`, `CLUSTER MEET`, `CLUSTER ADDSLOTS`, `CLUSTER COUNTKEYSINSLOT`, `CLUSTER GETKEYSINSLOT`).
- 🌊 **Redis Streams & Consumer Groups**: High-throughput, sub-millisecond event streaming and distributed task queues (`XADD`, `XREAD`, `XGROUP`, `XREADGROUP`, `XACK`, `XPENDING`, `XINFO`) with Pending Entries List (PEL) and zero-CPU blocking reads.
- 📡 **Real-time Web Studio & WebSocket Stream**: Embedded SPA dashboard running on port `7380` with zero external dependencies.
- 📊 **Cloud-Native Prometheus Observability**: Built-in Prometheus text exporter on `GET /metrics` and Kubernetes liveness/readiness probe on `GET /healthz`.
- 🚢 **Production Ready Orchestration**: Official Kubernetes Helm Chart (`deployments/helm/vortexkv`) and multi-container `docker-compose.yml` with metrics profiling.

---

## 📚 Complete Documentation Suite

Comprehensive production guides, client SDK integration code, and architectural references are available in the **[`/docs`](./docs/README.md)** directory:

- 🚀 [**Quickstart Guide**](./docs/quickstart.md) — 5-minute setup and tour.
- 🌐 [**Distributed Cluster Guide**](./docs/cluster.md) — 16,384 hash slots, multi-node routing, and failover.
- 🔁 [**Replication & High Availability Guide**](./docs/replication.md) — PSYNC, read-scaling, and failover topologies.
- 🌊 [**Streams & Consumer Groups Guide**](./docs/streams.md) — Distributed event streaming, worker pools, and PEL recovery.
- 🔌 [**Client SDK Integration Guides**](./docs/clients.md) — Drop-in code for Python, Node.js, Go, Java, Rust, and C#.
- 📖 [**Full Command Reference**](./docs/commands.md) — Standard RESP commands, Streams, and AI vector primitives.
- 🛡️ [**Production Hardening Guide**](./docs/production-hardening.md) — Linux kernel tuning, memory limits, TLS, and systemd.
- ⚙️ [**Architecture & Internals**](./docs/architecture.md) — Lock-striped concurrency, timing wheels, and wire parsers.

---

## 🛠️ Quick Start

### ⚡ 1. One-Line Universal Installer (macOS & Linux)
```bash
curl -fsSL https://raw.githubusercontent.com/vortexkv/vortexkv/main/install.sh | bash
```

### 🐳 2. Run via Docker Compose
```bash
docker compose up -d
# Or launch with complete Prometheus & Grafana monitoring stack:
docker compose --profile monitoring up -d
```

### 🔨 3. Build Single Executable from Source
```bash
make build
```
This generates:
- `bin/vortex-server`: The combined server daemon and embedded Web Studio in a single zero-dependency binary.
- `bin/vortex-cli`: Interactive terminal client connecting to `:7379` with syntax colors and microsecond execution timers.

### 2. Launch the Engine
```bash
./bin/vortex-server -requirepass "vortex_secure_2026" -maxmemory 1gb
```

You will see:
```text
  ██╗   ██╗ ██████╗ ██████╗ ████████╗███████╗██╗  ██╗    ██╗  ██╗██╗   ██╗
  ██║   ██║██╔═══██╗██╔══██╗╚══██╔══╝██╔════╝╚██╗██╔╝    ██║ ██╔╝██║   ██║
  ██║   ██║██║   ██║██████╔╝   ██║   █████╗   ╚███╔╝     █████╔╝ ██║   ██║
  ╚██╗ ██╔╝██║   ██║██╔══██╗   ██║   ██╔══╝   ██╔██╗     ██╔═██╗ ╚██╗ ██╔╝
   ╚████╔╝ ╚██████╔╝██║  ██║   ██║   ███████╗██╔╝ ██╗    ██║  ██╗ ╚████╔╝ 
    ╚═══╝   ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝  ╚═╝    ╚═╝  ╚═╝  ╚═══╝  
  » Next-Generation Hyper-Performance In-Memory Data Store & Studio «
  Version: 1.0.0-PROD  |  Protocol: RESP2/RESP3  |  Engine: Sharded Lock-Striped

[VortexKV] 🚀 Redis RESP Wire Server listening on 0.0.0.0:7379
[VortexKV] ⚡ Redis Client Port: 0.0.0.0:7379 (redis-cli -p 7379)
[VortexKV] 🔒 Security: Password authentication (requirepass) ACTIVE
[VortexKV] 💾 MaxMemory: 1gb with allkeys-lru eviction
[VortexKV] 🌌 Immersive Visual Studio: http://0.0.0.0:7380
```

### 3. Connect via Standard Redis CLI (Port 7379)
```bash
redis-cli -p 7379 -a "vortex_secure_2026" PING
# PONG

redis-cli -p 7379 -a "vortex_secure_2026" SET user:100 "HyperNova" EX 60
redis-cli -p 7379 -a "vortex_secure_2026" GET user:100
# "HyperNova"
```

### 4. Connect via Embedded Web Studio (Port 7380)
Open your browser at:
👉 **`http://localhost:7380`**

---

## 🧠 AI Vector Commands (Next-Gen)

VortexKV allows storing high-dimensional vector embeddings and performing top-K nearest neighbor searches natively:

```bash
# Store 4-dimensional embeddings:
redis-cli -p 7379 -a "vortex_secure_2026" VADD embeddings doc_ai 0.95 0.05 0.0 0.0
redis-cli -p 7379 -a "vortex_secure_2026" VADD embeddings doc_science 0.1 0.9 0.0 0.0

# Search Top-1 nearest neighbor using Cosine similarity:
redis-cli -p 7379 -a "vortex_secure_2026" VSEARCH embeddings 1 cosine 0.90 0.10 0.0 0.0
# 1) 1) "doc_ai"
#    2) "0.999512"
```

---

## 📊 Benchmark Results

Benchmarked with official `redis-benchmark` on port 7379:
```bash
redis-benchmark -p 7379 -a "vortex_secure_2026" -t set,get -n 50000 -q -c 50
```
- **SET**: `190,114 requests/sec` | `p50: 0.143ms`
- **GET**: `210,970 requests/sec` | `p50: 0.135ms`
