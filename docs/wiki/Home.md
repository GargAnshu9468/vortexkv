# 🌌 Welcome to the VortexKV Wiki

> **VortexKV** (*Vector-Optimized Redis-Compatible Throughput Extreme Key-Value Store*) is a next-generation, cyberpunk in-memory data store engineered from scratch in pure Go. It delivers **6,870,000+ ops/sec** pipelined throughput (with peak bursts up to **9,411,764 ops/sec**) and **210,000+ ops/sec** direct concurrency with **~111µs p50 latency**, hardware-accelerated kqueue/epoll event reactor, 64 cacheline-padded lock-striped shards, smart socket write coalescing, native HNSW AI vector graphs, Redis Streams with consumer groups, an autonomous cluster gossip bus, dual scripting (Lua 5.1 & Wasm), and an embedded visual command deck.

---

## 🧭 Wiki Table of Contents

| Topic | Description | Link |
| :--- | :--- | :--- |
| **🚀 Getting Started** | Installation via cURL, Docker, Helm, or source | [[Getting-Started]] |
| **🏎️ Architecture & Core** | 64 lock-striped shards, memory models, zero-alloc timing wheel | [[Architecture-and-Internals]] |
| **🧠 Native AI Vector Search** | HNSW skip-graphs, Cosine/Euclidean/Dot similarity (`VSEARCH`) | [[AI-Vector-Search]] |
| **🌊 Streams & Consumer Groups** | Event streaming, Pending Entries List (PEL), and worker pools | [[Streams-and-Consumer-Groups]] |
| **📡 Cluster & Gossip Protocol** | 16,384 hash slots, Port 17379 binary bus, autonomous failover | [[Cluster-and-Gossip-Protocol]] |
| **⚖️ Cluster Slot Rebalancer** | CLI automation for live online slot and key migrations | [[Cluster-Rebalancing]] |
| **🔁 Replication & Failover** | Master-Replica async replication, PSYNC handshake, instant failover | [[Replication-and-Failover]] |
| **📜 Scripting Engines** | Embedded Lua 5.1 & WebAssembly (Wasm) runtime via Wazero | [[Scripting-Lua-and-Wasm]] |
| **☸️ Kubernetes Operator** | Declarative CRD (`kind: VortexCluster`) and auto-scaling | [[Kubernetes-Operator]] |
| **💾 Persistence & Durability** | Binary RDB snapshots with CRC64 & durable Append-Only File (AOF) | [[Persistence-and-Durability]] |
| **🔒 Security & ACL** | Redis 6+ multi-user access control, TLS encryption, and tokens | [[Security-and-ACL]] |
| **📊 Observability** | Prometheus `/metrics` exporter and `/healthz` Kubernetes probes | [[Telemetry-and-Metrics]] |

---

## ⚡ Core Philosophy & Architecture

```
                       ┌───────────────────────────────┐
                       │   Client Application / SDK    │
                       │ (Python, Node, Go, redis-cli) │
                       └───────────────┬───────────────┘
                                       │ RESP2 / RESP3 (Port 7379)
                                       ▼
                       ┌───────────────────────────────┐
                       │   Vortex Wire Parser & Auth   │
                       └───────────────┬───────────────┘
                                       │
            ┌──────────────────────────┴──────────────────────────┐
            ▼                                                     ▼
┌───────────────────────┐                             ┌───────────────────────┐
│ 64-Shard Lock Striped │                             │ Embedded Visual Deck  │
│  Concurrent Keyspace  │                             │   (HTTP Port 7380)    │
└───────────┬───────────┘                             └───────────────────────┘
            │
            ├──────────────► 🧠 HNSW Vector Index (Multi-layer Skip Graphs)
            ├──────────────► 🌊 Event Streams Engine (PEL & Consumer Groups)
            ├──────────────► 📜 Lua 5.1 & Wasm Engine (Pure-Go Sandboxed)
            ├──────────────► 📡 Gossip Bus & Cluster Mesh (Port 17379)
            └──────────────► 💾 Dual Durability (AOF + Binary RDB Snapshots)
```

### 1. Breaking the Global Mutex Bottleneck
Standard Redis processes all commands through a single thread to avoid concurrency issues, limiting throughput to a single CPU core. VortexKV pairs a **hardware-accelerated Multi-Reactor engine (kqueue/epoll)** with **64 independent cacheline-padded lock-striped shards** and **smart socket write coalescing**, allowing high-concurrency workloads to utilize all CPU cores simultaneously and batch pipelined responses into consolidated kernel writes (**6.8M+ ops/sec** peak, **2.7M+ ops/sec** GET).

### 2. Dedicated Non-Conflicting Ports
VortexKV is engineered for seamless coexistence with existing database infrastructure:
- **`7379`**: RESP2/RESP3 Redis client wire protocol (leaves standard port `6379` open).
- **`7380`**: Embedded Cyberpunk Web Studio & HTTP REST API.
- **`17379`**: Cluster binary heartbeat and gossip bus (`client_port + 10000`).

---

## ⚡ 5-Second Quick Verification

```bash
# Connect via standard redis-cli:
redis-cli -p 7379 -a "vortex_secure_2026" PING
# PONG

# Store high-dimensional embeddings:
redis-cli -p 7379 -a "vortex_secure_2026" VADD embeddings doc_ai 0.95 0.05 0.0 0.0

# Search Top-1 nearest neighbor using Cosine similarity:
redis-cli -p 7379 -a "vortex_secure_2026" VSEARCH embeddings 1 cosine 0.90 0.10 0.0 0.0
# 1) 1) "doc_ai"
#    2) "0.999512"
```

---

## 🔗 External Links
- **GitHub Repository**: [GargAnshu9468/vortexkv](https://github.com/GargAnshu9468/vortexkv)
- **Live Web Sandbox**: [garganshu9468.github.io/vortexkv](https://garganshu9468.github.io/vortexkv/)
- **License**: [MIT Open Source](https://github.com/GargAnshu9468/vortexkv/blob/main/LICENSE)
