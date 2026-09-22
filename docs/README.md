# 📚 VortexKV Documentation

Welcome to the official **VortexKV** technical documentation. VortexKV is an ultra-fast, drop-in Redis-compatible in-memory data engine with native AI vector search primitives, sub-millisecond latency, and an embedded cyberpunk visual command deck.

---

## 🧭 Documentation Index

| Guide | Description |
| :--- | :--- |
| [🚀 **Quickstart**](./quickstart.md) | Install VortexKV via Docker, binaries, or source, and execute your first commands in under 5 minutes. |
| [🔌 **Client SDK Guides**](./clients.md) | Connect to VortexKV in Python, Node.js, Go, Java/Spring, Rust, and C# using existing Redis libraries. |
| [📖 **Command Reference**](./commands.md) | Comprehensive command specification including Core KV, Hashes, Lists, Sets, Sorted Sets, ACL, and AI Vectors. |
| [🛡️ **Production Hardening**](./production-hardening.md) | Essential Linux kernel parameters, memory limits, TLS certificates, systemd services, and backup policies. |
| [🔁 **Replication & High Availability**](./replication.md) | Master-replica asynchronous replication (PSYNC, REPLCONF), failover, read scaling, and clustering. |
| [🌊 **Streams & Consumer Groups**](./streams.md) | Distributed event streaming, consumer groups, PEL tracking, and zero-CPU blocking queues. |
| [⚙️ **Architecture Internals**](./architecture.md) | Deep dive into lock-striped sharding (256 shards), sub-millisecond TTL timing wheels, and the RESP parser. |

---

## ⚡ Key Architectural Advantages

- **Zero Port Collisions**: Listens on dedicated ports:
  - Wire Protocol: **`7379`** (instead of standard Redis 6379).
  - Cyberpunk Web Studio & Telemetry: **`7380`**.
- **Drop-In Wire Compatibility**: Implements standard RESP2/RESP3. Works seamlessly with `redis-cli`, ORMs, and official client drivers.
- **Concurrent Sharded Keyspace & Multi-Reactor Engine**: Hardware-accelerated kqueue/epoll multi-reactor engine and 256 cacheline-padded mutex-striped shards, delivering **>6,870,000 ops/sec** pipelined throughput (world record) and **>210,000 ops/sec** direct concurrency with ~111µs latency.
- **Embedded Visual Studio**: Single self-contained binary embeds a rich, zero-dependency visual command deck featuring an interactive 2D/3D keyspace galaxy, slowlog stream, and ACL manager.
- **Native AI Vector Search**: High-dimensional vector indexing and nearest-neighbor search (`VADD`, `VSEARCH`, `VSIM`) built directly into the storage engine.
- **Cloud-Native Observability**: Standard Prometheus exporter (`GET /metrics`) and Kubernetes liveness/readiness probes (`GET /healthz`).
