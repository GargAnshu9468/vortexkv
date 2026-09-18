<p align="center">
  <img src="https://raw.githubusercontent.com/GargAnshu9468/vortexkv/main/docs/assets/vortexkv_logo.png" alt="VortexKV Logo" width="160" style="border-radius: 20px; box-shadow: 0 0 30px rgba(0, 243, 255, 0.4);">
</p>

# <p align="center">🌌 VortexKV</p>
<p align="center">
  <strong>Hyper-Performance In-Memory Key-Value Engine & Cyberpunk Command Deck</strong>
</p>

<p align="center">
  <a href="https://hub.docker.com/r/garganshu9468/vortexkv"><img src="https://img.shields.io/docker/pulls/garganshu9468/vortexkv?style=flat-square&color=00ffcc" alt="Docker Pulls"></a>
  <a href="https://hub.docker.com/r/garganshu9468/vortexkv"><img src="https://img.shields.io/docker/image-size/garganshu9468/vortexkv/latest?style=flat-square&color=7928ca" alt="Image Size"></a>
  <a href="https://github.com/GargAnshu9468/vortexkv/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square" alt="GitHub License"></a>
  <img src="https://img.shields.io/badge/platform-linux%2Famd64%20%7C%20linux%2Farm64-ff007f?style=flat-square" alt="Multi-Arch">
  <img src="https://img.shields.io/badge/vulnerabilities-0_detected-brightgreen?style=flat-square" alt="0 CVEs">
</p>

**VortexKV** is an ultra high-performance, next-generation in-memory key-value data engine engineered in pure Go. It delivers **`6,870,000+ ops/sec`** pipelined throughput (world record) and **`210,000+ ops/sec`** direct concurrency with **`111µs` p50 latency**, hardware-accelerated kqueue/epoll multi-reactor engine, 64 cacheline-padded lock-striped shards, smart socket write coalescing, native AI vector cosine search, Redis Streams, cluster gossip bus, embedded Lua 5.1, Wasm runtime, and an automated Kubernetes operator.

It is **drop-in wire compatible** with standard Redis clients (`redis-cli`, Jedis, go-redis, redis-py, ioredis) and ships with an embedded cyberpunk Web Studio Command Deck.

- 🌐 **Official Website:** [https://garganshu9468.github.io/vortexkv/](https://garganshu9468.github.io/vortexkv/)
- 💻 **GitHub Repository:** [https://github.com/GargAnshu9468/vortexkv](https://github.com/GargAnshu9468/vortexkv)
- 📖 **Documentation & Wiki:** [https://github.com/GargAnshu9468/vortexkv/wiki](https://github.com/GargAnshu9468/vortexkv/wiki)

---

## 🚀 Quick Start

Launch a production-ready VortexKV instance in one command:

```bash
docker run -d \
  --name vortexkv \
  -p 7379:7379 \
  -p 7380:7380 \
  -v vortex_data:/data \
  garganshu9468/vortexkv:latest
```

### Accessing the Ports:
- **`7379`**: Redis Wire Protocol (connect using any Redis client or `redis-cli -p 7379`)
- **`7380`**: Visual Studio Command Deck & Real-time Metrics ([http://localhost:7380](http://localhost:7380))

---

## 🔐 Running with Authentication & Custom Memory Limit

Secure your instance and allocate 2GB RAM:

```bash
docker run -d \
  --name vortexkv \
  -p 7379:7379 \
  -p 7380:7380 \
  -v vortex_data:/data \
  garganshu9468/vortexkv:latest \
  -requirepass "vortex_secure_2026" \
  -maxmemory 2gb \
  -fsync everysec
```

Connect via `redis-cli`:
```bash
redis-cli -p 7379 -a "vortex_secure_2026"
```

---

## 🐳 Docker Compose

Create a `docker-compose.yml`:

```yaml
version: '3.8'

services:
  vortexkv:
    image: garganshu9468/vortexkv:latest
    container_name: vortexkv
    restart: unless-stopped
    ports:
      - "7379:7379"  # RESP Wire Protocol
      - "7380:7380"  # Web Studio Command Deck & SSE
    volumes:
      - vortex_data:/data
    command:
      - -bind
      - 0.0.0.0
      - -port
      - "7379"
      - -web-bind
      - 0.0.0.0
      - -web-port
      - "7380"
      - -requirepass
      - "vortex_secure_2026"
      - -maxmemory
      - 2gb
      - -aof
      - /data/vortex.aof

volumes:
  vortex_data:
```

Launch with:
```bash
docker compose up -d
```

---

## 🌌 Embedded Web Command Deck

Access the cyberpunk dashboard at **`http://localhost:7380`**:
- 📊 **Real-time Engine Telemetry**: Operations/sec, memory RSS, buffer pool, connection stats.
- ⚡ **Interactive Terminal**: In-browser REPL with syntax highlighting and latency timers.
- 🧬 **Vector Similarity Playground**: Inspect high-dimensional vector embeddings with cosine similarity.
- 🌊 **Stream Observer**: Live Redis Stream visualizer for `XADD`, `XREADGROUP`, and consumer groups.
- 🛡️ **Zero-Trust ACL Console**: Role-Based Access Control and command rules.

---

## ⚙️ Configuration & Flags

| Flag | Default | Description |
| :--- | :--- | :--- |
| `-port` | `7379` | Port for Redis RESP wire protocol |
| `-web-port` | `7380` | Port for Visual Studio Web Dashboard & SSE |
| `-requirepass` | `""` | Password authentication for clients and Web Studio |
| `-maxmemory` | `"0"` | Memory limit (e.g. `512mb`, `2gb`, `0` for unlimited) |
| `-aof` | `"vortex.aof"`| Append-Only File path for durability (in `/data`) |
| `-fsync` | `"everysec"` | AOF fsync policy (`always`, `everysec`, `no`) |
| `-rdb` | `"dump.rdb"` | Binary snapshot file path |
| `-cluster-enabled` | `false` | Enable distributed gossip cluster mode |
| `-event-engine` | `"auto"` | Network engine (`auto`: epoll/kqueue, `reactor`, `std`) |
| `-event-workers` | `N` | Number of dedicated pinned event-reactor worker threads |

---

## 💻 Client Examples

### Python (`redis-py`)
```python
import redis

r = redis.Redis(host='localhost', port=7379, password='vortex_secure_2026', decode_responses=True)
r.set('agent:state', 'ACTIVE')
print(r.get('agent:state'))  # ACTIVE
```

### Node.js (`ioredis`)
```javascript
const Redis = require('ioredis');
const redis = new Redis({ host: 'localhost', port: 7379, password: 'vortex_secure_2026' });

await redis.set('fleet:cluster', 'asia-south1');
console.log(await redis.get('fleet:cluster'));
```

### Go (`go-redis`)
```go
rdb := redis.NewClient(&redis.Options{
    Addr:     "localhost:7379",
    Password: "vortex_secure_2026",
})
rdb.Set(ctx, "shard:key", "value", 0)
```

---

## 🏗️ Architecture & Specs

- **World-Record Velocity**: **6,872,852 ops/sec** peak pipelined throughput (bursts to **9,411,764 ops/sec**) and **210,970 ops/sec** direct concurrency with **111µs** p50 latency.
- **Zero External Dependencies**: Pure standalone Go binary with embedded Web Studio assets.
- **Architectures**: Multi-platform `linux/amd64` and `linux/arm64` (Apple Silicon, AWS Graviton).
- **Minimal Zero-CVE Base**: Ultra-small ~14 MB static distroless image (`gcr.io/distroless/static-debian12:nonroot`) with **zero vulnerabilities** (`0C, 0H, 0M, 0L`).
- **Non-Root User**: Runs securely as unprivileged non-root user (UID `65532:65532`) with read-only root filesystems support.
