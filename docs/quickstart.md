# 🚀 Quickstart Guide

Get VortexKV running on your machine and execute your first operations in under 5 minutes.

---

## 1. Installation Methods

### Option A: Universal Installer Script (Recommended)
```bash
curl -fsSL https://raw.githubusercontent.com/GargAnshu9468/vortexkv/main/install.sh | bash
```

### Option B: Docker / Container
Run the official container directly:
```bash
docker run -d \
  --name vortexkv \
  -p 7379:7379 \
  -p 7380:7380 \
  -v vortex-data:/data \
  garganshu9468/vortexkv:latest \
  -requirepass "vortex_secure_2026" \
  -maxmemory 1gb
```

### Option C: Compile from Source
Prerequisites: Go 1.21+ installed.
```bash
git clone https://github.com/GargAnshu9468/vortexkv.git
cd vortexkv
make build
```
This generates:
- `bin/vortex-server`: The combined database engine and Web Studio.
- `bin/vortex-cli`: Terminal client with syntax highlighting and latency timers.

---

## 2. Launching the Engine

Start VortexKV with a password and memory limit:
```bash
./bin/vortex-server -requirepass "vortex_secure_2026" -maxmemory 1gb
```

Output:
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

---

## 3. Connecting to VortexKV

### Using Standard `redis-cli`
VortexKV operates on port **`7379`**:
```bash
redis-cli -p 7379 -a "vortex_secure_2026" PING
# Output: PONG

redis-cli -p 7379 -a "vortex_secure_2026" SET greetings "Hello, VortexKV!"
redis-cli -p 7379 -a "vortex_secure_2026" GET greetings
# Output: "Hello, VortexKV!"
```

### Using Embedded Web Studio Command Deck
Open your web browser at:
👉 **`http://localhost:7380`**

- **Authenticate**: Enter `vortex_secure_2026` or use the persistent session.
- **Keyspace Galaxy**: Explore your keys visualized in 2D/3D orbital graphs.
- **Interactive Console**: Run real-time queries directly in the browser.
- **Access & Users**: Visually configure multi-tenant ACL roles and key patterns.
- **Prometheus Metrics**: Scrape live operational data from `http://localhost:7380/metrics`.

---

## 4. Next-Gen AI Vector Search (Try It Now)

Store vector embeddings and find nearest neighbors natively:

```bash
# Add two 3-dimensional embeddings into index "tech_docs":
redis-cli -p 7379 -a "vortex_secure_2026" VADD tech_docs article:ai 0.95 0.10 0.05
redis-cli -p 7379 -a "vortex_secure_2026" VADD tech_docs article:quantum 0.10 0.90 0.15

# Search Top-1 nearest neighbor to a query vector using Cosine distance:
redis-cli -p 7379 -a "vortex_secure_2026" VSEARCH tech_docs 1 cosine 0.90 0.12 0.08
# 1) 1) "article:ai"
#    2) "0.999781"
```

---

## 5. Benchmarking Performance

VortexKV delivers **6,870,000+ ops/sec** pipelined throughput (world record) and **210,000+ ops/sec** direct concurrency with **~111µs** p50 latency.

Run the official benchmark suite:
```bash
./scripts/run_world_record_benchmark.sh
```

Or benchmark directly with `redis-benchmark`:
```bash
# Direct Concurrency (no pipelining, 50 clients)
redis-benchmark -p 7379 -a "vortex_secure_2026" -c 50 -n 100000 -t get,set -q

# Peak Pipelined Throughput (P=64 Multi-Reactor, 100 clients)
redis-benchmark -p 7379 -a "vortex_secure_2026" -c 100 -n 2000000 -P 64 -t ping,get -q
```
