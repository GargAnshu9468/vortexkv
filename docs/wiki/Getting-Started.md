# 🚀 Getting Started with VortexKV

VortexKV can be installed and running in under 30 seconds via multiple deployment methods.

---

## ⚡ 1. One-Line Universal Installer (macOS & Linux)

```bash
curl -fsSL https://raw.githubusercontent.com/GargAnshu9468/vortexkv/main/install.sh | bash
```

---

## 🐳 2. Docker & Docker Compose

Launch single container:
```bash
docker run -d -p 7379:7379 -p 7380:7380 \
  -e VORTEX_REQUIREPASS="vortex_secure_2026" \
  vortexkv/vortex:latest
```

Launch complete cluster with Prometheus & Grafana stack:
```bash
git clone https://github.com/GargAnshu9468/vortexkv.git
cd vortexkv
docker compose --profile monitoring up -d
```

---

## 🔨 3. Build from Source

Requirements: Go 1.22+

```bash
git clone https://github.com/GargAnshu9468/vortexkv.git
cd vortexkv
make build
```

Binaries generated in `bin/`:
- `bin/vortex-server`: Server daemon & embedded visual deck
- `bin/vortex-cli`: Interactive CLI client
- `bin/vortex-operator`: Kubernetes operator controller

---

## 🔌 Connecting via Redis CLI

```bash
redis-cli -p 7379 -a "vortex_secure_2026" PING
# PONG

redis-cli -p 7379 -a "vortex_secure_2026" SET mykey "CyberVortex"
redis-cli -p 7379 -a "vortex_secure_2026" GET mykey
# "CyberVortex"
```

## 🌌 Connecting via Web Studio

Open your browser at:
👉 **`http://localhost:7380`**
