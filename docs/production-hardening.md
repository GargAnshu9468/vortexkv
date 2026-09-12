# 🛡️ Production Hardening & Deployment Guide

This guide describes operational configurations, OS-level tuning, security postures, and backup recommendations for running VortexKV in high-throughput enterprise production environments.

---

## 1. Operating System & Kernel Tuning (Linux)

To support hundreds of thousands of concurrent operations with microsecond latencies, configure the host kernel parameters in `/etc/sysctl.conf`:

```ini
# Prevent Out-Of-Memory background fork failures
vm.overcommit_memory = 1

# Increase TCP connection backlog queue to absorb traffic spikes
net.core.somaxconn = 65535

# Increase ephemeral port range for massive outbound connection pools
net.ipv4.ip_local_port_range = 1024 65535

# Enable TCP SYN cookies to mitigate SYN flood attacks
net.ipv4.tcp_syncookies = 1

# Reduce FIN timeout to quickly recycle closed connections
net.ipv4.tcp_fin_timeout = 15
```

Apply immediately:
```bash
sudo sysctl -p
```

### File Descriptor Limits (`ulimit`)
In production, a high-volume cache frequently handles 10,000+ client sockets. Set file descriptor limits in `/etc/security/limits.conf`:
```ini
* soft nofile 65536
* hard nofile 65536
```

---

## 2. Memory Sizing & Eviction Policies

Always declare an explicit `-maxmemory` boundary to protect the host operating system from memory exhaustion:

```bash
vortex-server -maxmemory 16gb
```

When memory usage reaches the defined ceiling, VortexKV's **active LRU sampling algorithm** automatically evicts the least recently accessed keys:
- The eviction sample pool continuously selects candidate keys without allocating garbage collection overhead.
- If all keys are strictly required, operations returning errors instead of evicting can be configured by setting `maxmemory-policy noeviction`.

---

## 3. Network Encryption (Wire TLS/SSL)

For deployments across public subnets or zero-trust cloud architectures, launch VortexKV with mutual or server-side TLS:

```bash
./bin/vortex-server \
  -port 7379 \
  -tls-cert /etc/ssl/vortex.crt \
  -tls-key /etc/ssl/vortex.key \
  -requirepass "vortex_secure_2026"
```

Clients connect using TLS:
```bash
redis-cli -p 7379 --tls --cert /etc/ssl/client.crt --key /etc/ssl/client.key -a "vortex_secure_2026" PING
```

---

## 4. Durability & Backup Policies (AOF)

VortexKV uses an **Append-Only File (AOF)** journal for complete persistence:
- **`always`**: Fsyncs after every write command. Maximum durability, suitable for financial ledger records.
- **`everysec` (Recommended)**: Fsyncs once per second in a background thread. Near-zero performance penalty with at most 1 second of potential data loss during catastrophic power failure.
- **`no`**: Leaves flushing to OS buffer cache. Maximum throughput.

### Backing Up the Database
To take a clean snapshot of the data:
1. Copy the active AOF file:
   ```bash
   cp /var/lib/vortexkv/vortex.aof /backup/vortex-$(date +%Y%m%d%H%M).aof
   ```
2. Upload the copy to secure object storage (e.g. Amazon S3, Google Cloud Storage).

---

## 5. Systemd Production Service

Create `/etc/systemd/system/vortexkv.service`:

```ini
[Unit]
Description=VortexKV High-Performance In-Memory Data Engine
After=network.target

[Service]
Type=simple
User=vortexkv
Group=vortexkv
WorkingDirectory=/var/lib/vortexkv
ExecStart=/usr/local/bin/vortex-server -conf /etc/vortexkv/vortex.conf
Restart=always
RestartSec=3s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now vortexkv
```

---

## 6. Observability & Alerting Rules

Scrape metrics continuously via Prometheus:
- **Endpoint**: `http://<vortex-host>:7380/metrics`
- **Recommended Alert Thresholds**:
  - `vortex_memory_allocated_bytes / maxmemory > 0.90` (Trigger memory scale-up alert).
  - `vortex_command_duration_microseconds_p99 > 5000` (Trigger latency degradation alert).
  - `vortex_connected_clients > 8000` (Trigger connection saturation alert).
