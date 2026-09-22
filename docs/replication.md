# 🔁 VortexKV Master-Replica Asynchronous Replication

VortexKV features native, production-grade **Master-Replica Asynchronous Replication** modeled after Redis 6+ replication protocols (`PSYNC`, `REPLCONF`, `REPLICAOF`/`SLAVEOF`).

With zero external dependencies, replication enables:
- **Horizontal Read Scaling**: Distribute heavy read traffic across multiple read-only replicas to achieve millions of reads/sec.
- **High Availability & Instant Failover**: Seamlessly promote any replica to an independent writable master using `REPLICAOF NO ONE`.
- **Read-Only Safety**: Replicas strictly enforce read-only semantics (`-READONLY`) by default to prevent accidental split-brain mutations.
- **Cross-Topology Data Sync**: Automatically replicates Strings, Hashes, Lists, Sets, Sorted Sets, and high-dimensional AI Vectors (`VADD`).

---

## 🏛️ Architecture & Handshake Protocol

```
+-----------------------------------+
|      VORTEX MASTER (Port 7379)    |
|   Role: master, ReplID: 40-hex    |
+-----------------+-----------------+
                  |
         PSYNC Handshake
                  |
         Full Resync Dump
                  |
         Live Command Stream (SET, HSET, VADD, etc.)
                  |
       +----------+----------+
       |                     |
       v                     v
+---------------+     +---------------+
| VORTEX REPLICA|     | VORTEX REPLICA|
|  (Port 7381)  |     |  (Port 7383)  |
|  Role: slave  |     |  Role: slave  |
| (Read-Only)   |     | (Read-Only)   |
+---------------+     +---------------+
```

### Handshake Sequence
When a replica connects to a master (via CLI flag or dynamic `REPLICAOF` command), it executes the standard Redis handshake:

1. **`AUTH <password>`** *(if `masterauth` configured)*: Authenticates with the master node.
2. **`PING`**: Verifies socket liveness and network connectivity.
3. **`REPLCONF listening-port <port>`**: Informs the master of the replica's advertised listening port.
4. **`REPLCONF capa psync2`**: Advertises PSYNC2 capabilities.
5. **`PSYNC ? -1`**: Initiates synchronization. The master responds with `+FULLRESYNC <replid> <offset>`, streams the entire active keyspace across all 256 shards, and adds the replica to its live streaming broadcast ring.
6. **Continuous Streaming**: The master streams all write commands in real-time as RESP arrays with sub-millisecond propagation latency.

---

## 🚀 Quickstart

### 1. Start a Master Node
```bash
# Start Master on wire port 7379, studio port 7380
./bin/vortex-server -port 7379 -web-port 7380
```

### 2. Start a Read-Only Replica Node
```bash
# Start Replica on wire port 7381, studio port 7382, pointing to Master
./bin/vortex-server -port 7381 -web-port 7382 -replicaof 127.0.0.1:7379
```

### 3. Verify Live Replication
In terminal 1 (Master):
```bash
redis-cli -p 7379 SET welcome "Hello from Master"
redis-cli -p 7379 VADD embeddings:1 0.12 0.85 -0.44
```

In terminal 2 (Replica):
```bash
redis-cli -p 7381 GET welcome
# Output: "Hello from Master"

redis-cli -p 7381 VSIM embeddings:1 0.12 0.85 -0.44
# Output: "1.000000"
```

### 4. Verify Read-Only Guard
Attempting to write directly to a replica is rejected:
```bash
redis-cli -p 7381 SET test 123
# Output: (error) READONLY You can't write against a read only replica.
```

---

## ⚡ Dynamic Failover & Promotion

VortexKV supports zero-downtime dynamic reconfiguration without restarting the server process:

### Promote a Replica to Independent Master
If the master fails or maintenance is scheduled, promote a replica instantly:
```bash
redis-cli -p 7381 REPLICAOF NO ONE
# Output: OK
```
The node immediately detaches from the master, transitions to `RoleMaster`, drops the read-only guard, and begins accepting write commands.

### Convert an Existing Node to a Replica
```bash
redis-cli -p 7381 REPLICAOF 192.168.1.100 7379
# Output: OK
```

---

## 📋 CLI Configuration Reference

| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `-replicaof` | string | `""` | Address of master in `host:port` format. Connects as a replica on startup. |
| `-masterauth` | string | `""` | Authentication password for connecting to a password-protected master. |
| `-replica-read-only` | bool | `true` | When true, rejects write commands on replicas with `-READONLY`. |
| `-port` | int | `7379` | TCP port for Redis RESP wire protocol. |
| `-web-port` | int | `7380` | HTTP port for Web Studio dashboard and Prometheus `/metrics`. |

---

## 🔍 Replication Telemetry & Diagnostics

### Standard Redis Commands

#### `INFO replication`
```text
# Replication
role:master
connected_slaves:2
slave0:ip=127.0.0.1,port=7381,state=online,offset=1048
slave1:ip=127.0.0.1,port=7383,state=online,offset=1048
master_replid:a1b2c3d4e5f60718293a4b5c6d7e8f9012345678
master_repl_offset:1048
```

#### `ROLE`
On Master:
```json
["master", 1048, [["127.0.0.1", "7381", "1048"]]]
```
On Replica:
```json
["slave", "127.0.0.1", 7379, "connected", 1048]
```

### Prometheus Metrics (`GET /metrics`)

| Metric | Type | Description |
| :--- | :--- | :--- |
| `vortex_replication_role` | Gauge | `1` if Master, `0` if Replica. |
| `vortex_connected_replicas` | Gauge | Number of active replicas connected to master. |
| `vortex_master_repl_offset` | Counter | Monotonically increasing byte/command stream offset. |

---

## 🐳 Docker Compose Topology Example

Deploy a resilient 1-Master, 2-Replica cluster with dedicated healthchecks:

```yaml
version: '3.8'

services:
  vortex-master:
    image: vortexkv:latest
    container_name: vortex-master
    command: ["-port", "7379", "-web-port", "7380"]
    ports:
      - "7379:7379"
      - "7380:7380"
    healthcheck:
      test: ["CMD", "nc", "-z", "127.0.0.1", "7379"]
      interval: 5s
      timeout: 2s
      retries: 3

  vortex-replica-1:
    image: vortexkv:latest
    container_name: vortex-replica-1
    command: ["-port", "7381", "-web-port", "7382", "-replicaof", "vortex-master:7379"]
    ports:
      - "7381:7381"
      - "7382:7382"
    depends_on:
      vortex-master:
        condition: service_healthy

  vortex-replica-2:
    image: vortexkv:latest
    container_name: vortex-replica-2
    command: ["-port", "7383", "-web-port", "7384", "-replicaof", "vortex-master:7379"]
    ports:
      - "7383:7383"
      - "7384:7384"
    depends_on:
      vortex-master:
        condition: service_healthy
```
