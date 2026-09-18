# 🌐 Distributed Multi-Node Cluster Guide

VortexKV provides a high-performance, Redis-compatible **Distributed Multi-Node Cluster** supporting linear horizontal write and read scaling, automatic client redirection (`-MOVED`), and multi-key hash tag co-location (`{...}`).

---

## 🏛️ Cluster Architecture & 16,384 Hash Slots

In a VortexKV cluster, the global keyspace is deterministically partitioned across **16,384 logical hash slots** (indexed `0` to `16,383`):

```
                                 Client Query
                                      │
                         ┌────────────┴────────────┐
                         ▼                         ▼
                  Single Key                     Hash Tag
               GET user:100:name          GET {tenant:42}:orders
                         │                         │
                         ▼                         ▼
                  Target: "user:100:name"   Target: "tenant:42"
                         │                         │
                         └────────────┬────────────┘
                                      ▼
                             CRC16(target) & 0x3FFF
                                      │
                                      ▼
                                Hash Slot [0..16383]
                                      │
                         ┌────────────┴────────────┐
                         ▼                         ▼
                 Owned by Local Node?        Owned by Remote Peer?
                         │                         │
                        YES                        NO
                         │                         │
                         ▼                         ▼
                   Execute Query             Return Error:
                   and Return Result         -MOVED <slot> <peer_ip>:<peer_port>
```

### Hash Tag Algorithm (`{...}`)
If a key contains `{` followed by `}` with at least one character in between, **only** the substring inside the first balanced `{` and `}` is hashed:
- `user1000` -> hashes `"user1000"`
- `{user1000}:profile` -> hashes `"user1000"`
- `{user1000}:orders` -> hashes `"user1000"`
- `{user1000}:settings` -> hashes `"user1000"`

> [!TIP]
> **Co-Location with Hash Tags**: Using `{user1000}` ensures that profile, orders, and settings for that user are guaranteed to reside on the exact same physical node. This allows atomic multi-key transactions (`MGET`, `MSET`, `MULTI/EXEC`) without triggering `-CROSSSLOT` errors.

---

## ⚡ Cluster Wire Commands Reference

| Command | Arguments | Description |
| :--- | :--- | :--- |
| `CLUSTER KEYSLOT` | `<key>` | Returns the hash slot integer `[0..16383]` for the key |
| `CLUSTER NODES` | *none* | Serializes the complete cluster topology in standard Redis multi-line format |
| `CLUSTER SLOTS` | *none* | Returns nested RESP array of slot ranges and master/replica node endpoints |
| `CLUSTER INFO` | *none* | Returns state (`cluster_state:ok`), assigned slot count, and epoch info |
| `CLUSTER MEET` | `<ip> <port> [cport]` | Connects to a peer node and introduces it into the cluster topology |
| `CLUSTER ADDSLOTS` | `<slot> [slot ...]` | Assigns one or more hash slots to the local node |
| `CLUSTER DELSLOTS` | `<slot> [slot ...]` | Unassigns one or more hash slots from the local node |
| `CLUSTER REPLICATE` | `<node-id>` | Configures local node as a replica of another cluster master |
| `CLUSTER COUNTKEYSINSLOT` | `<slot>` | Returns the number of active keys stored in the specified hash slot |
| `CLUSTER GETKEYSINSLOT` | `<slot> <count>` | Returns up to `<count>` keys residing in the specified hash slot |
| `CLUSTER MYID` | *none* | Returns the local node's 40-character hexadecimal node identifier |
| `CLUSTER SAVECONFIG` | *none* | Forces saving the cluster state and topology to `nodes.conf` |
| `CLUSTER RESET` | `[HARD\|SOFT]` | Resets cluster state, clearing slots and peer node mappings |

---

## 🚀 Setting Up a 3-Node Production Cluster

### Step 1: Start 3 VortexKV Nodes in Cluster Mode

```bash
# Node 1 (Port 7379, Web Studio 7380)
./bin/vortex-server -port 7379 -web-port 7380 \
  -cluster-enabled -cluster-config-file nodes_7379.conf

# Node 2 (Port 7381, Web Studio 7382)
./bin/vortex-server -port 7381 -web-port 7382 \
  -cluster-enabled -cluster-config-file nodes_7381.conf

# Node 3 (Port 7383, Web Studio 7384)
./bin/vortex-server -port 7383 -web-port 7384 \
  -cluster-enabled -cluster-config-file nodes_7383.conf
```

### Step 2: Meet the Nodes into a Single Topology

Connect to Node 1 and introduce Node 2 and Node 3:
```bash
redis-cli -p 7379 CLUSTER MEET 127.0.0.1 7381
redis-cli -p 7379 CLUSTER MEET 127.0.0.1 7383
```

### Step 3: Distribute the 16,384 Hash Slots

Divide the 16,384 slots evenly across the 3 master nodes:
- **Node 1**: Slots `0` to `5460` (5,461 slots)
- **Node 2**: Slots `5461` to `10922` (5,462 slots)
- **Node 3**: Slots `10923` to `16383` (5,461 slots)

```bash
# Assign slots 0..5460 to Node 1
redis-cli -p 7379 CLUSTER ADDSLOTS $(seq 0 5460)

# Assign slots 5461..10922 to Node 2
redis-cli -p 7381 CLUSTER ADDSLOTS $(seq 5461 10922)

# Assign slots 10923..16383 to Node 3
redis-cli -p 7383 CLUSTER ADDSLOTS $(seq 10923 16383)
```

### Step 4: Verify Cluster State

```bash
redis-cli -p 7379 CLUSTER INFO
# cluster_state:ok
# cluster_slots_assigned:16384
# cluster_slots_ok:16384
# cluster_known_nodes:3
# cluster_size:3

redis-cli -p 7379 CLUSTER SLOTS
# 1) 1) (integer) 0
#    2) (integer) 5460
#    3) 1) "127.0.0.1"
#       2) (integer) 7379
#       3) "9f38a1b2..."
# 2) 1) (integer) 5461
#    2) (integer) 10922
#    3) 1) "127.0.0.1"
#       2) (integer) 7381
#       3) "8a42c9d1..."
# 3) 1) (integer) 10923
#    2) (integer) 16383
#    3) 1) "127.0.0.1"
#       2) (integer) 7383
#       3) "7b19e4f5..."
```

---

## 💻 Client Usage Examples

### 1. Redis CLI with Cluster Mode (`-c`)
When launching `redis-cli`, pass the `-c` flag. The client will automatically follow `-MOVED` redirection responses:

```bash
$ redis-cli -c -p 7379

127.0.0.1:7379> SET foo bar
-> Redirected to slot [12182] located at 127.0.0.1:7383
OK

127.0.0.1:7383> GET foo
"bar"

127.0.0.1:7383> SET alpha numeric
-> Redirected to slot [3134] located at 127.0.0.1:7379
OK
```

### 2. Go Client (`go-redis/v9`)

```go
package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{
			"127.0.0.1:7379",
			"127.0.0.1:7381",
			"127.0.0.1:7383",
		},
	})
	defer rdb.Close()

	// Writes and reads are routed automatically to the right slot
	err := rdb.Set(ctx, "user:42:profile", "Anshu Garg", 0).Err()
	if err != nil {
		panic(err)
	}

	val, _ := rdb.Get(ctx, "user:42:profile").Result()
	fmt.Println("Retrieved:", val)
}
```

### 3. Python (`redis-py`)

```python
from redis.cluster import RedisCluster

rc = RedisCluster(startup_nodes=[
    {"host": "127.0.0.1", "port": 7379},
    {"host": "127.0.0.1", "port": 7381},
    {"host": "127.0.0.1", "port": 7383},
])

rc.set("{tenant:10}:metric", "42.5")
print(rc.get("{tenant:10}:metric"))
```

### 4. Node.js (`ioredis`)

```javascript
const Redis = require('ioredis');

const cluster = new Redis.Cluster([
  { host: '127.0.0.1', port: 7379 },
  { host: '127.0.0.1', port: 7381 },
  { host: '127.0.0.1', port: 7383 },
]);

async function main() {
  await cluster.set('{session:abc}:token', 'xyz987');
  const token = await cluster.get('{session:abc}:token');
  console.log('Session token:', token);
}
main();
```

---

## 📡 Background Gossip Bus & Automatic Failover

VortexKV nodes continuously communicate out-of-band over a binary TCP bus operating on `port + 10000` (e.g. `17379` for wire port `7379`).

### Heartbeat Protocol & Framing
- **Magic Framing**: Custom high-throughput `VBUS` binary header.
- **Payload**: Exchanges current config epoch, 2048-byte hash slot bitmap (16,384 slots), and peer health reports.
- **Heartbeat Rate**: Sends ping packets every 250ms with 400ms connection timeout.

### Failure Detection (`PFAIL` & `FAIL`)
- **PFAIL (Possible Failure)**: If a node does not respond to heartbeats within `nodeTimeout` (default 2000ms), it is marked `PFAIL` (`fail?` in `CLUSTER NODES`).
- **FAIL (Confirmed Failure)**: When a majority of active cluster masters observe and acknowledge `PFAIL` for a node, the detecting master promotes the status to `FAIL` and broadcasts a `TypeFail` packet. All cluster nodes immediately update their topology table to `disconnected` and `fail`.

### Autonomous Replica Election & Failover
- When a replica detects its designated master is in `FAIL` state:
  1. The replica waits a short randomized delay (500ms) to avoid split-vote ties.
  2. The replica increments `CurrentEpoch` and broadcasts `TypeFailoverAuthReq` to all active masters on the gossip bus.
  3. Masters verify the candidate's epoch, verify the target master is indeed `FAIL`, and return `TypeFailoverAuthAck`.
  4. Upon securing majority master votes, the candidate replica wins the election.
  5. The promoted replica automatically:
     - Switches role to `master` (`REPLICAOF NO ONE`).
     - Takes over all 16,384 hash slots owned by the failed master.
     - Broadcasts an updated `PONG` announcement with new slot ownership to the entire cluster.
     - Commits new topology to `nodes.conf`.

---

## ⚖️ Automated Cluster Slot Rebalancing

When masters are added or removed, VortexKV provides an integrated slot rebalancer built into `vortex-cli`.

### Interactive or Automated Slot Redistribution
```bash
# Preview the migration plan without applying changes
vortex-cli -p 7379 -a "vortex_secure_2026" -dry-run cluster rebalance

# Execute automated rebalance across all master nodes
vortex-cli -p 7379 -a "vortex_secure_2026" -auto cluster rebalance
```

The rebalancer automatically:
1. Discovers all masters and computes the ideal slot target: $16384 / N$.
2. Generates the minimal donor-to-receiver slot migration plan.
3. Sets `IMPORTING` on receiver and `MIGRATING` on donor.
4. Moves stored keys across slots and finalizes ownership via `CLUSTER SETSLOT <slot> NODE <receiver-id>`.

---

## ☸️ VortexKV Kubernetes Operator (`kind: VortexCluster`)

VortexKV provides an official Kubernetes Operator that manages automated multi-master cluster topologies declaratively.

### 1. Install Operator CRD & RBAC
```bash
kubectl apply -f deployments/operator/crd.yaml
kubectl apply -f deployments/operator/rbac.yaml
kubectl apply -f deployments/operator/operator.yaml
```

### 2. Deploy a Production Cluster
```yaml
apiVersion: vortexkv.io/v1alpha1
kind: VortexCluster
metadata:
  name: prod-cluster
  namespace: default
spec:
  masters: 3
  replicasPerMaster: 1
  image: "garganshu9468/vortexkv:latest"
  requirepass: "vortex_k8s_secret_2026"
  maxmemory: "2gb"
  storage:
    size: "20Gi"
```
```bash
kubectl apply -f deployments/operator/example-cluster.yaml
```

The operator automatically reconciles StatefulSets, executes inter-pod `CLUSTER MEET`, partitions all 16,384 slots across masters, and handles dynamic scale-out and rebalancing!


