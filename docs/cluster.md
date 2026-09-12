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
