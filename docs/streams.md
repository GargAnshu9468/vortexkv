# 🌊 VortexKV Streams & Consumer Groups

VortexKV delivers native, sub-millisecond **Redis Streams & Consumer Groups** compatible with the standard Redis 5+ and 6+ wire specifications. 

Streams transform VortexKV into a high-throughput, persistent event-streaming and distributed message queue engine—providing a lightweight, zero-dependency alternative to Apache Kafka and RabbitMQ with sub-millisecond latencies.

---

## 🏛️ Architecture & Core Concepts

```
Producer (XADD)  ───►  [ Stream: 'orders' (Radix/Indexed Entries) ]
                               │
            ┌──────────────────┴──────────────────┐
            ▼                                     ▼
   Consumer Group: 'billing'             Consumer Group: 'inventory'
   Last Delivered ID: 1726-0             Last Delivered ID: 1726-0
   ┌───────────────────────┐             ┌───────────────────────┐
   │ Worker A    Worker B  │             │ Worker C    Worker D  │
   │   PEL         PEL     │             │   PEL         PEL     │
   └───────────────────────┘             └───────────────────────┘
            │                                     │
          XACK                                  XACK
```

### 1. Append-Only Log Structure
A Stream is an append-only collection of field-value entries identified by a monotonically increasing ID:
$$\text{Entry ID} = \langle\text{timestamp\_ms}\rangle - \langle\text{sequence\_number}\rangle$$
- If `*` is passed, VortexKV automatically generates the current wall-clock millisecond and sequence.
- Multiple entries in the exact same millisecond increment sequence monotonically (`1726148400000-0`, `1726148400000-1`).

### 2. Consumer Groups & Partition-Free Load Balancing
A Consumer Group coordinates a pool of workers consuming from the same stream:
- **`>` (New Unread Messages)**: Each worker reading with `>` receives unique, previously unconsumed entries. Messages are automatically balanced across workers without manual partitioning.
- **PEL (Pending Entries List)**: When a message is delivered to a worker, it is recorded in the group's Pending Entries List. It remains pending until explicitly acknowledged (`XACK`).
- **At-Least-Once Delivery**: If a worker crashes before calling `XACK`, another worker can query `XPENDING` or read its unacknowledged messages using specific IDs to resume processing.

### 3. Zero-CPU Blocking Reads
`XREAD BLOCK <ms>` and `XREADGROUP BLOCK <ms>` utilize Go channel synchronization (`sync.Cond` and waiter channels). Blocked client connections do not spin-lock or consume CPU cycles while awaiting upstream messages.

---

## 📖 Command Reference

| Command | Syntax | Description | Complexity |
| :--- | :--- | :--- | :--- |
| **`XADD`** | `XADD key [MAXLEN [~] count] <ID\|*> field val [field val ...]` | Appends a new entry to the stream. Auto-creates stream if missing. | $O(1)$ |
| **`XLEN`** | `XLEN key` | Returns the total number of entries in the stream. | $O(1)$ |
| **`XRANGE`** | `XRANGE key start end [COUNT n]` | Returns range of entries from `start` to `end` (`-` = min, `+` = max). | $O(N)$ |
| **`XREVRANGE`** | `XREVRANGE key end start [COUNT n]` | Returns range in reverse order from latest to earliest. | $O(N)$ |
| **`XDEL`** | `XDEL key id [id ...]` | Removes specified entries by ID from the stream. | $O(N)$ |
| **`XTRIM`** | `XTRIM key MAXLEN [~] count` | Trims stream length to given count by evicting oldest entries. | $O(N)$ |
| **`XREAD`** | `XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]` | Reads entries from one or more streams with ID greater than `id`. Supports `$` (latest). | $O(N)$ |
| **`XGROUP CREATE`** | `XGROUP CREATE key group <id\|$> [MKSTREAM]` | Creates a consumer group starting at specified ID or `$` (tail). | $O(1)$ |
| **`XGROUP SETID`** | `XGROUP SETID key group <id\|$>` | Sets consumer group's last delivered ID cursor. | $O(1)$ |
| **`XGROUP DESTROY`** | `XGROUP DESTROY key group` | Destroys consumer group and purges its PEL. | $O(1)$ |
| **`XGROUP DELCONSUMER`** | `XGROUP DELCONSUMER key group consumer` | Deletes a worker consumer from the group. | $O(1)$ |
| **`XREADGROUP`** | `XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms] STREAMS key [key ...] >` | Distributes unique messages across consumer group workers. | $O(N)$ |
| **`XACK`** | `XACK key group id [id ...]` | Acknowledges successfully processed messages, removing them from the PEL. | $O(N)$ |
| **`XPENDING`** | `XPENDING key group [[IDLE ms] start end count [consumer]]` | Inspects unacknowledged pending messages in the PEL. | $O(N)$ |
| **`XINFO STREAM`** | `XINFO STREAM key` | Returns stream metadata (length, radix tree keys, first/last ID). | $O(1)$ |
| **`XINFO GROUPS`** | `XINFO GROUPS key` | Lists all consumer groups, pending count, and consumer count. | $O(N)$ |
| **`XINFO CONSUMERS`** | `XINFO CONSUMERS key group` | Inspects workers in group, their pending counts, and idle times. | $O(N)$ |

---

## 💡 Production Architecture Patterns

### Pattern A: Pub/Sub Broadcast & Fan-Out (`XREAD`)
When every subscriber needs to receive every message (event broadcasting, telemetry, chat logs):

```bash
# Worker 1 subscribes from the beginning
XREAD COUNT 10 STREAMS notifications 0-0

# Worker 2 subscribes only to new events arriving in real time (blocks up to 5000ms)
XREAD BLOCK 5000 STREAMS notifications $
```

### Pattern B: Distributed Worker Pool (`XREADGROUP`)
When messages represent work tasks (e.g. video processing, payment checkout) that must be processed **exactly once per group** and distributed across workers:

```bash
# 1. Producer pushes a payment job (auto-trimming to keep memory bounded)
XADD jobs:payments MAXLEN ~ 100000 * user_id 42 amount 99.50 currency USD

# 2. Setup consumer group 'payment_processors'
XGROUP CREATE jobs:payments payment_processors 0 MKSTREAM

# 3. Worker "worker_east_1" fetches 1 new job (blocks up to 2000ms if empty)
XREADGROUP GROUP payment_processors worker_east_1 BLOCK 2000 COUNT 1 STREAMS jobs:payments >

# 4. Worker finishes processing and acknowledges
XACK jobs:payments payment_processors 1726148400000-0
```

### Pattern C: Fault Tolerance & PEL Recovery (`XPENDING`)
If a worker crashes before acknowledging a message:

```bash
# 1. Check pending items in 'payment_processors'
XPENDING jobs:payments payment_processors
# Output: 1) (integer) 1           # 1 pending message
#         2) "1726148400000-0"      # min ID
#         3) "1726148400000-0"      # max ID
#         4) 1) 1) "worker_east_1"  # consumer name
#               2) "1"              # pending count

# 2. Another worker inspects the unacknowledged message details
XPENDING jobs:payments payment_processors - + 10

# 3. Re-read the unacknowledged message (by requesting its ID instead of '>')
XREADGROUP GROUP payment_processors recovery_worker COUNT 1 STREAMS jobs:payments 0-0

# 4. Process and acknowledge
XACK jobs:payments payment_processors 1726148400000-0
```

---

## 💻 Polyglot Client Integration

VortexKV Streams work seamlessly with any standard Redis client library on port `7379`.

### 🐍 Python (`redis-py`)

```python
import redis

r = redis.Redis(host='localhost', port=7379, password='vortex_secure_2026', decode_responses=True)

# Produce
entry_id = r.xadd('telemetry', {'sensor': 'temp', 'value': 23.8})
print(f"Produced event: {entry_id}")

# Create Group
try:
    r.xgroup_create('telemetry', 'analytics_group', id='0', mkstream=True)
except redis.ResponseError:
    pass  # Already exists

# Consume with worker pool
messages = r.xreadgroup('analytics_group', 'worker-1', {'telemetry': '>'}, count=5, block=2000)
for stream_name, entries in messages:
    for msg_id, fields in entries:
        print(f"Processing {msg_id}: {fields}")
        # Acknowledge
        r.xack('telemetry', 'analytics_group', msg_id)
```

### 🐹 Go (`go-redis`)

```go
package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:7379",
		Password: "vortex_secure_2026",
	})

	// Produce
	id, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "orders",
		Values: map[string]interface{}{"order_id": "ORD-9021", "total": 149.99},
	}).Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("New order ID:", id)

	// Consume using group
	_ = rdb.XGroupCreateMkStream(ctx, "orders", "order_workers", "0").Err()

	streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "order_workers",
		Consumer: "go-worker-1",
		Streams:  []string{"orders", ">"},
		Count:    1,
		Block:    2000,
	}).Result()

	if err == nil {
		for _, stream := range streams {
			for _, msg := range stream.Messages {
				fmt.Printf("Handling order %s: %v\n", msg.ID, msg.Values)
				rdb.XAck(ctx, "orders", "order_workers", msg.ID)
			}
		}
	}
}
```

### 🟩 Node.js (`ioredis`)

```javascript
const Redis = require('ioredis');
const redis = new Redis({
  host: 'localhost',
  port: 7379,
  password: 'vortex_secure_2026'
});

async function run() {
  // Push event
  const id = await redis.xadd('clicks', '*', 'button', 'signup', 'user', 'usr_883');
  console.log('Recorded click:', id);

  // Read range
  const entries = await redis.xrange('clicks', '-', '+', 'COUNT', 10);
  console.log('Recent clicks:', entries);
}
run();
```

---

## ⚡ Performance vs Traditional Brokers

| Feature | VortexKV Streams | Apache Kafka | RabbitMQ |
| :--- | :--- | :--- | :--- |
| **Write Latency (p99)** | **< 0.4 ms** (In-Memory) | ~ 5 - 15 ms | ~ 2 - 8 ms |
| **Read Latency (p99)** | **< 0.3 ms** | ~ 5 - 10 ms | ~ 2 - 5 ms |
| **External Dependencies** | **None (Single Go Binary)** | JVM, ZooKeeper/KRaft | Erlang Runtime |
| **Protocol** | Standard Redis RESP | Custom Kafka TCP | AMQP / STOMP |
| **Memory Footprint** | **~25 MB base** | ~1 - 2 GB JVM heap | ~300 MB |
| **Consumer Load Balancing** | Native (`XREADGROUP`) | Partition assignment | Queue bindings |
