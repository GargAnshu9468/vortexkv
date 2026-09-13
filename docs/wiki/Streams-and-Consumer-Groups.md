# 🌊 Redis Streams & Consumer Groups

VortexKV provides high-throughput, sub-millisecond event streaming and distributed task distribution modeled after Redis 6+ Streams with full Pending Entries List (PEL) tracking.

---

## ⚡ Key Stream Commands

### 1. `XADD <stream> <ID|*> <field> <value> [field value...]`
Appends an entry to a stream. Passing `*` automatically generates a timestamp-sequence ID:

```bash
redis-cli -p 7379 -a "vortex_secure_2026" XADD orders:stream * user_id 101 amount 49.99 status "pending"
# "1789227091877-0"
```

### 2. `XGROUP CREATE <stream> <group> <ID|$> [MKSTREAM]`
Creates a distributed consumer group for collaborative stream processing:

```bash
redis-cli -p 7379 -a "vortex_secure_2026" XGROUP CREATE orders:stream billing_workers $ MKSTREAM
# OK
```

### 3. `XREADGROUP GROUP <group> <consumer> [COUNT n] [BLOCK ms] STREAMS <stream> >`
Workers in the group fetch new unassigned messages with zero-CPU blocking support:

```bash
redis-cli -p 7379 -a "vortex_secure_2026" XREADGROUP GROUP billing_workers worker_1 COUNT 10 BLOCK 2000 STREAMS orders:stream >
```

### 4. `XACK <stream> <group> <ID> [ID...]`
Acknowledges successful processing and removes message from the Pending Entries List (PEL):

```bash
redis-cli -p 7379 -a "vortex_secure_2026" XACK orders:stream billing_workers 1789227091877-0
# (integer) 1
```

### 5. `XPENDING <stream> <group>`
Inspects unacknowledged messages for worker failure recovery:

```bash
redis-cli -p 7379 -a "vortex_secure_2026" XPENDING orders:stream billing_workers
```
