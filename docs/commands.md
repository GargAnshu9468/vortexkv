# 📖 Command Reference

VortexKV supports the standard Redis RESP command set as well as next-generation vector search primitives.

---

## 🔑 Key & String Operations

| Command | Syntax | Description | Complexity |
| :--- | :--- | :--- | :--- |
| **`GET`** | `GET key` | Retrieve string value of `key`. | $O(1)$ |
| **`SET`** | `SET key value [EX seconds] [PX millis] [NX\|XX]` | Store string value with optional expiration and existence guards. | $O(1)$ |
| **`MGET`** | `MGET key [key ...]` | Retrieve values of multiple keys in a single atomic roundtrip. | $O(N)$ |
| **`MSET`** | `MSET key value [key value ...]` | Set multiple key-value pairs atomically. | $O(N)$ |
| **`DEL`** | `DEL key [key ...]` | Delete specified keys. | $O(N)$ |
| **`EXISTS`** | `EXISTS key [key ...]` | Return count of existing keys. | $O(N)$ |
| **`EXPIRE`** | `EXPIRE key seconds` | Set a timeout on `key` in seconds. | $O(1)$ |
| **`TTL`** | `TTL key` | Return remaining time to live in seconds (-1 = persistent, -2 = missing). | $O(1)$ |
| **`INCR`** | `INCR key` | Increment the integer value of `key` by 1. | $O(1)$ |
| **`INCRBY`** | `INCRBY key increment` | Increment integer value of `key` by given offset. | $O(1)$ |
| **`DECR`** | `DECR key` | Decrement the integer value of `key` by 1. | $O(1)$ |

---

## 🗂️ Hashes

| Command | Syntax | Description | Complexity |
| :--- | :--- | :--- | :--- |
| **`HSET`** | `HSET key field value [field value ...]` | Sets specified fields to their respective values in the hash. | $O(N)$ |
| **`HGET`** | `HGET key field` | Returns the value associated with field in the hash. | $O(1)$ |
| **`HMGET`** | `HMGET key field [field ...]` | Returns the values associated with the specified fields. | $O(N)$ |
| **`HGETALL`** | `HGETALL key` | Returns all fields and values of the hash. | $O(N)$ |
| **`HDEL`** | `HDEL key field [field ...]` | Removes the specified fields from the hash. | $O(N)$ |
| **`HLEN`** | `HLEN key` | Returns the number of fields contained in the hash. | $O(1)$ |

---

## 📋 Lists

| Command | Syntax | Description | Complexity |
| :--- | :--- | :--- | :--- |
| **`LPUSH`** | `LPUSH key element [element ...]` | Insert elements at the head of the list. | $O(1)$ |
| **`RPUSH`** | `RPUSH key element [element ...]` | Insert elements at the tail of the list. | $O(1)$ |
| **`LPOP`** | `LPOP key` | Remove and return the first element of the list. | $O(1)$ |
| **`RPOP`** | `RPOP key` | Remove and return the last element of the list. | $O(1)$ |
| **`LLEN`** | `LLEN key` | Returns the length of the list. | $O(1)$ |
| **`LRANGE`** | `LRANGE key start stop` | Returns the specified elements of the list within range. | $O(S+N)$ |

---

## 🏷️ Sets & Sorted Sets (ZSets)

| Command | Syntax | Description |
| :--- | :--- | :--- |
| **`SADD`** | `SADD key member [member ...]` | Add specified members to the set. |
| **`SMEMBERS`** | `SMEMBERS key` | Return all members of the set. |
| **`SREM`** | `SREM key member [member ...]` | Remove specified members from the set. |
| **`SCARD`** | `SCARD key` | Return set cardinality. |
| **`ZADD`** | `ZADD key score member [score member ...]` | Add members with scores into the sorted set. |
| **`ZRANGE`** | `ZRANGE key min max [WITHSCORES]` | Return a range of members sorted by index or score. |
| **`ZCARD`** | `ZCARD key` | Return the sorted set member count. |

---

## 🌊 Streams & Consumer Groups

Full Redis wire-compatible append-only streaming and distributed task queue operations:

| Command | Syntax | Description |
| :--- | :--- | :--- |
| **`XADD`** | `XADD key [MAXLEN [~] count] <ID\|*> field val [field val ...]` | Append entry to stream with auto or explicit ID. |
| **`XLEN`** | `XLEN key` | Return total number of items in stream. |
| **`XRANGE`** | `XRANGE key start end [COUNT n]` | Query range of entries between IDs (`-` = min, `+` = max). |
| **`XREVRANGE`** | `XREVRANGE key end start [COUNT n]` | Query range in reverse chronological order. |
| **`XDEL`** | `XDEL key id [id ...]` | Delete entries from stream by ID. |
| **`XTRIM`** | `XTRIM key MAXLEN [~] count` | Trim stream length to specified count. |
| **`XREAD`** | `XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]` | Read entries after given IDs with optional blocking. |
| **`XGROUP`** | `XGROUP CREATE\|SETID\|DESTROY\|DELCONSUMER ...` | Manage stream consumer groups and worker registrations. |
| **`XREADGROUP`** | `XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms] STREAMS key [key ...] >` | Distribute unique unread messages across worker pool. |
| **`XACK`** | `XACK key group id [id ...]` | Acknowledge processed messages and purge from PEL. |
| **`XPENDING`** | `XPENDING key group [[IDLE ms] start end count [consumer]]` | Inspect unacknowledged pending messages list (PEL). |
| **`XINFO`** | `XINFO STREAM\|GROUPS\|CONSUMERS key [group]` | Stream and consumer group telemetry and diagnostics. |

---

## 🔄 Atomic Transactions

VortexKV supports ACID atomic multi-command batches:

```text
MULTI
SET account:alice 950
SET account:bob 1050
EXEC
```
- **`MULTI`**: Marks the start of a transaction block. Subsequent commands are queued.
- **`EXEC`**: Executes all queued commands atomically.
- **`DISCARD`**: Flushes all queued commands in the transaction.

---

## 💾 Binary RDB Snapshots & Persistence

Standard Redis point-in-time binary snapshot persistence (`REDIS0009`) with CRC64-Jones integrity verification:

| Command | Syntax | Description |
| :--- | :--- | :--- |
| **`SAVE`** | `SAVE` | Synchronously write point-in-time snapshot to disk (`dump.rdb`). Blocks until complete. |
| **`BGSAVE`** | `BGSAVE` | Asynchronously save snapshot in background goroutine without blocking clients. |
| **`LASTSAVE`** | `LASTSAVE` | Return UNIX epoch timestamp of last successful disk save. |

---

## 👥 Multi-User Access Control Lists (ACL)

Standard Redis 6+ wire commands:

| Command | Syntax | Description |
| :--- | :--- | :--- |
| **`AUTH`** | `AUTH [username] <password>` | Authenticate client connection against master or specific ACL user. |
| **`ACL WHOAMI`** | `ACL WHOAMI` | Return the username of the current connection. |
| **`ACL USERS`** | `ACL USERS` | List all configured username accounts. |
| **`ACL LIST`** | `ACL LIST` | List all users in standard rule specification format. |
| **`ACL GETUSER`** | `ACL GETUSER <username>` | Retrieve flags, role, and allowed key patterns for a user. |
| **`ACL SETUSER`** | `ACL SETUSER <username> [on\|off] [>password] [+@all\|+@read] [~pattern]` | Create or update user permissions and namespace isolation rules. |
| **`ACL DELUSER`** | `ACL DELUSER <username>` | Delete a user account (`default` user is protected). |

---

## 🧠 AI Vector Search Primitives (Native HNSW Graph)

VortexKV includes native, first-class **Hierarchical Navigable Small World (HNSW)** multi-layer graph vector indexing without requiring external vector database sidecars like Pinecone, Milvus, or Qdrant:

### `VADD`
Store a named high-dimensional vector in a vector index:
```bash
VADD <index> <id> <f1> <f2> ... <fN>
```
*Example:*
```bash
VADD embeddings item_42 0.95 0.12 -0.34 0.81
```

### `VSEARCH`
Find the Top-K nearest neighbors using sub-millisecond HNSW graph search ($O(\log N)$) with optional beam size override (`EF`):
```bash
VSEARCH <index> <topK> <metric: cosine|euclidean|dot> <q1> <q2> ... <qN> [EF count]
```
*Example:*
```bash
VSEARCH embeddings 5 cosine 0.92 0.10 -0.30 0.85 EF 64
# Output:
# 1) 1) "item_42"
#    2) "0.998412"
```

### `VSIM`
Compute similarity directly between two stored vector IDs:
```bash
VSIM <index> <id1> <id2> <metric: cosine|euclidean|dot>
```

### `VINFO`
Inspect HNSW graph structure and index hyperparameters:
```bash
VINFO <index>
# Output:
# 1) "dimension"
# 2) "128"
# 3) "count"
# 4) "10000"
# 5) "hnsw_metric"
# 6) "cosine"
# 7) "hnsw_max_level"
# 8) "4"
# 9) "hnsw_m"
# 10) "16"
# 11) "hnsw_m0"
# 12) "32"
# 13) "hnsw_ef_construction"
# 14) "64"
# 15) "hnsw_ef_search"
# 16) "64"
# 17) "hnsw_entry_point"
# 18) "item_42"
```

### `VDEL`
Delete one or more vectors by ID from the index and rewire HNSW graph neighbors:
```bash
VDEL <index> <id> [<id> ...]
```
