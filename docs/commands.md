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

## 🧠 AI Vector Search Primitives (Native)

VortexKV includes first-class vector search operations without requiring external vector database sidecars:

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
Find the Top-K nearest neighbors using Cosine or Euclidean distance:
```bash
VSEARCH <index> <topK> <metric: cosine|euclidean> <q1> <q2> ... <qN>
```
*Example:*
```bash
VSEARCH embeddings 5 cosine 0.92 0.10 -0.30 0.85
# Output:
# 1) 1) "item_42"
#    2) "0.998412"
```

### `VSIM`
Compute similarity directly between two stored vector IDs:
```bash
VSIM <index> <id1> <id2> <metric: cosine|euclidean>
```
