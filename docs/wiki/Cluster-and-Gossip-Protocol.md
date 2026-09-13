# 📡 Cluster & Gossip Protocol

VortexKV supports linear horizontal sharding across distributed nodes with 16,384 hash slots and an autonomous binary heartbeat bus on port 17379.

---

## ⚡ Architecture & Routing

- **Slot Allocation**: 16,384 CRC16 hash slots (`slot = crc16(key) % 16384`).
- **Hash Tags**: Substrings enclosed in `{...}` force multi-key operations into the same slot (e.g. `{user:101}:profile` and `{user:101}:orders`).
- **Client Redirections**: Standard `-MOVED <slot> <ip:port>` and `-ASK <slot> <ip:port>`.

---

## 💓 Dedicated Binary Gossip Bus (Port 17379)

Cluster nodes exchange continuous binary heartbeats on `port + 10000` (e.g. client port `7379` ➔ gossip port `17379`).

### Failure Detection & Consensus:
1. **PFAIL (Possible Failure)**: If a node fails to respond to PINGs for `node_timeout` (default 2000ms), it is marked `PFAIL`.
2. **FAIL (Confirmed Failure)**: When a majority of masters agree on `PFAIL`, the node status transitions to `FAIL` and a broadcast alert is sent across the bus.
3. **Autonomous Replica Promotion**: If a master with slots goes `FAIL`, its connected replica automatically triggers an election, assumes master status, and takes over the slot ownership with zero human intervention.
