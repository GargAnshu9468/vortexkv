# 💾 Persistence & Durability

VortexKV offers dual persistence combining point-in-time binary RDB snapshots with Append-Only File (AOF) durability.

---

## 1. Append-Only File (AOF)

Logs every mutating command (`SET`, `HSET`, `XADD`, etc.) to disk:
- `fsync always`: Maximum durability; syncs after every write.
- `fsync everysec`: Recommended default; syncs once per second with < 1% throughput impact.
- `fsync no`: Delegates sync timing to the operating system kernel.

Launch with AOF:
```bash
./bin/vortex-server -aof vortex.aof -aof-fsync everysec
```

---

## 2. Binary RDB Snapshots

Point-in-time compact snapshots in standard `REDIS0009` format with 64-bit CRC64 checksum verification:
- `SAVE`: Synchronous blocking snapshot.
- `BGSAVE`: Asynchronous non-blocking background snapshot.
- `LASTSAVE`: Returns UNIX timestamp of the last successful disk snapshot.

Launch with RDB:
```bash
./bin/vortex-server -rdb dump.rdb
```
