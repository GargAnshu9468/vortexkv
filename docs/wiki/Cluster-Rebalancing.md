# ⚖️ Cluster Slot Rebalancing

VortexKV provides native CLI tools for live slot rebalancing without downtime.

---

## ⚡ Using `vortex-cli cluster rebalance`

The `vortex-cli` tool includes automated slot migration diff computation and execution:

### 1. Dry Run / Inspection
Checks the cluster for slot imbalance without moving keys:

```bash
./bin/vortex-cli --host 127.0.0.1 -p 7379 -a "vortex_secure_2026" cluster rebalance
```

Output:
```text
=== VortexKV Cluster Slot Rebalance ===
Connecting to cluster via 127.0.0.1:7379...
Discovered 3 masters. Target slots per master: 5461
Imbalance detected:
  - Node 127.0.0.1:7379: 8000 slots (+2539)
  - Node 127.0.0.1:7381: 4000 slots (-1461)
  - Node 127.0.0.1:7382: 4384 slots (-1077)
Run with --auto to execute slot migrations.
```

### 2. Live Automated Migration
```bash
./bin/vortex-cli --host 127.0.0.1 -p 7379 -a "vortex_secure_2026" cluster rebalance --auto
```
This automatically initiates `SETSLOT IMPORTING` / `SETSLOT MIGRATING`, migrates keys, and updates slot ownership on all nodes.
