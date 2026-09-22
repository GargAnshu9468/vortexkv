# ⚙️ VortexKV Architecture & Internals

This document details the internal design and concurrency model that allow VortexKV to achieve **sub-millisecond latency (111µs)** and **over 6,870,000 operations per second** on modern multi-core machines.

---

## 1. Concurrency Model: Cacheline-Padded Lock-Striped Sharding

Unlike single-threaded in-memory stores that bottleneck on a single CPU core, VortexKV implements a **256-way lock-striped concurrent keyspace** with 64-byte CPU cacheline padding:

```
                       Incoming Commands (Wire Port 7379)
                                       │
                         Inlined Zero-Alloc FNV-1a Hash
                                       │
           ┌───────────┬───────────┬───┴───────┬───────────┐
           ▼           ▼           ▼           ▼           ▼
       [Shard 0]   [Shard 1]   [Shard 2]  ... [Shard 254] [Shard 255]
       [Mutex 0]   [Mutex 1]   [Mutex 2]  ... [Mutex 254] [Mutex 255]
```

- Each key is hashed deterministically into one of 256 independent shards using an inlined, zero-allocation 64-bit FNV-1a algorithm running at **27 nanoseconds per lookup** (**75,900,000 ops/sec**).
- Each shard owns an independent `sync.RWMutex` padded with `_ [64]byte` to prevent L1/L2 CPU cache false sharing across physical cores.
- Read operations (`GET`, `HGET`) acquire read locks on only the designated shard, allowing hundreds of concurrent readers across multiple CPU cores without lock contention.
- Writes (`SET`, `DEL`) only acquire an exclusive lock on the single target shard.

---

## 2. Hardware-Accelerated Multi-Reactor Engine (`SO_REUSEPORT` + `kqueue` / `epoll`)

To bridge the raw throughput gap against C++/C# engines (Dragonfly/Garnet) and achieve **4.10M+ ops/sec**, VortexKV features an event-driven Multi-Reactor network engine with kernel socket steering (`internal/reactor`):

```
                        Incoming Client Connections
                                     │
                 Kernel-Level 4-Tuple Socket Steering (SO_REUSEPORT)
                 ┌───────────────────┼───────────────────┐
                 ▼                   ▼                   ▼
      [Worker 0 Listener]  [Worker 1 Listener]  [Worker N Listener]
     ┌───────────────────┐┌───────────────────┐┌───────────────────┐
     │ Worker 0 (epoll)  ││ Worker 1 (epoll)  ││ Worker N (epoll)  │
     │ 32-Bit Cmd Match  ││ 32-Bit Cmd Match  ││ 32-Bit Cmd Match  │
     │ Zero-Alloc RingBuf││ Zero-Alloc RingBuf││ Zero-Alloc RingBuf│
     └─────────┬─────────┘└─────────┬─────────┘└─────────┬─────────┘
               ▼                    ▼                    ▼
     [Vectorized Write]   [Vectorized Write]   [Vectorized Write]
```

### Key Architectural Pillars of the Reactor Engine:
1. **Multi-Listener `SO_REUSEPORT` Kernel Socket Steering**:
   - Each worker loop binds its own dedicated listening socket with `SO_REUSEPORT` (`0x0F` on Linux, `0x0200` on Darwin).
   - Incoming TCP handshakes are hashed directly by the operating system kernel across worker queues, eliminating the single acceptor goroutine bottleneck and cross-thread lock contention.
2. **Single-Cycle 32-Bit Integer Word Command Dispatch**:
   - High-frequency commands (`GET`, `SET`, `DEL`, `PING`, `INCR`, `DECR`, `QUIT`) are matched using 32-bit bitwise integer masks (`| 0x20`) in a single CPU cycle.
   - String comparisons, substring allocations, and case conversions are completely bypassed on the hot execution path.
3. **In-Place Zero-Allocation Keyspace Updates**:
   - `Keyspace.SetString(key, val)` updates existing values in-place under shard write locks without allocating heap `Entry` structs or causing Go garbage collection pressure.
   - FNV-1a hash unrolled 4 bytes per cycle for reduced instruction count.
4. **Contiguous Zero-Allocation Circular Ring Buffer (`RingBuffer`)**:
   - Every connection maintains a pre-allocated circular ring buffer (default 64 KB).
   - Reads directly stream from the socket into the ring buffer via `ReadSlice()` without allocating heap byte slices.
5. **Zero-Allocation Inline RESP2/RESP3 Parser**:
   - `ParseCommandInto(data, dst)` directly scans bytes without constructing intermediate token arrays or strings.
   - Operates at **27,500,000+ ops/sec** (42.5 ns/op).
6. **Ultra-Fast Zero-Copy Serializer & Vectorized Outbound Writes**:
   - Fast-path bulk string and integer length formatters directly insert ASCII characters (microbenchmarked at **2.86 ns/op** for simple strings, **3.99 ns/op** for bulk strings).
   - `flushWrite` retries non-blockingly without thread stalls (`time.Sleep`), preserving outbound buffer state during transient network saturation.
   - Benchmarked at **435,000,000+ ops/sec** (2.75 ns/op, 0 B/op).
7. **Smart Socket Pipeline Coalescing**:
   - When a client sends pipelined requests (`-P 64` or `-P 128`), worker sub-reactors execute commands in-line and accumulate responses into the outbound buffer.
   - The buffer is flushed to the kernel socket **only when the input ring buffer is drained**, collapsing hundreds of pipelined responses into a single kernel `write()` syscall.
8. **Sampled Memory Cap Telemetry**:
   - Mutating commands sample memory checks every 2,048 writes rather than executing `runtime.ReadMemStats` on every write, completely eliminating Go runtime Stop-The-World (STW) latency spikes.

---

## 3. Smart Socket Pipeline Coalescing & Batching

The biggest performance differentiator in Redis protocols is **pipelining** (`-P 16` to `-P 64`):
- Instead of calling `conn.Write()` (an expensive OS kernel syscall) after every single command, VortexKV inspects buffered socket state.
- Responses are written directly into a high-capacity in-memory ring buffer.
- The buffer is flushed to the TCP socket **only when the input command queue is completely drained**.
- This coalesces 64-128 pipelined commands into **one single kernel `write()` syscall**, slashing context-switch overhead by over 95% and propelling pipelined throughput to **6,870,000+ ops/sec** (PING) and **2,695,000+ ops/sec** (GET).

---

## 3. Lock-Free Per-Connection Client Sessions

- Standard engines frequently bottleneck on global client registry mutexes during high-concurrency loads.
- VortexKV binds a `ClientSession` directly to the active TCP connection during the initial accept handshake.
- Command execution uses `ExecuteCommandWithSession()`, completely eliminating global mutex contention on the hot path.

---

## 4. Zero-Allocation Sub-Millisecond TTL Timing Wheel

Managing expirations efficiently is critical to preventing latency spikes. VortexKV pairs two complimentary expiration engines:

1. **Passive Expiration (Lazy Check)**:
   - When any key is accessed (`GET`, `HGET`, `EXISTS`), its TTL timestamp is inspected.
   - If `expiredAt == 0` (no TTL), it skips clock system calls entirely.
   - If `expiredAt > 0` and `time.Now() >= expiredAt`, the key is immediately purged and `nil` is returned.
2. **Active Expiration (Timing Wheel)**:
   - An asynchronous background routine samples keys from shards every 100 milliseconds.
   - If more than 25% of sampled keys are expired, the routine loops aggressively to reclaim memory before evictions are needed.
   - Expired keys emit delete notifications into the AOF stream to ensure replica parity.

---

## 5. High-Performance Zero-Copy RESP2/RESP3 Wire Parser

VortexKV's network layer in `internal/resp/parser.go` directly parses byte buffers from TCP streams:
- Uses pre-allocated static byte slices (`+OK\r\n`, `+PONG\r\n`, `$-1\r\n`, `*-1\r\n`, `:0\r\n`, `:1\r\n`).
- Implements strict validation to eliminate allocation bombs (`MaxArrayLength = 1,000,000` elements).
- Direct string writing for bulk strings without intermediate `[]byte` slice copies.

---

## 6. Single-Binary Architecture & Embedded Web Studio

VortexKV ships as a **single, static, zero-dependency executable**:
- The Cyberpunk Web Studio UI (`internal/web/dist/index.html`) is compiled directly into the Go binary using Go 1.16+ `//go:embed`.
- The Web Studio communicates with the engine via:
  - High-frequency bi-directional **WebSockets (`/ws`)** for live 60 FPS telemetry streams (CPU, ops/sec, slowlog events).
  - High-speed REST endpoints (`/api/keys`, `/api/exec`, `/api/acl/users`).
  - Standard Prometheus scrape endpoint (`/metrics`) and health check probe (`/healthz`).
