# 📜 Scripting: Lua 5.1 & WebAssembly (Wasm)

VortexKV supports dual scripting runtimes for high-throughput in-engine processing.

---

## 1. Embedded Lua 5.1 Scripting

Pure-Go runtime supporting atomic multi-step scripts with `redis.call` and `redis.pcall`:

```bash
# Execute inline Lua:
redis-cli -p 7379 -a "vortex_secure_2026" EVAL "local v = redis.call('get', KEYS[1]); if not v then redis.call('set', KEYS[1], ARGV[1]); return 'CREATED'; else return 'EXISTS'; end" 1 lock:99 "worker-1"
# "CREATED"

# Load script into SHA1 cache:
redis-cli -p 7379 -a "vortex_secure_2026" SCRIPT LOAD "return redis.call('incr', KEYS[1])"
# "6b142468d20025f187a2d829fd240f92b0c360b8"

# Execute via SHA1:
redis-cli -p 7379 -a "vortex_secure_2026" EVALSHA 6b142468d20025f187a2d829fd240f92b0c360b8 1 my_counter
# (integer) 1
```

- **Runaway Guard**: Execution times out after 5 seconds with `ERR Script execution timeout`.

---

## 2. WebAssembly (Wasm) Engine

Pure-Go WebAssembly runtime powered by `wazero` (zero CGO required):

```bash
# List loaded modules:
redis-cli -p 7379 -a "vortex_secure_2026" WASM LIST

# Call compiled Wasm module with arguments:
redis-cli -p 7379 -a "vortex_secure_2026" WASM CALL fast_hash '{"input":"vortex"}'
# "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
```
