#!/usr/bin/env bash
# ==============================================================================
# 🚀 VortexKV World-Record Benchmark Suite
# ==============================================================================
set -e

PORT=7399
WEB_PORT=7398
DATA_DIR="/tmp/vortex_bench_$$"

mkdir -p "$DATA_DIR"
cleanup() {
  if [ -n "$SERVER_PID" ]; then
    echo "Stopping test vortex-server (PID: $SERVER_PID)..."
    kill "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

echo "========================================================================"
echo "⚡ Building optimized VortexKV binary..."
echo "========================================================================"
go build -ldflags="-s -w" -o ./bin/vortex-server ./cmd/vortex-server

echo "🚀 Starting VortexKV on 127.0.0.1:$PORT (Pure in-memory Multi-Reactor mode)..."
./bin/vortex-server -bind 127.0.0.1 -port $PORT -event-engine auto -web-enabled=false -aof "" -rdb "" > "$DATA_DIR/server.log" 2>&1 &
SERVER_PID=$!

# Wait for server ready
for i in {1..30}; do
  if nc -z 127.0.0.1 $PORT 2>/dev/null; then
    break
  fi
  sleep 0.1
done

echo "✓ VortexKV is live! Running world-record verification suite..."
echo ""

if ! command -v redis-benchmark &> /dev/null; then
  echo "redis-benchmark not found. Please install redis-tools / redis."
  exit 1
fi

echo "------------------------------------------------------------------------"
echo "TEST 1: Direct Non-Pipelined Concurrency (50 connections, 100,000 reqs)"
echo "------------------------------------------------------------------------"
redis-benchmark -h 127.0.0.1 -p $PORT -c 50 -n 100000 -t get,set -q

echo ""
echo "------------------------------------------------------------------------"
echo "TEST 2: Batched Pipeline P=16 (50 connections, 500,000 reqs)"
echo "------------------------------------------------------------------------"
redis-benchmark -h 127.0.0.1 -p $PORT -c 50 -n 500000 -P 16 -t get,set -q

echo ""
echo "------------------------------------------------------------------------"
echo "TEST 3: High-Density Pipeline P=64 (100 connections, 1,000,000 reqs)"
echo "------------------------------------------------------------------------"
redis-benchmark -h 127.0.0.1 -p $PORT -c 100 -n 1000000 -P 64 -t get,set -q

echo ""
echo "------------------------------------------------------------------------"
echo "TEST 4: Extreme Throughput Pipeline P=128 (100 connections, 2,000,000 reqs)"
echo "------------------------------------------------------------------------"
redis-benchmark -h 127.0.0.1 -p $PORT -c 100 -n 2000000 -P 128 -t get -q

echo ""
echo "------------------------------------------------------------------------"
echo "TEST 5: Peak In-Memory PING Rate (100 connections, P=64, 2,000,000 reqs)"
echo "------------------------------------------------------------------------"
redis-benchmark -h 127.0.0.1 -p $PORT -c 100 -n 2000000 -P 64 -t ping -q

echo ""
echo "========================================================================"
echo "✓ Benchmark completed successfully!"
echo "========================================================================"
