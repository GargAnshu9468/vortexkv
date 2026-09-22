#!/usr/bin/env bash
set -e

PORT=7379
HOST="127.0.0.1"

echo "=========================================================="
echo "⚡ VORTEXKV FULL DOCKER BENCHMARK SUITE"
echo "Image: ianshugarg/vortexkv:latest"
echo "Date: $(date)"
echo "=========================================================="

echo ""
echo "--- 1. NO PIPELINE (P=1, Sequential, -c 50, -n 100000) ---"
redis-benchmark -h $HOST -p $PORT -c 50 -n 100000 -t ping_inline,ping_mbulk,set,get,incr,lpush,lpop,sadd -q

echo ""
echo "--- 2. NO PIPELINE (P=1, Randomized -r 100000, -c 50, -n 100000) ---"
redis-benchmark -h $HOST -p $PORT -c 50 -n 100000 -r 100000 -t set,get -q

echo ""
echo "--- 3. PIPELINE P=16 (Sequential, -c 50, -n 200000) ---"
redis-benchmark -h $HOST -p $PORT -c 50 -n 200000 -P 16 -t ping_inline,ping_mbulk,set,get,incr,lpush,lpop,sadd -q

echo ""
echo "--- 4. PIPELINE P=16 (Randomized -r 100000, -c 50, -n 200000) ---"
redis-benchmark -h $HOST -p $PORT -c 50 -n 200000 -P 16 -r 100000 -t set,get -q

echo ""
echo "--- 5. PIPELINE P=64 (Sequential, -c 50, -n 300000) ---"
redis-benchmark -h $HOST -p $PORT -c 50 -n 300000 -P 64 -t ping_inline,ping_mbulk,set,get,incr,lpush,lpop,sadd -q

echo ""
echo "--- 6. PIPELINE P=64 (Randomized -r 100000, -c 50, -n 300000) ---"
redis-benchmark -h $HOST -p $PORT -c 50 -n 300000 -P 64 -r 100000 -t set,get -q

echo ""
echo "--- 7. PIPELINE P=128 (High Batch, -c 64, -n 500000) ---"
redis-benchmark -h $HOST -p $PORT -c 64 -n 500000 -P 128 -t ping_inline,ping_mbulk,set,get -q

echo ""
echo "--- 8. PIPELINE P=128 (Randomized -r 100000, -c 64, -n 500000) ---"
redis-benchmark -h $HOST -p $PORT -c 64 -n 500000 -P 128 -r 100000 -t set,get -q

echo ""
echo "--- 9. LATENCY PERCENTILES (-c 50, -n 100000, SET & GET) ---"
redis-benchmark -h $HOST -p $PORT -c 50 -n 100000 -t set,get --precision 3

echo ""
echo "=========================================================="
echo "✅ BENCHMARK SUITE COMPLETE"
echo "=========================================================="
