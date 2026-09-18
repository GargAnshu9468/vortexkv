#!/usr/bin/env bash
set -e

# ==============================================================================
# VortexKV Official 60-Second Benchmark Reproducibility Kit
# Verifies Multi-Reactor Peak Throughput and Low Latency on Your Machine
# ==============================================================================

CYAN='\033[38;2;0;243;255m'
PURPLE='\033[38;2;176;38;255m'
GREEN='\033[38;2;57;255;20m'
AMBER='\033[38;2;255;170;0m'
NC='\033[0m'
BOLD='\033[1m'

echo -e "${CYAN}${BOLD}"
cat << 'EOF'
  ██╗   ██╗ ██████╗ ██████╗ ████████╗███████╗██╗  ██╗    ██╗  ██╗██╗   ██╗
  ██║   ██║██╔═══██╗██╔══██╗╚══██╔══╝██╔════╝╚██╗██╔╝    ██║ ██╔╝██║   ██║
  ██║   ██║██║   ██║██████╔╝   ██║   █████╗   ╚███╔╝     █████╔╝ ██║   ██║
  ╚██╗ ██╔╝██║   ██║██╔══██╗   ██║   ██╔══╝   ██╔██╗     ██╔═██╗ ╚██╗ ██╔╝
   ╚████╔╝ ╚██████╔╝██║  ██║   ██║   ███████╗██╔╝ ██╗    ██║  ██╗ ╚████╔╝ 
    ╚═══╝   ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝  ╚═╝    ╚═╝  ╚═╝  ╚═══╝  
EOF
echo -e "${PURPLE}» 60-Second Benchmark Reproducibility Kit «${NC}"
echo -e "${GREEN}Engine: Pure Go Multi-Reactor (Zero CGO) | Target: 6M+ ops/sec Peak${NC}\n"

# 1. Dependency Checks
if ! command -v redis-benchmark &> /dev/null; then
    echo -e "${AMBER}⚠️  'redis-benchmark' is not installed or not in PATH.${NC}"
    echo -e "   Please install redis-tools (macOS: 'brew install redis', Debian/Ubuntu: 'sudo apt install redis-tools')"
    exit 1
fi

if ! command -v go &> /dev/null && [ ! -f "./bin/vortex-server" ]; then
    echo -e "${AMBER}⚠️  Neither Go compiler nor './bin/vortex-server' found.${NC}"
    exit 1
fi

# 2. Build or Locate VortexKV Binary
BINARY="./bin/vortex-server"
if [ ! -f "$BINARY" ]; then
    echo -e "${CYAN}🔨 Building VortexKV binary...${NC}"
    mkdir -p bin
    go build -ldflags="-s -w" -o ./bin/vortex-server ./cmd/vortex-server
fi

# 3. Choose Benchmark Port (uses isolated 18379 to avoid standard 7379/17379 ports)
BENCH_PORT=18379
BENCH_WEB_PORT=18380
BENCH_HOST="127.0.0.1"

# Clean up existing processes if any
pkill -f "vortex-server.*-port ${BENCH_PORT}" 2>/dev/null || true
sleep 0.5

# 4. Launch Isolated Server Daemon
echo -e "${CYAN}⚡ Starting isolated VortexKV reactor instance on port ${BENCH_PORT}...${NC}"
$BINARY -port ${BENCH_PORT} -web-port ${BENCH_WEB_PORT} -event-engine auto -aof "" -rdb "" -maxmemory 2gb > /dev/null 2>&1 &
SERVER_PID=$!

cleanup() {
    echo -e "\n${AMBER}🧹 Cleaning up benchmark server (PID: ${SERVER_PID})...${NC}"
    kill -9 $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Wait for server readiness
READY=0
for i in {1..30}; do
    if redis-benchmark -h ${BENCH_HOST} -p ${BENCH_PORT} -n 1 -t ping -q > /dev/null 2>&1; then
        READY=1
        break
    fi
    sleep 0.2
done

if [ $READY -ne 1 ]; then
    echo -e "\n❌ Failed to connect to VortexKV on ${BENCH_HOST}:${BENCH_PORT}"
    exit 1
fi

echo -e "${GREEN}✓ VortexKV is warm and ready! Running benchmark matrix...${NC}\n"

# 5. Benchmark Execution
echo -e "${BOLD}${PURPLE}========================================================================${NC}"
echo -e "${BOLD}1. Direct Concurrency Test (50 connections, non-pipelined)${NC}"
echo -e "${PURPLE}========================================================================${NC}"
redis-benchmark -h ${BENCH_HOST} -p ${BENCH_PORT} -c 50 -n 100000 -t get,set -q

echo -e "\n${BOLD}${PURPLE}========================================================================${NC}"
echo -e "${BOLD}2. Medium Pipeline Throughput (50 connections, Pipeline=16)${NC}"
echo -e "${PURPLE}========================================================================${NC}"
redis-benchmark -h ${BENCH_HOST} -p ${BENCH_PORT} -c 50 -n 500000 -P 16 -t get,set -q

echo -e "\n${BOLD}${PURPLE}========================================================================${NC}"
echo -e "${BOLD}3. Peak World-Record Pipeline (50 connections, Pipeline=64)${NC}"
echo -e "${PURPLE}========================================================================${NC}"
redis-benchmark -h ${BENCH_HOST} -p ${BENCH_PORT} -c 50 -n 1000000 -P 64 -t ping,get,set -q

echo -e "\n${GREEN}${BOLD}========================================================================${NC}"
echo -e "${GREEN}${BOLD}✓ Benchmark verification complete!${NC}"
echo -e "${CYAN}Share your results or star the project on GitHub:${NC}"
echo -e "${BOLD}https://github.com/GargAnshu9468/vortexkv${NC}"
echo -e "${GREEN}${BOLD}========================================================================${NC}\n"
