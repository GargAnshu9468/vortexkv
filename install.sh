#!/bin/sh
set -e

# ==============================================================================
# 🌌 VortexKV Universal Production Installer
# Usage: curl -fsSL https://get.vortexkv.io | sh
# ==============================================================================

BOLD='\033[1m'
CYAN='\033[0;36m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

printf "${CYAN}"
cat << "EOF"
  ██╗   ██╗ ██████╗ ██████╗ ████████╗███████╗██╗  ██╗    ██╗  ██╗██╗   ██╗
  ██║   ██║██╔═══██╗██╔══██╗╚══██╔══╝██╔════╝╚██╗██╔╝    ██║ ██╔╝██║   ██║
  ██║   ██║██║   ██║██████╔╝   ██║   █████╗   ╚███╔╝     █████╔╝ ██║   ██║
  ╚██╗ ██╔╝██║   ██║██╔══██╗   ██║   ██╔══╝   ██╔██╗     ██╔═██╗ ╚██╗ ██╔╝
   ╚████╔╝ ╚██████╔╝██║  ██║   ██║   ███████╗██╔╝ ██╗    ██║  ██╗ ╚████╔╝ 
    ╚═══╝   ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝  ╚═╝    ╚═╝  ╚═╝  ╚═══╝  
   » Next-Gen In-Memory Key-Value Engine & Cyberpunk Command Deck «
EOF
printf "${NC}\n"

INSTALL_DIR="/usr/local/bin"
CONF_DIR="/etc/vortexkv"
DATA_DIR="/var/lib/vortexkv"

# Detect Operating System
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$ARCH" in
    x86_64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *)
        printf "${RED}❌ Unsupported architecture: %s${NC}\n" "$ARCH"
        exit 1
        ;;
esac

case "$OS" in
    linux) OS="linux" ;;
    darwin) OS="darwin" ;;
    *)
        printf "${RED}❌ Unsupported operating system: %s${NC}\n" "$OS"
        exit 1
        ;;
esac

printf "${BOLD}🚀 Detected Environment:${NC} %s (%s)\n" "$OS" "$ARCH"

# Privilege check
if [ "$(id -u)" -ne 0 ]; then
    SUDO="sudo"
else
    SUDO=""
fi

# Build or Install Binaries
printf "${BOLD}📦 Preparing VortexKV Binaries...${NC}\n"
if [ -f "./bin/vortex-server" ]; then
    printf "   Using local compiled binaries from ./bin\n"
    $SUDO cp ./bin/vortex-server "$INSTALL_DIR/vortex-server"
    $SUDO cp ./bin/vortex-cli "$INSTALL_DIR/vortex-cli"
elif command -v go >/dev/null 2>&1; then
    printf "   Compiling from Go source...\n"
    go build -o /tmp/vortex-server ./cmd/vortex-server
    go build -o /tmp/vortex-cli ./cmd/vortex-cli
    $SUDO mv /tmp/vortex-server "$INSTALL_DIR/vortex-server"
    $SUDO mv /tmp/vortex-cli "$INSTALL_DIR/vortex-cli"
else
    printf "${CYAN}   Downloading release binary for %s-%s...${NC}\n" "$OS" "$ARCH"
    # Fallback release download URL (customizable with GitHub releases)
    RELEASE_URL="https://github.com/GargAnshu9468/vortexkv/releases/latest/download/vortexkv-${OS}-${ARCH}.tar.gz"
    curl -fsSL "$RELEASE_URL" -o /tmp/vortex.tar.gz
    tar -xzf /tmp/vortex.tar.gz -C /tmp/
    $SUDO mv /tmp/vortex-server "$INSTALL_DIR/vortex-server"
    $SUDO mv /tmp/vortex-cli "$INSTALL_DIR/vortex-cli"
fi

$SUDO chmod +x "$INSTALL_DIR/vortex-server"
$SUDO chmod +x "$INSTALL_DIR/vortex-cli"

# Setup configuration and data directories
$SUDO mkdir -p "$CONF_DIR"
$SUDO mkdir -p "$DATA_DIR"
$SUDO chmod 700 "$DATA_DIR"

if [ -f "./vortex.conf" ]; then
    $SUDO cp ./vortex.conf "$CONF_DIR/vortex.conf"
fi

# Install systemd service if Linux and systemd present
if [ "$OS" = "linux" ] && command -v systemctl >/dev/null 2>&1; then
    printf "${BOLD}⚙️  Configuring systemd service...${NC}\n"
    if [ -f "./vortexkv.service" ]; then
        $SUDO cp ./vortexkv.service /etc/systemd/system/vortexkv.service
        $SUDO systemctl daemon-reload
        $SUDO systemctl enable vortexkv
        printf "${GREEN}✓ systemd service installed and enabled (sudo systemctl start vortexkv)${NC}\n"
    fi
fi

printf "\n${GREEN}${BOLD}🎉 VortexKV successfully installed!${NC}\n"
printf "─────────────────────────────────────────────────────────────\n"
printf "  ⚡ ${BOLD}Wire Protocol:${NC}   port 7379 (redis-cli -p 7379)\n"
printf "  🌌 ${BOLD}Command Deck:${NC}    http://localhost:7380\n"
printf "  📊 ${BOLD}Metrics Probe:${NC}   http://localhost:7380/metrics\n"
printf "  ❤️  ${BOLD}Health Check:${NC}    http://localhost:7380/healthz\n"
printf "─────────────────────────────────────────────────────────────\n"
printf "To launch manually:\n"
printf "  ${CYAN}vortex-server -requirepass \"your_secure_password\" -maxmemory 1gb${NC}\n\n"
