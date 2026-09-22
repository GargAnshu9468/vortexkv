#!/usr/bin/env bash
set -e

# ==============================================================================
# VortexKV Official One-Line Installer
# Installs VortexKV binary (Multi-Reactor in-memory data engine & Studio)
# Usage: curl -fsSL https://raw.githubusercontent.com/GargAnshu9468/vortexkv/main/install.sh | bash
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
echo -e "${PURPLE}» Next-Gen Hyper-Performance In-Memory Data Store & Studio «${NC}"
echo -e "${GREEN}Installing VortexKV v1.0.2...${NC}\n"

# 1. OS & Architecture Detection
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$ARCH" in
    x86_64|amd64)
        ARCH="amd64"
        ;;
    arm64|aarch64)
        ARCH="arm64"
        ;;
    *)
        echo -e "${AMBER}❌ Unsupported CPU architecture: $ARCH${NC}"
        exit 1
        ;;
esac

case "$OS" in
    darwin|linux)
        ;;
    *)
        echo -e "${AMBER}❌ Unsupported Operating System: $OS${NC}"
        echo -e "VortexKV runs on Linux (x86_64/arm64) and macOS (Apple Silicon/Intel)."
        exit 1
        ;;
esac

VERSION="v1.0.2"
TAR_NAME="vortexkv-${VERSION}-${OS}-${ARCH}.tar.gz"
DOWNLOAD_URL="https://github.com/GargAnshu9468/vortexkv/releases/download/${VERSION}/${TAR_NAME}"

# 2. Determine Install Destination
if [ -w "/usr/local/bin" ]; then
    INSTALL_DIR="/usr/local/bin"
    SUDO=""
elif command -v sudo &> /dev/null && sudo -n true 2>/dev/null; then
    INSTALL_DIR="/usr/local/bin"
    SUDO="sudo"
else
    INSTALL_DIR="$HOME/.local/bin"
    mkdir -p "$INSTALL_DIR"
    SUDO=""
fi

TMP_DIR="$(mktemp -d)"
cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

echo -e "${CYAN}⬇️  Downloading ${TAR_NAME}...${NC}"
DOWNLOAD_SUCCESS=0
if command -v curl &> /dev/null; then
    if curl -fSL --connect-timeout 10 "$DOWNLOAD_URL" -o "$TMP_DIR/$TAR_NAME" 2>/dev/null; then
        DOWNLOAD_SUCCESS=1
    fi
elif command -v wget &> /dev/null; then
    if wget -q --timeout=10 "$DOWNLOAD_URL" -O "$TMP_DIR/$TAR_NAME" 2>/dev/null; then
        DOWNLOAD_SUCCESS=1
    fi
fi

if [ $DOWNLOAD_SUCCESS -eq 1 ] && [ -s "$TMP_DIR/$TAR_NAME" ]; then
    echo -e "${CYAN}📦 Extracting binary package...${NC}"
    tar -xzf "$TMP_DIR/$TAR_NAME" -C "$TMP_DIR"
    EXTRACTED_BIN="$(find "$TMP_DIR" -type f -name "vortex-server" | head -n 1)"
    if [ -n "$EXTRACTED_BIN" ]; then
        cp "$EXTRACTED_BIN" "$TMP_DIR/vortex-server"
    fi
fi

# Fallback: Compile from source if download failed and go compiler is present
if [ ! -f "$TMP_DIR/vortex-server" ]; then
    if command -v go &> /dev/null; then
        echo -e "${AMBER}⚠️  Direct download unavailable, building from source via Go...${NC}"
        GOBIN="$TMP_DIR" go install github.com/GargAnshu9468/vortexkv/cmd/vortex-server@latest
    else
        echo -e "${AMBER}❌ Could not download or build vortex-server.${NC}"
        exit 1
    fi
fi

echo -e "${CYAN}🚀 Installing vortex-server to ${INSTALL_DIR}...${NC}"
$SUDO cp "$TMP_DIR/vortex-server" "$INSTALL_DIR/vortex-server"
$SUDO chmod +x "$INSTALL_DIR/vortex-server"

# Check if INSTALL_DIR is in PATH
if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
    echo -e "\n${AMBER}⚠️  Note: ${INSTALL_DIR} is not in your \$PATH.${NC}"
    echo -e "   Add it by running: export PATH=\"\$PATH:${INSTALL_DIR}\""
fi

echo -e "\n${GREEN}${BOLD}========================================================================${NC}"
echo -e "${GREEN}${BOLD}✓ VortexKV installed successfully!${NC}"
echo -e "${GREEN}${BOLD}========================================================================${NC}\n"

echo -e "${BOLD}Quickstart Commands:${NC}"
echo -e "  1. Launch server & Cyberpunk Web Studio:"
echo -e "     ${CYAN}vortex-server -port 7379 -web-port 7380${NC}\n"
echo -e "  2. Connect with standard redis-cli:"
echo -e "     ${CYAN}redis-cli -p 7379 PING${NC}\n"
echo -e "  3. Open Visual Command Deck:"
echo -e "     ${PURPLE}http://localhost:7380${NC}\n"
echo -e "  4. Star the repository on GitHub:"
echo -e "     ${BOLD}https://github.com/GargAnshu9468/vortexkv${NC}\n"
