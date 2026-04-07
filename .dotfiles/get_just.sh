#!/usr/bin/env bash
set -euo pipefail

# Project-level wrapper for 'just'
# This allows running 'just' on restricted servers without system-wide installation.

VERSION="1.35.0"
BIN_DIR="$HOME/.local/bin"
JUST_BIN="$BIN_DIR/just"

install_just() {
    echo "tools: 'just' not found in $BIN_DIR. Downloading version $VERSION..."
    mkdir -p "$BIN_DIR"
    
    # Determine OS and Architecture
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)
    if [ "$ARCH" == "x86_64" ]; then ARCH="x86_64"; fi
    if [ "$ARCH" == "arm64" ] || [ "$ARCH" == "aarch64" ]; then ARCH="aarch64"; fi

    # Use the official installer script restricted to the user directory
    curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to "$BIN_DIR" --tag "$VERSION"
    
    echo "tools: 'just' installed successfully to $JUST_BIN"
}

# 1. Check if just is in path, otherwise check local bin
if ! command -v just &> /dev/null; then
    if [ ! -f "$JUST_BIN" ]; then
        install_just
    fi
    JUST_CMD="$JUST_BIN"
else
    JUST_CMD="just"
fi

# 2. Ensure local bin is in PATH for the current sub-shell
export PATH="$BIN_DIR:$PATH"

# 3. Execute just with all passed arguments
exec "$JUST_CMD" "$@"
