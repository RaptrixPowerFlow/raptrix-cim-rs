#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "Usage: $0 <target> <os-label> <arch-label> <version>" >&2
  echo "Example: $0 x86_64-unknown-linux-gnu linux x86_64 0.2.2" >&2
  exit 1
fi

TARGET="$1"
OS_LABEL="$2"
ARCH_LABEL="$3"
VERSION="$4"
BIN_NAME="raptrix-cim-rs"
DIST_DIR="dist"
PKG_ROOT="${DIST_DIR}/${BIN_NAME}-v${VERSION}-${OS_LABEL}-${ARCH_LABEL}"

mkdir -p "$PKG_ROOT"
cp "target/${TARGET}/release/${BIN_NAME}" "$PKG_ROOT/"
cp "README.md" "$PKG_ROOT/"
cp "LICENSE" "$PKG_ROOT/"

tar -czf "${DIST_DIR}/${BIN_NAME}-v${VERSION}-${OS_LABEL}-${ARCH_LABEL}.tar.gz" -C "$DIST_DIR" "$(basename "$PKG_ROOT")"
echo "Created ${DIST_DIR}/${BIN_NAME}-v${VERSION}-${OS_LABEL}-${ARCH_LABEL}.tar.gz"
