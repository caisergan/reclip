#!/bin/sh
# Install the pinned official rclone binary locally. Debian's DFSG build omits
# the MEGA backend, so merely having an `rclone` command is not sufficient.
set -eu

VERSION="${RECLIP_RCLONE_RELEASE:-1.75.0}"
DEST="${1:-$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/.tools}"
OS=$(uname -s)
ARCH=$(uname -m)

case "$OS" in
  Linux) platform=linux ;;
  Darwin) platform=osx ;;
  *) echo "Unsupported operating system: $OS" >&2; exit 1 ;;
esac
case "$ARCH" in
  x86_64|amd64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) echo "Unsupported CPU architecture: $ARCH" >&2; exit 1 ;;
esac

archive="rclone-v${VERSION}-${platform}-${arch}.zip"
base="https://downloads.rclone.org/v${VERSION}"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

fetch() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$1" -o "$2"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$2" "$1"
  else
    echo "curl or wget is required to install rclone" >&2
    exit 1
  fi
}

fetch "$base/$archive" "$tmp/$archive"
fetch "$base/SHA256SUMS" "$tmp/SHA256SUMS"
expected=$(awk -v name="$archive" '$2 == name {print $1}' "$tmp/SHA256SUMS")
[ -n "$expected" ] || { echo "No checksum found for $archive" >&2; exit 1; }
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$tmp/$archive" | awk '{print $1}')
else
  actual=$(shasum -a 256 "$tmp/$archive" | awk '{print $1}')
fi
[ "$actual" = "$expected" ] || { echo "rclone checksum mismatch" >&2; exit 1; }

if command -v unzip >/dev/null 2>&1; then
  unzip -q "$tmp/$archive" -d "$tmp/unpacked"
else
  echo "unzip is required to install rclone" >&2
  exit 1
fi
mkdir -p "$DEST"
find "$tmp/unpacked" -type f -name rclone -exec cp {} "$DEST/rclone" \;
chmod 0755 "$DEST/rclone"
"$DEST/rclone" help backends | grep -Eq '^[[:space:]]*mega[[:space:]]+Mega$' || {
  echo "Installed rclone does not include the MEGA backend" >&2
  exit 1
}
echo "Installed $("$DEST/rclone" version | head -n 1) at $DEST/rclone"
