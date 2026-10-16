#!/bin/bash
set -euo pipefail

# Regenerates MeshProto/Generated from the official meshtastic/protobufs at a
# pinned commit. Requires protoc + protoc-gen-swift on PATH (brew installs them
# under /opt/homebrew/bin). Run from anywhere; paths are absolute.

PROTO_COMMIT="da2fc413931c81c1d2bc9b6a838d739e2bf35bc1"
REPO="https://github.com/meshtastic/protobufs.git"
OUT="/Users/ell/development/ios/pods/MeshtasticKit/Sources/MeshProto/Generated"
WORK="$(mktemp -d)"

export PATH="/opt/homebrew/bin:$PATH"

git clone "$REPO" "$WORK/protobufs"
git -C "$WORK/protobufs" checkout "$PROTO_COMMIT"

rm -rf "$OUT"
mkdir -p "$OUT"

# Explicit file list = import closure of mesh.proto (verified transitively).
FILES="meshtastic/mesh.proto meshtastic/channel.proto meshtastic/config.proto meshtastic/device_ui.proto meshtastic/module_config.proto meshtastic/portnums.proto meshtastic/telemetry.proto meshtastic/xmodem.proto meshtastic/atak.proto"

protoc \
  --proto_path="$WORK/protobufs" \
  --swift_out="$OUT" \
  --swift_opt=Visibility=Public \
  --swift_opt=FileNaming=DropPath \
  $FILES

rm -rf "$WORK"
echo "Generated protos into $OUT"
