#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-cz-school-sankey-db}"

if flatpak-spawn --host podman ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
  flatpak-spawn --host podman stop "$CONTAINER_NAME" >/dev/null
fi
