#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-cz-school-sankey-db}"
VOLUME_NAME="${VOLUME_NAME:-cz-school-sankey-pgdata}"
IMAGE="${IMAGE:-docker.io/library/postgres:16-alpine}"
HOST_PORT="${HOST_PORT:-55432}"
POSTGRES_USER="${POSTGRES_USER:-app}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-app}"
POSTGRES_DB="${POSTGRES_DB:-cz_school_sankey}"

flatpak-spawn --host podman volume inspect "$VOLUME_NAME" >/dev/null 2>&1 \
  || flatpak-spawn --host podman volume create "$VOLUME_NAME" >/dev/null

if flatpak-spawn --host podman ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
  flatpak-spawn --host podman start "$CONTAINER_NAME" >/dev/null
else
  flatpak-spawn --host podman run -d \
    --name "$CONTAINER_NAME" \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
    -e POSTGRES_DB="$POSTGRES_DB" \
    -p "127.0.0.1:${HOST_PORT}:5432" \
    -v "${VOLUME_NAME}:/var/lib/postgresql/data" \
    "$IMAGE" >/dev/null
fi

until flatpak-spawn --host podman exec "$CONTAINER_NAME" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
  sleep 1
done

printf 'postgresql://%s:%s@127.0.0.1:%s/%s\n' \
  "$POSTGRES_USER" "$POSTGRES_PASSWORD" "$HOST_PORT" "$POSTGRES_DB"
