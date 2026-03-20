#!/usr/bin/env bash
set -euo pipefail

HOST_PORT="${HOST_PORT:-55432}"
POSTGRES_USER="${POSTGRES_USER:-app}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-app}"
POSTGRES_DB="${POSTGRES_DB:-cz_school_sankey}"

printf 'postgresql://%s:%s@127.0.0.1:%s/%s\n' \
  "$POSTGRES_USER" "$POSTGRES_PASSWORD" "$HOST_PORT" "$POSTGRES_DB"
