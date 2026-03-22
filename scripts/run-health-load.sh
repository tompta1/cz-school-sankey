#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${HEALTH_ENV_FILE:-$ROOT/.env.vercel.health}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: $ENV_FILE" >&2
  exit 1
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <etl/load_health_raw.py args...>" >&2
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

cd "$ROOT"
python3 -u etl/load_health_raw.py "$@"
