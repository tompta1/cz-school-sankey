from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = ROOT / "etl" / "data" / "raw" / "environment"
USER_AGENT = "cz-school-sankey-environment-etl/0.1"


def timestamp_label(explicit: str | None) -> str:
    if explicit:
        return explicit
    return datetime.now(UTC).strftime("%Y%m%d")


def fetch_bytes(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=180) as response:
        return response.read()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_sidecar(path: Path, payload: dict) -> None:
    sidecar_path = path.with_suffix(path.suffix + ".download.json")
    sidecar_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
