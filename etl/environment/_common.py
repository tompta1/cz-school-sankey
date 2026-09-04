from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, datetime
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = ROOT / "etl" / "data" / "raw" / "environment"
USER_AGENT = "cz-school-sankey-environment-etl/0.1"


def timestamp_label(explicit: str | None) -> str:
    if explicit:
        return explicit
    return datetime.now(UTC).strftime("%Y%m%d")


def fetch_bytes(url: str, *, attempts: int = 3) -> bytes:
    last_error: requests.RequestException | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                url,
                headers={"User-Agent": USER_AGENT},
                timeout=180,
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as error:
            last_error = error
            if attempt < attempts:
                time.sleep(2 ** (attempt - 1))

    assert last_error is not None
    raise last_error


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_sidecar(path: Path, payload: dict) -> None:
    sidecar_path = path.with_suffix(path.suffix + ".download.json")
    sidecar_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
