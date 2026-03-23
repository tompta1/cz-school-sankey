from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = ROOT / "etl" / "data" / "raw" / "mk"
USER_AGENT = "cz-school-sankey-mk-etl/0.1"


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


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_value.lower()).strip("-")
    return slug or "unknown"


def parse_money(value: str | None) -> float:
    if value is None:
        return 0.0
    compact = value.replace("\xa0", " ").replace(" ", "").replace(",", ".").strip()
    compact = re.sub(r"[^0-9.\-]+$", "", compact)
    if not compact:
        return 0.0
    return float(compact)
