from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = ROOT / "etl" / "data" / "raw" / "health"
USER_AGENT = "cz-school-sankey-health-etl/0.1"


def timestamp_label(explicit: str | None) -> str:
    if explicit:
        return explicit
    return datetime.now(UTC).strftime("%Y%m%d")


def filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name
    if not name:
        raise ValueError(f"Could not infer file name from URL: {url}")
    return name


def fetch_bytes(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=120) as response:
        return response.read()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_download(
    *,
    dataset_code: str,
    url: str,
    snapshot: str,
    out_dir: Path | None,
    metadata_url: str | None = None,
) -> tuple[Path, Path | None]:
    target_dir = out_dir or (RAW_ROOT / dataset_code)
    target_dir.mkdir(parents=True, exist_ok=True)

    data_name = filename_from_url(url)
    data_path = target_dir / f"{snapshot}__{data_name}"
    data = fetch_bytes(url)
    data_path.write_bytes(data)

    metadata_path: Path | None = None
    if metadata_url:
        metadata_name = filename_from_url(metadata_url)
        metadata_path = target_dir / f"{snapshot}__{metadata_name}"
        metadata_path.write_bytes(fetch_bytes(metadata_url))

    sidecar = {
        "dataset_code": dataset_code,
        "source_url": url,
        "metadata_url": metadata_url,
        "downloaded_at": datetime.now(UTC).isoformat(),
        "sha256": sha256_bytes(data),
        "size_bytes": len(data),
    }
    sidecar_path = data_path.with_suffix(data_path.suffix + ".download.json")
    sidecar_path.write_text(json.dumps(sidecar, ensure_ascii=False, indent=2), encoding="utf-8")
    return data_path, metadata_path
