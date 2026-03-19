#!/usr/bin/env python3
"""Fetch official names and municipalities from ARES for all IČOs in the dataset.

Outputs etl/data/ares_names.json  (ico → {name, municipality}).
Re-running is safe: already-cached IČOs are skipped.

Usage:
    python3 etl/enrich_names.py [--year 2025]
"""

from __future__ import annotations

import argparse
import csv
import json
import time
import urllib.request
import urllib.error
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARES_URL = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/vyhledat"
BATCH = 100
SLEEP = 0.3  # seconds between batches


def fetch_batch(icos: list[str]) -> dict[str, dict]:
    payload = json.dumps({"ico": icos, "start": 0, "pocet": len(icos)}).encode()
    req = urllib.request.Request(
        ARES_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read())
    except urllib.error.URLError as exc:
        print(f"  ARES error: {exc}")
        return {}

    result: dict[str, dict] = {}
    for subj in data.get("ekonomickeSubjekty", []):
        ico = subj.get("ico", "")
        name = subj.get("obchodniJmeno", "") or ""
        sidlo = subj.get("sidlo") or {}
        municipality = sidlo.get("nazevObce", "") or ""
        result[ico] = {"name": name, "municipality": municipality}
    return result


def collect_icos(year: int) -> set[str]:
    csv_path = ROOT / "etl" / "data" / "raw" / str(year) / "school_entities.csv"
    icos: set[str] = set()
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row.get("ico"):
                icos.add(row["ico"].zfill(8))
            # founder IČO lives in founder_id like "founder:00240427"
            fid = row.get("founder_id", "")
            if fid.startswith("founder:"):
                icos.add(fid.removeprefix("founder:").zfill(8))
    return icos


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2025)
    args = parser.parse_args()

    cache_path = ROOT / "etl" / "data" / "ares_names.json"
    cache: dict[str, dict] = {}
    if cache_path.exists():
        cache = json.loads(cache_path.read_text(encoding="utf-8"))

    icos = collect_icos(args.year)
    to_fetch = sorted(icos - set(cache.keys()))
    print(f"Total IČOs: {len(icos)}  cached: {len(cache)}  to fetch: {len(to_fetch)}")

    batches = [to_fetch[i : i + BATCH] for i in range(0, len(to_fetch), BATCH)]
    for idx, batch in enumerate(batches):
        print(f"  Batch {idx + 1}/{len(batches)} ({len(batch)} IČOs)…", end=" ", flush=True)
        result = fetch_batch(batch)
        # Fill in blanks for any IČOs ARES didn't return
        for ico in batch:
            cache[ico] = result.get(ico, {"name": "", "municipality": ""})
        print(f"got {len(result)}")
        cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
        if idx < len(batches) - 1:
            time.sleep(SLEEP)

    print(f"Done — {len(cache)} entries saved to {cache_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
