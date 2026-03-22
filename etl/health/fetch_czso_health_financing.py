#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _common import write_download, timestamp_label

DATASET_CODE = "health_financing_aggregates"
DATA_URL = "https://data.csu.gov.cz/opendata/sady/ZDR02/distribuce/csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch CZSO health financing aggregates by financing type and provider type")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    snapshot = timestamp_label(args.snapshot)
    data_path, _ = write_download(
        dataset_code=DATASET_CODE,
        url=DATA_URL,
        snapshot=snapshot,
        out_dir=args.out_dir,
    )
    print(f"Wrote {data_path}")


if __name__ == "__main__":
    main()
