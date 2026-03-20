#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _common import RAW_ROOT, timestamp_label, write_download

DATASET_CODE = "nrpzs_provider_sites"
DEFAULT_URL = (
    "https://datanzis.uzis.gov.cz/data/NR-01-NRPZS/NR-01-06/"
    "Otevrena-data-NR-01-06-nrpzs-mista-poskytovani-zdravotnich-sluzeb.csv"
)
DEFAULT_METADATA_URL = f"{DEFAULT_URL}-metadata.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch NRPZS provider and facility directory")
    parser.add_argument("--url", default=DEFAULT_URL, help="Source CSV URL")
    parser.add_argument("--metadata-url", default=DEFAULT_METADATA_URL, help="CSV metadata JSON URL")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Download directory",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    snapshot = timestamp_label(args.snapshot)
    data_path, metadata_path = write_download(
        dataset_code=DATASET_CODE,
        url=args.url,
        metadata_url=args.metadata_url,
        snapshot=snapshot,
        out_dir=args.out_dir,
    )
    print(f"Wrote {data_path}")
    if metadata_path:
        print(f"Wrote {metadata_path}")


if __name__ == "__main__":
    main()
