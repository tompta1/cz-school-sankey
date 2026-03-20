#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _common import RAW_ROOT, timestamp_label, write_download

DATASET_CODE = "health_insurer_codebook"
DEFAULT_URL = "https://rpp-opendata.egon.gov.cz/odrpp/datovasada/ciselnikyVdf/ciselnik_73_20240905.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch the official health insurer codebook")
    parser.add_argument("--url", default=DEFAULT_URL, help="Source CSV URL")
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
    data_path, _ = write_download(
        dataset_code=DATASET_CODE,
        url=args.url,
        snapshot=snapshot,
        out_dir=args.out_dir,
    )
    print(f"Wrote {data_path}")


if __name__ == "__main__":
    main()
