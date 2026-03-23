#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import struct
import time
import unicodedata
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET

import requests

from _common import RAW_ROOT, USER_AGENT, write_sidecar

DATASET_CODE = "agriculture_lpis_user_area"
OUTPUT_FILE_NAME = "lpis-user-area-yearly.csv"
EXPORT_INDEX_URL = "https://mze.gov.cz/public/app/eagriapp/LpisData/Cr.aspx"
WFS_URL = "https://mze.gov.cz/public/app/wms/plpis_wfs.fcgi"
WFS_LAYER_NAME = "ms:LPIS_DPB_UCINNE"
LAYER_EXTENT = (-923737.97, -1262669.028, -416118.97, -918143.428)
MAX_FEATURES = 7500
MIN_TILE_WIDTH = 10000
MIN_TILE_HEIGHT = 10000


@dataclass(frozen=True)
class Tile:
    minx: float
    miny: float
    maxx: float
    maxy: float
    depth: int = 0

    @property
    def width(self) -> float:
        return self.maxx - self.minx

    @property
    def height(self) -> float:
        return self.maxy - self.miny

    def can_split(self) -> bool:
        return self.width > MIN_TILE_WIDTH and self.height > MIN_TILE_HEIGHT

    def split(self) -> list["Tile"]:
        midx = (self.minx + self.maxx) / 2
        midy = (self.miny + self.maxy) / 2
        return [
            Tile(self.minx, self.miny, midx, midy, self.depth + 1),
            Tile(midx, self.miny, self.maxx, midy, self.depth + 1),
            Tile(self.minx, midy, midx, self.maxy, self.depth + 1),
            Tile(midx, midy, self.maxx, self.maxy, self.depth + 1),
        ]

    def bbox(self) -> str:
        return f"{self.minx},{self.miny},{self.maxx},{self.maxy}"


@dataclass
class LpisField:
    name: str
    length: int
    offset: int
    decimals: int = 0


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(value.replace("\xa0", " ").split())


def normalize_match_key(value: str | None) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(character for character in text if not unicodedata.combining(character))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def parse_decimal(value: str | None) -> Decimal:
    if value is None:
        return Decimal("0")
    text = value.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except InvalidOperation:
        return Decimal("0")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build yearly LPIS user-area aggregates from the dated public LPIS export, "
            "with user-name enrichment from the public LPIS WFS."
        )
    )
    parser.add_argument("--year", action="append", type=int, required=True, help="Reporting year label to emit")
    parser.add_argument(
        "--snapshot",
        default=None,
        help="Snapshot label for the output CSV. Defaults to the export timestamp embedded in the zip file name.",
    )
    parser.add_argument(
        "--zip-path",
        type=Path,
        default=None,
        help="Path to a previously downloaded LPIS export zip. Defaults to the discovered 2024 EPSG:5514 archive.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def discover_export_url(year: int) -> str:
    response = requests.get(EXPORT_INDEX_URL, timeout=60)
    response.raise_for_status()
    pattern = re.compile(rf'href="(?P<href>\.\./Files/dpb_verejny_gui_{year}-[^"]+_epsg5514\.zip)"', re.IGNORECASE)
    match = pattern.search(response.text)
    if not match:
        raise RuntimeError(f"Could not find an LPIS EPSG:5514 export link for year {year}")
    href = match.group("href")
    return requests.compat.urljoin(EXPORT_INDEX_URL, href)


def snapshot_label_from_zip_name(path: Path) -> str:
    match = re.search(r"_(\d{12})_epsg5514\.zip$", path.name, re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"(\d{8})", path.name)
    if match:
        return match.group(1)
    return time.strftime("%Y%m%d")


def ensure_export_zip(year: int, out_dir: Path, zip_path: Path | None) -> tuple[Path, str]:
    if zip_path:
        if not zip_path.exists():
            raise FileNotFoundError(zip_path)
        return zip_path, ""

    export_url = discover_export_url(year)
    local_name = export_url.rstrip("/").rsplit("/", 1)[-1]
    local_path = out_dir / local_name
    if local_path.exists() and local_path.stat().st_size > 0:
        return local_path, export_url

    with requests.get(export_url, stream=True, timeout=(30, 600)) as response:
        response.raise_for_status()
        with local_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)

    return local_path, export_url


def dbf_fields_from_zip(zip_path: Path) -> tuple[str, int, int, dict[str, LpisField]]:
    with zipfile.ZipFile(zip_path) as archive:
        dbf_name = next(name for name in archive.namelist() if name.lower().endswith(".dbf"))
        with archive.open(dbf_name) as handle:
            header = handle.read(32)
            header_len = struct.unpack("<H", header[8:10])[0]
            record_len = struct.unpack("<H", header[10:12])[0]
            field_block = handle.read(header_len - 32)

    fields: dict[str, LpisField] = {}
    offset = 1
    for index in range(0, len(field_block), 32):
        chunk = field_block[index : index + 32]
        if not chunk or chunk[0] == 0x0D:
            break
        name = chunk[:11].split(b"\x00", 1)[0].decode("latin1")
        length = chunk[16]
        decimals = chunk[17]
        fields[name] = LpisField(name=name, length=length, offset=offset, decimals=decimals)
        offset += length

    return dbf_name, header_len, record_len, fields


def iter_dpb_area_rows(zip_path: Path) -> Iterable[tuple[str, str, Decimal]]:
    dbf_name, header_len, record_len, fields = dbf_fields_from_zip(zip_path)
    required = ("ID_UZ", "VYMERA")
    missing = [name for name in required if name not in fields]
    if missing:
        raise RuntimeError(f"LPIS export is missing expected DBF fields: {', '.join(missing)}")

    id_field = fields["ID_UZ"]
    area_field = fields["VYMERA"]

    with zipfile.ZipFile(zip_path) as archive:
        with archive.open(dbf_name) as handle:
            header = handle.read(32)
            num_records = struct.unpack("<I", header[4:8])[0]
            handle.read(header_len - 32)
            for _ in range(num_records):
                record = handle.read(record_len)
                if not record:
                    break
                if record[0:1] == b"*":
                    continue
                user_id = record[id_field.offset : id_field.offset + id_field.length].decode("latin1").strip()
                if not user_id:
                    continue
                area_text = record[area_field.offset : area_field.offset + area_field.length].decode("latin1")
                area_ha = parse_decimal(area_text)
                if area_ha <= 0:
                    continue
                yield user_id, "", area_ha


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def fetch_tile(session: requests.Session, tile: Tile) -> str:
    params = {
        "SERVICE": "WFS",
        "VERSION": "1.0.0",
        "REQUEST": "GetFeature",
        "TYPENAME": WFS_LAYER_NAME,
        "MAXFEATURES": str(MAX_FEATURES),
        "OUTPUTFORMAT": "gml2",
        "PROPERTYNAME": "idUzivatele,JIuzivatele,uzivatel",
        "BBOX": tile.bbox(),
    }

    last_error: Exception | None = None
    for attempt in range(4):
        try:
            response = session.get(WFS_URL, params=params, timeout=(15, 240))
            response.raise_for_status()
            return response.text
        except Exception as error:  # pragma: no cover - network retries are best-effort
            last_error = error
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"Failed to fetch LPIS tile {tile.bbox()}") from last_error


def parse_tile_features(xml_text: str) -> list[tuple[str, str, str]]:
    root = ET.fromstring(xml_text)
    features: list[tuple[str, str, str]] = []
    for member in root.findall(".//{http://www.opengis.net/gml}featureMember"):
        feature = next(iter(member), None)
        if feature is None:
            continue
        user_id = ""
        user_ji = ""
        user_name = ""
        for child in feature:
            tag = local_name(child.tag)
            text = normalize_text(child.text)
            if tag == "idUzivatele":
                user_id = text
            elif tag == "JIuzivatele":
                user_ji = text
            elif tag == "uzivatel":
                user_name = text
        if user_id:
            features.append((user_id, user_ji, user_name))
    return features


def fetch_user_mapping(session: requests.Session, target_ids: set[str]) -> dict[str, tuple[str, str]]:
    stack = Tile(*LAYER_EXTENT).split()
    mapping: dict[str, tuple[str, str]] = {}
    processed_tiles = 0

    while stack and len(mapping) < len(target_ids):
        tile = stack.pop()
        xml_text = fetch_tile(session, tile)
        features = parse_tile_features(xml_text)
        processed_tiles += 1
        if len(features) >= MAX_FEATURES and tile.can_split():
            stack.extend(tile.split())
            continue

        for user_id, user_ji, user_name in features:
            if user_id in target_ids and user_id not in mapping:
                mapping[user_id] = (user_name, user_ji)

        if processed_tiles % 10 == 0:
            print(
                "Processed",
                processed_tiles,
                "LPIS name tiles; matched",
                len(mapping),
                "of",
                len(target_ids),
                "user ids",
                flush=True,
            )

    return mapping


def main() -> None:
    args = parse_args()
    reporting_years = sorted(set(args.year))
    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    base_year = reporting_years[0]
    zip_path, discovered_export_url = ensure_export_zip(base_year, out_dir, args.zip_path)
    snapshot_label = args.snapshot or snapshot_label_from_zip_name(zip_path)
    output_path = out_dir / f"{snapshot_label}__{OUTPUT_FILE_NAME}"

    print(f"Reading hectare aggregates from {zip_path.name}", flush=True)
    area_by_user_id: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    block_count_by_user_id: dict[str, int] = defaultdict(int)
    for user_id, _user_ji_unused, area_ha in iter_dpb_area_rows(zip_path):
        area_by_user_id[user_id] += area_ha
        block_count_by_user_id[user_id] += 1

    target_ids = set(area_by_user_id)
    print(f"Aggregated {len(target_ids)} LPIS users from dated export", flush=True)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    user_mapping = fetch_user_mapping(session, target_ids)
    print(f"Resolved {len(user_mapping)} LPIS user names from public WFS", flush=True)

    fieldnames = [
        "reporting_year",
        "user_name",
        "lpis_user_ji",
        "area_ha",
        "block_count",
        "source_url",
    ]

    row_count = 0
    unresolved_count = 0
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for reporting_year in reporting_years:
            for user_id in sorted(target_ids, key=lambda value: (-area_by_user_id[value], value)):
                user_name, user_ji = user_mapping.get(user_id, ("", ""))
                if not user_name:
                    unresolved_count += 1
                    user_name = f"LPIS uzivatel {user_id}"
                writer.writerow(
                    {
                        "reporting_year": reporting_year,
                        "user_name": user_name,
                        "lpis_user_ji": user_ji,
                        "area_ha": f"{area_by_user_id[user_id]:.2f}",
                        "block_count": block_count_by_user_id[user_id],
                        "source_url": discovered_export_url or str(zip_path),
                    }
                )
                row_count += 1

    write_sidecar(
        output_path,
        {
            "dataset": DATASET_CODE,
            "snapshot_label": snapshot_label,
            "reporting_years": reporting_years,
            "source_url": discovered_export_url or str(zip_path),
            "zip_path": str(zip_path),
            "name_lookup_source_url": WFS_URL,
            "row_count": row_count,
            "unique_users": len(target_ids),
            "resolved_user_names": len(user_mapping),
            "unresolved_user_names": len(target_ids) - len(user_mapping),
            "note": (
                "Hektarovy jmenovatel vychazi z datovaneho verejneho exportu LPIS DPB. "
                "Jmena uzivatelu jsou do nej doplnena z aktualni verejne WFS vrstvy LPIS_DPB_UCINNE. "
                "Pro nevyresene uzivatele zustava technicky placeholder podle ID_UZ."
            ),
        },
    )
    print(
        f"Wrote {output_path} rows={row_count} unique_users={len(target_ids)} "
        f"resolved_names={len(user_mapping)} unresolved={len(target_ids) - len(user_mapping)}",
        flush=True,
    )


if __name__ == "__main__":
    main()
