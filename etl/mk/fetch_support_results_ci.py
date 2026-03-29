#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

from pypdf import PdfReader

from _common import RAW_ROOT, fetch_bytes, parse_money, sha256_bytes, slugify, timestamp_label, write_sidecar

AWARD_DATASET_CODE = "mk_support_awards"
REGION_DATASET_CODE = "mk_region_metrics"

AWARD_OUTPUT_FILE_NAME = "mk-support-awards.csv"
REGION_OUTPUT_FILE_NAME = "mk-region-metrics.csv"

CULTURE_MUSEUMS_URL = "https://mk.gov.cz/doc/cms_library/kulturni-aktivity-pro-spolky-2024-konecne-vysledky-18473.doc"
HERITAGE_ACTIVITIES_URL = "https://mk.gov.cz/doc/cms_library/vysledky-dotacniho-rizeni-pro-rok-2024-18577.pdf"
PZAD_URL = "https://www.mk.gov.cz/doc/cms_library/pzad-2024-souhrnne-tabulky-vyhodnoceni-pzad-a-vzorova-akce-20058.pdf"

PROGRAMS = {
    "CULTURE_MUSEUMS": {
        "program_name": "Kulturní aktivity pro spolky v muzejnictví",
        "source_url": CULTURE_MUSEUMS_URL,
    },
    "HERITAGE_ACTIVITIES": {
        "program_name": "Kulturní aktivity v památkové péči",
        "source_url": HERITAGE_ACTIVITIES_URL,
    },
    "PZAD": {
        "program_name": "Program záchrany architektonického dědictví",
        "source_url": PZAD_URL,
    },
}

REGION_CODES = {
    "Jihočeský": "CZ031",
    "Jihomoravský": "CZ064",
    "Karlovarský": "CZ041",
    "Královéhradecký": "CZ052",
    "Liberecký": "CZ051",
    "Liberecký kraj": "CZ051",
    "Moravskoslezský": "CZ080",
    "Olomoucký": "CZ071",
    "Pardubický": "CZ053",
    "Plzeňský": "CZ032",
    "Hl. m. Praha": "CZ010",
    "Středočeský": "CZ020",
    "Ústecký": "CZ042",
    "Kraj Vysočina": "CZ063",
    "Zlínský": "CZ072",
}

LEGAL_FORM_RE = re.compile(
    r"^(.+?(?:,\s*z\.\s*s\.|,\s*o\.\s*p\.\s*s\.|,\s*o\.p\.s\.|,\s*s\.r\.o\.|,\s*a\.s\.|,\s*z\. ú\.|,\s*o\. ?p\. ?s\.))\s+(.*)$",
    re.IGNORECASE,
)
AMOUNT_TOKEN_RE = re.compile(r"\d{1,3}(?:[ \xa0]\d{3})*(?:,\d+)?")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch MK support program results")
    parser.add_argument("--year", action="append", type=int, required=True, help="Reporting year to fetch. Only 2024 is currently supported.")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT,
        help="Output root directory",
    )
    return parser.parse_args()


def antiword_text(doc_bytes: bytes) -> str:
    with tempfile.NamedTemporaryFile(suffix=".doc", delete=False) as handle:
        handle.write(doc_bytes)
        temp_path = Path(handle.name)
    try:
        antiword_binary = shutil.which("antiword")
        if antiword_binary:
            command = [antiword_binary, "-m", "UTF-8", str(temp_path)]
        elif shutil.which("flatpak-spawn"):
            command = ["flatpak-spawn", "--host", "antiword", "-m", "UTF-8", str(temp_path)]
        else:
            raise RuntimeError("Missing antiword executable; install antiword or provide flatpak-spawn access")
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout
    finally:
        temp_path.unlink(missing_ok=True)


def parse_culture_museums_awards(reporting_year: int) -> tuple[list[dict[str, object]], dict[str, object]]:
    source_url = CULTURE_MUSEUMS_URL
    doc_bytes = fetch_bytes(source_url)
    text = antiword_text(doc_bytes)

    groups: list[list[str]] = []
    current: list[str] = []
    for line in text.splitlines():
        if not line.startswith("|"):
            continue
        if re.match(r"^\|\d+\.", line):
            if current:
                groups.append(current)
            current = [line]
        elif current:
            current.append(line)
    if current:
        groups.append(current)

    rows: list[dict[str, object]] = []
    for group in groups:
        columns = [[part.strip() for part in line.split("|")[1:-1]] for line in group]
        first = columns[0]
        label_parts = [cols[0] for cols in columns if cols and cols[0]]
        label = " ".join(part.replace("\xa0", " ") for part in label_parts).strip()
        label = re.sub(r"^\d+\.\s*", "", label).strip()
        recipient_name, _, project_name = label.partition(":")
        recipient_name = recipient_name.strip()
        project_name = project_name.strip() or label
        requested_czk = parse_money(first[1] if len(first) > 1 else None)
        awarded_czk = parse_money(first[2] if len(first) > 2 else None)
        if not recipient_name:
            continue
        rows.append(
            {
                "reporting_year": reporting_year,
                "program_code": "CULTURE_MUSEUMS",
                "program_name": PROGRAMS["CULTURE_MUSEUMS"]["program_name"],
                "recipient_key": slugify(recipient_name),
                "recipient_name": recipient_name,
                "recipient_ico": "",
                "project_name": project_name,
                "requested_czk": requested_czk,
                "awarded_czk": awarded_czk,
                "source_url": source_url,
            }
        )

    metadata = {
        "source_url": source_url,
        "content_sha256": sha256_bytes(doc_bytes),
        "row_count": len(rows),
    }
    return rows, metadata


def split_recipient_and_project(body: str) -> tuple[str, str]:
    cleaned = re.sub(r"\s+", " ", body).strip()
    match = LEGAL_FORM_RE.match(cleaned)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return cleaned, cleaned


def parse_heritage_awards(reporting_year: int) -> tuple[list[dict[str, object]], dict[str, object]]:
    source_url = HERITAGE_ACTIVITIES_URL
    pdf_bytes = fetch_bytes(source_url)
    text = PdfReader(BytesIO(pdf_bytes)).pages[0].extract_text() or ""

    rows: list[dict[str, object]] = []
    buffer: list[str] = []
    started = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("Žadatel") or line in {"DOTACE", "Kč"}:
            continue
        if line.startswith("Významné akce"):
            continue
        if line.startswith("Nepodpořené projekty") or line.startswith("Celkem:"):
            break
        if line.startswith("Společnost pro technologie ochrany") or line.startswith("Institut pro památky"):
            started = True
        if not started:
            continue
        buffer.append(line)
        amount_tokens = AMOUNT_TOKEN_RE.findall(line)
        if len(amount_tokens) < 3:
            continue
        cost_text, requested_text, awarded_text = amount_tokens[-3:]
        body = " ".join(buffer)
        body = re.sub(r"\s+", " ", body)
        body = re.sub(
            rf"{re.escape(cost_text)}\s+{re.escape(requested_text)}\s+{re.escape(awarded_text)}$",
            "",
            body,
        ).strip()
        recipient_name, project_name = split_recipient_and_project(body)
        rows.append(
            {
                "reporting_year": reporting_year,
                "program_code": "HERITAGE_ACTIVITIES",
                "program_name": PROGRAMS["HERITAGE_ACTIVITIES"]["program_name"],
                "recipient_key": slugify(recipient_name),
                "recipient_name": recipient_name,
                "recipient_ico": "",
                "project_name": project_name,
                "requested_czk": parse_money(requested_text),
                "awarded_czk": parse_money(awarded_text),
                "source_url": source_url,
            }
        )
        buffer = []

    metadata = {
        "source_url": source_url,
        "content_sha256": sha256_bytes(pdf_bytes),
        "row_count": len(rows),
    }
    return rows, metadata


def parse_pzad_region_metrics(reporting_year: int) -> tuple[list[dict[str, object]], dict[str, object]]:
    source_url = PZAD_URL
    pdf_bytes = fetch_bytes(source_url)
    reader = PdfReader(BytesIO(pdf_bytes))
    page_amounts = reader.pages[0].extract_text() or ""
    page_counts = reader.pages[2].extract_text() or ""

    amount_by_region: dict[str, float] = {}
    amount_section_started = False
    for raw_line in page_amounts.splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip()
        if not line:
            continue
        if line == "Kraj Finanční objem v Kč":
            amount_section_started = True
            continue
        if not amount_section_started:
            continue
        if line.startswith("Celkem"):
            break

        amount_match = re.search(r"(.+?)\s+(\d[\d ]*(?:,\d+)?)$", line)
        if not amount_match:
            continue
        region_name = amount_match.group(1).strip()
        amount_by_region[region_name] = parse_money(amount_match.group(2))

    count_by_region: dict[str, int] = {}
    count_section_started = False
    for raw_line in page_counts.splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip()
        if not line:
            continue
        if line == "Kraj FO FP Obec Církev Spolek PO OPS Celkem":
            count_section_started = True
            continue
        if not count_section_started:
            continue
        if line.startswith("Počet"):
            break

        count_match = re.search(r"(.+?)\s+(?:\d+\s+)*(\d+)$", line)
        if not count_match:
            continue

        count_by_region[count_match.group(1).strip()] = int(count_match.group(2))

    rows: list[dict[str, object]] = []
    for region_name, amount_czk in amount_by_region.items():
        recipient_count = count_by_region.get(region_name)
        if recipient_count is None and not region_name.endswith(" kraj"):
            recipient_count = count_by_region.get(f"{region_name} kraj")
        if recipient_count is None and region_name.endswith(" kraj"):
            recipient_count = count_by_region.get(region_name.removesuffix(" kraj"))
        rows.append(
            {
                "reporting_year": reporting_year,
                "program_code": "PZAD",
                "program_name": PROGRAMS["PZAD"]["program_name"],
                "region_code": REGION_CODES.get(region_name, ""),
                "region_name": region_name,
                "recipient_count": recipient_count or 0,
                "awarded_czk": amount_czk,
                "source_url": source_url,
            }
        )

    metadata = {
        "source_url": source_url,
        "content_sha256": sha256_bytes(pdf_bytes),
        "row_count": len(rows),
    }
    return rows, metadata


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    years = sorted(set(args.year))
    unsupported = [year for year in years if year != 2024]
    if unsupported:
        raise SystemExit(f"MK support results currently support only 2024, got: {', '.join(map(str, unsupported))}")

    snapshot = timestamp_label(args.snapshot)
    all_awards: list[dict[str, object]] = []
    all_regions: list[dict[str, object]] = []
    metadata = []

    for year in years:
        culture_rows, culture_meta = parse_culture_museums_awards(year)
        heritage_rows, heritage_meta = parse_heritage_awards(year)
        region_rows, region_meta = parse_pzad_region_metrics(year)
        all_awards.extend(culture_rows)
        all_awards.extend(heritage_rows)
        all_regions.extend(region_rows)
        metadata.extend([culture_meta, heritage_meta, region_meta])

    award_path = args.out_dir / AWARD_DATASET_CODE / f"{snapshot}__{AWARD_OUTPUT_FILE_NAME}"
    write_csv(
        award_path,
        ["reporting_year", "program_code", "program_name", "recipient_key", "recipient_name", "recipient_ico", "project_name", "requested_czk", "awarded_czk", "source_url"],
        all_awards,
    )
    write_sidecar(
        award_path,
        {
            "dataset_code": AWARD_DATASET_CODE,
            "downloaded_at": datetime.now(UTC).isoformat(),
            "years": years,
            "row_count": len(all_awards),
            "programs": metadata[:2],
            "generator": "etl/mk/fetch_support_results.py",
        },
    )

    region_path = args.out_dir / REGION_DATASET_CODE / f"{snapshot}__{REGION_OUTPUT_FILE_NAME}"
    write_csv(
        region_path,
        ["reporting_year", "program_code", "program_name", "region_code", "region_name", "recipient_count", "awarded_czk", "source_url"],
        all_regions,
    )
    write_sidecar(
        region_path,
        {
            "dataset_code": REGION_DATASET_CODE,
            "downloaded_at": datetime.now(UTC).isoformat(),
            "years": years,
            "row_count": len(all_regions),
            "programs": metadata[2:],
            "generator": "etl/mk/fetch_support_results.py",
        },
    )

    print(f"Wrote {award_path}")
    print(f"Wrote {region_path}")
    print(f"Award rows: {len(all_awards)}")
    print(f"Region rows: {len(all_regions)}")


if __name__ == "__main__":
    main()
