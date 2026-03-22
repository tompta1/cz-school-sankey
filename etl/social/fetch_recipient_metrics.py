#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import Request, urlopen

from pypdf import PdfReader

from _common import RAW_ROOT, USER_AGENT, timestamp_label

DATASET_CODE = "social_recipient_metrics"
OUTPUT_FILE_NAME = "social-recipient-metrics.csv"
PENSIONS_URL = "https://data.cssz.cz/dump/duchodci-v-cr-krajich-okresech.csv"
UNEMPLOYMENT_PAGE_URL = "https://data.mpsv.cz/mesicni-statistiky-uchazecu-o-zamestnani-a-volnych-pracovnich-mist-od-roku-2014"
MPSV_BENEFITS_PDF_URLS = {
    2024: "https://data.mpsv.cz/documents/20142/7393973/Informace%2Bo%2Bvyplacen%C3%BDch%2Bd%C3%A1vk%C3%A1ch%2Bv%2Bprosinci%2B2024.pdf/a8e89cce-b4a6-d562-2e2c-d4ee4e043a9f?t=1738832892949",
}
NUMBER_TIS_RE = re.compile(r"(\d+,\d)\s+(\d+,\d)\s+\d+,\d")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch social recipient denominators for selected MPSV program buckets")
    parser.add_argument("--year", action="append", type=int, required=True, help="Reporting year to fetch")
    parser.add_argument("--snapshot", default=None, help="Snapshot label, defaults to YYYYMMDD")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=RAW_ROOT / DATASET_CODE,
        help="Output directory",
    )
    return parser.parse_args()


def fetch_bytes(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    with urlopen(request, timeout=120) as response:
        return response.read()


def load_pension_rows() -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(fetch_bytes(PENSIONS_URL).decode("utf-8"))))


def pension_recipient_count(pension_rows: list[dict[str, str]], year: int) -> int:
    period = f"{year}-12-31"
    total = 0
    for row in pension_rows:
        if row["referencni_obdobi"] != period:
            continue
        if row["pohlavi"] != "Celkem":
            continue
        if row["druh_duchodu"] != "Celkem v ČR":
            continue
        if not row["referencni_oblast_kod"].startswith("OK."):
            continue
        total += int(float(row["pocet_duchodcu"]))
    if total == 0:
        raise RuntimeError(f"No pension-recipient rows found for {year}")
    return total


def load_mpsv_benefits_pdf_text(year: int) -> tuple[str, str]:
    url = MPSV_BENEFITS_PDF_URLS.get(year)
    if not url:
        raise RuntimeError(f"No MPSV benefits PDF is configured for {year}")

    pdf_path = Path(f"/tmp/mpsv-benefits-{year}.pdf")
    pdf_path.write_bytes(fetch_bytes(url))
    reader = PdfReader(str(pdf_path))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)
    return text, url


def parse_unemployment_supported_year_end_count(pdf_text: str, year: int) -> int:
    pattern = re.compile(
        rf"Počet uchazečů o zaměstnání .*? prosince {year} .*? dosáhl cca (\d+,\d) tis",
        re.DOTALL,
    )
    match = pattern.search(pdf_text)
    if not match:
        raise RuntimeError(f"Could not find unemployment supported count for {year}")
    return int(round(float(match.group(1).replace(",", ".")) * 1000))


def parse_paid_count_tis(pdf_text: str, label: str) -> int:
    idx = pdf_text.find(label)
    if idx == -1:
        raise RuntimeError(f"Could not find table row for {label!r}")
    excerpt = pdf_text[idx : idx + 240]
    match = NUMBER_TIS_RE.search(excerpt)
    if not match:
        raise RuntimeError(f"Could not parse paid-count row for {label!r}")
    return int(round(float(match.group(2).replace(",", ".")) * 1000))


def build_rows(years: list[int]) -> list[dict[str, object]]:
    pension_rows = load_pension_rows()
    rows: list[dict[str, object]] = []

    for year in years:
        pdf_text, pdf_url = load_mpsv_benefits_pdf_text(year) if year in MPSV_BENEFITS_PDF_URLS else ("", "")
        rows.append(
            {
                "reporting_year": year,
                "metric_code": "pensions_recipients_year_end",
                "metric_name": "Příjemci důchodů k 31. 12.",
                "denominator_kind": "persons_year_end",
                "recipient_count": pension_recipient_count(pension_rows, year),
                "source_url": PENSIONS_URL,
            }
        )

        if year <= 2024:
            rows.append(
                {
                    "reporting_year": year,
                    "metric_code": "unemployment_support_year_end_recipients",
                    "metric_name": "Uchazeči s nárokem na podporu v nezaměstnanosti k 31. 12.",
                    "denominator_kind": "persons_year_end",
                    "recipient_count": parse_unemployment_supported_year_end_count(pdf_text, year),
                    "source_url": pdf_url or UNEMPLOYMENT_PAGE_URL,
                }
            )
            rows.append(
                {
                    "reporting_year": year,
                    "metric_code": "care_allowance_december_recipients",
                    "metric_name": "Příjemci příspěvku na péči v prosinci",
                    "denominator_kind": "persons_month_end",
                    "recipient_count": parse_paid_count_tis(pdf_text, "Příspěvek na péči"),
                    "source_url": pdf_url,
                }
            )
            rows.append(
                {
                    "reporting_year": year,
                    "metric_code": "substitute_alimony_december_recipients",
                    "metric_name": "Příjemci náhradního výživného v prosinci",
                    "denominator_kind": "persons_month_end",
                    "recipient_count": parse_paid_count_tis(pdf_text, "Náhradní výživné"),
                    "source_url": pdf_url,
                }
            )

    return rows


def write_snapshot(*, out_dir: Path, snapshot: str, rows: list[dict[str, object]], years: list[int]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f"{snapshot}__{OUTPUT_FILE_NAME}"
    fieldnames = [
        "reporting_year",
        "metric_code",
        "metric_name",
        "denominator_kind",
        "recipient_count",
        "source_url",
    ]

    with data_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    sidecar = {
        "dataset_code": DATASET_CODE,
        "downloaded_at": datetime.now(UTC).isoformat(),
        "years": years,
        "row_count": len(rows),
        "generator": "etl/social/fetch_recipient_metrics.py",
        "user_agent": USER_AGENT,
        "sources": [
            PENSIONS_URL,
            UNEMPLOYMENT_PAGE_URL,
        ],
    }
    sidecar_path = data_path.with_suffix(data_path.suffix + ".download.json")
    sidecar_path.write_text(json.dumps(sidecar, ensure_ascii=False, indent=2), encoding="utf-8")
    return data_path


def main() -> None:
    args = parse_args()
    years = sorted(set(args.year))
    snapshot = timestamp_label(args.snapshot)
    rows = build_rows(years)
    data_path = write_snapshot(out_dir=args.out_dir, snapshot=snapshot, rows=rows, years=years)
    print(f"Wrote {data_path}")
    print(f"Years: {', '.join(str(year) for year in years)}")
    print(f"Rows written: {len(rows)}")


if __name__ == "__main__":
    main()
