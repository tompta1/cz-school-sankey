#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from decimal import Decimal
import gzip
import io
import json
import os
import subprocess
from collections.abc import Iterable
from datetime import date
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = ROOT / "etl" / "data" / "raw" / "health"

SOURCES = {
    "uzis": ("UZIS", "https://datanzis.uzis.gov.cz"),
    "dia": ("DIA / RPP", "https://rpp-opendata.egon.gov.cz"),
    "monitor_mf": ("Monitor Státní pokladny", "https://monitor.statnipokladna.gov.cz"),
    "czso": ("Český statistický úřad", "https://data.csu.gov.cz"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load downloaded health files into raw Postgres tables")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection string. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        choices=[
            "nrpzs_provider_sites",
            "nrhzs_claims_provider_specialty",
            "nrhzs_claims_payer",
            "health_insurer_codebook",
            "health_monitor_indicators",
            "health_mz_budget_entities",
            "health_financing_aggregates",
            "health_zzs_activity_aggregates",
        ],
        help="Restrict loading to one or more dataset codes.",
    )
    return parser.parse_args()


def parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    return int(text)


def parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    return date.fromisoformat(text)


def parse_decimal(value: str | None) -> Decimal:
    if value is None:
        return Decimal("0")
    text = value.strip()
    if not text:
        return Decimal("0")
    return Decimal(text)


def latest_data_path(dataset_code: str) -> Path | None:
    dataset_dir = RAW_ROOT / dataset_code
    if not dataset_dir.exists():
        return None

    candidates = [
        path
        for path in dataset_dir.iterdir()
        if path.is_file()
        and not path.name.endswith(".download.json")
        and not path.name.endswith("-metadata.json")
        and path.suffix != ".json"
    ]
    if not candidates:
        return None
    return sorted(candidates)[-1]


def snapshot_label_for(path: Path) -> str:
    return path.name.split("__", 1)[0]


def sidecar_for(path: Path) -> dict:
    sidecar_path = Path(str(path) + ".download.json")
    if not sidecar_path.exists():
        return {}
    return json.loads(sidecar_path.read_text(encoding="utf-8"))


def metadata_path_for(path: Path) -> Path | None:
    snapshot = snapshot_label_for(path)
    matches = sorted(path.parent.glob(f"{snapshot}__*-metadata.json"))
    return matches[-1] if matches else None


def read_rows(path: Path) -> Iterable[dict[str, str]]:
    if path.suffix == ".gz":
        handle = gzip.open(path, "rt", encoding="utf-8-sig", newline="")
    else:
        handle = path.open("r", encoding="utf-8-sig", newline="")
    with handle:
        yield from csv.DictReader(handle)


def open_text_data(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8-sig", newline="")
    return path.open("r", encoding="utf-8-sig", newline="")


def upsert_source_system(conn: psycopg.Connection, code: str, name: str, base_url: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into meta.source_system (code, name, base_url)
            values (%s, %s, %s)
            on conflict (code) do update
              set name = excluded.name,
                  base_url = excluded.base_url
            returning source_system_id
            """,
            (code, name, base_url),
        )
        return int(cur.fetchone()[0])


def upsert_dataset_release(
    conn: psycopg.Connection,
    *,
    source_system_id: int,
    dataset_code: str,
    snapshot_label: str,
    source_url: str | None,
    local_path: Path,
    metadata: dict,
    content_sha256: str | None,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into meta.dataset_release (
              source_system_id,
              domain_code,
              dataset_code,
              snapshot_label,
              source_url,
              local_path,
              content_sha256,
              row_count,
              metadata,
              status
            )
            values (%s, 'health', %s, %s, %s, %s, %s, 0, %s, 'staged')
            on conflict (domain_code, dataset_code, snapshot_label) do update
              set source_url = excluded.source_url,
                  local_path = excluded.local_path,
                  content_sha256 = excluded.content_sha256,
                  metadata = excluded.metadata,
                  status = excluded.status
            returning dataset_release_id
            """,
            (
                source_system_id,
                dataset_code,
                snapshot_label,
                source_url,
                str(local_path.relative_to(ROOT)),
                content_sha256,
                Jsonb(metadata),
            ),
        )
        return int(cur.fetchone()[0])


def finalize_dataset_release(conn: psycopg.Connection, *, dataset_release_id: int, row_count: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update meta.dataset_release
            set row_count = %s,
                status = 'staged'
            where dataset_release_id = %s
            """,
            (row_count, dataset_release_id),
        )


def copy_cell(value: object) -> object:
    if value is None:
        return r"\N"
    if isinstance(value, Jsonb):
        return json.dumps(value.obj, ensure_ascii=False)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, date):
        return value.isoformat()
    return value


def copy_rows(cur: psycopg.Cursor, table: str, columns: list[str], rows: Iterable[tuple], chunk_size: int = 10000) -> int:
    total = 0
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    copy_sql = f"copy {table} ({', '.join(columns)}) from stdin with (format csv, null '\\N')"
    with cur.copy(copy_sql) as copy:
        for row in rows:
            writer.writerow([copy_cell(value) for value in row])
            total += 1
            if total % chunk_size == 0:
                copy.write(buffer.getvalue())
                buffer.seek(0)
                buffer.truncate(0)
        if buffer.tell():
            copy.write(buffer.getvalue())
    return total


def stream_source_csv(cur: psycopg.Cursor, copy_sql: str, path: Path, chunk_size: int = 8 * 1024 * 1024) -> None:
    if path.suffix == ".gz":
        proc = subprocess.Popen(
            ["gzip", "-cd", str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert proc.stdout is not None
        with proc.stdout as handle:
            with cur.copy(copy_sql) as copy:
                while True:
                    chunk = handle.read(chunk_size)
                    if not chunk:
                        break
                    copy.write(chunk)
        stderr = proc.stderr.read() if proc.stderr is not None else ""
        code = proc.wait()
        if code != 0:
            raise subprocess.CalledProcessError(code, proc.args, stderr=stderr)
        return

    with open_text_data(path) as handle:
        with cur.copy(copy_sql) as copy:
            while True:
                chunk = handle.read(chunk_size)
                if not chunk:
                    break
                copy.write(chunk)


def provider_site_rows(dataset_release_id: int, path: Path, rows: Iterable[dict[str, str]]) -> Iterable[tuple]:
    snapshot_date = parse_date(snapshot_label_for(path))
    payload = Jsonb({"local_path": str(path.relative_to(ROOT))})
    for row in rows:
        street_parts = [row.get("ZZ_ulice", "").strip(), row.get("ZZ_cislo_domovni_orientacni", "").strip()]
        street = " ".join(part for part in street_parts if part) or None
        yield (
            dataset_release_id,
            snapshot_date,
            row.get("poskytovatel_ICO") or None,
            parse_int(row.get("ZZ_ID")),
            row.get("ZZ_kod") or None,
            row.get("PCZ") or None,
            row.get("PCDP") or None,
            row.get("ZZ_nazev") or None,
            row.get("ZZ_druh_kod") or None,
            row.get("ZZ_druh_nazev") or None,
            row.get("ZZ_kraj_kod") or None,
            row.get("ZZ_kraj_nazev") or None,
            row.get("ZZ_obec") or None,
            street,
            row.get("ZZ_RUIAN_kod") or None,
            parse_date(row.get("ZZ_datum_zahajeni_cinnosti")),
            row.get("ZZ_obor_pece") or None,
            row.get("ZZ_forma_pece") or None,
            row.get("ZZ_druh_pece") or None,
            row.get("poskytovatel_nazev") or None,
            row.get("poskytovatel_druh") or None,
            row.get("poskytovatel_pravni_forma_nazev") or None,
            row.get("poskytovatel_sidlo_kraj_nazev") or None,
            row.get("poskytovatel_sidlo_obec") or None,
            row.get("poskytovatel_email") or None,
            row.get("poskytovatel_web") or None,
            row.get("zrizovatel_typ") or None,
            payload,
        )


def provider_specialty_rows(dataset_release_id: int, path: Path, rows: Iterable[dict[str, str]]) -> Iterable[tuple]:
    payload = Jsonb({"local_path": str(path.relative_to(ROOT))})
    for row in rows:
        yield (
            dataset_release_id,
            int(row["rok"]),
            int(row["mesic"]),
            row["kod"],
            row.get("nazev") or None,
            row.get("ICZ") or None,
            row.get("odbornost") or None,
            int(row["mnozstvi"]),
            payload,
        )


def payer_rows(dataset_release_id: int, path: Path, rows: Iterable[dict[str, str]]) -> Iterable[tuple]:
    payload = Jsonb({"local_path": str(path.relative_to(ROOT))})
    for row in rows:
        yield (
            dataset_release_id,
            int(row["rok"]),
            int(row["mesic"]),
            row["kod"],
            row.get("nazev") or None,
            row["pojistovna"],
            int(row["mnozstvi"]),
            payload,
        )


def insurer_codebook_rows(dataset_release_id: int, path: Path, rows: Iterable[dict[str, str]]) -> Iterable[tuple]:
    payload = Jsonb({"local_path": str(path.relative_to(ROOT))})
    for row in rows:
        yield (
            dataset_release_id,
            parse_date(row.get("číselník_platnost_začátek_datum")),
            row.get("číselník_kód") or None,
            row["číselník_položka_kód"],
            row["číselník_položka_název_cs"],
            None,
            None,
            payload,
        )


def monitor_indicator_rows(dataset_release_id: int, path: Path, rows: Iterable[dict[str, str]]) -> Iterable[tuple]:
    payload_base = {"local_path": str(path.relative_to(ROOT))}
    for row in rows:
        payload = Jsonb(
            {
                **payload_base,
                "provider_name": row.get("provider_name") or None,
                "region_name": row.get("region_name") or None,
                "hospital_like": row.get("hospital_like") == "true",
                "public_health_like": row.get("public_health_like") == "true",
            }
        )
        yield (
            dataset_release_id,
            int(row["reporting_year"]),
            row["period_code"],
            row["provider_ico"],
            parse_decimal(row.get("revenues_czk")),
            parse_decimal(row.get("costs_czk")),
            parse_decimal(row.get("result_czk")),
            parse_decimal(row.get("assets_czk")),
            parse_decimal(row.get("receivables_czk")),
            parse_decimal(row.get("liabilities_czk")),
            parse_decimal(row.get("short_term_liabilities_czk")),
            parse_decimal(row.get("long_term_liabilities_czk")),
            parse_decimal(row.get("total_debt_czk")),
            row.get("source_url") or None,
            payload,
        )


def mz_budget_entity_rows(dataset_release_id: int, path: Path, rows: Iterable[dict[str, str]]) -> Iterable[tuple]:
    payload_base = {"local_path": str(path.relative_to(ROOT))}
    for row in rows:
        payload = Jsonb(
            {
                **payload_base,
                "entity_kind": row.get("entity_kind") or None,
                "region_name": row.get("region_name") or None,
            }
        )
        yield (
            dataset_release_id,
            int(row["reporting_year"]),
            row["period_code"],
            row["entity_ico"],
            row["entity_name"],
            row["entity_kind"],
            row["region_name"],
            parse_decimal(row.get("expenses_czk")),
            parse_decimal(row.get("costs_czk")),
            parse_decimal(row.get("revenues_czk")),
            parse_decimal(row.get("result_czk")),
            parse_decimal(row.get("assets_czk")),
            parse_decimal(row.get("receivables_czk")),
            parse_decimal(row.get("liabilities_czk")),
            row.get("source_url") or None,
            payload,
        )


def health_financing_aggregate_rows(dataset_release_id: int, path: Path, rows: Iterable[dict[str, str]]) -> Iterable[tuple]:
    payload_base = {"local_path": str(path.relative_to(ROOT))}
    for row in rows:
        payload = Jsonb(
            {
                **payload_base,
                "indicator_name": row.get("Ukazatel") or None,
                "territory_name": row.get("Území") or None,
                "territory_code": row.get("Uz0") or None,
            }
        )
        yield (
            dataset_release_id,
            int(row["Roky"]),
            row["HF.HFU1"],
            row["Typ financování zdravotní péče-Úroveň 1"],
            row.get("HF.HFU2") or None,
            row.get("Typ financování zdravotní péče-Úroveň 2") or None,
            row["HP.HPU1"],
            row["Poskytovatel zdravotní péče-Úroveň 1"],
            row.get("HP.HPU2") or None,
            row.get("Poskytovatel zdravotní péče-Úroveň 2") or None,
            parse_decimal(row.get("Hodnota")) * Decimal("1000000"),
            sidecar_for(path).get("source_url"),
            payload,
        )


def zzs_activity_aggregate_rows(dataset_release_id: int, path: Path, rows: Iterable[dict[str, str]]) -> Iterable[tuple]:
    payload = Jsonb({"local_path": str(path.relative_to(ROOT))})
    for row in rows:
        yield (
            dataset_release_id,
            int(row["reporting_year"]),
            row["indicator_code"],
            row["indicator_name"],
            parse_decimal(row.get("count_value")),
            row.get("source_url") or None,
            payload,
        )


def load_claims_provider_specialty(cur: psycopg.Cursor, dataset_release_id: int, path: Path) -> int:
    payload = Jsonb({"local_path": str(path.relative_to(ROOT))})
    cur.execute(
        """
        create temporary table _stage_health_claims_provider_specialty (
          rok text,
          mesic text,
          kod text,
          nazev text,
          icz text,
          odbornost text,
          mnozstvi text
        ) on commit drop
        """
    )
    stream_source_csv(
        cur,
        """
        copy _stage_health_claims_provider_specialty
          (rok, mesic, kod, nazev, icz, odbornost, mnozstvi)
        from stdin with (format csv, header true)
        """,
        path,
    )
    cur.execute(
        """
        insert into raw.health_claims_provider_monthly (
          dataset_release_id,
          reporting_year,
          reporting_month,
          icz,
          total_quantity,
          payload
        )
        select
          %s,
          rok::integer,
          mesic::integer,
          nullif(icz, ''),
          sum(mnozstvi::bigint),
          %s
        from _stage_health_claims_provider_specialty
        group by rok::integer, mesic::integer, nullif(icz, '')
        """,
        (dataset_release_id, payload),
    )
    return cur.rowcount


def load_claims_payer(cur: psycopg.Cursor, dataset_release_id: int, path: Path) -> int:
    payload = Jsonb({"local_path": str(path.relative_to(ROOT))})
    cur.execute(
        """
        create temporary table _stage_health_claims_payer (
          rok text,
          mesic text,
          kod text,
          nazev text,
          pojistovna text,
          mnozstvi text
        ) on commit drop
        """
    )
    stream_source_csv(
        cur,
        """
        copy _stage_health_claims_payer
          (rok, mesic, kod, nazev, pojistovna, mnozstvi)
        from stdin with (format csv, header true)
        """,
        path,
    )
    cur.execute(
        """
        insert into raw.health_claims_payer_monthly (
          dataset_release_id,
          reporting_year,
          reporting_month,
          payer_code,
          total_quantity,
          payload
        )
        select
          %s,
          rok::integer,
          mesic::integer,
          pojistovna,
          sum(mnozstvi::bigint),
          %s
        from _stage_health_claims_payer
        group by rok::integer, mesic::integer, pojistovna
        """,
        (dataset_release_id, payload),
    )
    return cur.rowcount


DATASETS = [
    {
        "dataset_code": "nrpzs_provider_sites",
        "source_code": "uzis",
        "table": "raw.health_provider_site",
        "columns": [
            "dataset_release_id",
            "snapshot_date",
            "provider_ico",
            "zz_id",
            "zz_kod",
            "pcz",
            "pcdp",
            "zz_name",
            "zz_type_code",
            "zz_type_name",
            "region_code",
            "region_name",
            "municipality",
            "street",
            "ruian_code",
            "started_on",
            "care_field",
            "care_form",
            "care_kind",
            "provider_name",
            "provider_type",
            "provider_legal_form_name",
            "provider_region_name",
            "provider_municipality",
            "provider_email",
            "provider_web",
            "founder_type",
            "payload",
        ],
        "build_rows": provider_site_rows,
    },
    {
        "dataset_code": "nrhzs_claims_provider_specialty",
        "source_code": "uzis",
        "table": "raw.health_claims_provider_monthly",
        "bulk_loader": load_claims_provider_specialty,
        "columns": [
            "dataset_release_id",
            "reporting_year",
            "reporting_month",
            "icz",
            "total_quantity",
            "payload",
        ],
        "build_rows": provider_specialty_rows,
    },
    {
        "dataset_code": "nrhzs_claims_payer",
        "source_code": "uzis",
        "table": "raw.health_claims_payer_monthly",
        "bulk_loader": load_claims_payer,
        "columns": [
            "dataset_release_id",
            "reporting_year",
            "reporting_month",
            "payer_code",
            "total_quantity",
            "payload",
        ],
        "build_rows": payer_rows,
    },
    {
        "dataset_code": "health_insurer_codebook",
        "source_code": "dia",
        "table": "raw.health_insurer_codebook",
        "columns": [
            "dataset_release_id",
            "effective_date",
            "codebook_code",
            "payer_code",
            "payer_name",
            "valid_from",
            "valid_to",
            "payload",
        ],
        "build_rows": insurer_codebook_rows,
    },
    {
        "dataset_code": "health_monitor_indicators",
        "source_code": "monitor_mf",
        "table": "raw.health_monitor_indicator",
        "columns": [
            "dataset_release_id",
            "reporting_year",
            "period_code",
            "provider_ico",
            "revenues_czk",
            "costs_czk",
            "result_czk",
            "assets_czk",
            "receivables_czk",
            "liabilities_czk",
            "short_term_liabilities_czk",
            "long_term_liabilities_czk",
            "total_debt_czk",
            "source_url",
            "payload",
        ],
        "build_rows": monitor_indicator_rows,
    },
    {
        "dataset_code": "health_mz_budget_entities",
        "source_code": "monitor_mf",
        "table": "raw.health_mz_budget_entity",
        "columns": [
            "dataset_release_id",
            "reporting_year",
            "period_code",
            "entity_ico",
            "entity_name",
            "entity_kind",
            "region_name",
            "expenses_czk",
            "costs_czk",
            "revenues_czk",
            "result_czk",
            "assets_czk",
            "receivables_czk",
            "liabilities_czk",
            "source_url",
            "payload",
        ],
        "build_rows": mz_budget_entity_rows,
    },
    {
        "dataset_code": "health_financing_aggregates",
        "source_code": "czso",
        "table": "raw.health_financing_aggregate",
        "columns": [
            "dataset_release_id",
            "reporting_year",
            "financing_type_code",
            "financing_type_name",
            "financing_subtype_code",
            "financing_subtype_name",
            "provider_type_code",
            "provider_type_name",
            "provider_subtype_code",
            "provider_subtype_name",
            "amount_czk",
            "source_url",
            "payload",
        ],
        "build_rows": health_financing_aggregate_rows,
    },
    {
        "dataset_code": "health_zzs_activity_aggregates",
        "source_code": "uzis",
        "table": "raw.health_zzs_activity_aggregate",
        "columns": [
            "dataset_release_id",
            "reporting_year",
            "indicator_code",
            "indicator_name",
            "count_value",
            "source_url",
            "payload",
        ],
        "build_rows": zzs_activity_aggregate_rows,
    },
]


def load_dataset(conn: psycopg.Connection, *, config: dict[str, object]) -> int:
    dataset_code = str(config["dataset_code"])
    path = latest_data_path(dataset_code)
    if path is None:
        return 0

    snapshot_label = snapshot_label_for(path)
    download_sidecar = sidecar_for(path)
    metadata_path = metadata_path_for(path)
    source_code = str(config["source_code"])
    source_name, source_base_url = SOURCES[source_code]
    source_system_id = upsert_source_system(conn, source_code, source_name, source_base_url)

    metadata = {"loader": "etl/load_health_raw.py"}
    if metadata_path is not None:
        metadata["metadata_path"] = str(metadata_path.relative_to(ROOT))
    if download_sidecar.get("metadata_url"):
        metadata["metadata_url"] = download_sidecar["metadata_url"]

    dataset_release_id = upsert_dataset_release(
        conn,
        source_system_id=source_system_id,
        dataset_code=dataset_code,
        snapshot_label=snapshot_label,
        source_url=download_sidecar.get("source_url"),
        local_path=path,
        metadata=metadata,
        content_sha256=download_sidecar.get("sha256"),
    )

    with conn.cursor() as cur:
        cur.execute(f"delete from {config['table']} where dataset_release_id = %s", (dataset_release_id,))
        if config.get("bulk_loader"):
            row_count = config["bulk_loader"](cur, dataset_release_id, path)
        else:
            row_iter = config["build_rows"](dataset_release_id, path, read_rows(path))
            row_count = copy_rows(cur, str(config["table"]), list(config["columns"]), row_iter)
    finalize_dataset_release(conn, dataset_release_id=dataset_release_id, row_count=row_count)
    return row_count


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    selected = set(args.dataset or [])
    datasets = [d for d in DATASETS if not selected or d["dataset_code"] in selected]

    with psycopg.connect(args.database_url, autocommit=False) as conn:
        total_rows = 0
        for dataset in datasets:
            count = load_dataset(conn, config=dataset)
            conn.commit()
            total_rows += count
            print(f"{dataset['dataset_code']}: {count} rows", flush=True)
    print(f"Loaded {total_rows} raw health rows")


if __name__ == "__main__":
    main()
