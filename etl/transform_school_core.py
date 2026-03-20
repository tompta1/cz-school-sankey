#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform raw school tables into core warehouse tables")
    parser.add_argument("--year", type=int, required=True, help="Budget year already loaded into raw tables")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres connection string. Defaults to DATABASE_URL.",
    )
    return parser.parse_args()


def ensure_reporting_period(conn: psycopg.Connection, year: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into core.reporting_period (
              domain_code,
              calendar_year,
              period_code,
              period_start,
              period_end,
              is_final
            )
            values (%s, %s, %s, %s::date, %s::date, true)
            on conflict (period_code) do update
              set domain_code = excluded.domain_code,
                  calendar_year = excluded.calendar_year,
                  period_start = excluded.period_start,
                  period_end = excluded.period_end,
                  is_final = excluded.is_final
            returning reporting_period_id
            """,
            (
                "school",
                year,
                f"school:{year}",
                f"{year}-01-01",
                f"{year}-12-31",
            ),
        )
        return int(cur.fetchone()[0])


class OrganizationStore:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn
        self.cache: dict[tuple[str, str], int] = {}
        self.records: dict[tuple[str, str], dict] = {}

    def register(
        self,
        *,
        organization_type: str,
        name: str,
        key: str,
        ico: str | None = None,
        region_name: str | None = None,
        municipality_name: str | None = None,
        attributes: dict | None = None,
    ) -> None:
        cache_key = (organization_type, key)
        if cache_key in self.records:
            return

        self.records[cache_key] = {
            "organization_type": organization_type,
            "name": name,
            "ico": ico,
            "region_name": region_name,
            "municipality_name": municipality_name,
            "attributes": {**(attributes or {}), "stable_key": key},
        }

    def persist(self) -> None:
        if not self.records:
            return

        org_types = sorted({organization_type for organization_type, _ in self.records})
        with self.conn.cursor() as cur:
            cur.execute(
                """
                select organization_id, organization_type, attributes ->> 'stable_key' as stable_key
                from core.organization
                where organization_type = any(%s)
                  and attributes ? 'stable_key'
                """,
                (org_types,),
            )
            for organization_id, organization_type, stable_key in cur.fetchall():
                cache_key = (organization_type, stable_key)
                if cache_key in self.records:
                    self.cache[cache_key] = int(organization_id)

        update_rows: list[tuple] = []
        insert_rows: list[tuple] = []
        for cache_key, record in self.records.items():
            row = (
                record["name"],
                record["name"],
                record["ico"],
                record["region_name"],
                record["municipality_name"],
                Jsonb(record["attributes"]),
            )
            organization_id = self.cache.get(cache_key)
            if organization_id:
                update_rows.append((*row, organization_id))
            else:
                insert_rows.append((record["organization_type"], *row))

        with self.conn.cursor() as cur:
            if update_rows:
                cur.executemany(
                    """
                    update core.organization
                    set name = %s,
                        canonical_name = %s,
                        ico = coalesce(%s, ico),
                        region_name = coalesce(%s, region_name),
                        municipality_name = coalesce(%s, municipality_name),
                        attributes = %s
                    where organization_id = %s
                    """,
                    update_rows,
                )
            if insert_rows:
                cur.executemany(
                    """
                    insert into core.organization (
                      organization_type,
                      name,
                      canonical_name,
                      ico,
                      region_name,
                      municipality_name,
                      attributes
                    )
                    values (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    insert_rows,
                )

        with self.conn.cursor() as cur:
            cur.execute(
                """
                select organization_id, organization_type, attributes ->> 'stable_key' as stable_key
                from core.organization
                where organization_type = any(%s)
                  and attributes ? 'stable_key'
                """,
                (org_types,),
            )
            self.cache = {
                (organization_type, stable_key): int(organization_id)
                for organization_id, organization_type, stable_key in cur.fetchall()
                if (organization_type, stable_key) in self.records
            }

    def get_id(self, *, organization_type: str, key: str) -> int:
        return self.cache[(organization_type, key)]


def clear_existing_period(conn: psycopg.Connection, *, reporting_period_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "delete from core.financial_flow where budget_domain = 'school' and reporting_period_id = %s",
            (reporting_period_id,),
        )
        cur.execute(
            "delete from core.school_capacity where reporting_period_id = %s",
            (reporting_period_id,),
        )


def fetch_school_entities(conn: psycopg.Connection, year: int) -> list[dict]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            select dataset_release_id, institution_id, institution_name, ico, founder_id, founder_name, founder_type,
                   municipality, region, capacity
            from raw.school_entities
            where reporting_year = %s
            order by institution_id
            """,
            (year,),
        )
        return list(cur.fetchall())


def fetch_school_allocations(conn: psycopg.Connection, year: int) -> list[dict]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            select dataset_release_id, institution_id, ico, pedagogical_amount, nonpedagogical_amount, oniv_amount,
                   other_amount, operations_amount, investment_amount,
                   bucket_basis, bucket_certainty
            from raw.school_allocations
            where reporting_year = %s
            order by institution_id nulls last, ico nulls last
            """,
            (year,),
        )
        return list(cur.fetchall())


def fetch_eu_projects(conn: psycopg.Connection, year: int) -> list[dict]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            select dataset_release_id, institution_id, ico, programme, project_name, amount_czk, basis, certainty
            from raw.school_eu_projects
            where reporting_year = %s
            order by institution_id nulls last, programme, project_name
            """,
            (year,),
        )
        return list(cur.fetchall())


def fetch_founder_support(conn: psycopg.Connection, year: int) -> list[dict]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            select dataset_release_id, institution_id, ico, amount_czk, basis, certainty, note
            from raw.school_founder_support
            where reporting_year = %s
            order by institution_id nulls last
            """,
            (year,),
        )
        return list(cur.fetchall())


def fetch_state_budget(conn: psycopg.Connection, year: int) -> list[dict]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            select dataset_release_id, node_id, node_name, node_category, flow_type, amount_czk, basis, certainty, source_url
            from raw.school_state_budget
            where reporting_year = %s
            order by node_id
            """,
            (year,),
        )
        return list(cur.fetchall())


def insert_school_capacity(
    conn: psycopg.Connection,
    *,
    reporting_period_id: int,
    school_entities: list[dict],
    school_org_by_inst: dict[str, int],
) -> None:
    rows = [
        (
            reporting_period_id,
            int(row["dataset_release_id"]),
            school_org_by_inst[row["institution_id"]],
            int(row["capacity"]),
        )
        for row in school_entities
        if row.get("capacity") is not None and row.get("dataset_release_id") is not None
    ]
    if not rows:
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into core.school_capacity (
              reporting_period_id,
              dataset_release_id,
              school_organization_id,
              capacity
            )
            values (%s, %s, %s, %s)
            """,
            rows,
        )


def insert_financial_flows(
    conn: psycopg.Connection,
    *,
    reporting_period_id: int,
    year: int,
    state_org_id: int,
    ministry_org_id: int,
    school_org_by_inst: dict[str, int],
    founder_org_by_inst: dict[str, int],
    programme_org_by_name: dict[str, int],
    other_org_by_node_id: dict[str, int],
    allocations: list[dict],
    eu_projects: list[dict],
    founder_support_rows: list[dict],
    state_budget_rows: list[dict],
) -> None:
    direct_rows: list[tuple] = []
    spend_rows: list[tuple] = []
    founder_rows: list[tuple] = []
    eu_rows: list[tuple] = []
    state_rows: list[tuple] = []

    direct_total = 0
    for row in allocations:
        school_org_id = school_org_by_inst.get(row.get("institution_id") or "")
        if not school_org_id:
            continue

        total = (
            int(row["pedagogical_amount"])
            + int(row["nonpedagogical_amount"])
            + int(row["oniv_amount"])
            + int(row["other_amount"])
        )
        direct_total += total
        direct_rows.append(
            (
                "school",
                reporting_period_id,
                int(row["dataset_release_id"]),
                ministry_org_id,
                school_org_id,
                None,
                "direct_school_finance",
                "allocated",
                "observed",
                None,
                total,
                None,
                None,
                None,
                None,
                Jsonb({"year": year}),
            )
        )

        for bucket_code, amount in (
            ("pedagogical", int(row["pedagogical_amount"])),
            ("nonpedagogical", int(row["nonpedagogical_amount"])),
            ("oniv", int(row["oniv_amount"])),
            ("other", int(row["other_amount"])),
            ("operations", int(row["operations_amount"])),
            ("investment", int(row["investment_amount"])),
        ):
            if amount <= 0:
                continue
            spend_rows.append(
                (
                    "school",
                    reporting_period_id,
                    int(row["dataset_release_id"]),
                    school_org_id,
                    None,
                    None,
                    "school_expenditure",
                    row.get("bucket_basis") or "allocated",
                    row.get("bucket_certainty") or "observed",
                    bucket_code,
                    amount,
                    None,
                    None,
                    None,
                    None,
                    Jsonb({"year": year, "institution_id": row.get("institution_id")}),
                )
            )

    for row in founder_support_rows:
        school_org_id = school_org_by_inst.get(row.get("institution_id") or "")
        founder_org_id = founder_org_by_inst.get(row.get("institution_id") or "")
        if not school_org_id or not founder_org_id:
            continue
        founder_rows.append(
            (
                "school",
                reporting_period_id,
                int(row["dataset_release_id"]),
                founder_org_id,
                school_org_id,
                None,
                "founder_support",
                row.get("basis") or "realized",
                row.get("certainty") or "observed",
                None,
                int(row["amount_czk"]),
                None,
                None,
                row.get("note"),
                None,
                Jsonb({"year": year, "institution_id": row.get("institution_id")}),
            )
        )

    for row in eu_projects:
        school_org_id = school_org_by_inst.get(row.get("institution_id") or "")
        programme_org_id = programme_org_by_name.get(row["programme"])
        if not school_org_id or not programme_org_id:
            continue
        eu_rows.append(
            (
                "school",
                reporting_period_id,
                int(row["dataset_release_id"]),
                programme_org_id,
                school_org_id,
                None,
                "eu_project_support",
                row.get("basis") or "allocated",
                row.get("certainty") or "observed",
                None,
                int(row["amount_czk"]),
                None,
                None,
                row.get("project_name"),
                None,
                Jsonb({"year": year, "institution_id": row.get("institution_id"), "project_name": row.get("project_name")}),
            )
        )

    for row in state_budget_rows:
        other_org_id = other_org_by_node_id[row["node_id"]]
        if row["flow_type"] == "state_revenue":
            source_org_id = other_org_id
            target_org_id = state_org_id
        else:
            source_org_id = state_org_id
            target_org_id = other_org_id

        state_rows.append(
            (
                "school",
                reporting_period_id,
                int(row["dataset_release_id"]),
                source_org_id,
                target_org_id,
                None,
                row["flow_type"],
                row.get("basis") or "allocated",
                row.get("certainty") or "observed",
                None,
                int(row["amount_czk"]),
                None,
                None,
                None,
                row.get("source_url"),
                Jsonb({"year": year, "node_id": row["node_id"]}),
            )
        )

    state_rows.append(
        (
            "school",
            reporting_period_id,
            int(allocations[0]["dataset_release_id"]) if allocations else None,
            state_org_id,
            ministry_org_id,
            None,
            "state_to_ministry",
            "allocated",
            "observed",
            None,
            direct_total,
            None,
            None,
            "Roll-up from school allocations",
            None,
            Jsonb({"year": year}),
        )
    )

    with conn.cursor() as cur:
        insert_sql = """
            insert into core.financial_flow (
              budget_domain,
              reporting_period_id,
              dataset_release_id,
              source_organization_id,
              target_organization_id,
              intermediary_organization_id,
              flow_type,
              basis,
              certainty,
              cost_bucket_code,
              amount_czk,
              quantity,
              unit,
              note,
              source_url,
              lineage
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.executemany(insert_sql, direct_rows)
        cur.executemany(insert_sql, spend_rows)
        cur.executemany(insert_sql, founder_rows)
        cur.executemany(insert_sql, eu_rows)
        cur.executemany(insert_sql, state_rows)


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise SystemExit("Missing --database-url or DATABASE_URL")

    with psycopg.connect(args.database_url, autocommit=False) as conn:
        reporting_period_id = ensure_reporting_period(conn, args.year)
        clear_existing_period(conn, reporting_period_id=reporting_period_id)

        school_entities = fetch_school_entities(conn, args.year)
        allocations = fetch_school_allocations(conn, args.year)
        eu_projects = fetch_eu_projects(conn, args.year)
        founder_support_rows = fetch_founder_support(conn, args.year)
        state_budget_rows = fetch_state_budget(conn, args.year)

        orgs = OrganizationStore(conn)
        orgs.register(
            organization_type="state",
            name="State budget",
            key="state:cr",
            attributes={"node_id": "state:cr", "budget_domain": "school"},
        )
        orgs.register(
            organization_type="ministry",
            name="MŠMT direct school finance",
            key="msmt",
            attributes={"node_id": "msmt", "budget_domain": "school"},
        )

        school_org_by_inst: dict[str, int] = {}
        founder_org_by_inst: dict[str, int] = {}
        programme_org_by_name: dict[str, int] = {}
        other_org_by_node_id: dict[str, int] = {}

        for row in school_entities:
            founder_type = row.get("founder_type") or "founder"
            founder_name = row.get("founder_name") or "Unknown founder"
            founder_key = row.get("founder_id") or founder_name
            founder_ico = None
            if row.get("founder_id") and str(row["founder_id"]).startswith("founder:"):
                founder_ico = str(row["founder_id"]).removeprefix("founder:")
            founder_org_type = "region" if founder_type == "kraj" else "municipality"
            orgs.register(
                organization_type=founder_org_type,
                name=founder_name,
                key=founder_key,
                ico=founder_ico,
                region_name=row.get("region"),
                municipality_name=row.get("municipality"),
                attributes={
                    "founder_id": row.get("founder_id"),
                    "founder_type": founder_type,
                    "budget_domain": "school",
                },
            )

            orgs.register(
                organization_type="school_entity",
                name=row["institution_name"],
                key=row["institution_id"],
                ico=row.get("ico") or None,
                region_name=row.get("region"),
                municipality_name=row.get("municipality"),
                attributes={
                    "institution_id": row["institution_id"],
                    "founder_id": row.get("founder_id"),
                    "founder_type": founder_type,
                    "budget_domain": "school",
                },
            )

        for row in eu_projects:
            programme = row["programme"]
            if programme in programme_org_by_name:
                continue
            orgs.register(
                organization_type="eu_programme",
                name=programme,
                key=f"eu_programme:{programme}",
                attributes={"budget_domain": "school", "programme_name": programme},
            )

        for row in state_budget_rows:
            orgs.register(
                organization_type="other",
                name=row["node_name"],
                key=row["node_id"],
                attributes={"budget_domain": "school", "node_id": row["node_id"]},
            )

        orgs.persist()
        state_org_id = orgs.get_id(organization_type="state", key="state:cr")
        ministry_org_id = orgs.get_id(organization_type="ministry", key="msmt")

        for row in school_entities:
            founder_type = row.get("founder_type") or "founder"
            founder_name = row.get("founder_name") or "Unknown founder"
            founder_key = row.get("founder_id") or founder_name
            founder_org_type = "region" if founder_type == "kraj" else "municipality"
            founder_org_by_inst[row["institution_id"]] = orgs.get_id(
                organization_type=founder_org_type,
                key=founder_key,
            )
            school_org_by_inst[row["institution_id"]] = orgs.get_id(
                organization_type="school_entity",
                key=row["institution_id"],
            )

        for row in eu_projects:
            programme = row["programme"]
            if programme in programme_org_by_name:
                continue
            programme_org_by_name[programme] = orgs.get_id(
                organization_type="eu_programme",
                key=f"eu_programme:{programme}",
            )

        for row in state_budget_rows:
            other_org_by_node_id[row["node_id"]] = orgs.get_id(
                organization_type="other",
                key=row["node_id"],
            )

        insert_school_capacity(
            conn,
            reporting_period_id=reporting_period_id,
            school_entities=school_entities,
            school_org_by_inst=school_org_by_inst,
        )
        insert_financial_flows(
            conn,
            reporting_period_id=reporting_period_id,
            year=args.year,
            state_org_id=state_org_id,
            ministry_org_id=ministry_org_id,
            school_org_by_inst=school_org_by_inst,
            founder_org_by_inst=founder_org_by_inst,
            programme_org_by_name=programme_org_by_name,
            other_org_by_node_id=other_org_by_node_id,
            allocations=allocations,
            eu_projects=eu_projects,
            founder_support_rows=founder_support_rows,
            state_budget_rows=state_budget_rows,
        )
        conn.commit()

    print(f"Transformed school core data for {args.year}")


if __name__ == "__main__":
    main()
