# Database Workbench

This directory is the first warehouse scaffold for moving the project from static yearly JSON files to Neon Postgres.

## Files

- `schema.sql`
  - desired schema state for Atlas
- `migrations/20260320_000001_initial_warehouse.sql`
  - initial baseline migration
- `atlas.hcl`
  - local and Neon Atlas environments
- `apply_schema.py`
  - lightweight local schema applier using `psycopg`

## Scope of the initial warehouse

The schema covers two domains:

- `school`
  - current app data already present in the repo
- `health`
  - first provider, insurer, and hospital-adjacent datasets from MZ/UZIS

The layout is intentionally simple:

- `meta`
  - provenance and ETL releases
- `raw`
  - source-shaped landing tables
- `core`
  - conformed dimensions and facts
- `mart`
  - read-optimized views for the UI and APIs

## Suggested Atlas commands

If Atlas is installed:

```bash
atlas schema inspect --env local
atlas migrate lint --env local --latest 1
atlas migrate apply --env neon
```

If you want to generate future diffs:

```bash
atlas migrate diff --env local add_health_hospitalization_cases
```

## Local Podman database

This repo now includes host-Podman helpers for Silverblue or toolbox-based development:

```bash
bash ./scripts/dev-db-up.sh
bash ./scripts/dev-db-url.sh
bash ./scripts/dev-db-down.sh
```

Default local DSN:

```bash
postgresql://app:app@127.0.0.1:55432/cz_school_sankey
```

Apply the schema without Atlas:

```bash
python3 db/apply_schema.py --database-url "$(bash ./scripts/dev-db-url.sh)"
```

Load the existing school CSVs:

```bash
python3 etl/load_school_raw.py --year 2025 --database-url "$(bash ./scripts/dev-db-url.sh)"
```

Transform the school raw layer into `core.*`:

```bash
python3 etl/transform_school_core.py --year 2025 --database-url "$(bash ./scripts/dev-db-url.sh)"
```

## First migration plan

### School data

Load the current CSV outputs into:

- `raw.school_entities`
- `raw.school_allocations`
- `raw.school_eu_projects`
- `raw.school_founder_support`
- `raw.school_state_budget`

Then transform into:

- `core.organization`
- `core.reporting_period`
- `core.financial_flow`
- `core.school_capacity`

### Health data

Fetch and land:

- provider sites from NRPZS
- monthly claims counts by provider and specialty from NRHZS
- monthly claims counts by payer from NRHZS
- insurer codebook from DIA/RPP

Then transform into:

- `core.organization`
  - `health_provider`
  - `health_facility`
  - `health_insurer`
- `core.health_service_activity`

## Important modeling decision

The current school app is money-centric.

The first health slice is not purely money-centric, because the most accessible open datasets expose reported service quantities rather than reimbursement amounts. Because of that, the warehouse keeps:

- `core.financial_flow` for monetary flows
- `core.health_service_activity` for provider and payer activity volumes

If a reliable open reimbursement-amount dataset appears later, it can be added without changing the organization model.
