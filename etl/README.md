# Local ETL contract

This starter keeps the pipeline deliberately simple:

1. Download machine-readable source files manually.
2. Normalize them into local CSV files under `etl/data/raw/<year>/`.
3. Run `python3 etl/build_school_year.py --year <year>`.
4. Commit the generated `public/data/sankey/<year>.json` for static hosting.

## Why this shape

For Czech school finance, the weak point is usually not charting. It is provenance and join quality. This ETL contract therefore stores certainty directly in the edge payload and keeps raw-source normalization outside the UI.

## Minimal input files

### `school_entities.csv`

One row per school legal entity.

| column | required | note |
| --- | --- | --- |
| `institution_id` | yes | stable internal ID such as `school:ico-70992967` |
| `institution_name` | yes | display name |
| `ico` | no | legal entity IČO, preferred join key |
| `founder_id` | no | stable node ID such as `obec:1234` or `kraj:stredocesky` |
| `founder_name` | no | display name |
| `founder_type` | no | `obec`, `kraj`, or other |
| `municipality` | no | display-only helper |
| `region` | no | display-only helper |

### `msmt_allocations.csv`

One row per school legal entity and year.

| column | required | note |
| --- | --- | --- |
| `institution_id` or `ico` | yes | join key |
| `pedagogical_amount` | yes | CZK |
| `nonpedagogical_amount` | yes | CZK |
| `oniv_amount` | yes | CZK |
| `other_amount` | no | extra MŠMT direct amounts |
| `operations_amount` | no | school-side operational bucket |
| `investment_amount` | no | school-side investment bucket |
| `bucket_basis` | no | defaults to `budgeted` |
| `bucket_certainty` | no | defaults to `observed` |

### `eu_projects.csv`

One row per project-to-school link.

| column | required | note |
| --- | --- | --- |
| `institution_id` or `ico` | yes | join key |
| `programme` | yes | e.g. `OP JAK` |
| `project_name` | yes | project label |
| `amount` | yes | CZK |
| `basis` | no | defaults to `allocated` |
| `certainty` | no | defaults to `observed` |

### `founder_support.csv`

One row per school with public-transfer revenue attributed to its registered
founder. VYKZZ account 672/673 provides the observed school amount; the source
attribution is inferred because the account can combine several public budgets.

| column | required | note |
| --- | --- | --- |
| `institution_id` or `ico` | yes | join key |
| `amount` | yes | CZK |
| `basis` | no | defaults to `budgeted` |
| `certainty` | no | `inferred`; VYKZZ does not identify the sender and FIN 2-12 M fallbacks are pro-rated |
| `note` | no | explains source dataset and reconstruction method |

This file is produced by `fetch_founder_budgets.py` (see below).

### `school_costs.csv`

One observed VYKZZ cost profile per public school, with the reported total and compact categories for materials, energy, repairs, services including rent, personnel, depreciation, and other net costs. It is a use-of-funds profile, not an additional funding source.

## Suggested manual process

- Use IČO as the primary key whenever possible.
- Keep a small notebook or markdown note explaining how each inferred founder edge was reconstructed.
- Treat annual regime changes as schema events; for example, 2026 should be modeled separately from 2025 because founder responsibilities changed.

## Command examples

```bash
# Parse MŠMT XLSX into school_entities.csv and msmt_allocations.csv
python3 etl/parse_msmt_xlsx.py --year 2025

# Fetch founder support and school cost profiles from MONITOR
python3 etl/fetch_founder_budgets.py --year 2025

# Inspect actual column headers (run this first if detection fails)
python3 etl/fetch_founder_budgets.py --year 2025 --list-columns

# Use a specific MONITOR period (YYYYMM)
python3 etl/fetch_founder_budgets.py --year 2025 --period 202512

# Use locally downloaded CSV files (skips network download)
python3 etl/fetch_founder_budgets.py --year 2025 \
    --fin12m path/to/fin2-12m.csv \
    --finpo  path/to/fin2-01po.csv

# Skip the per-school VYKZZ cost pass
python3 etl/fetch_founder_budgets.py --year 2025 --no-costs
```

### Founder budget pipeline notes

`fetch_founder_budgets.py` runs two nationwide passes against MONITOR extracts:

**Pass 1 — VYKZZ** (school-level amount, inferred sender):
Reads realized public-transfer revenue on accounts 672/673 for every school and
routes it through the municipality or region recorded as founder in the MŠMT
registry. The amount is observed, but the edge is marked `certainty=inferred`
because VYKZZ does not identify which public budget sent each part.

The VYKZZ pass also reads realized main-activity cost accounts. Account 502 gives
energy, 511 gives repairs and maintenance, and 518 contains services including
rent. The output is `school_costs.csv`.

**Pass 2 — FIN 2-12 M** (fallback):
For schools without VYKZZ transfer revenue, founder own-budget items 5331/6351
in education paragraphs are pro-rated by the schools' MŠMT allocation weights.

This restores broad coverage, but account 672 can overlap direct MŠMT funding.
The UI therefore presents the edge as inferred attribution, not a verified
founder-only payment.

Downloaded ZIPs are cached under `etl/data/monitor_cache/`. Re-run with
`--no-cache` to force a fresh download.

If column detection fails, run with `--list-columns` to print the actual
headers and update the `*_COLS_*` lists at the top of the script.

### State-budget envelope

`fetch_state_budget.py` downloads workbook G of the official MF state final
account and parses realized totals from tables 1 and 2a plus 14 ministry
chapter totals from table 7. It writes exact revenue classes, deficit financing,
total expenditure and chapter reconciliation metadata, and the residual after
the MŠMT direct-school allocation rollup.

```bash
python3 etl/fetch_state_budget.py --year 2025

# Refresh only the tiny state summary in Neon without rebuilding school detail.
python3 etl/load_school_raw.py --year 2025 \
    --dataset school_state_budget --database-url "$DATABASE_URL"
python3 etl/transform_school_core.py --year 2025 \
    --state-budget-only --database-url "$DATABASE_URL"
```

The `state_budget_total` CSV row is metadata used by DQ checks, not a graph
edge. The `state_to_other` row is inferred so that it plus the observed MŠMT
school rollup equals official expenditure. The API then reduces that residual
by other visible Atlas roots. Since some roots include state funds or regional
spending, the final residual must not be interpreted as an observed sum of all
unshown chapters.

```bash

# Fetch EU grants from DotaceEU and produce eu_projects.csv
python3 etl/fetch_eu_grants.py --year 2025

# Inspect actual XLSX column headers (run this first if matching fails)
python3 etl/fetch_eu_grants.py --year 2025 --list-columns

# Use a specific monthly snapshot instead of the default end-of-year one
python3 etl/fetch_eu_grants.py --year 2025 --snapshot 2025_12

# Use a locally downloaded XLSX (skips network download)
python3 etl/fetch_eu_grants.py --year 2025 --xlsx path/to/local.xlsx

# Build the Sankey JSON from all CSVs
python3 etl/build_school_year.py --year 2025
python3 etl/build_school_year.py --year 2025 --demo
```

### EU grants pipeline notes

`fetch_eu_grants.py` downloads the DotaceEU "Seznam operací" monthly XLSX for
the 2021–2027 programming period. Downloaded files are cached under
`etl/data/dotaceeu_cache/` — re-run with `--no-cache` to force a fresh
download.

The script joins by IČO (8-digit zero-padded) and produces one row per
project-to-school link. Amounts come from the "Celkové způsobilé výdaje"
column (total eligible expenditure), falling back to EU contribution. All
matched rows are marked `basis=allocated, certainty=observed`.

If column detection fails, run with `--list-columns` to print the actual
headers and update the `*_CANDIDATES` lists at the top of the script.
