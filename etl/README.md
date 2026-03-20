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

One row per founder-to-school pass-through.

| column | required | note |
| --- | --- | --- |
| `institution_id` or `ico` | yes | join key |
| `amount` | yes | CZK |
| `basis` | no | defaults to `budgeted` |
| `certainty` | no | `observed` (FIN 2-01 PO) or `inferred` (FIN 2-12 M pro-rated) |
| `note` | no | explains source dataset and reconstruction method |

This file is produced by `fetch_founder_budgets.py` (see below).

## Suggested manual process

- Use IČO as the primary key whenever possible.
- Keep a small notebook or markdown note explaining how each inferred founder edge was reconstructed.
- Treat annual regime changes as schema events; for example, 2026 should be modeled separately from 2025 because founder responsibilities changed.

## Command examples

```bash
# Parse MŠMT XLSX into school_entities.csv and msmt_allocations.csv
python3 etl/parse_msmt_xlsx.py --year 2025

# Fetch founder budget transfers from MONITOR and produce founder_support.csv
python3 etl/fetch_founder_budgets.py --year 2025

# Inspect actual column headers (run this first if detection fails)
python3 etl/fetch_founder_budgets.py --year 2025 --list-columns

# Use a specific MONITOR period (YYYYMM)
python3 etl/fetch_founder_budgets.py --year 2025 --period 202512

# Use locally downloaded CSV files (skips network download)
python3 etl/fetch_founder_budgets.py --year 2025 \
    --fin12m path/to/fin2-12m.csv \
    --finpo  path/to/fin2-01po.csv

# Skip the per-school FIN 2-01 PO pass (produce only inferred aggregate rows)
python3 etl/fetch_founder_budgets.py --year 2025 --no-po
```

### Founder budget pipeline notes

`fetch_founder_budgets.py` runs two passes against MONITOR national extracts:

**Pass 1 — FIN 2-01 PO** (school-level, observed):
Downloads the FIN 2-01 PO national extract, filters for school IČOs in
`school_entities.csv`, and extracts accounts 672/673
("přijaté příspěvky/transfery od zřizovatele"). Rows produced this way are
marked `certainty=observed, basis=realized`.

**Pass 2 — FIN 2-12 M** (founder aggregate, inferred):
Downloads the FIN 2-12 M national extract, filters for founder IČOs and
education paragraphs §3100–§3299 with transfer items 5331/5336/6351
(neinvestiční/investiční příspěvky zřízeným PO). The per-founder total is
pro-rated across that founder's schools weighted by their MŠMT allocation
share. These rows are marked `certainty=inferred, basis=realized`.

Downloaded ZIPs are cached under `etl/data/monitor_cache/`. Re-run with
`--no-cache` to force a fresh download.

If column detection fails, run with `--list-columns` to print the actual
headers and update the `*_COLS_*` lists at the top of the script.

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
