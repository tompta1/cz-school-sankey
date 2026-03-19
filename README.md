# Czech School Finance — Interactive Sankey Explorer

An open-source, fully static interactive visualisation of how public money flows through the Czech school system — from the state budget through MŠMT (Ministry of Education) and regional founders down to individual schools and cost categories. Designed for transparency, reproducibility, and zero-backend hosting on GitHub Pages.

**Live demo:** [tompt.github.io/cz-school-sankey](https://tompt.github.io/cz-school-sankey)

---

## What you can explore

- **State → Ministry → Regions → Founders → Schools → Cost buckets** — six-level drilldown Sankey
- **EU co-financing overlay** — DotaceEU grants aggregated at every hierarchy level
- **Year-over-year ghost comparison** — toggle the previous year as a translucent background to spot changes
- **Sliding window navigation** — paginate through large lists of schools or founders without losing context
- **School search** — fuzzy diacritic-insensitive search across 8 000+ institutions
- **CZK amounts** on every node and link hover

---

## Data sources

| Source | What it provides | Coverage |
|---|---|---|
| **MŠMT "Podrobný rozpis rozpočtu" XLSX** | Per-school MŠMT allocation broken down into: pedagogical salaries (by school type and tariff band), non-pedagogical salaries, ONIV (other non-investment expenses), FKSP, insurance | Annual; downloaded from [msmt.cz](https://www.msmt.cz) |
| **DotaceEU "Seznam operací"** | EU structural-fund projects with beneficiary IČO and granted amount | Rolling; downloaded from [dotaceeu.cz](https://www.dotaceeu.cz) |
| **ARES (Administrativní registr ekonomických subjektů)** | Official legal name, municipality, and region for every IČO | On-demand REST API; results cached in `etl/data/ares_names.json` |

### Data quality notes

- 2024 MŠMT XLSX omits the `ICO_ZRIZ` (founder IČO) column — only a type code (`ZRIZ`) is present. The ETL resolves founders via a cross-year lookup from 2025 data; remaining kraj-funded schools fall back to region-name matching.
- EU grant amounts are matched to schools by IČO. Projects with no matching school IČO are silently dropped.
- ARES lookups are best-effort; if an IČO returns no record the school name falls back to `IČO XXXXXXXX`.

---

## ETL pipeline

```
etl/
├── parse_msmt_xlsx.py   # MŠMT XLSX → school_entities.csv + msmt_allocations.csv
├── fetch_eu_grants.py   # DotaceEU XLSX → eu_grants.csv (matched by IČO)
├── fetch_ares_names.py  # IČO list → ares_names.json (ARES REST API)
└── build_school_year.py # All CSVs + ares_names.json → public/data/sankey/<year>.json
```

### Cleanup and normalisation steps

1. **IČO normalisation** — all IČOs zero-padded to 8 digits before any join.
2. **Founder resolution** — 2025+ XLSXes contain `ICO_ZRIZ`; for older years the ETL cross-references a 2025 entity file, then falls back to KRAJ-name matching for `ZRIZ=7` (kraj-funded) schools.
3. **Salary disaggregation** — `PP_COL_NAMES` and `NPZ_COLS_NAMES` column lists sum tariff/above-tariff/adaptation columns across all school types (MŠ, ZŠ, ŠD, SŠ, KN, VOŠ, ZUŠ) into two buckets: pedagogical and non-pedagogical wages.
4. **EU match filter** — only EU projects with a beneficiary IČO that appears in the school entity list are included; unmatched projects are excluded to avoid inflating amounts.
5. **Sanity cross-check** — `parse_msmt_xlsx.py` prints a `NIV_CELKEM` vs reconstructed total diff; expected to be within rounding (< 1 000 CZK per school).

### Running the ETL locally

```bash
# 1. Place the MŠMT XLSX at etl/data/raw/2025/msmt_2025_raw.xlsx
# 2. Place the DotaceEU XLSX at etl/data/raw/2025/dotaceeu_2025_raw.xlsx

# Parse MŠMT (full country, all schools)
python3 etl/parse_msmt_xlsx.py --year 2025

# Fetch EU grants
python3 etl/fetch_eu_grants.py --year 2025

# Build the JSON (also calls ARES for any unknown IČOs)
python3 etl/build_school_year.py --year 2025
```

To run with the bundled demo fixture (no raw XLSXes needed):

```bash
npm run etl:demo   # loads pre-built 2025 JSON fixture
```

---

## Development

### Prerequisites

- Node.js ≥ 22
- Python ≥ 3.11 with `openpyxl`, `requests`

### Quick start

```bash
npm install
npm run etl:demo   # hydrate public/data/ from the demo fixture
npm run dev        # Vite dev server at http://localhost:5173
```

### Tests

```bash
npm test           # vitest run (all unit tests)
npm run test:watch # watch mode
```

The test suite covers:
- `filterGraph` — certainty, threshold, flow-view filters, totals
- `aggregateGraph` — region aggregation, EU programme → region links
- `drillRegion` — founder aggregation, EU flows, sliding-window overflow, offset navigation
- `drillFounder` — school aggregation, EU flows, sliding-window overflow, offset navigation, duplicate-name safety
- `drillIntoNode` — routing to the correct drill function for each node category

### Build & deploy

```bash
npm run build      # tsc + vite build → dist/
```

GitHub Pages deployment is automated via `.github/workflows/deploy-pages.yml` on every push to `main`.

---

## Architecture

```
src/
├── App.tsx                   # Top-level state: dataset, drilldown stack, year toggle, search
├── components/
│   └── SankeyChartCard.tsx   # ECharts Sankey with ghost (prev-year) overlay
├── lib/
│   ├── graph.ts              # Pure graph transforms: filterGraph, aggregateGraph, drillRegion, drillFounder
│   ├── format.ts             # CZK compact formatter
│   └── __tests__/
│       └── graph.test.ts     # Vitest unit tests
└── types.ts                  # Shared TypeScript interfaces
```

The UI is intentionally thin: all graph logic lives in pure functions in `graph.ts` that can be tested in isolation without a browser or ECharts.

---

## Licence

MIT — see [LICENSE](LICENSE).
