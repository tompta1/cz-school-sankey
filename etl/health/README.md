# Health ETL

This directory starts the raw-data fetch layer for the future health branch.

## Datasets

- `fetch_provider_sites.py`
  - NRPZS provider and facility directory
- `fetch_claims_by_provider_specialty.py`
  - NRHZS monthly service counts by provider `ICZ` and specialty
- `fetch_claims_by_provider_ico.py`
  - NRHZS yearly service counts by provider `IČO`
- `fetch_claims_by_payer.py`
  - NRHZS monthly service counts by health insurer
- `fetch_insurer_codebook.py`
  - official insurer codebook from DIA/RPP
- `fetch_monitor_indicators.py`
  - local pull of Monitor MF finance indicators for focused providers discovered in Neon
- `fetch_mz_budget_entities.py`
  - official Monitor MF pull for Ministry of Health chapter entities and KHS bodies
- `fetch_czso_health_financing.py`
  - CZSO healthcare accounts aggregate financing by financing source and provider type

## Output layout

Files are downloaded under:

`etl/data/raw/health/<dataset_code>/`

Each fetch writes:

- the source file
- optional metadata sidecar from the source publisher
- a local `.download.json` sidecar with URL, checksum, and timestamp

## Example commands

```bash
python3 etl/health/fetch_provider_sites.py
python3 etl/health/fetch_claims_by_provider_specialty.py
python3 etl/health/fetch_claims_by_provider_ico.py
python3 etl/health/fetch_claims_by_payer.py
python3 etl/health/fetch_insurer_codebook.py
python3 etl/health/fetch_monitor_indicators.py --database-url "$DATABASE_URL" --year 2024
python3 etl/health/fetch_mz_budget_entities.py --year 2024
python3 etl/health/fetch_czso_health_financing.py
```

To force a different snapshot label:

```bash
python3 etl/health/fetch_provider_sites.py --snapshot 20260320
```

To override the source URL:

```bash
python3 etl/health/fetch_claims_by_payer.py --url 'https://.../custom.csv.gz'
```
