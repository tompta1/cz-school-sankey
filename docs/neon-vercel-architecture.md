# Neon + Vercel Architecture Proposal

## Current repo model inferred from code

The repo already has a clear canonical model, even though it is exported as one large yearly JSON file.

Current normalized inputs per year under `etl/data/raw/<year>/`:

- `school_entities.csv`
  - one row per school legal entity
  - join key: `institution_id` and usually `ico`
  - carries founder, municipality, region, capacity
- `msmt_allocations.csv`
  - one row per school-year
  - direct MŠMT amounts split into `pedagogical`, `nonpedagogical`, `oniv`, `other`, `operations`, `investment`
- `eu_projects.csv`
  - one row per project-to-school link
  - has programme, project name, amount
- `founder_support.csv`
  - one row per founder-to-school transfer
  - carries `basis`, `certainty`, `note`
- `state_budget.csv`
  - small state-budget summary for revenues and residual non-MŠMT expenditure

Current yearly export shape in `public/data/sankey/<year>.json`:

- `nodes`
  - categories seen in 2025: `state`, `ministry`, `region`, `municipality`, `eu_programme`, `eu_project`, `school_entity`, `cost_bucket`, `other`
- `links`
  - flow types seen in 2025: `state_revenue`, `state_to_ministry`, `state_to_other`, `direct_school_finance`, `founder_support`, `eu_project_support`, `project_to_school`, `school_expenditure`
- `institutions`
  - school search and drill metadata
- `sources`
  - source provenance for the export

Observed 2025 scale from the repo:

- `school_entities.csv`: 7,985 rows
- `msmt_allocations.csv`: 7,985 rows
- `eu_projects.csv`: 15,020 rows
- `founder_support.csv`: 7,978 rows
- `public/data/sankey/2025.json`: 34 MB
- 2025 JSON contains 25,426 nodes and 77,939 links

Important modeling detail: the frontend is not using an arbitrary graph engine. It repeatedly computes a small set of views:

- top-level overview
- EU drill
- founder-type drill
- region drill
- founder drill
- school / bucket detail
- school search
- previous-year comparison

That means the backend should optimize for a handful of read shapes, not for generic graph traversal.

## Recommendation

Use one Neon Postgres database with three logical layers:

- `raw`
  - source-shaped landing tables, close to CSV/XLSX extracts
- `core`
  - conformed dimensions and canonical finance facts
- `mart`
  - UI-serving tables or materialized views for Sankey and future budget apps

Do not create separate databases per ministry/domain yet.

Use a logical multi-datamart design inside one database:

- one shared `core`
- multiple marts such as `mart_school_finance`, `mart_health_budget`, `mart_state_revenue`

This keeps joins and governance simple on the free tier, while still leaving room for other Czech state-budget domains later.

## Why this shape

### Keep ETL out of Vercel

The current ETL is file-heavy and source-specific:

- XLSX parsing
- MONITOR extract downloads
- ARES enrichment
- annual reconciliation logic

Vercel Hobby is a poor place to run that pipeline continuously. Vercel documents currently say Hobby cron jobs run at most once per day, with hourly precision, and cron jobs execute Vercel Functions subject to normal function limits. Hobby functions are currently limited to 60 seconds max duration. That is fine for a read API, not for reliable ETL.

Recommendation:

- Vercel hosts the read API only
- ETL runs outside Vercel
  - preferred: GitHub Actions with manual dispatch and scheduled refreshes
  - acceptable: localhost/manual annual rebuild

### Do not pull MONITOR on demand for user API calls

Do not hit MONITOR, DotaceEU, or ARES from end-user requests.

Reasons:

- source latency and availability become production risk
- free-tier usage becomes unpredictable
- source corrections would silently change user-visible results
- you lose reproducibility and auditability
- serverless cold starts plus external downloads will feel bad

Use source pulls only in ingestion jobs. Serve users from pre-loaded Neon tables and pre-aggregated marts.

## Target database model

### Schemas

- `raw`
- `core`
- `mart`
- `meta`

### `meta` schema

Use this for provenance and refresh tracking.

Suggested tables:

- `meta.source_system`
  - `source_system_id`
  - `code` such as `msmt`, `monitor_finm`, `monitor_vykzz`, `dotaceeu`, `ares`
  - `name`
  - `base_url`
- `meta.dataset_release`
  - `dataset_release_id`
  - `source_system_id`
  - `domain_code`
  - `reporting_year`
  - `period_code`
  - `snapshot_label`
  - `source_url`
  - `fetched_at`
  - `checksum`
  - `row_count`
  - `status`
  - `etl_version`
- `meta.etl_run`
  - `etl_run_id`
  - `started_at`
  - `finished_at`
  - `status`
  - `trigger_type`
  - `git_sha`
  - `notes`

### `raw` schema

Keep source-shaped landing tables. Do not over-model here.

Suggested tables:

- `raw.msmt_school_entity`
- `raw.msmt_allocation`
- `raw.dotaceeu_project`
- `raw.monitor_founder_transfer`
- `raw.monitor_state_budget`
- `raw.ares_lookup_cache`

Each raw table should include:

- source release reference
- original business keys
- parsed typed fields
- `payload jsonb` for source leftovers

### `core` schema

This is the long-lived warehouse layer.

Suggested dimensions:

- `core.dim_reporting_period`
  - `reporting_period_id`
  - `fiscal_year`
  - `period_code`
  - `period_end_date`
  - `is_final`
- `core.dim_budget_domain`
  - `budget_domain_id`
  - `code` such as `school`, `health`, `internal_affairs`, `environment`, `culture`
- `core.dim_organization`
  - `organization_id`
  - `organization_type`
    - `state`
    - `ministry`
    - `region`
    - `municipality`
    - `founder`
    - `school_entity`
    - `eu_programme`
    - `eu_project`
    - `other`
  - `name`
  - `canonical_name`
  - `ico`
  - `region_code`
  - `municipality_name`
  - `parent_organization_id`
  - `valid_from`
  - `valid_to`
  - `attributes jsonb`
- `core.dim_flow_type`
  - `flow_type_id`
  - `code`
  - `direction_class`
  - `ui_group`
- `core.dim_basis`
  - `basis_id`
  - `code` such as `allocated`, `budgeted`, `realized`
- `core.dim_certainty`
  - `certainty_id`
  - `code` such as `observed`, `inferred`
- `core.dim_cost_bucket`
  - `cost_bucket_id`
  - `code`
  - `name`
  - `sort_order`

Suggested canonical fact:

- `core.fact_financial_flow`
  - `financial_flow_id`
  - `budget_domain_id`
  - `reporting_period_id`
  - `dataset_release_id`
  - `source_organization_id`
  - `target_organization_id`
  - `flow_type_id`
  - `basis_id`
  - `certainty_id`
  - `cost_bucket_id null`
  - `amount_czk bigint`
  - `institution_organization_id null`
  - `note`
  - `source_url`
  - `lineage jsonb`
  - `created_at`

Suggested supporting fact:

- `core.fact_school_capacity`
  - `reporting_period_id`
  - `school_organization_id`
  - `capacity`
  - `dataset_release_id`

Why one generic flow fact:

- it matches the Sankey product directly
- it also supports future non-school domains
- it avoids making the API depend on domain-specific table names
- source-specific raw tables still preserve nuance

### `mart` schema

Do not let the frontend query `core` directly.

Create read-optimized marts:

- `mart.school_institution_search`
  - one row per school-year for search and labels
- `mart.school_sankey_node_year`
  - stable yearly node payloads
- `mart.school_sankey_link_year`
  - stable yearly edge payloads
- `mart.school_region_overview`
  - pre-aggregated region-level overview
- `mart.school_region_founder_drill`
  - founder totals per region
- `mart.school_founder_school_drill`
  - school totals per founder
- `mart.school_eu_region_drill`
  - programme-to-region aggregates
- `mart.school_year_summary`
  - metadata and source notes for a year

Materialize the expensive repeated views. The UI is deterministic enough that this is worth it.

## API design

Use Vercel serverless functions as a thin read layer over Neon.

Recommended endpoints:

- `GET /api/years`
  - list available years and statuses
- `GET /api/summary?year=2025`
  - title, subtitle, source notes, counts, refresh metadata
- `GET /api/search/institutions?year=2025&q=brno`
  - school search only
- `GET /api/graph/overview?year=2025`
  - current top-level Sankey
- `GET /api/graph/eu?year=2025`
  - EU programme to region drill
- `GET /api/graph/founders?year=2025&founderType=obec&offset=0`
  - founder-type drill
- `GET /api/graph/region?year=2025&region=Jihomoravsky&offset=0`
  - region drill
- `GET /api/graph/founder?year=2025&founderId=...&offset=0`
  - founder drill
- `GET /api/graph/node?year=2025&nodeId=school:60552255`
  - school or bucket detail
- `GET /api/compare?year=2025&previousYear=2024&view=overview`
  - optional if you want one round-trip for ghost overlays

Implementation rules:

- keep endpoints read-only
- query marts, not raw tables
- return almost the same shape as today: `nodes`, `links`, `institutions`, `sources`
- cache aggressively on Vercel because data changes rarely

## Refresh strategy

### Recommended cadence

Use hybrid refreshes, not a single universal cadence.

- MŠMT direct allocations
  - annual full refresh
  - trigger after official annual XLSX is available
- founder support from MONITOR
  - annual full refresh after December data is stable
- state budget summary
  - annual full refresh after December data is stable
- EU grants
  - optional monthly refresh while a year is active
  - freeze the annual snapshot once you publish the year
- ARES names
  - on ingestion only, cached

### Operational recommendation

For now:

- keep ETL manual or GitHub-Action-triggered
- publish one immutable annual release per year
- allow explicit republish only when source corrections are needed

This is much safer than trying to build a fully live public-finance ingestion product on the free tier.

### ETL job stages

Recommended pipeline:

1. `extract`
   - download XLSX/ZIP/CSV
   - compute checksums
   - register `meta.dataset_release`
2. `stage`
   - load `raw.*` tables
3. `transform`
   - resolve organizations and periods
   - build `core.fact_financial_flow`
   - build school capacities and search rows
4. `publish`
   - refresh `mart.*`
   - update `school_year_summary`
5. `verify`
   - row-count checks
   - sum reconciliation against current ETL expectations
   - duplicate key checks

## Versioning with Atlas

Atlas is a reasonable fit here.

Recommended layout:

- `db/atlas.hcl`
- `db/schema.sql` or `db/schema.hcl`
- `db/migrations/`

Workflow:

1. change desired schema locally
2. run `atlas migrate diff`
3. commit generated SQL migration
4. CI runs `atlas migrate lint`
5. deployment job runs `atlas migrate apply`

Important Atlas behavior to account for:

- Atlas records applied migrations in `atlas_schema_revisions`
- baseline the initial production schema explicitly
- do not allow ad hoc manual DDL in Neon outside migrations

Recommended environments:

- local docker Postgres for schema diffing
- one Neon `dev` branch
- one Neon `main` branch

On the current Neon docs, the Free plan includes 10 branches per project, 100 CU-hours per month, and 0.5 GB storage per project. Because of that, do not build a PR-preview database branch for every Vercel preview deployment on free tier. Keep branch usage disciplined.

## Neon-specific usage guidance

Neon is a good fit for this app because:

- serverless Postgres is fine for bursty read traffic
- pooled connections are built in
- branching helps with safe schema migration testing
- the dataset is still small in normalized relational form

But on free tier, assume constraints:

- keep only a few persistent branches
- keep marts compact
- avoid storing large duplicated JSON blobs per view
- precompute heavy joins once, not on every request

## What not to store

Do not store only the giant yearly JSON blobs in Neon.

That would move the current bottleneck into the database without improving anything.

Instead:

- store normalized facts and dimensions
- optionally store one compact published yearly JSON cache table for rollback/debugging
- serve UI responses from marts or prebuilt API payload tables

## Migration path from this repo

Phase 1:

- keep existing Python ETL
- load yearly CSV outputs into Neon
- create `core` and `mart`
- build read API on Vercel
- switch frontend from static file fetches to API fetches

Phase 2:

- move ETL outputs from CSV files to direct DB loads
- persist raw-source metadata and checksums
- add refresh audit tables

Phase 3:

- add second domain mart, for example state-budget chapter relationships beyond schools
- introduce `budget_domain` and shared organization dimension fully

## Concrete call

If the goal is a practical free-tier architecture, the best choice is:

- Neon for one relational warehouse database
- Vercel for a thin read API and frontend
- Atlas for schema evolution
- GitHub Actions or local runs for ETL
- annual immutable snapshots, with optional monthly EU refreshes while a year is open
- logical multi-datamart design inside one database, not multiple databases

That gets the oversized JSON out of the frontend without overbuilding a platform you do not need yet.

## References

- Neon pricing and plan docs: https://neon.com/pricing
- Neon plans doc: https://neon.com/docs/introduction/plans
- Vercel Hobby plan docs: https://vercel.com/docs/plans/hobby
- Vercel cron docs: https://vercel.com/docs/cron-jobs/usage-and-pricing
- Atlas versioned migration docs: https://atlasgo.io/versioned/apply
