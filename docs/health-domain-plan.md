# Health Domain Plan

## Goal

Add a health feature branch that can eventually show relationships between:

- Ministry of Health
- public health insurers
- providers and hospitals
- selected health-service activity or budgetary flows

The right first cut is not a full Sankey clone of the school app.

The open-data sources currently easiest to use expose provider registries and counts of reimbursed services, not a full transparent payment ledger from each insurer to each hospital. That means the first health release should be modeled as:

- provider and hospital directory
- insurer dimension
- provider and insurer activity views
- later, money flows when a strong source is confirmed

This is an inference from the official datasets listed below.

## Official datasets to anchor the health branch

### Phase 1 datasets

- NRPZS provider sites
  - purpose: provider and facility directory, hospital identification, geography, care type
  - official metadata: https://datanzis.uzis.gov.cz/data/NR-01-NRPZS/NR-01-06/Otevrena-data-NR-01-06-nrpzs-mista-poskytovani-zdravotnich-sluzeb.csv-metadata.json
- NRHZS claims by provider and specialty
  - purpose: activity volume by month, provider `ICZ`, service code, and specialty
  - official dataset page: https://data.gov.cz/datov%C3%A1-sada?iri=https%3A%2F%2Fdata.gov.cz%2Fzdroj%2Fdatov%C3%A9-sady%2F00024341%2Ff21d1f11eadd86ea57d47f54ca215cdb
  - direct metadata: https://datanzis.uzis.gov.cz/data/NR-04-NRHZS/NR-04-24/Otevrena-data-NR-04-24-vykony-rok-mesic-icz-odbornost.csv-metadata.json
- NRHZS claims by payer
  - purpose: activity volume by month and health insurer
  - official dataset page: https://data.gov.cz/datov%C3%A1-sada?iri=https%3A%2F%2Fdata.gov.cz%2Fzdroj%2Fdatov%C3%A9-sady%2F00024341%2Fdc996f5fcdd81a5a3fb47078a1c87fae
  - direct metadata: https://datanzis.uzis.gov.cz/data/NR-04-NRHZS/NR-04-23/Otevrena-data-NR-04-23-vykony-rok-mesic-zp.csv-metadata.json
- health insurer codebook
  - purpose: stable insurer dimension and code mapping
  - official dataset page: https://data.gov.cz/dataset?iri=https%3A%2F%2Fdata.gov.cz%2Fzdroj%2Fdatov%C3%A9-sady%2F17651921%2F225088f392e9377b3cec80e96b6eb409
  - direct CSV: https://rpp-opendata.egon.gov.cz/odrpp/datovasada/ciselnikyVdf/ciselnik_73_20240905.csv

### Phase 2 datasets

- acute inpatient cases by facility type
  - purpose: hospital case-mix and inpatient drilldowns
  - official dataset page: https://data.gov.cz/dataset?iri=https%3A%2F%2Fdata.gov.cz%2Fzdroj%2Fdatov%C3%A9-sady%2F00024341%2F7f80387bc2129751e11b92275cf97051

## Recommended first health product

Do this first:

1. provider directory
   - provider
   - facility
   - region
   - municipality
   - care forms
   - likely hospital classification
2. provider monthly activity
   - total reported services by provider `ICZ`
   - drill by specialty
   - drill by service code
3. insurer monthly activity
   - total reported services by payer code
4. provider versus insurer comparison views
   - not direct money flow yet
   - parallel activity views using common time axes

Do not promise this yet:

- exact insurer-to-hospital payment Sankey

That requires a reimbursement-amount source with payer-provider granularity, and the datasets inspected here do not expose that directly.

## Shared model with the school domain

The warehouse should share:

- `meta.source_system`
- `meta.dataset_release`
- `core.reporting_period`
- `core.organization`

The domain split should happen in facts:

- school
  - `core.financial_flow`
  - `core.school_capacity`
- health
  - `core.health_service_activity`
  - later possibly `core.financial_flow` if payment amounts are available

## Health organization model

Map health entities like this:

- ministry
  - `organization_type = ministry`
- insurer
  - `organization_type = health_insurer`
  - key: `payer_code`
- provider legal entity
  - `organization_type = health_provider`
  - key: `ico`
- facility or site
  - `organization_type = health_facility`
  - keys: `ICZ`, `ZZ_ID`, `ZZ_kod`

Relationship:

- provider legal entity can own multiple facilities
- one facility belongs to one provider
- facility belongs to a region and municipality

## Migration plan

### Stage 1

Move current school raw CSVs into Postgres landing tables.

Deliverables already added in this repo:

- warehouse schemas and tables under `db/`
- health ETL fetch scaffolding under `etl/health/`

### Stage 2

Populate conformed dimensions:

- school organizations
- ministry nodes
- founders
- EU programme and project nodes
- health insurers
- health providers
- health facilities

### Stage 3

Populate facts:

- school money flows into `core.financial_flow`
- health activity into `core.health_service_activity`

### Stage 4

Build Vercel API views:

- school overview and drilldowns
- health provider directory
- health provider monthly activity
- health insurer monthly activity

## Branch recommendation

Create a dedicated branch for implementation after this scaffold lands:

- `feature/health-warehouse`

Use it for:

- first DB loads
- first Vercel API handlers
- first health exploration UI

The current repo did not have such a branch yet when this note was written.
