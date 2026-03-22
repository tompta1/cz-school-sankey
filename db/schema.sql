create schema if not exists meta;
create schema if not exists raw;
create schema if not exists core;
create schema if not exists mart;

create table if not exists meta.source_system (
  source_system_id bigserial primary key,
  code text not null unique,
  name text not null,
  base_url text,
  default_license text,
  created_at timestamptz not null default now()
);

create table if not exists meta.dataset_release (
  dataset_release_id bigserial primary key,
  source_system_id bigint not null references meta.source_system(source_system_id),
  domain_code text not null,
  dataset_code text not null,
  reporting_year integer,
  period_code text,
  snapshot_label text not null,
  source_url text,
  local_path text,
  content_sha256 text,
  row_count bigint,
  metadata jsonb not null default '{}'::jsonb,
  fetched_at timestamptz not null default now(),
  published_at timestamptz,
  status text not null default 'fetched' check (status in ('fetched', 'staged', 'published', 'failed'))
);

create unique index if not exists dataset_release_identity_uidx
  on meta.dataset_release (domain_code, dataset_code, snapshot_label);

create table if not exists meta.etl_run (
  etl_run_id bigserial primary key,
  domain_code text not null,
  trigger_type text not null check (trigger_type in ('manual', 'scheduled', 'local', 'ci')),
  git_sha text,
  status text not null check (status in ('running', 'succeeded', 'failed')),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  notes text
);

create table if not exists core.reporting_period (
  reporting_period_id bigserial primary key,
  domain_code text not null,
  calendar_year integer not null,
  calendar_month integer,
  period_code text not null unique,
  period_start date,
  period_end date,
  is_final boolean not null default false,
  created_at timestamptz not null default now()
);

create table if not exists core.organization (
  organization_id bigserial primary key,
  organization_type text not null check (
    organization_type in (
      'state',
      'ministry',
      'region',
      'municipality',
      'founder',
      'school_entity',
      'eu_programme',
      'eu_project',
      'health_provider',
      'health_facility',
      'health_insurer',
      'other'
    )
  ),
  name text not null,
  canonical_name text,
  ico text,
  icz text,
  zz_id bigint,
  zz_kod text,
  payer_code text,
  region_code text,
  region_name text,
  municipality_name text,
  parent_organization_id bigint references core.organization(organization_id),
  source_system_id bigint references meta.source_system(source_system_id),
  valid_from date,
  valid_to date,
  attributes jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create unique index if not exists organization_type_ico_uidx
  on core.organization (organization_type, ico)
  where ico is not null;

create unique index if not exists organization_type_icz_uidx
  on core.organization (organization_type, icz)
  where icz is not null;

create unique index if not exists organization_type_zzid_uidx
  on core.organization (organization_type, zz_id)
  where zz_id is not null;

create unique index if not exists organization_type_payer_uidx
  on core.organization (organization_type, payer_code)
  where payer_code is not null;

create table if not exists core.financial_flow (
  financial_flow_id bigserial primary key,
  budget_domain text not null,
  reporting_period_id bigint not null references core.reporting_period(reporting_period_id),
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  source_organization_id bigint references core.organization(organization_id),
  target_organization_id bigint references core.organization(organization_id),
  intermediary_organization_id bigint references core.organization(organization_id),
  flow_type text not null,
  basis text not null check (basis in ('allocated', 'budgeted', 'realized', 'reported', 'other')),
  certainty text not null check (certainty in ('observed', 'inferred', 'estimated')),
  cost_bucket_code text,
  amount_czk bigint not null check (amount_czk >= 0),
  quantity numeric(20, 2),
  unit text,
  note text,
  source_url text,
  lineage jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists financial_flow_domain_period_idx
  on core.financial_flow (budget_domain, reporting_period_id);

create index if not exists financial_flow_source_target_idx
  on core.financial_flow (source_organization_id, target_organization_id);

create table if not exists core.school_capacity (
  school_capacity_id bigserial primary key,
  reporting_period_id bigint not null references core.reporting_period(reporting_period_id),
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  school_organization_id bigint not null references core.organization(organization_id),
  capacity integer not null check (capacity >= 0),
  unique (reporting_period_id, school_organization_id)
);

create table if not exists core.health_service_activity (
  health_service_activity_id bigserial primary key,
  reporting_period_id bigint not null references core.reporting_period(reporting_period_id),
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  provider_organization_id bigint references core.organization(organization_id),
  payer_organization_id bigint references core.organization(organization_id),
  service_code text not null,
  service_name text,
  specialty_code text,
  quantity bigint not null check (quantity >= 0),
  unit text not null default 'reported_services',
  source_url text,
  lineage jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists health_activity_period_idx
  on core.health_service_activity (reporting_period_id);

create index if not exists health_activity_provider_idx
  on core.health_service_activity (provider_organization_id);

create index if not exists health_activity_payer_idx
  on core.health_service_activity (payer_organization_id);

create table if not exists raw.school_entities (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  institution_id text not null,
  institution_name text not null,
  ico text,
  founder_id text,
  founder_name text,
  founder_type text,
  municipality text,
  region text,
  capacity integer,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create table if not exists raw.school_allocations (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  institution_id text,
  ico text,
  pedagogical_amount bigint not null default 0,
  nonpedagogical_amount bigint not null default 0,
  oniv_amount bigint not null default 0,
  other_amount bigint not null default 0,
  operations_amount bigint not null default 0,
  investment_amount bigint not null default 0,
  bucket_basis text,
  bucket_certainty text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create table if not exists raw.school_eu_projects (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  institution_id text,
  ico text,
  programme text not null,
  project_name text not null,
  amount_czk bigint not null default 0,
  basis text,
  certainty text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create table if not exists raw.school_founder_support (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  institution_id text,
  ico text,
  amount_czk bigint not null default 0,
  basis text,
  certainty text,
  note text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create table if not exists raw.school_state_budget (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  node_id text not null,
  node_name text not null,
  node_category text,
  flow_type text not null,
  amount_czk bigint not null default 0,
  basis text,
  certainty text,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create table if not exists raw.health_provider_site (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  snapshot_date date,
  provider_ico text,
  zz_id bigint,
  zz_kod text,
  pcz text,
  pcdp text,
  zz_name text,
  zz_type_code text,
  zz_type_name text,
  region_code text,
  region_name text,
  municipality text,
  street text,
  ruian_code text,
  started_on date,
  care_field text,
  care_form text,
  care_kind text,
  provider_name text,
  provider_type text,
  provider_legal_form_name text,
  provider_region_name text,
  provider_municipality text,
  provider_email text,
  provider_web text,
  founder_type text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create table if not exists raw.health_claims_provider_specialty (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  reporting_month integer not null,
  service_code text not null,
  service_name text,
  icz text,
  specialty_code text,
  quantity bigint not null,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create table if not exists raw.health_claims_payer (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  reporting_month integer not null,
  service_code text not null,
  service_name text,
  payer_code text not null,
  quantity bigint not null,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create table if not exists raw.health_claims_provider_monthly (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  reporting_month integer not null,
  icz text,
  total_quantity bigint not null,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create table if not exists raw.health_claims_provider_ico_yearly (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  provider_ico text not null,
  total_quantity bigint not null,
  patient_count bigint not null default 0,
  contact_count bigint not null default 0,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create table if not exists raw.health_claims_payer_monthly (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  reporting_month integer not null,
  payer_code text not null,
  total_quantity bigint not null,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create table if not exists raw.health_insurer_codebook (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  effective_date date,
  codebook_code text,
  payer_code text not null,
  payer_name text not null,
  valid_from date,
  valid_to date,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create table if not exists raw.health_monitor_indicator (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  period_code text not null,
  provider_ico text not null,
  revenues_czk numeric(20, 2) not null default 0,
  costs_czk numeric(20, 2) not null default 0,
  result_czk numeric(20, 2) not null default 0,
  assets_czk numeric(20, 2) not null default 0,
  receivables_czk numeric(20, 2) not null default 0,
  liabilities_czk numeric(20, 2) not null default 0,
  short_term_liabilities_czk numeric(20, 2) not null default 0,
  long_term_liabilities_czk numeric(20, 2) not null default 0,
  total_debt_czk numeric(20, 2) not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists health_monitor_indicator_year_ico_idx
  on raw.health_monitor_indicator (reporting_year, provider_ico);

create table if not exists raw.health_mz_budget_entity (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  period_code text not null,
  entity_ico text not null,
  entity_name text not null,
  entity_kind text not null,
  region_name text not null,
  expenses_czk numeric(20, 2) not null default 0,
  costs_czk numeric(20, 2) not null default 0,
  revenues_czk numeric(20, 2) not null default 0,
  result_czk numeric(20, 2) not null default 0,
  assets_czk numeric(20, 2) not null default 0,
  receivables_czk numeric(20, 2) not null default 0,
  liabilities_czk numeric(20, 2) not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists health_mz_budget_entity_year_ico_idx
  on raw.health_mz_budget_entity (reporting_year, entity_ico);

create table if not exists raw.health_financing_aggregate (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  financing_type_code text not null,
  financing_type_name text not null,
  financing_subtype_code text,
  financing_subtype_name text,
  provider_type_code text not null,
  provider_type_name text not null,
  provider_subtype_code text,
  provider_subtype_name text,
  amount_czk numeric(20, 2) not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists health_financing_aggregate_year_idx
  on raw.health_financing_aggregate (reporting_year, financing_subtype_code, provider_type_code, provider_subtype_code);

create table if not exists raw.social_mpsv_aggregate (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  chapter_code text not null,
  chapter_name text not null,
  metric_group text not null,
  metric_code text not null,
  metric_name text not null,
  amount_czk numeric(20, 2) not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists social_mpsv_aggregate_year_metric_idx
  on raw.social_mpsv_aggregate (reporting_year, metric_group, metric_code);

create table if not exists raw.social_recipient_metric (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  metric_code text not null,
  metric_name text not null,
  denominator_kind text not null,
  recipient_count numeric(20, 2) not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists social_recipient_metric_year_code_idx
  on raw.social_recipient_metric (reporting_year, metric_code);

create or replace view mart.school_available_years as
select distinct reporting_year as year
from raw.school_entities
order by year;

create or replace view mart.health_provider_directory as
select
  provider_ico,
  zz_id,
  zz_kod,
  max(zz_name) as facility_name,
  max(zz_type_name) as facility_type_name,
  max(provider_name) as provider_name,
  max(provider_type) as provider_type,
  max(provider_legal_form_name) as provider_legal_form_name,
  coalesce(max(provider_region_name), max(region_name)) as region_name,
  max(municipality) as municipality,
  max(care_field) as care_field,
  max(care_form) as care_form,
  max(care_kind) as care_kind,
  max(founder_type) as founder_type
from raw.health_provider_site
group by provider_ico, zz_id, zz_kod;

create or replace view mart.health_claims_provider_monthly as
select
  reporting_year,
  reporting_month,
  icz,
  sum(total_quantity) as total_quantity
from raw.health_claims_provider_monthly
group by reporting_year, reporting_month, icz;

create or replace view mart.health_claims_provider_yearly as
select
  reporting_year,
  provider_ico,
  sum(total_quantity) as total_quantity,
  sum(patient_count) as patient_count,
  sum(contact_count) as contact_count
from raw.health_claims_provider_ico_yearly
group by reporting_year, provider_ico;

create or replace view mart.health_claims_payer_monthly as
select
  reporting_year,
  reporting_month,
  payer_code,
  sum(total_quantity) as total_quantity
from raw.health_claims_payer_monthly
group by reporting_year, reporting_month, payer_code;

create or replace view mart.health_monitor_indicator_latest as
select distinct on (r.reporting_year, r.provider_ico)
  r.reporting_year,
  r.period_code,
  r.provider_ico,
  r.revenues_czk,
  r.costs_czk,
  r.result_czk,
  r.assets_czk,
  r.receivables_czk,
  r.liabilities_czk,
  r.short_term_liabilities_czk,
  r.long_term_liabilities_czk,
  r.total_debt_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.health_monitor_indicator r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.provider_ico,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.health_mz_budget_entity_latest as
select distinct on (r.reporting_year, r.entity_ico)
  r.reporting_year,
  r.period_code,
  r.entity_ico,
  r.entity_name,
  r.entity_kind,
  r.region_name,
  r.expenses_czk,
  r.costs_czk,
  r.revenues_czk,
  r.result_czk,
  r.assets_czk,
  r.receivables_czk,
  r.liabilities_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.health_mz_budget_entity r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.entity_ico,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.health_financing_aggregate_latest as
select distinct on (
  r.reporting_year,
  r.financing_type_code,
  coalesce(r.financing_subtype_code, ''),
  r.provider_type_code,
  coalesce(r.provider_subtype_code, '')
)
  r.reporting_year,
  r.financing_type_code,
  r.financing_type_name,
  r.financing_subtype_code,
  r.financing_subtype_name,
  r.provider_type_code,
  r.provider_type_name,
  r.provider_subtype_code,
  r.provider_subtype_name,
  r.amount_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.health_financing_aggregate r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.financing_type_code,
  coalesce(r.financing_subtype_code, ''),
  r.provider_type_code,
  coalesce(r.provider_subtype_code, ''),
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.social_mpsv_aggregate_latest as
select distinct on (r.reporting_year, r.metric_group, r.metric_code)
  r.reporting_year,
  r.chapter_code,
  r.chapter_name,
  r.metric_group,
  r.metric_code,
  r.metric_name,
  r.amount_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.social_mpsv_aggregate r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.metric_group,
  r.metric_code,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.social_recipient_metric_latest as
select distinct on (r.reporting_year, r.metric_code)
  r.reporting_year,
  r.metric_code,
  r.metric_name,
  r.denominator_kind,
  r.recipient_count,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.social_recipient_metric r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.metric_code,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.health_provider_finance_yearly as
with provider_directory as (
  select
    provider_ico,
    max(provider_name) as provider_name,
    max(provider_type) as provider_type,
    max(provider_legal_form_name) as provider_legal_form_name,
    max(region_name) as region_name,
    bool_or(
      lower(coalesce(facility_type_name, '')) like '%nemoc%'
      or lower(coalesce(provider_type, '')) like '%nemoc%'
    ) as hospital_like,
    bool_or(
      lower(coalesce(facility_type_name, '')) like '%zdravotní ústav%'
      or lower(coalesce(provider_type, '')) like '%zdravotní ústav%'
      or lower(coalesce(provider_type, '')) like '%státní zdravotní ústav%'
    ) as public_health_like
  from mart.health_provider_directory
  group by provider_ico
)
select
  m.reporting_year,
  m.period_code,
  m.provider_ico,
  d.provider_name,
  d.provider_type,
  d.provider_legal_form_name,
  d.region_name,
  coalesce(d.hospital_like, false) as hospital_like,
  coalesce(d.public_health_like, false) as public_health_like,
  m.revenues_czk,
  m.costs_czk,
  m.result_czk,
  m.assets_czk,
  m.receivables_czk,
  m.liabilities_czk,
  m.short_term_liabilities_czk,
  m.long_term_liabilities_czk,
  m.total_debt_czk,
  coalesce(c.total_quantity, 0) as total_quantity,
  coalesce(c.patient_count, 0) as patient_count,
  coalesce(c.contact_count, 0) as contact_count,
  m.source_url,
  m.snapshot_label
from mart.health_monitor_indicator_latest m
left join provider_directory d using (provider_ico)
left join mart.health_claims_provider_yearly c
  on c.reporting_year = m.reporting_year
 and c.provider_ico = m.provider_ico;
