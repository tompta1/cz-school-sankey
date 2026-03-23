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

create table if not exists raw.health_zzs_activity_aggregate (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  indicator_code text not null,
  indicator_name text not null,
  count_value numeric(20, 2) not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists health_zzs_activity_aggregate_year_indicator_idx
  on raw.health_zzs_activity_aggregate (reporting_year, indicator_code);

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

create table if not exists raw.mv_budget_aggregate (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  basis text not null,
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

create index if not exists mv_budget_aggregate_year_metric_idx
  on raw.mv_budget_aggregate (reporting_year, metric_group, metric_code);

create table if not exists raw.mv_police_crime_aggregate (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  region_name text not null,
  region_code text not null,
  indicator_code text not null,
  indicator_name text not null,
  crime_class_code text not null,
  crime_class_name text not null,
  count_value numeric(20, 2) not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists mv_police_crime_year_region_indicator_idx
  on raw.mv_police_crime_aggregate (reporting_year, region_code, indicator_code, crime_class_code);

create table if not exists raw.mv_fire_rescue_activity_aggregate (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  region_name text not null,
  region_code text not null,
  indicator_code text not null,
  indicator_name text not null,
  count_value numeric(20, 2) not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists mv_fire_rescue_activity_year_region_indicator_idx
  on raw.mv_fire_rescue_activity_aggregate (reporting_year, region_code, indicator_code);

create table if not exists raw.justice_budget_aggregate (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  basis text not null,
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

create index if not exists justice_budget_aggregate_year_metric_idx
  on raw.justice_budget_aggregate (reporting_year, metric_group, metric_code);

create table if not exists raw.justice_activity_aggregate (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  activity_domain text not null,
  metric_code text not null,
  metric_name text not null,
  count_value numeric(20, 2) not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists justice_activity_aggregate_year_metric_idx
  on raw.justice_activity_aggregate (reporting_year, activity_domain, metric_code);

create table if not exists raw.transport_budget_entity (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  period_code text not null,
  entity_ico text not null,
  entity_name text not null,
  entity_kind text not null,
  expenses_czk numeric(20, 2) not null default 0,
  costs_czk numeric(20, 2) not null default 0,
  revenues_czk numeric(20, 2) not null default 0,
  result_czk numeric(20, 2) not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists transport_budget_entity_year_ico_idx
  on raw.transport_budget_entity (reporting_year, entity_ico);

create table if not exists raw.transport_sfdi_project (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  action_id text not null,
  budget_area_code text not null,
  action_type_code text,
  financing_code text,
  status_code text,
  project_name text not null,
  total_cost_czk numeric(20, 2) not null default 0,
  adjusted_budget_czk numeric(20, 2) not null default 0,
  paid_czk numeric(20, 2) not null default 0,
  sfdi_paid_czk numeric(20, 2) not null default 0,
  eu_paid_czk numeric(20, 2) not null default 0,
  region_code text,
  investor_name text not null,
  investor_ico text,
  investor_address text,
  start_period text,
  end_period text,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists transport_sfdi_project_year_action_idx
  on raw.transport_sfdi_project (reporting_year, action_id, budget_area_code);

create table if not exists raw.transport_activity_metric (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  activity_domain text not null,
  metric_code text not null,
  metric_name text not null,
  count_value numeric(20, 2) not null default 0,
  reference_amount_czk numeric(20, 2) not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists transport_activity_metric_year_code_idx
  on raw.transport_activity_metric (reporting_year, activity_domain, metric_code);

create table if not exists raw.agriculture_budget_entity (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  period_code text not null,
  entity_ico text not null,
  entity_name text not null,
  entity_kind text not null,
  expenses_czk numeric(20, 2) not null default 0,
  costs_czk numeric(20, 2) not null default 0,
  revenues_czk numeric(20, 2) not null default 0,
  result_czk numeric(20, 2) not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists agriculture_budget_entity_year_ico_idx
  on raw.agriculture_budget_entity (reporting_year, entity_ico);

drop view if exists mart.agriculture_lpis_user_area_yearly_latest;
drop view if exists mart.agriculture_szif_family_metric_latest;
drop view if exists mart.agriculture_szif_family_recipient_yearly_latest;
drop view if exists mart.agriculture_szif_recipient_metric_latest;
drop view if exists mart.agriculture_szif_recipient_yearly_latest;
drop view if exists mart.agriculture_szif_payment_latest;
drop table if exists raw.agriculture_szif_payment;

create table if not exists raw.agriculture_szif_recipient_yearly (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  funding_source_code text not null,
  funding_source_name text not null,
  recipient_name text not null,
  recipient_ico text,
  recipient_key text not null,
  municipality text,
  district text,
  eu_source_czk numeric(20, 2) not null default 0,
  cz_source_czk numeric(20, 2) not null default 0,
  amount_czk numeric(20, 2) not null default 0,
  payment_count integer not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists agriculture_szif_recipient_year_source_recipient_idx
  on raw.agriculture_szif_recipient_yearly (reporting_year, funding_source_code, recipient_key);

create index if not exists agriculture_szif_recipient_year_source_amount_idx
  on raw.agriculture_szif_recipient_yearly (reporting_year, funding_source_code, amount_czk desc);

create table if not exists raw.agriculture_szif_family_recipient_yearly (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  funding_source_code text not null,
  funding_source_name text not null,
  family_code text not null,
  family_name text not null,
  recipient_name text not null,
  recipient_name_normalized text not null,
  recipient_ico text,
  recipient_key text not null,
  municipality text,
  district text,
  amount_czk numeric(20, 2) not null default 0,
  payment_count integer not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists agriculture_szif_family_year_family_amount_idx
  on raw.agriculture_szif_family_recipient_yearly (reporting_year, family_code, amount_czk desc);

create index if not exists agriculture_szif_family_year_family_recipient_idx
  on raw.agriculture_szif_family_recipient_yearly (reporting_year, family_code, recipient_key);

create table if not exists raw.agriculture_lpis_user_area_yearly (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  user_name text not null,
  user_name_normalized text not null,
  lpis_user_ji text,
  area_ha numeric(20, 2) not null default 0,
  block_count integer not null default 0,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists agriculture_lpis_user_area_year_name_idx
  on raw.agriculture_lpis_user_area_yearly (reporting_year, user_name_normalized);

create table if not exists raw.environment_budget_entity (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id) on delete cascade,
  reporting_year integer not null,
  period_code text not null,
  entity_ico text not null,
  entity_name text not null,
  entity_kind text not null,
  expenses_czk numeric(18,2) not null,
  costs_czk numeric(18,2) not null,
  revenues_czk numeric(18,2) not null,
  result_czk numeric(18,2) not null,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists environment_budget_entity_year_ico_idx
  on raw.environment_budget_entity (reporting_year, entity_ico);

create table if not exists raw.environment_sfzp_support_yearly (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id) on delete cascade,
  reporting_year integer not null,
  program_code text not null,
  program_name text not null,
  recipient_key text not null,
  recipient_name text not null,
  recipient_ico text,
  municipality text,
  support_czk numeric(18,2) not null,
  paid_czk numeric(18,2) not null,
  project_count integer not null default 1,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists environment_sfzp_support_year_program_amount_idx
  on raw.environment_sfzp_support_yearly (reporting_year, program_code, support_czk desc);

create index if not exists environment_sfzp_support_year_recipient_idx
  on raw.environment_sfzp_support_yearly (reporting_year, recipient_key);

create table if not exists raw.mmr_budget_aggregate (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id) on delete cascade,
  reporting_year integer not null,
  metric_code text not null,
  metric_name text not null,
  metric_group text not null,
  amount_czk numeric(18,2) not null,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists mmr_budget_aggregate_year_code_idx
  on raw.mmr_budget_aggregate (reporting_year, metric_code);

create table if not exists raw.mmr_irop_operation_yearly (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id) on delete cascade,
  reporting_year integer not null,
  branch_code text not null,
  branch_name text not null,
  region_code text,
  region_name text,
  recipient_key text not null,
  recipient_name text not null,
  recipient_ico text,
  project_id text not null,
  project_name text not null,
  priority_name text,
  intervention_name text,
  allocated_total_czk numeric(18,2) not null,
  union_support_czk numeric(18,2) not null,
  national_public_czk numeric(18,2) not null,
  charged_total_czk numeric(18,2) not null,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists mmr_irop_operation_year_branch_region_idx
  on raw.mmr_irop_operation_yearly (reporting_year, branch_code, region_code);

create index if not exists mmr_irop_operation_year_recipient_idx
  on raw.mmr_irop_operation_yearly (reporting_year, branch_code, recipient_key);

create table if not exists raw.mpo_budget_entity (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id) on delete cascade,
  reporting_year integer not null,
  period_code text not null,
  entity_ico text not null,
  entity_name text not null,
  entity_kind text not null,
  expenses_czk numeric(18,2) not null,
  costs_czk numeric(18,2) not null,
  revenues_czk numeric(18,2) not null,
  result_czk numeric(18,2) not null,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists mpo_budget_entity_year_ico_idx
  on raw.mpo_budget_entity (reporting_year, entity_ico);

create table if not exists raw.mpo_optak_operation_yearly (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id) on delete cascade,
  reporting_year integer not null,
  region_code text,
  region_name text,
  recipient_key text not null,
  recipient_name text not null,
  recipient_ico text,
  project_id text not null,
  project_name text not null,
  priority_name text,
  specific_objective_name text,
  intervention_name text,
  allocated_total_czk numeric(18,2) not null,
  union_support_czk numeric(18,2) not null,
  national_public_czk numeric(18,2) not null,
  charged_total_czk numeric(18,2) not null,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists mpo_optak_operation_year_region_idx
  on raw.mpo_optak_operation_yearly (reporting_year, region_code);

create index if not exists mpo_optak_operation_year_recipient_idx
  on raw.mpo_optak_operation_yearly (reporting_year, recipient_key);

create table if not exists raw.mk_budget_entity (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id) on delete cascade,
  reporting_year integer not null,
  period_code text not null,
  entity_ico text not null,
  entity_name text not null,
  entity_kind text not null,
  expenses_czk numeric(18,2) not null,
  costs_czk numeric(18,2) not null,
  revenues_czk numeric(18,2) not null,
  result_czk numeric(18,2) not null,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists mk_budget_entity_year_ico_idx
  on raw.mk_budget_entity (reporting_year, entity_ico);

create table if not exists raw.mk_budget_aggregate (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id) on delete cascade,
  reporting_year integer not null,
  metric_code text not null,
  metric_name text not null,
  pvs_code text,
  amount_czk numeric(18,2) not null,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists mk_budget_aggregate_year_metric_idx
  on raw.mk_budget_aggregate (reporting_year, metric_code);

create table if not exists raw.mk_support_award (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id) on delete cascade,
  reporting_year integer not null,
  program_code text not null,
  program_name text not null,
  recipient_key text not null,
  recipient_name text not null,
  recipient_ico text,
  project_name text not null,
  requested_czk numeric(18,2) not null,
  awarded_czk numeric(18,2) not null,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists mk_support_award_year_program_idx
  on raw.mk_support_award (reporting_year, program_code);

create index if not exists mk_support_award_year_recipient_idx
  on raw.mk_support_award (reporting_year, recipient_key);

create table if not exists raw.mk_region_metric (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id) on delete cascade,
  reporting_year integer not null,
  program_code text not null,
  program_name text not null,
  region_code text,
  region_name text not null,
  recipient_count integer not null,
  awarded_czk numeric(18,2) not null,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists mk_region_metric_year_program_idx
  on raw.mk_region_metric (reporting_year, program_code);

create index if not exists mk_region_metric_year_region_idx
  on raw.mk_region_metric (reporting_year, region_code);

create table if not exists raw.mzv_budget_entity (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id) on delete cascade,
  reporting_year integer not null,
  period_code text not null,
  entity_ico text not null,
  entity_name text not null,
  entity_kind text not null,
  expenses_czk numeric(18,2) not null,
  costs_czk numeric(18,2) not null,
  revenues_czk numeric(18,2) not null,
  result_czk numeric(18,2) not null,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists mzv_budget_entity_year_ico_idx
  on raw.mzv_budget_entity (reporting_year, entity_ico);

create table if not exists raw.mzv_diplomatic_metric (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id) on delete cascade,
  reporting_year integer not null,
  metric_code text not null,
  metric_name text not null,
  count_value integer not null,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists mzv_diplomatic_metric_year_code_idx
  on raw.mzv_diplomatic_metric (reporting_year, metric_code);

create table if not exists raw.mzv_aid_operation_yearly (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id) on delete cascade,
  reporting_year integer not null,
  branch_code text not null,
  branch_name text not null,
  source_workbook text not null,
  section_code text,
  section_name text,
  country_name text not null,
  sector_name text,
  manager_code text,
  manager_name text,
  recipient_key text not null,
  recipient_name text not null,
  recipient_ico text,
  project_key text not null,
  project_name text not null,
  planned_czk numeric(18,2) not null,
  actual_czk numeric(18,2) not null,
  source_url text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists mzv_aid_operation_year_branch_country_idx
  on raw.mzv_aid_operation_yearly (reporting_year, branch_code, country_name);

create index if not exists mzv_aid_operation_year_project_idx
  on raw.mzv_aid_operation_yearly (reporting_year, project_key);

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

create or replace view mart.health_zzs_activity_aggregate_latest as
select distinct on (
  r.reporting_year,
  r.indicator_code
)
  r.reporting_year,
  r.indicator_code,
  r.indicator_name,
  r.count_value,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.health_zzs_activity_aggregate r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.indicator_code,
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

create or replace view mart.mv_budget_aggregate_latest as
select distinct on (r.reporting_year, r.metric_group, r.metric_code)
  r.reporting_year,
  r.basis,
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
from raw.mv_budget_aggregate r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.metric_group,
  r.metric_code,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.mv_police_crime_aggregate_latest as
select distinct on (
  r.reporting_year,
  r.region_code,
  r.indicator_code,
  r.crime_class_code
)
  r.reporting_year,
  r.region_name,
  r.region_code,
  r.indicator_code,
  r.indicator_name,
  r.crime_class_code,
  r.crime_class_name,
  r.count_value,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.mv_police_crime_aggregate r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.region_code,
  r.indicator_code,
  r.crime_class_code,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.mv_fire_rescue_activity_aggregate_latest as
select distinct on (
  r.reporting_year,
  r.region_code,
  r.indicator_code
)
  r.reporting_year,
  r.region_name,
  r.region_code,
  r.indicator_code,
  r.indicator_name,
  r.count_value,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.mv_fire_rescue_activity_aggregate r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.region_code,
  r.indicator_code,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.justice_budget_aggregate_latest as
select distinct on (r.reporting_year, r.metric_group, r.metric_code)
  r.reporting_year,
  r.basis,
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
from raw.justice_budget_aggregate r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.metric_group,
  r.metric_code,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.justice_activity_aggregate_latest as
select distinct on (r.reporting_year, r.activity_domain, r.metric_code)
  r.reporting_year,
  r.activity_domain,
  r.metric_code,
  r.metric_name,
  r.count_value,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.justice_activity_aggregate r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.activity_domain,
  r.metric_code,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.transport_budget_entity_latest as
select distinct on (r.reporting_year, r.entity_ico)
  r.reporting_year,
  r.period_code,
  r.entity_ico,
  r.entity_name,
  r.entity_kind,
  r.expenses_czk,
  r.costs_czk,
  r.revenues_czk,
  r.result_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.transport_budget_entity r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.entity_ico,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.transport_sfdi_project_latest as
select distinct on (r.reporting_year, r.action_id, r.budget_area_code, r.investor_name)
  r.reporting_year,
  r.action_id,
  r.budget_area_code,
  r.action_type_code,
  r.financing_code,
  r.status_code,
  r.project_name,
  r.total_cost_czk,
  r.adjusted_budget_czk,
  r.paid_czk,
  r.sfdi_paid_czk,
  r.eu_paid_czk,
  r.region_code,
  r.investor_name,
  r.investor_ico,
  r.investor_address,
  r.start_period,
  r.end_period,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.transport_sfdi_project r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.action_id,
  r.budget_area_code,
  r.investor_name,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.transport_activity_metric_latest as
select distinct on (r.reporting_year, r.activity_domain, r.metric_code)
  r.reporting_year,
  r.activity_domain,
  r.metric_code,
  r.metric_name,
  r.count_value,
  r.reference_amount_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.transport_activity_metric r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.activity_domain,
  r.metric_code,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.agriculture_budget_entity_latest as
select distinct on (r.reporting_year, r.entity_ico)
  r.reporting_year,
  r.period_code,
  r.entity_ico,
  r.entity_name,
  r.entity_kind,
  r.expenses_czk,
  r.costs_czk,
  r.revenues_czk,
  r.result_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.agriculture_budget_entity r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.entity_ico,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.agriculture_szif_recipient_yearly_latest as
select distinct on (r.reporting_year, r.funding_source_code, r.recipient_key)
  r.reporting_year,
  r.funding_source_code,
  r.funding_source_name,
  r.recipient_name,
  r.recipient_ico,
  r.recipient_key,
  r.municipality,
  r.district,
  r.eu_source_czk,
  r.cz_source_czk,
  r.amount_czk,
  r.payment_count,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.agriculture_szif_recipient_yearly r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.funding_source_code,
  r.recipient_key,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.agriculture_szif_family_recipient_yearly_latest as
select distinct on (r.reporting_year, r.funding_source_code, r.family_code, r.recipient_key)
  r.reporting_year,
  r.funding_source_code,
  r.funding_source_name,
  r.family_code,
  r.family_name,
  r.recipient_name,
  r.recipient_name_normalized,
  r.recipient_ico,
  r.recipient_key,
  r.municipality,
  r.district,
  r.amount_czk,
  r.payment_count,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.agriculture_szif_family_recipient_yearly r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.funding_source_code,
  r.family_code,
  r.recipient_key,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.agriculture_szif_recipient_metric_latest as
with per_source as (
  select
    reporting_year,
    funding_source_code,
    max(funding_source_name) as funding_source_name,
    count(*) filter (where amount_czk > 0)::integer as recipient_count,
    sum(amount_czk) as amount_czk
  from mart.agriculture_szif_recipient_yearly_latest
  group by reporting_year, funding_source_code
),
all_sources as (
  select
    reporting_year,
    recipient_key,
    sum(amount_czk) as total_amount_czk
  from mart.agriculture_szif_recipient_yearly_latest
  group by reporting_year, recipient_key
)
select
  reporting_year,
  funding_source_code,
  funding_source_name,
  recipient_count,
  amount_czk
from per_source
union all
select
  reporting_year,
  'TOTAL' as funding_source_code,
  'Zemedelske dotace pres SZIF' as funding_source_name,
  count(*) filter (where total_amount_czk > 0)::integer as recipient_count,
  sum(total_amount_czk) as amount_czk
from all_sources
group by reporting_year;

create or replace view mart.agriculture_szif_family_metric_latest as
select
  reporting_year,
  family_code,
  max(family_name) as family_name,
  count(*) filter (where amount_czk > 0)::integer as recipient_count,
  sum(amount_czk) as amount_czk
from mart.agriculture_szif_family_recipient_yearly_latest
group by reporting_year, family_code;

create or replace view mart.agriculture_lpis_user_area_yearly_latest as
select distinct on (r.reporting_year, r.user_name_normalized)
  r.reporting_year,
  r.user_name,
  r.user_name_normalized,
  r.lpis_user_ji,
  r.area_ha,
  r.block_count,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.agriculture_lpis_user_area_yearly r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.user_name_normalized,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.environment_budget_entity_latest as
select distinct on (r.reporting_year, r.entity_ico)
  r.reporting_year,
  r.period_code,
  r.entity_ico,
  r.entity_name,
  r.entity_kind,
  r.expenses_czk,
  r.costs_czk,
  r.revenues_czk,
  r.result_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.environment_budget_entity r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.entity_ico,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.environment_sfzp_support_yearly_latest as
select distinct on (r.reporting_year, r.program_code, r.recipient_key)
  r.reporting_year,
  r.program_code,
  r.program_name,
  r.recipient_key,
  r.recipient_name,
  r.recipient_ico,
  r.municipality,
  r.support_czk,
  r.paid_czk,
  r.project_count,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.environment_sfzp_support_yearly r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.program_code,
  r.recipient_key,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.environment_sfzp_family_metric_latest as
select
  reporting_year,
  program_code,
  max(program_name) as program_name,
  count(*) filter (where support_czk > 0)::integer as recipient_count,
  sum(support_czk) as support_czk,
  sum(paid_czk) as paid_czk
from mart.environment_sfzp_support_yearly_latest
group by reporting_year, program_code;

create or replace view mart.environment_sfzp_recipient_metric_latest as
select
  reporting_year,
  count(*) filter (where support_czk > 0)::integer as recipient_count,
  sum(support_czk) as support_czk,
  sum(paid_czk) as paid_czk
from mart.environment_sfzp_support_yearly_latest
group by reporting_year;

create or replace view mart.mmr_budget_aggregate_latest as
select distinct on (r.reporting_year, r.metric_code)
  r.reporting_year,
  r.metric_code,
  r.metric_name,
  r.metric_group,
  r.amount_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.mmr_budget_aggregate r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.metric_code,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.mmr_irop_operation_yearly_latest as
select distinct on (r.reporting_year, r.branch_code, r.project_id)
  r.reporting_year,
  r.branch_code,
  r.branch_name,
  r.region_code,
  r.region_name,
  r.recipient_key,
  r.recipient_name,
  r.recipient_ico,
  r.project_id,
  r.project_name,
  r.priority_name,
  r.intervention_name,
  r.allocated_total_czk,
  r.union_support_czk,
  r.national_public_czk,
  r.charged_total_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.mmr_irop_operation_yearly r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.branch_code,
  r.project_id,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.mmr_irop_recipient_metric_latest as
select
  reporting_year,
  branch_code,
  max(branch_name) as branch_name,
  count(distinct recipient_key)::integer as recipient_count,
  count(*)::integer as project_count,
  sum(allocated_total_czk) as allocated_total_czk,
  sum(union_support_czk) as union_support_czk,
  sum(national_public_czk) as national_public_czk,
  sum(charged_total_czk) as charged_total_czk
from mart.mmr_irop_operation_yearly_latest
group by reporting_year, branch_code;

create or replace view mart.mmr_irop_region_metric_latest as
select
  reporting_year,
  branch_code,
  max(branch_name) as branch_name,
  coalesce(region_code, 'UNKNOWN') as region_code,
  max(coalesce(region_name, 'Neurceny kraj')) as region_name,
  count(distinct recipient_key)::integer as recipient_count,
  count(*)::integer as project_count,
  sum(allocated_total_czk) as allocated_total_czk,
  sum(union_support_czk) as union_support_czk,
  sum(national_public_czk) as national_public_czk,
  sum(charged_total_czk) as charged_total_czk
from mart.mmr_irop_operation_yearly_latest
group by reporting_year, branch_code, coalesce(region_code, 'UNKNOWN');

create or replace view mart.mmr_irop_recipient_yearly_latest as
select
  reporting_year,
  branch_code,
  max(branch_name) as branch_name,
  coalesce(region_code, 'UNKNOWN') as region_code,
  max(coalesce(region_name, 'Neurceny kraj')) as region_name,
  recipient_key,
  max(recipient_name) as recipient_name,
  max(recipient_ico) as recipient_ico,
  count(*)::integer as project_count,
  sum(allocated_total_czk) as allocated_total_czk,
  sum(union_support_czk) as union_support_czk,
  sum(national_public_czk) as national_public_czk,
  sum(charged_total_czk) as charged_total_czk
from mart.mmr_irop_operation_yearly_latest
group by reporting_year, branch_code, coalesce(region_code, 'UNKNOWN'), recipient_key;

create or replace view mart.mpo_budget_entity_latest as
select distinct on (r.reporting_year, r.entity_ico)
  r.reporting_year,
  r.period_code,
  r.entity_ico,
  r.entity_name,
  r.entity_kind,
  r.expenses_czk,
  r.costs_czk,
  r.revenues_czk,
  r.result_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.mpo_budget_entity r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.entity_ico,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.mpo_optak_operation_yearly_latest as
select distinct on (r.reporting_year, r.project_id)
  r.reporting_year,
  r.region_code,
  r.region_name,
  r.recipient_key,
  r.recipient_name,
  r.recipient_ico,
  r.project_id,
  r.project_name,
  r.priority_name,
  r.specific_objective_name,
  r.intervention_name,
  r.allocated_total_czk,
  r.union_support_czk,
  r.national_public_czk,
  r.charged_total_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.mpo_optak_operation_yearly r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.project_id,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.mpo_optak_recipient_metric_latest as
select
  reporting_year,
  count(distinct recipient_key)::integer as recipient_count,
  count(*)::integer as project_count,
  sum(allocated_total_czk) as allocated_total_czk,
  sum(union_support_czk) as union_support_czk,
  sum(national_public_czk) as national_public_czk,
  sum(charged_total_czk) as charged_total_czk
from mart.mpo_optak_operation_yearly_latest
group by reporting_year;

create or replace view mart.mpo_optak_region_metric_latest as
select
  reporting_year,
  coalesce(region_code, 'UNKNOWN') as region_code,
  max(coalesce(region_name, 'Neurceny kraj')) as region_name,
  count(distinct recipient_key)::integer as recipient_count,
  count(*)::integer as project_count,
  sum(allocated_total_czk) as allocated_total_czk,
  sum(union_support_czk) as union_support_czk,
  sum(national_public_czk) as national_public_czk,
  sum(charged_total_czk) as charged_total_czk
from mart.mpo_optak_operation_yearly_latest
group by reporting_year, coalesce(region_code, 'UNKNOWN');

create or replace view mart.mpo_optak_recipient_yearly_latest as
select
  reporting_year,
  coalesce(region_code, 'UNKNOWN') as region_code,
  max(coalesce(region_name, 'Neurceny kraj')) as region_name,
  recipient_key,
  max(recipient_name) as recipient_name,
  max(recipient_ico) as recipient_ico,
  count(*)::integer as project_count,
  sum(allocated_total_czk) as allocated_total_czk,
  sum(union_support_czk) as union_support_czk,
  sum(national_public_czk) as national_public_czk,
  sum(charged_total_czk) as charged_total_czk
from mart.mpo_optak_operation_yearly_latest
group by reporting_year, coalesce(region_code, 'UNKNOWN'), recipient_key;

create or replace view mart.mk_budget_entity_latest as
select distinct on (r.reporting_year, r.entity_ico)
  r.reporting_year,
  r.period_code,
  r.entity_ico,
  r.entity_name,
  r.entity_kind,
  r.expenses_czk,
  r.costs_czk,
  r.revenues_czk,
  r.result_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.mk_budget_entity r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.entity_ico,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.mk_budget_aggregate_latest as
select distinct on (r.reporting_year, r.metric_code)
  r.reporting_year,
  r.metric_code,
  r.metric_name,
  r.pvs_code,
  r.amount_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.mk_budget_aggregate r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.metric_code,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.mk_support_award_latest as
select distinct on (r.reporting_year, r.program_code, r.recipient_key, r.project_name)
  r.reporting_year,
  r.program_code,
  r.program_name,
  r.recipient_key,
  r.recipient_name,
  r.recipient_ico,
  r.project_name,
  r.requested_czk,
  r.awarded_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.mk_support_award r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.program_code,
  r.recipient_key,
  r.project_name,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.mk_region_metric_latest as
select distinct on (r.reporting_year, r.program_code, coalesce(r.region_code, r.region_name))
  r.reporting_year,
  r.program_code,
  r.program_name,
  r.region_code,
  r.region_name,
  r.recipient_count,
  r.awarded_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.mk_region_metric r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.program_code,
  coalesce(r.region_code, r.region_name),
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.mk_support_program_metric_latest as
select
  reporting_year,
  program_code,
  max(program_name) as program_name,
  count(distinct recipient_key)::integer as recipient_count,
  sum(awarded_czk) as awarded_czk
from mart.mk_support_award_latest
group by reporting_year, program_code
union all
select
  reporting_year,
  program_code,
  max(program_name) as program_name,
  sum(recipient_count)::integer as recipient_count,
  sum(awarded_czk) as awarded_czk
from mart.mk_region_metric_latest
group by reporting_year, program_code;

create or replace view mart.mk_support_recipient_latest as
select
  reporting_year,
  program_code,
  max(program_name) as program_name,
  recipient_key,
  max(recipient_name) as recipient_name,
  max(recipient_ico) as recipient_ico,
  count(*)::integer as project_count,
  sum(requested_czk) as requested_czk,
  sum(awarded_czk) as awarded_czk
from mart.mk_support_award_latest
group by reporting_year, program_code, recipient_key;

create or replace view mart.mzv_budget_entity_latest as
select distinct on (r.reporting_year, r.entity_ico)
  r.reporting_year,
  r.period_code,
  r.entity_ico,
  r.entity_name,
  r.entity_kind,
  r.expenses_czk,
  r.costs_czk,
  r.revenues_czk,
  r.result_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.mzv_budget_entity r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.entity_ico,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.mzv_diplomatic_metric_latest as
select distinct on (r.reporting_year, r.metric_code)
  r.reporting_year,
  r.metric_code,
  r.metric_name,
  r.count_value,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.mzv_diplomatic_metric r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.metric_code,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.mzv_aid_operation_yearly_latest as
select distinct on (r.reporting_year, r.source_workbook, r.project_key)
  r.reporting_year,
  r.branch_code,
  r.branch_name,
  r.source_workbook,
  r.section_code,
  r.section_name,
  r.country_name,
  r.sector_name,
  r.manager_code,
  r.manager_name,
  r.recipient_key,
  r.recipient_name,
  r.recipient_ico,
  r.project_key,
  r.project_name,
  r.planned_czk,
  r.actual_czk,
  r.source_url,
  r.payload,
  d.snapshot_label,
  d.dataset_release_id
from raw.mzv_aid_operation_yearly r
join meta.dataset_release d on d.dataset_release_id = r.dataset_release_id
order by
  r.reporting_year,
  r.source_workbook,
  r.project_key,
  d.snapshot_label desc,
  r.loaded_at desc,
  r.raw_id desc;

create or replace view mart.mzv_aid_branch_metric_latest as
select
  reporting_year,
  branch_code,
  max(branch_name) as branch_name,
  count(*)::integer as project_count,
  count(distinct recipient_key)::integer as recipient_count,
  sum(actual_czk) as actual_czk,
  sum(planned_czk) as planned_czk
from mart.mzv_aid_operation_yearly_latest
group by reporting_year, branch_code;

create or replace view mart.mzv_aid_country_metric_latest as
select
  reporting_year,
  branch_code,
  max(branch_name) as branch_name,
  country_name,
  count(*)::integer as project_count,
  count(distinct recipient_key)::integer as recipient_count,
  sum(actual_czk) as actual_czk,
  sum(planned_czk) as planned_czk
from mart.mzv_aid_operation_yearly_latest
group by reporting_year, branch_code, country_name;

create or replace view mart.mzv_aid_project_latest as
select
  reporting_year,
  branch_code,
  max(branch_name) as branch_name,
  country_name,
  project_key,
  max(project_name) as project_name,
  max(recipient_key) as recipient_key,
  max(recipient_name) as recipient_name,
  max(recipient_ico) as recipient_ico,
  max(sector_name) as sector_name,
  max(manager_code) as manager_code,
  max(manager_name) as manager_name,
  max(source_workbook) as source_workbook,
  sum(actual_czk) as actual_czk,
  sum(planned_czk) as planned_czk
from mart.mzv_aid_operation_yearly_latest
group by reporting_year, branch_code, country_name, project_key;

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
    ) as public_health_like,
    bool_or(
      lower(coalesce(provider_type, '')) like '%zdravotnická zachranná služba%'
    ) as zzs_like
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
  m.snapshot_label,
  coalesce(d.zzs_like, false) as zzs_like
from mart.health_monitor_indicator_latest m
left join provider_directory d using (provider_ico)
left join mart.health_claims_provider_yearly c
  on c.reporting_year = m.reporting_year
 and c.provider_ico = m.provider_ico;
