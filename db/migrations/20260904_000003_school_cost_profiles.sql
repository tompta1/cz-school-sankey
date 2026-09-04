create table if not exists raw.school_cost_profile (
  raw_id bigserial primary key,
  dataset_release_id bigint not null references meta.dataset_release(dataset_release_id),
  reporting_year integer not null,
  institution_id text not null,
  ico text,
  total_costs_amount bigint not null default 0,
  materials_amount bigint not null default 0,
  energy_amount bigint not null default 0,
  repairs_amount bigint not null default 0,
  services_amount bigint not null default 0,
  personnel_amount bigint not null default 0,
  depreciation_amount bigint not null default 0,
  other_costs_amount bigint not null default 0,
  basis text,
  certainty text,
  note text,
  payload jsonb not null default '{}'::jsonb,
  loaded_at timestamptz not null default now()
);

create index if not exists school_cost_profile_year_institution_idx
  on raw.school_cost_profile (reporting_year, institution_id);
