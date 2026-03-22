import { query } from './db.js';

export async function getAvailableHealthYears() {
  const result = await query(
    `
      select distinct reporting_year as year
      from (
        select reporting_year from mart.health_claims_provider_yearly
        union
        select reporting_year from mart.health_claims_payer_monthly
      ) years
      order by year
    `,
  );
  return result.rows.map((row) => Number(row.year));
}

export async function getHealthSummary() {
  const [counts, years, sources] = await Promise.all([
    query(
      `
        select
          count(*) as facilities,
          count(distinct provider_ico) filter (where provider_ico is not null and provider_ico <> '') as providers,
          count(*) filter (
            where lower(coalesce(facility_type_name, '')) like '%nemoc%'
               or lower(coalesce(provider_type, '')) like '%nemoc%'
          ) as hospital_like_facilities
        from mart.health_provider_directory
      `,
    ),
    getAvailableHealthYears(),
    query(
      `
        select dataset_code, snapshot_label, row_count, status
        from meta.dataset_release
        where domain_code = 'health'
        order by dataset_code, snapshot_label desc
      `,
    ),
  ]);

  const insurerCount = await query(
    `
      select count(distinct payer_code) as insurers
      from raw.health_insurer_codebook
    `,
  );
  const providerClaimCount = await query(
    `
      select count(*) as row_count
      from raw.health_claims_provider_ico_yearly
    `,
  );
  const payerClaimCount = await query(
    `
      select count(*) as row_count
      from raw.health_claims_payer_monthly
    `,
  );

  const row = counts.rows[0] ?? {};
  return {
    years,
    counts: {
      providers: Number(row.providers ?? 0),
      facilities: Number(row.facilities ?? 0),
      hospitalLikeFacilities: Number(row.hospital_like_facilities ?? 0),
      insurers: Number(insurerCount.rows[0]?.insurers ?? 0),
      providerClaimRows: Number(providerClaimCount.rows[0]?.row_count ?? 0),
      payerClaimRows: Number(payerClaimCount.rows[0]?.row_count ?? 0),
    },
    sources: sources.rows.map((source) => ({
      datasetCode: source.dataset_code,
      snapshotLabel: source.snapshot_label,
      rowCount: Number(source.row_count ?? 0),
      status: source.status,
    })),
  };
}

export async function getHealthProviders({ q = '', region = '', hospitalOnly = false, limit = 50, offset = 0 }) {
  const result = await query(
    `
      with filtered as (
        select
          provider_ico,
          zz_id,
          zz_kod,
          facility_name,
          facility_type_name,
          provider_name,
          provider_type,
          provider_legal_form_name,
          region_name,
          municipality,
          care_field,
          care_form,
          care_kind,
          founder_type
        from mart.health_provider_directory
        where (
          $1 = ''
          or lower(coalesce(provider_name, '')) like lower('%' || $1 || '%')
          or lower(coalesce(facility_name, '')) like lower('%' || $1 || '%')
          or lower(coalesce(care_field, '')) like lower('%' || $1 || '%')
        )
          and ($2 = '' or region_name = $2)
          and (
            not $3
            or lower(coalesce(facility_type_name, '')) like '%nemoc%'
            or lower(coalesce(provider_type, '')) like '%nemoc%'
          )
      )
      select *
      from filtered
      order by region_name nulls last, municipality nulls last, provider_name nulls last, facility_name nulls last
      limit $4 offset $5
    `,
    [q.trim(), region.trim(), hospitalOnly, limit, offset],
  );

  return result.rows.map((row) => ({
    providerIco: row.provider_ico,
    zzId: row.zz_id == null ? null : Number(row.zz_id),
    zzKod: row.zz_kod,
    facilityName: row.facility_name,
    facilityTypeName: row.facility_type_name,
    providerName: row.provider_name,
    providerType: row.provider_type,
    providerLegalFormName: row.provider_legal_form_name,
    regionName: row.region_name,
    municipality: row.municipality,
    careField: row.care_field,
    careForm: row.care_form,
    careKind: row.care_kind,
    founderType: row.founder_type,
  }));
}

export async function getHealthPayerMonthly({ year = null, payerCode = '', limit = 500 }) {
  const result = await query(
    `
      with latest_names as (
        select distinct on (payer_code)
          payer_code,
          payer_name
        from raw.health_insurer_codebook
        where payer_code is not null
        order by payer_code, effective_date desc nulls last, raw_id desc
      )
      select
        p.reporting_year,
        p.reporting_month,
        p.payer_code,
        n.payer_name,
        p.total_quantity
      from mart.health_claims_payer_monthly p
      left join latest_names n using (payer_code)
      where ($1::int is null or p.reporting_year = $1)
        and ($2 = '' or p.payer_code = $2)
      order by p.reporting_year desc, p.reporting_month desc, p.total_quantity desc, p.payer_code
      limit $3
    `,
    [year, payerCode.trim(), limit],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    month: Number(row.reporting_month),
    payerCode: row.payer_code,
    payerName: row.payer_name,
    totalQuantity: Number(row.total_quantity),
  }));
}

export async function getHealthProviderMonthly({ year = null, icz = '', limit = 500 }) {
  const result = await query(
    `
      select
        reporting_year,
        reporting_month,
        icz,
        total_quantity
      from mart.health_claims_provider_monthly
      where ($1::int is null or reporting_year = $1)
        and ($2 = '' or icz = $2)
      order by reporting_year desc, reporting_month desc, total_quantity desc, icz
      limit $3
    `,
    [year, icz.trim(), limit],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    month: Number(row.reporting_month),
    icz: row.icz,
    totalQuantity: Number(row.total_quantity),
  }));
}

export async function getHealthProviderYearly({ year = null, providerIco = '', limit = 500 }) {
  const result = await query(
    `
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
        p.reporting_year,
        p.provider_ico,
        d.provider_name,
        d.provider_type,
        d.provider_legal_form_name,
        d.region_name,
        d.hospital_like,
        d.public_health_like,
        p.total_quantity,
        p.patient_count,
        p.contact_count
      from mart.health_claims_provider_yearly p
      left join provider_directory d using (provider_ico)
      where ($1::int is null or p.reporting_year = $1)
        and ($2 = '' or p.provider_ico = $2)
      order by p.reporting_year desc, p.total_quantity desc, p.provider_ico
      limit $3
    `,
    [year, providerIco.trim(), limit],
  );

  return result.rows.map((row) => ({
    year: Number(row.reporting_year),
    providerIco: row.provider_ico,
    providerName: row.provider_name,
    providerType: row.provider_type,
    providerLegalFormName: row.provider_legal_form_name,
    regionName: row.region_name,
    hospitalLike: Boolean(row.hospital_like),
    publicHealthLike: Boolean(row.public_health_like),
    totalQuantity: Number(row.total_quantity),
    patientCount: Number(row.patient_count),
    contactCount: Number(row.contact_count),
  }));
}
