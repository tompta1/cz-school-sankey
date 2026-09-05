import type { ApiGraph } from '../types';

import { normalizationGroup } from './sankeyOrdering';

export interface DatasetReference {
  datasetKey: string;
  title: string;
  url?: string;
}

export interface AtlasReferenceSummary {
  datasets: DatasetReference[];
}

type SourceDefinition = Omit<DatasetReference, 'datasetKey'>;

const MF_FINAL_ACCOUNT_URL = 'https://mf.gov.cz/cs/rozpoctova-politika/statni-rozpocet/plneni-statniho-rozpoctu';
const MONITOR_URL = 'https://monitor.statnipokladna.gov.cz';
const MSMT_URL = 'https://www.msmt.cz/vzdelavani/skolstvi-v-cr/statistika-skolstvi';
const MSP_URL = 'https://msp.gov.cz';
const MO_URL = 'https://mocr.mo.gov.cz/finance-a-zakazky/resortni-rozpocet/1resortni-rozpocet--263042/';

const DATASET_SOURCES: Record<string, SourceDefinition> = {
  'api.aggregated': { title: 'MŠMT: školská data', url: MSMT_URL },
  aggregated: { title: 'MŠMT: školská data', url: MSMT_URL },
  school_cost_profiles: { title: 'Monitor MF: účetní výkazy škol', url: MONITOR_URL },
  school_founder_support: { title: 'Monitor MF: transferové výnosy škol', url: MONITOR_URL },
  school_state_budget: { title: 'MF: souhrnný státní závěrečný účet', url: MF_FINAL_ACCOUNT_URL },
  'core.financial_flow': { title: 'MF: státní závěrečný účet', url: MF_FINAL_ACCOUNT_URL },
  health_mz_budget_entities: { title: 'Monitor MF: MZ a veřejné zdravotní organizace', url: MONITOR_URL },
  health_monitor_indicators: { title: 'Monitor MF: účetní výkazy poskytovatelů', url: MONITOR_URL },
  nrhzs_claims_provider_ico: { title: 'ÚZIS NRHZS: vykázané výkony podle IČO', url: 'https://datanzis.uzis.gov.cz/data/NR-04-NRHZS/NR-04-02/' },
  health_financing_aggregates: { title: 'ČSÚ ZDR02: financování zdravotnictví', url: 'https://data.csu.gov.cz/opendata/sady/ZDR02/distribuce/csv' },
  health_zzs_activity_aggregates: { title: 'NZIP A038: zdravotnická záchranná služba', url: 'https://www.nzip.cz/data/1802-vykaz-a038-zdravotnicka-zachranna-sluzba-datovy-souhrn' },
  social_mpsv_aggregates: { title: 'MF: kapitolní výdaje MPSV', url: MF_FINAL_ACCOUNT_URL },
  social_recipient_metrics: { title: 'ČSSZ a MPSV: počty příjemců', url: 'https://data.cssz.cz/web/otevrena-data/' },
  mv_budget_aggregates: { title: 'MV: rozpočet kapitoly', url: 'https://mv.gov.cz/clanek/rozpocet-ministerstva-vnitra.aspx' },
  mv_police_crime_aggregates: { title: 'ČSÚ / Policie ČR: registrované skutky', url: 'https://data.csu.gov.cz/opendata/sady/KRI10/distribuce/csv' },
  mv_fire_rescue_activity_aggregates: { title: 'HZS: statistická ročenka zásahů', url: 'https://hzscr.gov.cz/hasicien/ViewFile.aspx?docid=22436114' },
  justice_budget_aggregates: { title: 'MSp: rozpočet a závěrečný účet', url: MSP_URL },
  justice_activity_aggregates: { title: 'MSp: soudní a vězeňské statistiky', url: 'https://msp.gov.cz/documents/d/msp/data_soudy_2024-xlsm' },
  transport_budget_entities: { title: 'Monitor MF: MD a SFDI', url: MONITOR_URL },
  transport_sfdi_projects: { title: 'SFDI: projektové čerpání', url: 'https://sfdi.gov.cz' },
  transport_activity_metrics: { title: 'SYDOS, eDalnice a CzechToll: dopravní výkon', url: 'https://www.sydos.cz/cs/rocenka-2024' },
  agriculture_budget_entities: { title: 'Monitor MF: MZe a SZIF', url: MONITOR_URL },
  agriculture_szif_payments: { title: 'SZIF: seznamy příjemců dotací', url: 'https://szif.gov.cz/cs/seznam-prijemcu-dotaci' },
  agriculture_lpis_user_area: { title: 'MZe pLPIS: výměra uživatelů', url: 'https://mze.gov.cz/public/app/eagriapp/LpisData/Cr.aspx' },
  environment_budget_entities: { title: 'Monitor MF: MŽP a SFŽP', url: MONITOR_URL },
  environment_sfzp_supports: { title: 'SFŽP: registr podpor', url: 'https://otevrenadata.sfzp.cz/' },
  mmr_budget_aggregates: { title: 'MMR: otevřená rozpočtová data', url: 'https://mmr.gov.cz/cs/ministerstvo/urad/povinne-zverejnene-informace/otevrena-data-mmr' },
  mmr_irop_operations: { title: 'DotaceEU: IROP seznam operací', url: 'https://www.dotaceeu.cz/cs/informace-o-cerpani/seznamy-prijemcu' },
  mpo_budget_entities: { title: 'Monitor MF: MPO', url: MONITOR_URL },
  mpo_optak_operations: { title: 'DotaceEU: OP TAK seznam operací', url: 'https://www.dotaceeu.cz/cs/statistiky-a-analyzy/seznam-operaci-%28prijemcu%29' },
  mk_budget_entities: { title: 'Monitor MF: MK', url: MONITOR_URL },
  mk_budget_aggregates: { title: 'MK: závěrečný účet kapitoly 334', url: 'https://mk.gov.cz' },
  mk_support_awards: { title: 'MK: výsledky dotačních programů', url: 'https://mk.gov.cz' },
  mk_region_metrics: { title: 'MK: PZAD souhrnné tabulky', url: 'https://mk.gov.cz' },
  mzv_budget_entities: { title: 'Monitor MF: MZV', url: MONITOR_URL },
  mzv_diplomatic_metrics: { title: 'MZV: Česká diplomacie 2024', url: 'https://mzv.gov.cz/jnp/cz/zahranicni_vztahy/vyrocni_zpravy_a_dokumenty/publikace_ceska_diplomacie_2024.html' },
  mzv_aid_operations: { title: 'MZV a ČRA: projektové přehledy pomoci', url: 'https://mzv.gov.cz/jnp/cz/zahranicni_vztahy/rozvojova_spoluprace/koncepce_publikace/vyrocni_prehledy/prehled_rozvojove_spoluprace_a_2.html' },
  mo_budget_entities: { title: 'Monitor MF: MO', url: MONITOR_URL },
  mo_budget_aggregates: { title: 'MO: Fakta a trendy 2025', url: MO_URL },
  mo_personnel_metrics: { title: 'MO: počty vojáků z povolání', url: MO_URL },
  mf_budget_entities: { title: 'Monitor MF: MF, GFŘ a GŘC', url: MONITOR_URL },
  mf_activity_metrics: { title: 'Finanční správa: počty daňových subjektů', url: 'https://www.financnisprava.cz/cs/financni-sprava/zpravy-a-analyzy/vyrocni-zpravy' },
  'atlas.inferred': { title: 'Odvozený výpočet atlasu' },
};

const METRIC_DATASET_KEYS: Record<string, string[]> = {
  health_billed_procedure: ['health_monitor_indicators', 'nrhzs_claims_provider_ico'],
  health_zzs_departure: ['health_monitor_indicators', 'health_zzs_activity_aggregates'],
};

function datasetReference(datasetKey: string, sourceUrl?: string): DatasetReference {
  const source = DATASET_SOURCES[datasetKey];
  return {
    datasetKey,
    title: source?.title ?? datasetKey,
    ...(sourceUrl || source?.url ? { url: sourceUrl ?? source?.url } : {}),
  };
}

export function buildAtlasReferenceSummary(graph: ApiGraph, perUnit: boolean): AtlasReferenceSummary {
  const sourceUrls = new Map<string, string>();
  const datasetKeys = new Set<string>();

  for (const link of graph.links) {
    if (link.sourceDataset) datasetKeys.add(link.sourceDataset);
    if (link.sourceDataset && link.sourceUrl?.startsWith('http')) {
      sourceUrls.set(link.sourceDataset, link.sourceUrl);
    }
    if (!perUnit) continue;
    const metricGroup = normalizationGroup(link);
    for (const datasetKey of metricGroup ? METRIC_DATASET_KEYS[metricGroup] ?? [] : []) {
      datasetKeys.add(datasetKey);
    }
  }

  if (datasetKeys.size > 1) datasetKeys.delete('atlas.inferred');

  return {
    datasets: [...datasetKeys]
      .map((datasetKey) => datasetReference(datasetKey, sourceUrls.get(datasetKey)))
      .sort((left, right) => left.title.localeCompare(right.title, 'cs')),
  };
}
