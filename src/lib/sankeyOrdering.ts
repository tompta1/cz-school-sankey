import type { SankeyLink, SankeyNode } from '../types';

const NON_NORMALIZABLE_ALLOCATED_FLOW_TYPES = new Set([
  'mv_police_region_allocated_cost',
  'mv_police_crime_class_allocated_cost',
  'mv_fire_rescue_region_allocated_cost',
  'health_outpatient_region_group',
  'health_outpatient_specialty_group',
  'health_outpatient_provider_allocated_cost',
  'mzv_foreign_service_type_allocated',
]);

export function normalizedValue(amountCzk: number, capacity: number | null, perUnit: boolean): number {
  if (!perUnit) return amountCzk;
  if (!capacity || capacity <= 0) return 0;
  return amountCzk / capacity;
}

export function normalizationCapacity(
  link: SankeyLink,
  capacityMap: Map<string, number>,
  perUnit: boolean,
): number | null {
  if (!perUnit) return null;
  if (NON_NORMALIZABLE_ALLOCATED_FLOW_TYPES.has(link.flowType)) return null;
  if (!normalizationGroup(link)) return null;
  if (link.institutionId) return capacityMap.get(link.institutionId) ?? null;
  return capacityMap.get(link.target) ?? capacityMap.get(link.source) ?? null;
}

export function normalizationGroup(link: SankeyLink): string | null {
  if (
    link.flowType === 'direct_school_finance' ||
    link.flowType === 'school_expenditure' ||
    link.flowType === 'founder_support' ||
    link.flowType === 'eu_project_support' ||
    link.flowType === 'state_to_founders'
  ) {
    return 'school_pupil';
  }

  if (
    link.flowType === 'mv_police_region_allocated_cost' ||
    link.flowType === 'mv_police_crime_class_allocated_cost'
  ) {
    return 'police_registered_case';
  }

  if (link.flowType === 'mv_fire_rescue_region_allocated_cost') {
    return 'fire_rescue_intervention';
  }

  if (link.flowType === 'transport_rail_branch') {
    return 'transport_rail_passenger';
  }

  if (link.flowType === 'transport_road_vignette_branch') {
    return 'transport_vignette_sale';
  }

  if (link.flowType === 'transport_road_toll_branch') {
    return 'transport_toll_vehicle';
  }

  if (link.flowType === 'transport_sfdi_investor' || link.flowType === 'transport_sfdi_project') {
    return 'transport_project_count';
  }

  if (link.flowType === 'mzv_foreign_service_branch') {
    return 'mzv_foreign_post';
  }

  if (
    link.flowType === 'mzv_aid_branch' ||
    link.flowType === 'mzv_aid_country' ||
    link.flowType === 'mzv_aid_project'
  ) {
    return 'mzv_aid_project';
  }

  if (
    link.flowType === 'agriculture_subsidy_family_recipient'
  ) {
    return 'agriculture_subsidy_recipient';
  }

  if (link.flowType === 'agriculture_subsidy_family_area') {
    return 'agriculture_area_hectare';
  }

  if (
    link.flowType === 'agriculture_subsidy_recipient_detail' ||
    link.flowType === 'agriculture_subsidy_recipient_page'
  ) {
    if (link.source === 'agriculture:subsidy:family:area') return 'agriculture_area_hectare';
    return 'agriculture_subsidy_recipient';
  }

  if (
    link.flowType === 'environment_support_family_allocated' ||
    link.flowType === 'environment_support_recipient_allocated' ||
    link.flowType === 'environment_support_recipient_page'
  ) {
    return 'environment_support_recipient';
  }

  if (
    (link.flowType === 'mmr_budget_group' &&
      (link.target === 'mmr:branch:regional' || link.target === 'mmr:branch:housing')) ||
    link.flowType === 'mmr_irop_region_allocated' ||
    link.flowType === 'mmr_irop_recipient_allocated' ||
    link.flowType === 'mmr_irop_recipient_page'
  ) {
    return 'mmr_support_recipient';
  }

  if (
    link.flowType === 'mpo_optak_support_branch' ||
    link.flowType === 'mpo_optak_region_allocated' ||
    link.flowType === 'mpo_optak_recipient_allocated' ||
    link.flowType === 'mpo_optak_recipient_page'
  ) {
    return 'mpo_support_recipient';
  }

  if (
    link.flowType === 'mk_support_program' ||
    link.flowType === 'mk_support_recipient' ||
    link.flowType === 'mk_support_region' ||
    link.flowType === 'mk_support_page'
  ) {
    return 'mk_support_recipient';
  }

  if (link.flowType === 'justice_branch_cost') {
    if (link.target === 'justice:courts') return 'justice_resolved_case';
    if (link.target === 'justice:prison-service') return 'justice_inmate';
    return null;
  }

  if (link.flowType === 'social_benefit_group') {
    if (link.target === 'social:benefit:pensions') return 'social_pension_recipient';
    if (link.target === 'social:benefit:unemployment') return 'social_unemployment_recipient';
    if (link.target === 'social:benefit:care-allowance') return 'social_care_recipient';
    if (link.target === 'social:benefit:substitute-alimony') return 'social_substitute_alimony_recipient';
    return null;
  }

  return null;
}

export function comparableNodeMetric(
  nodeId: string,
  links: SankeyLink[],
  capacityMap: Map<string, number>,
  perUnit: boolean,
): { totalAmount: number; totalCapacity: number; group: string } | null {
  if (!perUnit) return null;

  const incomingLinks = links.filter((link) => link.target === nodeId);
  if (!incomingLinks.length) return null;

  let metricGroup: string | null = null;
  let totalAmount = 0;
  let totalCapacity = 0;

  for (const link of incomingLinks) {
    const capacity = normalizationCapacity(link, capacityMap, true);
    const group = normalizationGroup(link);
    if (!capacity || !group) return null;
    if (metricGroup && metricGroup !== group) return null;
    metricGroup = group;
    totalAmount += link.amountCzk;
    totalCapacity += capacity;
  }

  if (!metricGroup || totalCapacity <= 0) return null;
  return { totalAmount, totalCapacity, group: metricGroup };
}

function linkWeight(
  link: SankeyLink,
  capacityMap: Map<string, number>,
  perUnit: boolean,
): number {
  const capacity = normalizationCapacity(link, capacityMap, perUnit);
  return normalizedValue(link.amountCzk, capacity, perUnit);
}

export function normalizedNodeWeight(
  nodeId: string,
  links: SankeyLink[],
  capacityMap: Map<string, number>,
  perUnit: boolean,
): number {
  const incoming = links
    .filter((link) => link.target === nodeId)
    .reduce((sum, link) => sum + linkWeight(link, capacityMap, perUnit), 0);
  const outgoing = links
    .filter((link) => link.source === nodeId)
    .reduce((sum, link) => sum + linkWeight(link, capacityMap, perUnit), 0);
  return Math.max(incoming, outgoing);
}

export function orderSankeyGraph(
  nodes: SankeyNode[],
  links: SankeyLink[],
  capacityMap: Map<string, number>,
  perUnit: boolean,
): { orderedNodes: SankeyNode[]; orderedLinks: SankeyLink[] } {
  const orderedLinks = [...links].sort((a, b) => {
    const diff = linkWeight(b, capacityMap, perUnit) - linkWeight(a, capacityMap, perUnit);
    if (diff !== 0) return diff;
    return b.amountCzk - a.amountCzk || a.target.localeCompare(b.target, 'cs');
  });

  const orderedNodes = [...nodes].sort((a, b) => {
    if (a.level !== b.level) return a.level - b.level;
    return (
      normalizedNodeWeight(b.id, orderedLinks, capacityMap, perUnit) -
        normalizedNodeWeight(a.id, orderedLinks, capacityMap, perUnit) ||
      a.name.localeCompare(b.name, 'cs')
    );
  });

  return { orderedNodes, orderedLinks };
}

export function chartValuesForLinks(
  links: SankeyLink[],
  capacityMap: Map<string, number>,
  perUnit: boolean,
): number[] {
  const normalized = links.map((link) => linkWeight(link, capacityMap, perUnit));
  if (!perUnit) return normalized;
  if (normalized.some((value) => value > 0)) return normalized;
  return links.map((link) => link.amountCzk);
}
