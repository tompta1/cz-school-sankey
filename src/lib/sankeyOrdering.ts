import type { SankeyLink, SankeyNode } from '../types';

const NON_NORMALIZABLE_ALLOCATED_FLOW_TYPES = new Set([
  'mv_police_region_allocated_cost',
  'mv_police_crime_class_allocated_cost',
  'mv_fire_rescue_region_allocated_cost',
  'health_outpatient_region_group',
  'health_outpatient_specialty_group',
  'health_outpatient_provider_allocated_cost',
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
  if (link.institutionId) return capacityMap.get(link.institutionId) ?? null;
  return capacityMap.get(link.target) ?? capacityMap.get(link.source) ?? null;
}

export function normalizedNodeWeight(
  nodeId: string,
  links: SankeyLink[],
  capacityMap: Map<string, number>,
  perUnit: boolean,
): number {
  const incoming = links
    .filter((link) => link.target === nodeId)
    .reduce((sum, link) => {
      const capacity = normalizationCapacity(link, capacityMap, perUnit);
      return sum + normalizedValue(link.amountCzk, capacity, perUnit);
    }, 0);
  const outgoing = links
    .filter((link) => link.source === nodeId)
    .reduce((sum, link) => {
      const capacity = normalizationCapacity(link, capacityMap, perUnit);
      return sum + normalizedValue(link.amountCzk, capacity, perUnit);
    }, 0);
  return Math.max(incoming, outgoing);
}

export function orderSankeyGraph(
  nodes: SankeyNode[],
  links: SankeyLink[],
  capacityMap: Map<string, number>,
  perUnit: boolean,
): { orderedNodes: SankeyNode[]; orderedLinks: SankeyLink[] } {
  const orderedLinks = perUnit
    ? [...links].sort((a, b) => {
        const capA = normalizationCapacity(a, capacityMap, true);
        const capB = normalizationCapacity(b, capacityMap, true);
        return normalizedValue(b.amountCzk, capB, true) - normalizedValue(a.amountCzk, capA, true);
      })
    : links;

  const orderedNodes = perUnit
    ? [...nodes].sort((a, b) => {
        if (a.level !== b.level) return a.level - b.level;
        return (
          normalizedNodeWeight(b.id, orderedLinks, capacityMap, true) -
            normalizedNodeWeight(a.id, orderedLinks, capacityMap, true) ||
          a.name.localeCompare(b.name, 'cs')
        );
      })
    : nodes;

  return { orderedNodes, orderedLinks };
}
