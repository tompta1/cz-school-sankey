import type { SankeyLink, SankeyNode } from '../types';

export function normalizedValue(amountCzk: number, capacity: number | null, perUnit: boolean): number {
  if (!perUnit) return amountCzk;
  if (!capacity || capacity <= 0) return 0;
  return amountCzk / capacity;
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
      const capacity = link.institutionId
        ? capacityMap.get(link.institutionId) ?? null
        : capacityMap.get(link.target) ?? capacityMap.get(link.source) ?? null;
      return sum + normalizedValue(link.amountCzk, capacity, perUnit);
    }, 0);
  const outgoing = links
    .filter((link) => link.source === nodeId)
    .reduce((sum, link) => {
      const capacity = link.institutionId
        ? capacityMap.get(link.institutionId) ?? null
        : capacityMap.get(link.target) ?? capacityMap.get(link.source) ?? null;
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
  const linkCapacity = (link: SankeyLink): number | null => {
    if (!perUnit) return null;
    if (link.institutionId) return capacityMap.get(link.institutionId) ?? null;
    return capacityMap.get(link.target) ?? capacityMap.get(link.source) ?? null;
  };

  const orderedLinks = perUnit
    ? [...links].sort((a, b) => {
        const capA = linkCapacity(a);
        const capB = linkCapacity(b);
        return normalizedValue(b.amountCzk, capB, true) - normalizedValue(a.amountCzk, capA, true);
      })
    : links;

  const orderedNodes = perUnit
    ? [...nodes].sort(
        (a, b) =>
          normalizedNodeWeight(b.id, orderedLinks, capacityMap, true) -
          normalizedNodeWeight(a.id, orderedLinks, capacityMap, true),
      )
    : nodes;

  return { orderedNodes, orderedLinks };
}
