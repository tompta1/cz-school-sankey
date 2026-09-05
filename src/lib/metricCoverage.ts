import type { ApiGraph } from '../types';
import { normalizationCapacity, normalizationGroup } from './sankeyOrdering';

export function metricCoverage(graph: ApiGraph) {
  const capacities = new Map(graph.nodes.flatMap((node) =>
    typeof node.metadata?.capacity === 'number' ? [[node.id, node.metadata.capacity] as const] : []));
  const links = graph.links.filter((link) => link.amountCzk > 0);
  const available = links.filter((link) => (normalizationCapacity(link, capacities, true) ?? 0) > 0);
  return {
    total: links.length,
    available: available.length,
    missing: links.filter((link) => !available.includes(link)).map((link) => ({
      link,
      reason: normalizationGroup(link)
        ? 'Chybí použitelný údaj pro srovnání tohoto toku.'
        : 'Pro tento souhrn není definována společná srovnávací jednotka.',
    })),
  };
}
