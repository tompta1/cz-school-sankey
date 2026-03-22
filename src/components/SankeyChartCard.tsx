import * as echarts from 'echarts';
import { useEffect, useRef } from 'react';

import { formatCompactCzk, formatInteger, formatPerPupil, formatPerUnit } from '../lib/format';
import type { HoverInfo, SankeyLink, SankeyNode } from '../types';

interface Props {
  nodes: SankeyNode[];
  links: SankeyLink[];
  prevNodes?: SankeyNode[];
  prevLinks?: SankeyLink[];
  prevActive?: boolean;
  perPupil?: boolean;
  perUnitLabel?: string;
  unitCountLabel?: string;
  totalAmountLabel?: string;
  onNodeClick: (nodeId: string) => void;
  onNodeHover: (info: HoverInfo | null) => void;
  onLinkHover: (info: HoverInfo | null) => void;
}

const COLOR: Record<string, string> = {
  state:         '#1d4ed8',
  ministry:      '#2563eb',
  region:        '#7c3aed',
  municipality:  '#0f766e',
  health_system: '#0f766e',
  health_provider:'#f97316',
  eu_programme:  '#0ea5e9',
  eu_project:    '#06b6d4',
  school_entity: '#f59e0b',
  cost_bucket:   '#ef4444',
  other:         '#6b7280',
};

function chartLayout(containerWidth: number) {
  if (containerWidth < 480) return { top: 16, right: 68, bottom: 16, left: 68 };
  if (containerWidth < 768) return { top: 18, right: 110, bottom: 18, left: 110 };
  return { top: 20, right: 200, bottom: 20, left: 200 };
}

function nodeGapForGraph(nodes: SankeyNode[], mobile: boolean): number {
  const providerLikeCount = nodes.filter((node) => node.category === 'health_provider' || node.category === 'school_entity').length;
  const maxLevel = nodes.reduce((max, node) => Math.max(max, node.level), 0);

  if (providerLikeCount >= 28) return mobile ? 8 : 10;
  if (providerLikeCount >= 16) return mobile ? 10 : 12;
  if (maxLevel <= 2 && nodes.length <= 18) return mobile ? 16 : 26;
  if (maxLevel <= 3 && nodes.length <= 24) return mobile ? 14 : 22;
  if (nodes.length > 35) return mobile ? 9 : 12;
  return mobile ? 12 : 18;
}

function maxLabelLengthForGraph(nodes: SankeyNode[], mobile: boolean): number {
  const providerLikeCount = nodes.filter((node) => node.category === 'health_provider' || node.category === 'school_entity').length;
  const maxLevel = nodes.reduce((max, node) => Math.max(max, node.level), 0);

  if (providerLikeCount >= 28) return mobile ? 16 : 22;
  if (providerLikeCount >= 16) return mobile ? 17 : 24;
  if (maxLevel <= 2 && nodes.length <= 18) return mobile ? 24 : 42;
  if (maxLevel <= 3 && nodes.length <= 24) return mobile ? 22 : 34;
  if (nodes.length > 35) return mobile ? 17 : 24;
  return mobile ? 20 : 28;
}

function normalizedValue(amountCzk: number, capacity: number | null, perUnit: boolean): number {
  if (!perUnit) return amountCzk;
  if (!capacity || capacity <= 0) return 0;
  return amountCzk / capacity;
}

function unavailableMetricMarkup(perUnitLabel: string): string {
  return `N/A<br/><small style="color:#94a3b8">Metoda ${perUnitLabel} není pro tento tok k dispozici</small>`;
}

function normalizedNodeWeight(
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

function buildActiveOption(
  nodes: SankeyNode[],
  links: SankeyLink[],
  idToDisplay: Map<string, string>,
  perPupil: boolean,
  capacityMap: Map<string, number>,
  containerWidth: number,
  perUnitLabel: string,
  unitCountLabel: string,
  totalAmountLabel: string,
) {
  const mobile = containerWidth < 640;
  const maxLabelLen = maxLabelLengthForGraph(nodes, mobile);
  const labelFontSize = mobile ? 10 : 11;
  const nodeGap = nodeGapForGraph(nodes, mobile);

  const linkCapacity = (l: SankeyLink): number | null => {
    if (!perPupil) return null;
    if (l.institutionId) return capacityMap.get(l.institutionId) ?? null;
    // Aggregate links (no institutionId): scale by the region node's total capacity
    return capacityMap.get(l.target) ?? capacityMap.get(l.source) ?? null;
  };

  // In per-pupil mode sort links descending by Kč/žák so higher-value flows
  // appear at the top and ECharts orders nodes accordingly.
  const orderedLinks = perPupil
    ? [...links].sort((a, b) => {
        const capA = linkCapacity(a);
        const capB = linkCapacity(b);
        return normalizedValue(b.amountCzk, capB, true) - normalizedValue(a.amountCzk, capA, true);
      })
    : links;
  const orderedNodes = perPupil
    ? [...nodes].sort((a, b) => normalizedNodeWeight(b.id, orderedLinks, capacityMap, true) - normalizedNodeWeight(a.id, orderedLinks, capacityMap, true))
    : nodes;

  return {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'item',
      confine: true,
      formatter(params: unknown) {
        const p = params as { dataType: string; data: Record<string, unknown> };
        if (p.dataType === 'edge') {
          const { source, target, amountCzk, capacity } =
            p.data as { source: string; target: string; amountCzk: number; capacity: number | null };
          const amt = perPupil
            ? capacity
              ? (perUnitLabel === 'žák/rok' ? formatPerPupil(amountCzk / capacity) : formatPerUnit(amountCzk / capacity, perUnitLabel))
              : unavailableMetricMarkup(perUnitLabel)
            : formatCompactCzk(amountCzk);
          return `<strong>${idToDisplay.get(source) ?? source} → ${idToDisplay.get(target) ?? target}</strong><br/>${amt}`;
        }
        const nodeId = (p.data as { name: string }).name;
        const displayName = idToDisplay.get(nodeId) ?? nodeId;
        const total = links.filter((l) => l.target === nodeId).reduce((s, l) => s + l.amountCzk, 0);
        if (total === 0) return `<strong>${displayName}</strong>`;
        const cap = perPupil ? (capacityMap.get(nodeId) ?? null) : null;
        const totalFmt = perPupil
          ? cap
            ? (perUnitLabel === 'žák/rok' ? formatPerPupil(total / cap) : formatPerUnit(total / cap, perUnitLabel))
            : 'N/A'
          : formatCompactCzk(total);
        const suffix = perPupil
          ? cap
            ? `<br/><small style="color:#94a3b8">${formatInteger(cap)} ${unitCountLabel}</small>`
            : `<br/><small style="color:#94a3b8">Metoda ${perUnitLabel} není pro tento uzel k dispozici</small>`
          : ` ${totalAmountLabel}`;
        return `<strong>${displayName}</strong><br/>${totalFmt}${suffix}`;
      },
      backgroundColor: 'rgba(8,16,30,0.92)',
      borderColor: 'rgba(148,163,184,0.2)',
      textStyle: { color: '#e5eefc', fontSize: 13 },
      extraCssText: 'backdrop-filter:blur(8px); border-radius:10px; padding:10px 14px;',
    },
    series: [{
      type: 'sankey',
      ...chartLayout(containerWidth),
      nodeWidth: mobile ? 14 : 18,
      nodeGap,
      layoutIterations: 64,
      draggable: !mobile,
      nodeAlign: 'justify',
      emphasis: { focus: 'adjacency', lineStyle: { opacity: 0.9 } },
      blur: { itemStyle: { opacity: 0.08 }, lineStyle: { opacity: 0.05 } },
      lineStyle: { color: 'source', opacity: 0.45, curveness: 0.5 },
      label: {
        color: '#cbd5e1',
        fontSize: labelFontSize,
        fontFamily: 'Inter, ui-sans-serif, system-ui, sans-serif',
        formatter: (params: { name: string }) => {
          const name = idToDisplay.get(params.name) ?? params.name;
          return name.length > maxLabelLen ? name.slice(0, maxLabelLen - 2) + '…' : name;
        },
      },
      data: orderedNodes.map((n) => ({
        name: n.id,
        itemStyle: { color: COLOR[n.category] ?? COLOR.other, borderColor: '#0b1220', borderWidth: 1 },
      })),
      links: orderedLinks.map((l) => {
        const cap = linkCapacity(l);
        return {
          source: l.source,
          target: l.target,
          value: normalizedValue(l.amountCzk, cap, perPupil),
          amountCzk: l.amountCzk,
          capacity: cap,
          lineStyle: { opacity: perPupil && !cap ? 0.08 : l.certainty === 'observed' ? 0.55 : 0.28 },
        };
      }),
    }],
  };
}

function buildGhostOption(
  nodes: SankeyNode[],
  links: SankeyLink[],
  perPupil: boolean,
  capacityMap: Map<string, number>,
  containerWidth: number,
) {
  const mobile = containerWidth < 640;
  const nodeGap = nodeGapForGraph(nodes, mobile);
  return {
    backgroundColor: 'transparent',
    series: [{
      type: 'sankey',
      ...chartLayout(containerWidth),
      nodeWidth: mobile ? 14 : 18,
      nodeGap,
      layoutIterations: 64,
      draggable: false,
      nodeAlign: 'justify',
      silent: true,
      label: { show: false },
      lineStyle: { color: '#94a3b8', opacity: 0.15, curveness: 0.5 },
      data: nodes.map((n) => ({
        name: n.id,
        itemStyle: { color: '#94a3b8', opacity: 0.22, borderWidth: 0 },
      })),
      links: links.map((l) => {
        const cap = perPupil
          ? (l.institutionId ? capacityMap.get(l.institutionId) : capacityMap.get(l.target) ?? capacityMap.get(l.source)) ?? null
          : null;
        return {
          source: l.source,
          target: l.target,
          value: normalizedValue(l.amountCzk, cap, perPupil),
        };
      }),
    }],
  };
}

export function SankeyChartCard({
  nodes,
  links,
  prevNodes,
  prevLinks,
  prevActive = false,
  perPupil = false,
  perUnitLabel = 'žák/rok',
  unitCountLabel = 'žáků (RSSZ kapacita)',
  totalAmountLabel = 'celkový příjem',
  onNodeClick,
  onNodeHover,
  onLinkHover,
}: Props) {
  const curRef  = useRef<HTMLDivElement>(null);
  const prevRef = useRef<HTMLDivElement>(null);

  const onNodeClickRef = useRef(onNodeClick);
  const onNodeHoverRef = useRef(onNodeHover);
  const onLinkHoverRef = useRef(onLinkHover);
  onNodeClickRef.current = onNodeClick;
  onNodeHoverRef.current = onNodeHover;
  onLinkHoverRef.current = onLinkHover;

  // Current-year chart
  useEffect(() => {
    const el = curRef.current;
    if (!el) return;
    const idToDisplay = new Map(nodes.map((n) => [n.id, n.name]));
    const capacityMap = new Map(
      nodes
        .filter((n) => typeof n.metadata?.capacity === 'number')
        .map((n) => [n.id, n.metadata!.capacity as number]),
    );
    const chart = echarts.init(el, undefined, { renderer: 'canvas' });

    if (!prevActive) {
      chart.on('click', (params) => {
        if (params.dataType === 'node')
          onNodeClickRef.current((params.data as { name: string }).name);
      });
      chart.on('mouseover', (params) => {
        if (params.dataType === 'node') {
          onNodeHoverRef.current({ label: idToDisplay.get((params.data as { name: string }).name) ?? '', amount: null, isLink: false });
        } else if (params.dataType === 'edge') {
          const e = params.data as { source: string; target: string; amountCzk: number };
          onLinkHoverRef.current({ label: `${idToDisplay.get(e.source) ?? e.source} → ${idToDisplay.get(e.target) ?? e.target}`, amount: e.amountCzk, isLink: true });
        }
      });
      chart.on('mouseout', () => onNodeHoverRef.current(null));
    }

    const buildOption = (w: number) => prevActive
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ? buildGhostOption(nodes, links, perPupil, capacityMap, w) as any
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      : buildActiveOption(nodes, links, idToDisplay, perPupil, capacityMap, w, perUnitLabel, unitCountLabel, totalAmountLabel) as any;

    chart.setOption(buildOption(el.clientWidth));

    const ro = new ResizeObserver(() => {
      chart.setOption(buildOption(el.clientWidth));
      chart.resize();
    });
    ro.observe(el);
    return () => { ro.disconnect(); chart.dispose(); };
  }, [nodes, links, prevActive, perPupil, perUnitLabel, unitCountLabel, totalAmountLabel]);

  // Previous-year chart
  useEffect(() => {
    const el = prevRef.current;
    if (!el || !prevNodes || !prevLinks) return;
    const idToDisplay = new Map(prevNodes.map((n) => [n.id, n.name]));
    const capacityMap = new Map(
      prevNodes
        .filter((n) => typeof n.metadata?.capacity === 'number')
        .map((n) => [n.id, n.metadata!.capacity as number]),
    );
    const chart = echarts.init(el, undefined, { renderer: 'canvas' });

    if (prevActive) {
      chart.on('click', (params) => {
        if (params.dataType === 'node')
          onNodeClickRef.current((params.data as { name: string }).name);
      });
      chart.on('mouseover', (params) => {
        if (params.dataType === 'node') {
          onNodeHoverRef.current({ label: idToDisplay.get((params.data as { name: string }).name) ?? '', amount: null, isLink: false });
        } else if (params.dataType === 'edge') {
          const e = params.data as { source: string; target: string; amountCzk: number };
          onLinkHoverRef.current({ label: `${idToDisplay.get(e.source) ?? e.source} → ${idToDisplay.get(e.target) ?? e.target}`, amount: e.amountCzk, isLink: true });
        }
      });
      chart.on('mouseout', () => onNodeHoverRef.current(null));
    }

    const buildOption = (w: number) => {
      if (prevActive) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return buildActiveOption(prevNodes, prevLinks, idToDisplay, perPupil, capacityMap, w, perUnitLabel, unitCountLabel, totalAmountLabel) as any;
      }
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return buildGhostOption(prevNodes, prevLinks, perPupil, capacityMap, w) as any;
    };

    chart.setOption(buildOption(el.clientWidth));

    const ro = new ResizeObserver(() => {
      chart.setOption(buildOption(el.clientWidth));
      chart.resize();
    });
    ro.observe(el);
    return () => { ro.disconnect(); chart.dispose(); };
  }, [prevNodes, prevLinks, prevActive, perPupil, perUnitLabel, unitCountLabel, totalAmountLabel]);

  const curHeight  = Math.min(Math.max(500, nodes.length * 38), 16_000);
  const prevHeight = prevNodes ? Math.min(Math.max(500, prevNodes.length * 38), 16_000) : 0;
  const totalHeight = Math.max(curHeight, prevHeight);

  // Active canvas on top (rendered last = higher z-index)
  const ghostDiv  = (ref: React.RefObject<HTMLDivElement | null>) =>
    <div ref={ref} style={{ position: 'absolute', inset: 0, pointerEvents: 'none' }} />;
  const activeDiv = (ref: React.RefObject<HTMLDivElement | null>) =>
    <div ref={ref} style={{ position: 'absolute', inset: 0 }} />;

  return (
    <div style={{ position: 'relative', width: '100%', height: totalHeight, background: '#08101e' }}>
      {prevActive ? (
        <>
          {ghostDiv(curRef)}
          {prevNodes && prevLinks && activeDiv(prevRef)}
        </>
      ) : (
        <>
          {prevNodes && prevLinks && ghostDiv(prevRef)}
          {activeDiv(curRef)}
        </>
      )}
    </div>
  );
}
