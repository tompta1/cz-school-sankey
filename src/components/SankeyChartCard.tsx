import * as echarts from 'echarts';
import { useEffect, useRef } from 'react';

import { formatCompactCzk, formatPerPupil } from '../lib/format';
import type { HoverInfo, SankeyLink, SankeyNode } from '../types';

interface Props {
  nodes: SankeyNode[];
  links: SankeyLink[];
  prevNodes?: SankeyNode[];
  prevLinks?: SankeyLink[];
  prevActive?: boolean;
  perPupil?: boolean;
  onNodeClick: (nodeId: string) => void;
  onNodeHover: (info: HoverInfo | null) => void;
  onLinkHover: (info: HoverInfo | null) => void;
}

const COLOR: Record<string, string> = {
  state:         '#1d4ed8',
  ministry:      '#2563eb',
  region:        '#7c3aed',
  municipality:  '#0f766e',
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

function buildActiveOption(
  nodes: SankeyNode[],
  links: SankeyLink[],
  idToDisplay: Map<string, string>,
  perPupil: boolean,
  capacityMap: Map<string, number>,
  containerWidth: number,
) {
  const mobile = containerWidth < 640;
  const maxLabelLen = mobile ? 18 : 26;
  const labelFontSize = mobile ? 10 : 11;
  const nodeGap = nodes.length > 35 ? 4 : nodes.length > 20 ? 8 : 14;

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
        return (capB ? b.amountCzk / capB : b.amountCzk) - (capA ? a.amountCzk / capA : a.amountCzk);
      })
    : links;

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
          const amt = perPupil && capacity ? formatPerPupil(amountCzk / capacity) : formatCompactCzk(amountCzk);
          return `<strong>${idToDisplay.get(source) ?? source} → ${idToDisplay.get(target) ?? target}</strong><br/>${amt}`;
        }
        const nodeId = (p.data as { name: string }).name;
        const displayName = idToDisplay.get(nodeId) ?? nodeId;
        const total = links.filter((l) => l.target === nodeId).reduce((s, l) => s + l.amountCzk, 0);
        if (total === 0) return `<strong>${displayName}</strong>`;
        const cap = perPupil ? (capacityMap.get(nodeId) ?? null) : null;
        const totalFmt = cap ? formatPerPupil(total / cap) : formatCompactCzk(total);
        const suffix = cap ? `<br/><small style="color:#94a3b8">${cap} žáků (RSSZ kapacita)</small>` : ' celkový příjem';
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
      data: nodes.map((n) => ({
        name: n.id,
        itemStyle: { color: COLOR[n.category] ?? COLOR.other, borderColor: '#0b1220', borderWidth: 1 },
      })),
      links: orderedLinks.map((l) => {
        const cap = linkCapacity(l);
        return {
          source: l.source,
          target: l.target,
          value: cap ? l.amountCzk / cap : l.amountCzk,
          amountCzk: l.amountCzk,
          capacity: cap,
          lineStyle: { opacity: l.certainty === 'observed' ? 0.55 : 0.28 },
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
  const nodeGap = nodes.length > 35 ? 4 : nodes.length > 20 ? 8 : 14;
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
          value: cap ? l.amountCzk / cap : l.amountCzk,
        };
      }),
    }],
  };
}

export function SankeyChartCard({ nodes, links, prevNodes, prevLinks, prevActive = false, perPupil = false, onNodeClick, onNodeHover, onLinkHover }: Props) {
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
      : buildActiveOption(nodes, links, idToDisplay, perPupil, capacityMap, w) as any;

    chart.setOption(buildOption(el.clientWidth));

    const ro = new ResizeObserver(() => {
      chart.setOption(buildOption(el.clientWidth));
      chart.resize();
    });
    ro.observe(el);
    return () => { ro.disconnect(); chart.dispose(); };
  }, [nodes, links, prevActive, perPupil]);

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

    const buildOption = (w: number) => prevActive
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ? buildActiveOption(prevNodes, prevLinks, idToDisplay, perPupil, capacityMap, w) as any
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      : buildGhostOption(prevNodes, prevLinks, perPupil, capacityMap, w) as any;

    chart.setOption(buildOption(el.clientWidth));

    const ro = new ResizeObserver(() => {
      chart.setOption(buildOption(el.clientWidth));
      chart.resize();
    });
    ro.observe(el);
    return () => { ro.disconnect(); chart.dispose(); };
  }, [prevNodes, prevLinks, prevActive, perPupil]);

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
