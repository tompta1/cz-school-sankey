import { useEffect, useMemo, useRef, useState } from 'react';

import { SankeyChartCard } from './components/SankeyChartCard';
import { aggregateGraph, drillIntoNode, PREV_WINDOW_ID, NEXT_WINDOW_ID, TOP_SCHOOLS } from './lib/graph';
import { formatCompactCzk } from './lib/format';
import type { DrilldownEntry, HoverInfo, InstitutionSummary, Manifest, YearDataset } from './types';

function normalize(s: string): string {
  return s.normalize('NFD').replace(/\p{Diacritic}/gu, '').toLowerCase();
}

const MAX_RESULTS = 8;

export default function App() {
  const [manifest, setManifest] = useState<Manifest | null>(null);
  const [dataset, setDataset] = useState<YearDataset | null>(null);
  const [prevDataset, setPrevDataset] = useState<YearDataset | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [drilldownStack, setDrilldownStack] = useState<DrilldownEntry[]>([]);
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);
  const [prevActive, setPrevActive] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchOpen, setSearchOpen] = useState(false);
  const searchRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let active = true;
    fetch('./data/manifest.json')
      .then((r) => { if (!r.ok) throw new Error(`${r.status}`); return r.json(); })
      .then((m: Manifest) => {
        if (!active) return;
        setManifest(m);
        // Load the two most recent years
        const sorted = [...m.years].sort((a, b) => a.year - b.year);
        const cur = sorted.at(-1);
        const prev = sorted.at(-2);
        if (!cur) return;

        const load = (file: string) =>
          fetch(file).then((r) => { if (!r.ok) throw new Error(`${r.status}`); return r.json(); });

        setLoading(true);
        load(cur.file)
          .then((d: YearDataset) => { if (active) { setDataset(d); setError(null); } })
          .catch((e) => { if (active) setError(String(e)); })
          .finally(() => { if (active) setLoading(false); });

        if (prev) {
          load(prev.file)
            .then((d: YearDataset) => { if (active) setPrevDataset(d); })
            .catch(() => { if (active) setPrevDataset(null); });
        }
      })
      .catch((e) => { if (active) setError(String(e)); });
    return () => { active = false; };
  }, []);

  useEffect(() => {
    function onPointerDown(e: PointerEvent) {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) {
        setSearchOpen(false);
      }
    }
    document.addEventListener('pointerdown', onPointerDown);
    return () => document.removeEventListener('pointerdown', onPointerDown);
  }, []);

  const currentEntry  = drilldownStack.at(-1);
  const currentNodeId = currentEntry?.nodeId ?? null;
  const currentOffset = currentEntry?.offset ?? 0;

  const graph = useMemo(() => {
    if (!dataset) return null;
    return currentNodeId ? drillIntoNode(dataset, currentNodeId, currentOffset) : aggregateGraph(dataset);
  }, [dataset, currentNodeId, currentOffset]);

  const prevGraph = useMemo(() => {
    if (!prevDataset) return null;
    return currentNodeId ? drillIntoNode(prevDataset, currentNodeId, currentOffset) : aggregateGraph(prevDataset);
  }, [prevDataset, currentNodeId, currentOffset]);

  const searchResults = useMemo<InstitutionSummary[]>(() => {
    if (!dataset || searchQuery.trim().length < 2) return [];
    const q = normalize(searchQuery.trim());
    const results: InstitutionSummary[] = [];
    for (const inst of dataset.institutions) {
      if (results.length >= MAX_RESULTS) break;
      if (normalize(`${inst.name} ${inst.municipality ?? ''} ${inst.region ?? ''}`).includes(q))
        results.push(inst);
    }
    return results;
  }, [dataset, searchQuery]);

  function handleNodeClick(nodeId: string) {
    if (nodeId === PREV_WINDOW_ID || nodeId === NEXT_WINDOW_ID) {
      setDrilldownStack((prev) => {
        const last = prev.at(-1);
        if (!last) return prev;
        const PAGE = nodeId === PREV_WINDOW_ID ? -TOP_SCHOOLS : TOP_SCHOOLS;
        const newOffset = Math.max(0, (last.offset ?? 0) + PAGE);
        return [...prev.slice(0, -1), { ...last, offset: newOffset }];
      });
      return;
    }
    if (nodeId.startsWith('synthetic:')) return;
    const node = graph?.nodes.find((n) => n.id === nodeId) ?? dataset?.nodes.find((n) => n.id === nodeId);
    if (!node || node.category === 'state' || node.category === 'ministry') return;
    setDrilldownStack((prev) => [...prev, { nodeId, label: node.name }]);
  }

  function handleBack() {
    setDrilldownStack((prev) => prev.slice(0, -1));
  }

  function handleSearchSelect(inst: InstitutionSummary) {
    setSearchQuery('');
    setSearchOpen(false);
    setDrilldownStack([{ nodeId: inst.id, label: inst.name }]);
  }

  if (loading && !dataset) return <div className="centered">Loading…</div>;
  if (error && !dataset) return <div className="centered error">Error: {error}</div>;
  if (!manifest || !dataset || !graph) return <div className="centered">No data.</div>;

  const curYear  = dataset.year;
  const prevYear = prevDataset?.year;
  const backLabel = drilldownStack.length > 1
    ? drilldownStack.at(-2)!.label
    : dataset.title;

  return (
    <>
      <div className="topbar">
        <div className="topbar__left">
          {drilldownStack.length > 0 ? (
            <button className="back-btn" onClick={handleBack}>← {backLabel}</button>
          ) : (
            <span className="topbar__title">Czech school finance</span>
          )}
        </div>

        <div className="topbar__hover">
          {hoverInfo ? (
            <>
              <span className="topbar__hover-label">{hoverInfo.label}</span>
              {hoverInfo.amount !== null && (
                <span className="topbar__hover-amount">{formatCompactCzk(hoverInfo.amount)}</span>
              )}
            </>
          ) : drilldownStack.length > 0 ? (
            <span className="topbar__hover-context">
              {drilldownStack.map((e) => e.label).join(' › ')}
            </span>
          ) : null}
          {prevGraph && prevYear && (
            <button className="year-toggle-btn" onClick={() => setPrevActive((v) => !v)}>
              {prevActive ? prevYear : curYear}
            </button>
          )}
        </div>

        <div className="topbar__right">
          <div className="search-wrap" ref={searchRef}>
            <input
              className="search-input"
              type="search"
              placeholder="Search school…"
              value={searchQuery}
              onChange={(e) => { setSearchQuery(e.target.value); setSearchOpen(true); }}
              onFocus={() => setSearchOpen(true)}
            />
            {searchOpen && searchResults.length > 0 && (
              <ul className="search-dropdown">
                {searchResults.map((inst) => (
                  <li key={inst.id} className="search-result" onPointerDown={() => handleSearchSelect(inst)}>
                    <span className="search-result__name">{inst.name}</span>
                    <span className="search-result__meta">
                      {[inst.municipality, inst.region].filter(Boolean).join(', ')}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
      </div>

      <div className="chart-area">
        <SankeyChartCard
          nodes={graph.nodes}
          links={graph.links}
          prevNodes={prevGraph?.nodes}
          prevLinks={prevGraph?.links}
          prevActive={prevActive}
          onNodeClick={handleNodeClick}
          onNodeHover={setHoverInfo}
          onLinkHover={setHoverInfo}
        />
      </div>
    </>
  );
}
