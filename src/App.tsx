import { useEffect, useRef, useState } from 'react';

import { SankeyChartCard } from './components/SankeyChartCard';
import {
  EU_ALL_ID,
  FOUNDERS_KRAJ,
  FOUNDERS_OBEC,
  NEXT_WINDOW_ID,
  PREV_WINDOW_ID,
  TOP_FOUNDERS,
  TOP_SCHOOLS,
} from './lib/graph';
import { formatCompactCzk } from './lib/format';
import type { ApiGraph, ApiYearsResponse, DrilldownEntry, HoverInfo, InstitutionSummary } from './types';

const ROOT_TITLE = 'Finance českého školství';

const MAX_RESULTS = 8;
const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? '').replace(/\/$/, '');

function buildApiUrl(path: string): string {
  return API_BASE_URL ? `${API_BASE_URL}${path}` : path;
}

async function fetchJson<T>(url: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(url, { signal });
  if (!response.ok) {
    throw new Error(`${response.status}`);
  }
  return response.json() as Promise<T>;
}

function buildGraphUrl(year: number, nodeId: string | null, offset: number): string {
  const params = new URLSearchParams({ year: String(year) });
  if (!nodeId) return `/api/graph/overview?${params.toString()}`;

  if (nodeId === EU_ALL_ID) {
    return `/api/graph/eu?${params.toString()}`;
  }
  if (nodeId === FOUNDERS_KRAJ || nodeId === FOUNDERS_OBEC) {
    params.set('founderType', nodeId === FOUNDERS_KRAJ ? 'kraj' : 'obec');
    params.set('offset', String(offset));
    return `/api/graph/founders?${params.toString()}`;
  }
  if (nodeId.startsWith('region:')) {
    params.set('region', nodeId.replace('region:', ''));
    params.set('offset', String(offset));
    return `/api/graph/region?${params.toString()}`;
  }
  if (nodeId.startsWith('founder:')) {
    params.set('founderId', nodeId);
    params.set('offset', String(offset));
    return `/api/graph/founder?${params.toString()}`;
  }

  params.set('nodeId', nodeId);
  params.set('offset', String(offset));
  return `/api/graph/node?${params.toString()}`;
}

function pageSizeForNode(nodeId: string | null): number {
  if (!nodeId) return TOP_SCHOOLS;
  if (nodeId === FOUNDERS_KRAJ || nodeId === FOUNDERS_OBEC || nodeId.startsWith('region:')) {
    return TOP_FOUNDERS;
  }
  return TOP_SCHOOLS;
}

export default function App() {
  const [currentYear, setCurrentYear] = useState<number | null>(null);
  const [previousYear, setPreviousYear] = useState<number | null>(null);
  const [graph, setGraph] = useState<ApiGraph | null>(null);
  const [prevGraph, setPrevGraph] = useState<ApiGraph | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [drilldownStack, setDrilldownStack] = useState<DrilldownEntry[]>([]);
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);
  const [prevActive, setPrevActive] = useState(false);
  const [perPupil, setPerPupil] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<InstitutionSummary[]>([]);
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchExpanded, setSearchExpanded] = useState(false);
  const searchRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let active = true;
    fetchJson<ApiYearsResponse>(buildApiUrl('/api/years'))
      .then((response) => {
        if (!active) return;
        const sorted = [...response.years].sort((a, b) => a.year - b.year);
        const cur = sorted.at(-1)?.year ?? null;
        const prev = sorted.at(-2)?.year ?? null;
        setCurrentYear(cur);
        setPreviousYear(prev);
        setError(null);
      })
      .catch((e) => {
        if (active) {
          setError(String(e));
          setLoading(false);
        }
      });
    return () => { active = false; };
  }, []);

  useEffect(() => {
    if (!currentYear) return;
    const controller = new AbortController();
    const currentEntry = drilldownStack.at(-1);
    const nodeId = currentEntry?.nodeId ?? null;
    const offset = currentEntry?.offset ?? 0;

    setLoading(true);
    fetchJson<ApiGraph>(buildApiUrl(buildGraphUrl(currentYear, nodeId, offset)), controller.signal)
      .then((response) => {
        setGraph(response);
        setError(null);
      })
      .catch((e) => {
        if (controller.signal.aborted) return;
        setError(String(e));
        setGraph(null);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });

    return () => controller.abort();
  }, [currentYear, drilldownStack]);

  useEffect(() => {
    if (!previousYear) {
      setPrevGraph(null);
      return;
    }
    const controller = new AbortController();
    const currentEntry = drilldownStack.at(-1);
    const nodeId = currentEntry?.nodeId ?? null;
    const offset = currentEntry?.offset ?? 0;

    fetchJson<ApiGraph>(buildApiUrl(buildGraphUrl(previousYear, nodeId, offset)), controller.signal)
      .then((response) => setPrevGraph(response))
      .catch(() => {
        if (!controller.signal.aborted) setPrevGraph(null);
      });

    return () => controller.abort();
  }, [previousYear, drilldownStack]);

  useEffect(() => {
    function onPointerDown(e: PointerEvent) {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) {
        setSearchOpen(false);
        setSearchExpanded(false);
      }
    }
    document.addEventListener('pointerdown', onPointerDown);
    return () => document.removeEventListener('pointerdown', onPointerDown);
  }, []);

  useEffect(() => {
    if (!currentYear || searchQuery.trim().length < 2) {
      setSearchResults([]);
      return;
    }

    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => {
      const params = new URLSearchParams({
        year: String(currentYear),
        q: searchQuery.trim(),
        limit: String(MAX_RESULTS),
      });
      fetchJson<{ institutions: InstitutionSummary[] }>(
        buildApiUrl(`/api/search/institutions?${params.toString()}`),
        controller.signal,
      )
        .then((response) => setSearchResults(response.institutions))
        .catch(() => {
          if (!controller.signal.aborted) setSearchResults([]);
        });
    }, 120);

    return () => {
      controller.abort();
      window.clearTimeout(timeoutId);
    };
  }, [currentYear, searchQuery]);

  function handleNodeClick(nodeId: string) {
    if (nodeId === PREV_WINDOW_ID || nodeId === NEXT_WINDOW_ID) {
      setDrilldownStack((prev) => {
        const last = prev.at(-1);
        if (!last) return prev;
        const pageSize = pageSizeForNode(last.nodeId);
        const PAGE = nodeId === PREV_WINDOW_ID ? -pageSize : pageSize;
        const newOffset = Math.max(0, (last.offset ?? 0) + PAGE);
        return [...prev.slice(0, -1), { ...last, offset: newOffset }];
      });
      return;
    }
    if (nodeId.startsWith('synthetic:')) return;
    const node = graph?.nodes.find((n) => n.id === nodeId);
    if (!node || node.category === 'state' || node.category === 'ministry' || node.category === 'other') return;
    setDrilldownStack((prev) => [...prev, { nodeId, label: node.name }]);
  }

  function handleBack() {
    setDrilldownStack((prev) => prev.slice(0, -1));
  }

  function handleSearchSelect(inst: InstitutionSummary) {
    setSearchQuery('');
    setSearchOpen(false);
    setSearchExpanded(false);
    setDrilldownStack([{ nodeId: inst.id, label: inst.name }]);
  }

  if (loading && !graph) return <div className="centered">Načítání…</div>;
  if (error && !graph) return <div className="centered error">Chyba: {error}</div>;
  if (!currentYear || !graph) return <div className="centered">Žádná data.</div>;

  const curYear = currentYear;
  const prevYear = previousYear;
  const backLabel = drilldownStack.length > 1
    ? drilldownStack.at(-2)!.label
    : ROOT_TITLE;

  return (
    <>
      <div className="topbar">
        <div className="topbar__main">
          <div className="topbar__left">
            {drilldownStack.length > 0 ? (
              <button className="back-btn" onClick={handleBack}>← {backLabel}</button>
            ) : (
              <span className="topbar__title">{ROOT_TITLE}</span>
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
          </div>

          <div className="topbar__controls">
            <button
              className={`year-toggle-btn${perPupil ? ' year-toggle-btn--active' : ''}`}
              onClick={() => setPerPupil((v) => !v)}
              title="Přepnout pohled Kč/žák (kapacita RSSZ)"
            >
              {perPupil ? 'Kč/žák/rok' : 'celkem'}
            </button>
            {prevGraph && prevYear && (
              <button className="year-toggle-btn" onClick={() => setPrevActive((v) => !v)}>
                {prevActive ? prevYear : curYear}
              </button>
            )}
            <button
              className="search-icon-btn"
              onClick={() => setSearchExpanded((v) => !v)}
              aria-label="Hledat školu"
            >
              {searchExpanded ? '✕' : '⌕'}
            </button>
          </div>

          <div className={`topbar__search${searchExpanded ? ' topbar__search--open' : ''}`} ref={searchRef}>
            <div className="search-wrap">
              <input
                className="search-input"
                type="search"
                placeholder="Hledat školu…"
                value={searchQuery}
                onChange={(e) => { setSearchQuery(e.target.value); setSearchOpen(true); }}
                onFocus={() => { setSearchOpen(true); setSearchExpanded(true); }}
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
      </div>

      <div className="chart-area">
        <SankeyChartCard
          nodes={graph.nodes}
          links={graph.links}
          prevNodes={prevGraph?.nodes}
          prevLinks={prevGraph?.links}
          prevActive={prevActive}
          perPupil={perPupil}
          onNodeClick={handleNodeClick}
          onNodeHover={setHoverInfo}
          onLinkHover={setHoverInfo}
        />
      </div>
    </>
  );
}
