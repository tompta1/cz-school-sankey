import { useEffect, useRef, useState } from 'react';

import { buildApiUrl, fetchJson } from '../lib/api';
import { atlasBackLabel, backAtlasView, pageAtlasView, pushAtlasView, type AtlasDrilldownState } from '../lib/atlasNavigation';
import {
  EU_ALL_ID,
  FOUNDERS_KRAJ,
  FOUNDERS_OBEC,
  NEXT_WINDOW_ID,
  PREV_WINDOW_ID,
  TOP_FOUNDERS,
  TOP_SCHOOLS,
} from '../lib/graph';
import { formatCompactCzk } from '../lib/format';
import type { ApiGraph, AtlasSearchHit, AtlasSearchResponse, AtlasYearsResponse, HoverInfo, SankeyNode } from '../types';
import { SankeyChartCard } from './SankeyChartCard';

const ROOT_TITLE = 'Sjednoceny Sankey';
const HEALTH_MINISTRY_ID = 'health:ministry:mzcr';
const HEALTH_INSURANCE_ID = 'health:system:public-insurance';
const HEALTH_PUBLIC_HEALTH_ID = 'health:public-health';
const HEALTH_ZZS_ID = 'health:zzs';
const MV_MINISTRY_ID = 'security:ministry:mv';
const MV_POLICE_ID = 'security:police';
const JUSTICE_MINISTRY_ID = 'justice:ministry:msp';
const MAX_RESULTS = 8;

function buildSchoolGraphUrl(year: number, nodeId: string, offset: number): string {
  const params = new URLSearchParams({ year: String(year) });

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

function pageSizeForSchoolNode(nodeId: string): number {
  if (nodeId === FOUNDERS_KRAJ || nodeId === FOUNDERS_OBEC || nodeId.startsWith('region:')) {
    return TOP_FOUNDERS;
  }
  return TOP_SCHOOLS;
}

function isClickableSchoolNode(node: SankeyNode): boolean {
  if (node.id.startsWith('synthetic:')) return false;
  return node.category !== 'state' && node.category !== 'ministry' && node.category !== 'other';
}

function isClickableHealthNode(node: SankeyNode): boolean {
  if (node.id === HEALTH_INSURANCE_ID || node.id === HEALTH_MINISTRY_ID) return true;
  if (node.id === HEALTH_PUBLIC_HEALTH_ID) return true;
  if (node.id === HEALTH_ZZS_ID) return true;
  if (node.id === 'health:outpatient:hp31' || node.id === 'health:outpatient:hp32') return true;
  if (node.id.startsWith('health:owner:')) return true;
  if (node.id.startsWith('health:region:')) return true;
  if (node.id.startsWith('health:specialty:')) return true;
  if (node.id.startsWith('health:provider:')) return true;
  return false;
}

function isClickableMvNode(node: SankeyNode): boolean {
  if (node.id === MV_MINISTRY_ID) return true;
  if (node.id === MV_POLICE_ID && node.metadata?.drilldownAvailable === true) return true;
  if (node.id === 'security:fire-rescue' && node.metadata?.drilldownAvailable === true) return true;
  if (node.id.startsWith('security:police:region:') && node.metadata?.drilldownAvailable === true) return true;
  return false;
}

function isClickableJusticeNode(node: SankeyNode): boolean {
  return node.id === JUSTICE_MINISTRY_ID;
}

export function AtlasDashboard() {
  const [years, setYears] = useState<number[]>([]);
  const [selectedYear, setSelectedYear] = useState<number | null>(null);
  const [graph, setGraph] = useState<ApiGraph | null>(null);
  const [viewStack, setViewStack] = useState<AtlasDrilldownState[]>([]);
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);
  const [perPerson, setPerPerson] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<AtlasSearchHit[]>([]);
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchExpanded, setSearchExpanded] = useState(false);
  const searchRef = useRef<HTMLDivElement>(null);

  const currentView = viewStack.at(-1) ?? { scope: 'root' as const };

  useEffect(() => {
    let active = true;
    fetchJson<AtlasYearsResponse>(buildApiUrl('/api/atlas/years'))
      .then((response) => {
        if (!active) return;
        const nextYears = response.years.map((row) => row.year);
        setYears(nextYears);
        setSelectedYear((current) => current ?? nextYears.at(-1) ?? null);
      })
      .catch((reason) => {
        if (!active) return;
        setError(String(reason));
        setLoading(false);
      });

    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!selectedYear) return;
    const controller = new AbortController();
    let path = `/api/atlas/overview?year=${selectedYear}&metric=cost`;

    if (currentView.scope === 'school') {
      path = buildSchoolGraphUrl(selectedYear, currentView.nodeId, currentView.offset);
    } else if (currentView.scope === 'health') {
      const params = new URLSearchParams({ year: String(selectedYear) });
      if (currentView.nodeId) params.set('nodeId', currentView.nodeId);
      if (currentView.offset > 0) params.set('offset', String(currentView.offset));
      path = `/api/atlas/health?${params.toString()}`;
    } else if (currentView.scope === 'mv') {
      const params = new URLSearchParams({ year: String(selectedYear) });
      if (currentView.nodeId) params.set('nodeId', currentView.nodeId);
      path = `/api/atlas/mv?${params.toString()}`;
    } else if (currentView.scope === 'justice') {
      const params = new URLSearchParams({ year: String(selectedYear) });
      if (currentView.nodeId) params.set('nodeId', currentView.nodeId);
      path = `/api/atlas/justice?${params.toString()}`;
    }

    setLoading(true);
    fetchJson<ApiGraph>(buildApiUrl(path), controller.signal)
      .then((response) => {
        setGraph(response);
        setError(null);
      })
      .catch((reason) => {
        if (controller.signal.aborted) return;
        setGraph(null);
        setError(String(reason));
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });

    return () => controller.abort();
  }, [selectedYear, viewStack]);

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
    if (!selectedYear || searchQuery.trim().length < 2) {
      setSearchResults([]);
      return;
    }

    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => {
      const params = new URLSearchParams({
        year: String(selectedYear),
        q: searchQuery.trim(),
        limit: String(MAX_RESULTS),
      });
      fetchJson<AtlasSearchResponse>(buildApiUrl(`/api/atlas/search?${params.toString()}`), controller.signal)
        .then((response) => setSearchResults(response.results))
        .catch(() => {
          if (!controller.signal.aborted) setSearchResults([]);
        });
    }, 120);

    return () => {
      controller.abort();
      window.clearTimeout(timeoutId);
    };
  }, [selectedYear, searchQuery]);

  function handleSchoolNodeClick(node: SankeyNode) {
    if (node.id === PREV_WINDOW_ID || node.id === NEXT_WINDOW_ID) {
      setViewStack((prev) => {
        const current = prev.at(-1);
        if (!current || current.scope !== 'school') return prev;
        const step = node.id === PREV_WINDOW_ID ? -pageSizeForSchoolNode(current.nodeId) : pageSizeForSchoolNode(current.nodeId);
        return pageAtlasView(prev, step);
      });
      return;
    }

    if (!isClickableSchoolNode(node)) return;
    setViewStack((prev) => pushAtlasView(prev, { scope: 'school', nodeId: node.id, label: node.name, offset: 0 }));
  }

  function handleHealthNodeClick(node: SankeyNode) {
    if (node.id === PREV_WINDOW_ID || node.id === NEXT_WINDOW_ID) {
      setViewStack((prev) => {
        const current = prev.at(-1);
        if (!current || current.scope !== 'health') return prev;
        const step = node.id === PREV_WINDOW_ID ? -28 : 28;
        return pageAtlasView(prev, step);
      });
      return;
    }

    if (!isClickableHealthNode(node)) return;
    const nextNodeId =
      node.id === HEALTH_INSURANCE_ID || node.id === HEALTH_MINISTRY_ID
        ? null
        : node.id;
    setViewStack((prev) => pushAtlasView(prev, { scope: 'health', nodeId: nextNodeId, label: node.name, offset: 0 }));
  }

  function handleMvNodeClick(node: SankeyNode) {
    if (!isClickableMvNode(node)) return;
    const nextNodeId = node.id === MV_MINISTRY_ID ? null : node.id;
    setViewStack((prev) => pushAtlasView(prev, { scope: 'mv', nodeId: nextNodeId, label: node.name, offset: 0 }));
  }

  function handleJusticeNodeClick(node: SankeyNode) {
    if (!isClickableJusticeNode(node)) return;
    const nextNodeId = node.id === JUSTICE_MINISTRY_ID ? null : node.id;
    setViewStack((prev) => pushAtlasView(prev, { scope: 'justice', nodeId: nextNodeId, label: node.name, offset: 0 }));
  }

  function handleNodeClick(nodeId: string) {
    const node = graph?.nodes.find((entry) => entry.id === nodeId);
    if (!node) return;

    if (currentView.scope === 'root') {
      if (node.id.startsWith('justice:')) {
        handleJusticeNodeClick(node);
        return;
      }
      if (node.id.startsWith('security:')) {
        handleMvNodeClick(node);
        return;
      }
      if (node.id.startsWith('health:') || node.id === HEALTH_INSURANCE_ID || node.id === HEALTH_MINISTRY_ID) {
        handleHealthNodeClick(node);
        return;
      }
      handleSchoolNodeClick(node);
      return;
    }

    if (currentView.scope === 'school') {
      handleSchoolNodeClick(node);
      return;
    }

    if (currentView.scope === 'health') {
      handleHealthNodeClick(node);
      return;
    }

    if (currentView.scope === 'mv') {
      handleMvNodeClick(node);
      return;
    }

    handleJusticeNodeClick(node);
  }

  function handleBack() {
    setViewStack((prev) => backAtlasView(prev));
  }

  function handleSearchSelect(result: AtlasSearchHit) {
    if (result.available === false) {
      return;
    }
    setSearchQuery('');
    setSearchOpen(false);
    setSearchExpanded(false);
    if (result.domain === 'health') {
      setViewStack([{ scope: 'health', nodeId: result.id, label: result.name, offset: 0 }]);
      return;
    }
    setViewStack([{ scope: 'school', nodeId: result.id, label: result.name, offset: 0 }]);
  }

  if (loading && !graph) return <div className="centered">Nacitani atlasu…</div>;
  if (error && !graph) return <div className="centered error">Chyba: {error}</div>;
  if (!graph || !selectedYear) return <div className="centered">Zatim neni k dispozici sjednoceny pohled.</div>;

  const backLabel = atlasBackLabel(viewStack, ROOT_TITLE);

  return (
    <div className="dashboard-shell">
      <div className="topbar">
        <div className="topbar__main">
          <div className="topbar__left">
            {viewStack.length > 0 ? (
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
            ) : viewStack.length > 0 ? (
              <span className="topbar__hover-context">
                {viewStack.map((entry) => entry.label).join(' › ')}
              </span>
            ) : (
              <span className="topbar__hover-context">Rok {selectedYear} · naklady</span>
            )}
          </div>

          <div className="topbar__controls topbar__controls--atlas">
            <button
              className={`year-toggle-btn${perPerson ? ' year-toggle-btn--active' : ''}`}
              onClick={() => setPerPerson((value) => !value)}
            >
              {perPerson ? 'Kč/jednotku' : 'celkem'}
            </button>
            {years.map((year) => (
              <button
                key={year}
                className={`year-toggle-btn${year === selectedYear ? ' year-toggle-btn--active' : ''}`}
                onClick={() => {
                  setSelectedYear(year);
                  setViewStack([]);
                }}
              >
                {year}
              </button>
            ))}
            <button
              className="search-icon-btn"
              onClick={() => setSearchExpanded((value) => !value)}
              aria-label="Hledat instituci"
            >
              {searchExpanded ? '✕' : '⌕'}
            </button>
          </div>

          <div className={`topbar__search${searchExpanded ? ' topbar__search--open' : ''}`} ref={searchRef}>
            <div className="search-wrap">
              <input
                className="search-input"
                type="search"
                placeholder="Hledat školu, nemocnici, KHS…"
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setSearchOpen(true);
                }}
                onFocus={() => {
                  setSearchOpen(true);
                  setSearchExpanded(true);
                }}
              />
              {searchOpen && searchResults.length > 0 && (
                <ul className="search-dropdown">
                  {searchResults.map((result) => (
                    <li
                      key={`${result.domain}:${result.id}`}
                      className={`search-result${result.available === false ? ' search-result--disabled' : ''}`}
                      onPointerDown={() => handleSearchSelect(result)}
                    >
                      <span className="search-result__name">{result.name}</span>
                      <span className="search-result__meta">
                        {[
                          result.domain === 'health' ? 'zdraví' : 'škola',
                          result.municipality,
                          result.region,
                          result.providerType,
                          result.reason,
                        ].filter(Boolean).join(', ')}
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
          perPupil={perPerson}
          perUnitLabel="jednotku"
          unitCountLabel="žáků / pacientů / příjemců / případů / klientů za rok"
          totalAmountLabel="celkové náklady"
          onNodeClick={handleNodeClick}
          onNodeHover={setHoverInfo}
          onLinkHover={setHoverInfo}
        />
      </div>

      {error && <div className="atlas-error atlas-error--inline">Chyba API: {error}</div>}
    </div>
  );
}
