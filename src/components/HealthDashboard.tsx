import { useDeferredValue, useEffect, useMemo, useRef, useState } from 'react';

import { buildApiUrl, fetchJson } from '../lib/api';
import type {
  HealthProviderActivityRow,
  HealthProviderDirectoryEntry,
  HealthPayerActivityRow,
  HealthSummaryResponse,
  HealthYearsResponse,
} from '../types';

type FocusMode = 'all' | 'hospital' | 'public_health';

const FOCUS_LABELS: Record<FocusMode, string> = {
  all: 'Vše',
  hospital: 'Nemocnice',
  public_health: 'Hygiena a veřejné zdraví',
};

function formatInteger(value: number): string {
  return new Intl.NumberFormat('cs-CZ').format(value);
}

function matchesFocus(row: HealthProviderActivityRow, focus: FocusMode): boolean {
  if (focus === 'hospital') return row.hospitalLike;
  if (focus === 'public_health') return row.publicHealthLike;
  return true;
}

export function HealthDashboard() {
  const [years, setYears] = useState<number[]>([]);
  const [selectedYear, setSelectedYear] = useState<number | null>(null);
  const [summary, setSummary] = useState<HealthSummaryResponse | null>(null);
  const [providerRows, setProviderRows] = useState<HealthProviderActivityRow[]>([]);
  const [payerRows, setPayerRows] = useState<HealthPayerActivityRow[]>([]);
  const [providersLoading, setProvidersLoading] = useState(false);
  const [payerLoading, setPayerLoading] = useState(false);
  const [providerSearch, setProviderSearch] = useState('');
  const [providerResults, setProviderResults] = useState<HealthProviderDirectoryEntry[]>([]);
  const [providerSearchOpen, setProviderSearchOpen] = useState(false);
  const [selectedProvider, setSelectedProvider] = useState<HealthProviderDirectoryEntry | null>(null);
  const [focusMode, setFocusMode] = useState<FocusMode>('all');
  const [error, setError] = useState<string | null>(null);
  const searchRef = useRef<HTMLDivElement>(null);
  const deferredProviderSearch = useDeferredValue(providerSearch);

  useEffect(() => {
    let active = true;
    fetchJson<HealthYearsResponse>(buildApiUrl('/api/health/years'))
      .then((response) => {
        if (!active) return;
        setYears(response.years);
        setSelectedYear((current) => current ?? response.years.at(-1) ?? null);
      })
      .catch((e) => {
        if (!active) return;
        setError(String(e));
      });

    fetchJson<HealthSummaryResponse>(buildApiUrl('/api/health/summary'))
      .then((response) => {
        if (!active) return;
        setSummary(response);
      })
      .catch((e) => {
        if (!active) return;
        setError(String(e));
      });

    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!selectedYear) return;
    const controller = new AbortController();
    const params = new URLSearchParams({ year: String(selectedYear), limit: '120' });
    if (selectedProvider?.providerIco) {
      params.set('providerIco', selectedProvider.providerIco);
    }

    setProvidersLoading(true);
    fetchJson<{ rows: HealthProviderActivityRow[] }>(
      buildApiUrl(`/api/health/activity/providers?${params.toString()}`),
      controller.signal,
    )
      .then((response) => {
        setProviderRows(response.rows);
        setError(null);
      })
      .catch((e) => {
        if (controller.signal.aborted) return;
        setError(String(e));
        setProviderRows([]);
      })
      .finally(() => {
        if (!controller.signal.aborted) setProvidersLoading(false);
      });

    return () => controller.abort();
  }, [selectedProvider, selectedYear]);

  useEffect(() => {
    if (!selectedYear) return;
    const controller = new AbortController();
    const params = new URLSearchParams({ year: String(selectedYear), limit: '12' });

    setPayerLoading(true);
    fetchJson<{ rows: HealthPayerActivityRow[] }>(
      buildApiUrl(`/api/health/activity/payers?${params.toString()}`),
      controller.signal,
    )
      .then((response) => setPayerRows(response.rows))
      .catch(() => {
        if (!controller.signal.aborted) setPayerRows([]);
      })
      .finally(() => {
        if (!controller.signal.aborted) setPayerLoading(false);
      });

    return () => controller.abort();
  }, [selectedYear]);

  useEffect(() => {
    function onPointerDown(e: PointerEvent) {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) {
        setProviderSearchOpen(false);
      }
    }
    document.addEventListener('pointerdown', onPointerDown);
    return () => document.removeEventListener('pointerdown', onPointerDown);
  }, []);

  useEffect(() => {
    const q = deferredProviderSearch.trim();
    if (q.length < 2) {
      setProviderResults([]);
      return;
    }

    const controller = new AbortController();
    const params = new URLSearchParams({
      q,
      limit: '8',
      hospitalOnly: focusMode === 'hospital' ? 'true' : 'false',
    });

    fetchJson<{ providers: HealthProviderDirectoryEntry[] }>(
      buildApiUrl(`/api/health/providers?${params.toString()}`),
      controller.signal,
    )
      .then((response) => setProviderResults(response.providers))
      .catch(() => {
        if (!controller.signal.aborted) setProviderResults([]);
      });

    return () => controller.abort();
  }, [deferredProviderSearch, focusMode]);

  const filteredProviderRows = useMemo(
    () => providerRows.filter((row) => matchesFocus(row, focusMode)),
    [focusMode, providerRows],
  );

  const providerHeadline = selectedProvider?.providerName
    ? `${selectedProvider.providerName} · ${selectedProvider.regionName ?? 'bez kraje'}`
    : 'Největší poskytovatelé podle vykázaných výkonů';

  return (
    <div className="health-shell">
      <section className="health-hero">
        <div className="health-hero__copy">
          <span className="health-hero__eyebrow">Zdravotnictví</span>
          <h1 className="health-hero__title">Nemocnice, hygiena a hrazené výkony</h1>
          <p className="health-hero__text">
            Frontend teď čte živá data z Neon přes Vercel API. Pohled poskytovatelů je zúžený na
            nemocniční a veřejně-zdravotní část registru, protože ta jde korektně propojit přes IČO.
          </p>
        </div>

        <div className="health-year-strip">
          {years.map((year) => (
            <button
              key={year}
              className={`health-year-pill${year === selectedYear ? ' health-year-pill--active' : ''}`}
              onClick={() => setSelectedYear(year)}
            >
              {year}
            </button>
          ))}
        </div>
      </section>

      <section className="health-metrics">
        <article className="health-metric-card">
          <span className="health-metric-card__label">Poskytovatelé v registru</span>
          <strong className="health-metric-card__value">
            {summary ? formatInteger(summary.counts.providers) : '…'}
          </strong>
        </article>
        <article className="health-metric-card">
          <span className="health-metric-card__label">Místa poskytování</span>
          <strong className="health-metric-card__value">
            {summary ? formatInteger(summary.counts.facilities) : '…'}
          </strong>
        </article>
        <article className="health-metric-card">
          <span className="health-metric-card__label">Nemocniční zařízení</span>
          <strong className="health-metric-card__value">
            {summary ? formatInteger(summary.counts.hospitalLikeFacilities) : '…'}
          </strong>
        </article>
        <article className="health-metric-card">
          <span className="health-metric-card__label">Řádky poskytovatelů</span>
          <strong className="health-metric-card__value">
            {summary ? formatInteger(summary.counts.providerClaimRows) : '…'}
          </strong>
        </article>
      </section>

      <section className="health-grid">
        <article className="health-panel health-panel--filters">
          <div className="health-panel__header">
            <div>
              <h2 className="health-panel__title">Filtry a lookup</h2>
              <p className="health-panel__subtle">Vyber poskytovatele nebo omez výpis na konkrétní focus.</p>
            </div>
          </div>

          <div className="health-focus-chips">
            {(Object.keys(FOCUS_LABELS) as FocusMode[]).map((mode) => (
              <button
                key={mode}
                className={`health-chip${focusMode === mode ? ' health-chip--active' : ''}`}
                onClick={() => setFocusMode(mode)}
              >
                {FOCUS_LABELS[mode]}
              </button>
            ))}
          </div>

          <div className="health-search" ref={searchRef}>
            <input
              className="health-search__input"
              type="search"
              placeholder="Hledat poskytovatele nebo nemocnici…"
              value={providerSearch}
              onChange={(e) => {
                setProviderSearch(e.target.value);
                setProviderSearchOpen(true);
              }}
              onFocus={() => setProviderSearchOpen(true)}
            />

            {providerSearchOpen && providerResults.length > 0 && (
              <ul className="health-search__results">
                {providerResults.map((provider) => (
                  <li
                    key={`${provider.providerIco ?? 'none'}-${provider.zzId ?? 'none'}`}
                    className="health-search__result"
                    onPointerDown={() => {
                      setSelectedProvider(provider);
                      setProviderSearch(provider.providerName ?? provider.facilityName ?? '');
                      setProviderSearchOpen(false);
                    }}
                  >
                    <span className="health-search__name">{provider.providerName ?? provider.facilityName}</span>
                    <span className="health-search__meta">
                      {[provider.regionName, provider.providerType ?? provider.facilityTypeName].filter(Boolean).join(' · ')}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </div>

          {selectedProvider && (
            <div className="health-provider-spotlight">
              <div>
                <span className="health-provider-spotlight__eyebrow">Aktivní filtr</span>
                <strong className="health-provider-spotlight__title">{selectedProvider.providerName}</strong>
                <p className="health-provider-spotlight__meta">
                  {[selectedProvider.providerIco, selectedProvider.regionName, selectedProvider.providerType]
                    .filter(Boolean)
                    .join(' · ')}
                </p>
              </div>
              <button
                className="health-clear-btn"
                onClick={() => {
                  setSelectedProvider(null);
                  setProviderSearch('');
                }}
              >
                Zrušit filtr
              </button>
            </div>
          )}

          <div className="health-note">
            <strong>Coverage</strong>
            <span>
              Poskytovatelé: roční agregace přes IČO. Pojišťovny: měsíční agregace přes kód pojišťovny.
            </span>
          </div>
        </article>

        <article className="health-panel health-panel--wide">
          <div className="health-panel__header">
            <div>
              <h2 className="health-panel__title">{providerHeadline}</h2>
              <p className="health-panel__subtle">
                {selectedYear ? `Rok ${selectedYear}` : 'Vyber rok'} · {FOCUS_LABELS[focusMode]}
              </p>
            </div>
          </div>

          {providersLoading ? (
            <div className="health-empty">Načítání poskytovatelů…</div>
          ) : filteredProviderRows.length === 0 ? (
            <div className="health-empty">Pro zvolený filtr nejsou dostupná data.</div>
          ) : (
            <div className="health-table-wrap">
              <table className="health-table">
                <thead>
                  <tr>
                    <th>Poskytovatel</th>
                    <th>Region</th>
                    <th>Pacienti</th>
                    <th>Kontakty</th>
                    <th>Výkony</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredProviderRows.slice(0, 24).map((row) => (
                    <tr key={`${row.year}-${row.providerIco}`}>
                      <td>
                        <div className="health-provider-cell">
                          <strong>{row.providerName ?? row.providerIco}</strong>
                          <span>{[row.providerIco, row.providerType].filter(Boolean).join(' · ')}</span>
                        </div>
                      </td>
                      <td>{row.regionName ?? '—'}</td>
                      <td>{formatInteger(row.patientCount)}</td>
                      <td>{formatInteger(row.contactCount)}</td>
                      <td>{formatInteger(row.totalQuantity)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </article>

        <article className="health-panel">
          <div className="health-panel__header">
            <div>
              <h2 className="health-panel__title">Pojišťovny</h2>
              <p className="health-panel__subtle">Měsíční intenzita vykázaných výkonů podle plátce.</p>
            </div>
          </div>

          {payerLoading ? (
            <div className="health-empty">Načítání pojišťoven…</div>
          ) : (
            <div className="health-list">
              {payerRows.map((row) => (
                <div key={`${row.year}-${row.month}-${row.payerCode}`} className="health-list__item">
                  <div>
                    <strong>{row.payerName ?? row.payerCode}</strong>
                    <span>{`${row.month}. ${row.year}`}</span>
                  </div>
                  <span>{formatInteger(row.totalQuantity)}</span>
                </div>
              ))}
            </div>
          )}
        </article>

        <article className="health-panel">
          <div className="health-panel__header">
            <div>
              <h2 className="health-panel__title">Zdrojové sady</h2>
              <p className="health-panel__subtle">Stav aktuálně připojených snapshotů v Neon skladu.</p>
            </div>
          </div>

          <div className="health-source-list">
            {summary?.sources.map((source) => (
              <div key={`${source.datasetCode}-${source.snapshotLabel}`} className="health-source-card">
                <span className="health-source-card__code">{source.datasetCode}</span>
                <strong>{source.snapshotLabel}</strong>
                <span>{formatInteger(source.rowCount)} řádků</span>
                <span className="health-source-card__status">{source.status}</span>
              </div>
            )) ?? <div className="health-empty">Zdroje nejsou k dispozici.</div>}
          </div>
        </article>
      </section>

      {error && <div className="health-error">Chyba API: {error}</div>}

      <section className="health-footer-note">
        <span>Výkony poskytovatelů jsou zatím zobrazované jako roční agregace přes IČO.</span>
        <span>
          {selectedYear
            ? `Aktivní rok: ${selectedYear}, zobrazených řádků: ${formatInteger(filteredProviderRows.length)}`
            : 'Vyber rok pro detail.'}
        </span>
      </section>
    </div>
  );
}
