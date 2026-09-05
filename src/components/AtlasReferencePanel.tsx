import type { ApiGraph } from '../types';

import { buildAtlasReferenceSummary } from '../lib/atlasReference';
import { metricCoverage } from '../lib/metricCoverage';

interface Props {
  open: boolean;
  onClose: () => void;
  graph: ApiGraph;
  perUnit: boolean;
  selectedYear: number;
}

export function AtlasReferencePanel({ open, onClose, graph, perUnit, selectedYear }: Props) {
  const summary = buildAtlasReferenceSummary(graph, perUnit);
  const coverage = metricCoverage(graph);
  if (!open) return null;

  return (
    <>
      {open && <button className="reference-backdrop" onClick={onClose} aria-label="Zavřít zdroje" />}
      <aside className={`reference-panel${open ? ' reference-panel--open' : ''}`} aria-hidden={!open}>
        <div className="reference-panel__header">
          <div>
            <div className="reference-panel__eyebrow">
              Rok {selectedYear} · {perUnit ? 'srovnávací metrika' : 'celkem'}
            </div>
            <h2 className="reference-panel__title">Zdroje aktuálního pohledu</h2>
          </div>
          <button className="reference-panel__close" onClick={onClose} aria-label="Zavřít zdroje">✕</button>
        </div>

        <section className="reference-panel__section">
          {perUnit && <>
            <p>Srovnávací údaj: {coverage.available} z {coverage.total} toků v tomto pohledu. Jde o pokrytí metrik, nikoli podíl vysvětleného rozpočtu.</p>
            <ul className="reference-list">
              {graph.nodes.filter((node) => node.metadata?.denominatorYear).map((node) => (
                <li key={node.id} className="reference-card">
                  <div>{node.name}: {node.metadata?.denominatorLabel}</div>
                  <div>Rok údaje: {node.metadata?.denominatorYear}</div>
                  {typeof node.metadata?.denominatorSourceUrl === 'string' && (
                    <a className="reference-card__url" href={node.metadata.denominatorSourceUrl} target="_blank" rel="noreferrer">Zdroj srovnávacího údaje</a>
                  )}
                </li>
              ))}
              {coverage.missing.map(({ link, reason }) => (
                <li key={`${link.flowType}:${link.source}:${link.target}`} className="reference-card">
                  <div>{graph.nodes.find((node) => node.id === link.target)?.name ?? link.target}: N/A</div>
                  <div>{reason}</div>
                </li>
              ))}
            </ul>
          </>}
          <ul className="reference-list">
            {summary.datasets.map((dataset) => (
              <li key={dataset.datasetKey} className="reference-card">
                <div className="reference-card__title">{dataset.title}</div>
                {dataset.url ? (
                  <a className="reference-card__url" href={dataset.url} target="_blank" rel="noreferrer">
                    {dataset.url}
                  </a>
                ) : (
                  <div className="reference-card__missing-url">Přímý odkaz zatím není evidován.</div>
                )}
                <div className="reference-card__key">{dataset.datasetKey}</div>
              </li>
            ))}
          </ul>
        </section>
      </aside>
    </>
  );
}
