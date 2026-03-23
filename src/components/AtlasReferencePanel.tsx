import type { ApiGraph } from '../types';

import { buildAtlasReferenceSummary } from '../lib/atlasReference';

interface Props {
  open: boolean;
  onClose: () => void;
  graph: ApiGraph;
  perUnit: boolean;
  selectedYear: number;
}

export function AtlasReferencePanel({ open, onClose, graph, perUnit, selectedYear }: Props) {
  const summary = buildAtlasReferenceSummary(
    graph,
    perUnit,
    'srovnávací jednotku',
    'srovnávacích jednotek',
  );

  return (
    <>
      {open && <button className="reference-backdrop" onClick={onClose} aria-label="Zavřít metodiku" />}
      <aside className={`reference-panel${open ? ' reference-panel--open' : ''}`} aria-hidden={!open}>
        <div className="reference-panel__header">
          <div>
            <div className="reference-panel__eyebrow">Metodika atlasu</div>
            <h2 className="reference-panel__title">Zdroje a význam metrik</h2>
          </div>
          <button className="reference-panel__close" onClick={onClose} aria-label="Zavřít panel">✕</button>
        </div>

        <section className="reference-panel__section">
          <h3>Aktivní pohled</h3>
          <p>
            Rok <strong>{selectedYear}</strong> · režim{' '}
            <strong>{perUnit ? 'Srovnávací metrika' : 'Celkem'}</strong>
          </p>
          <p>
            {perUnit
              ? 'Srovnávací metrika ukazuje Kč na vhodnou jednotku pouze tam, kde má větev obhajitelný jmenovatel. Smíšené nebo nepodložené poměry zůstávají jako N/A.'
              : 'Celkem ukazuje roční objemy v Kč. Mohou obsahovat jak přímo reportované částky, tak synteticky odvozené vazby, pokud stát nepublikuje celý tok v jednom datasetu.'}
          </p>
          {summary.inferredFlowCount > 0 && (
            <p>
              V tomto pohledu je <strong>{summary.inferredFlowCount}</strong> odvozených toků. Jejich důvod je popsán v poznámkách níže.
            </p>
          )}
        </section>

        {perUnit && summary.metrics.length > 0 && (
          <section className="reference-panel__section">
            <h3>Srovnávací metriky v tomto pohledu</h3>
            <ul className="reference-list">
              {summary.metrics.map((metric) => (
                <li key={metric.group ?? metric.title} className="reference-card">
                  <div className="reference-card__title">{metric.title}</div>
                  <div className="reference-card__text">{metric.description}</div>
                  <div className="reference-card__meta">Proč tato volba: {metric.rationale}</div>
                </li>
              ))}
            </ul>
          </section>
        )}

        <section className="reference-panel__section">
          <h3>Datové zdroje v tomto pohledu</h3>
          <ul className="reference-list">
            {summary.datasets.map((dataset) => (
              <li key={dataset.datasetKey} className="reference-card">
                <div className="reference-card__title">
                  {dataset.url ? (
                    <a href={dataset.url} target="_blank" rel="noreferrer">{dataset.title}</a>
                  ) : (
                    dataset.title
                  )}
                </div>
                <div className="reference-card__text">{dataset.description}</div>
                <div className="reference-card__meta">Freshness: {dataset.freshness}</div>
                <div className="reference-card__meta">Proč je zde: {dataset.rationale}</div>
              </li>
            ))}
          </ul>
        </section>

        {summary.notes.length > 0 && (
          <section className="reference-panel__section">
            <h3>Poznámky k aktuální vrstvě</h3>
            <ul className="reference-note-list">
              {summary.notes.map((note) => (
                <li key={note}>{note}</li>
              ))}
            </ul>
          </section>
        )}
      </aside>
    </>
  );
}
