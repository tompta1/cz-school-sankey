# Czech school budget Sankey starter

A first draft of a **static** Sankey explorer for Czech school finance.

It is designed for the workflow you described:

- **local ETL first**
- explicit **observed vs inferred** edges
- yearly static exports for GitHub Pages or plain Vercel static hosting
- no mandatory backend for the first public version

## What is inside

- `src/` — Vite + React + TypeScript UI
- `public/data/` — versioned static data exports read by the UI
- `etl/` — local normalization and export scripts
- `.github/workflows/` — optional static deploy stub

## Quick start

```bash
npm install
npm run etl:demo
npm run dev
```

Open the Vite URL shown in the terminal.

## Core idea

The ETL writes one annual JSON file like `public/data/sankey/2025.json`. The UI loads `public/data/manifest.json`, lets the user switch year and filters, and never depends on a live API.

That keeps the first release simple:

- deterministic builds
- easier review of joins and inferred edges
- trivial hosting on GitHub Pages
- simple diffs between years

## Suggested next implementation steps

1. Replace the demo export with one real MŠMT workbook import.
2. Add a registry join step so every school legal entity has a stable internal node ID plus IČO.
3. Add a provenance file per year that documents which edges are inferred.
4. Add 2026 as a separate schema-aware year because the financing logic changed.

## Static hosting notes

This scaffold sets `base: './'` in Vite so the built files can be hosted from a repository subpath on GitHub Pages.

A backend becomes useful later only if you want:

- ad hoc search over many years and sectors
- user-defined graph queries
- API reuse by other apps
- incremental refresh without rebuilding the site
