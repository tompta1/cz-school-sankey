# Atlas Data Sources And Metrics

## Purpose

The atlas mixes several ministries and public systems in one Sankey. That means `celkem` and `Srovnávací metrika` do not mean the same thing.

- `Celkem` is annual CZK volume for the active branch.
- `Srovnávací metrika` is only shown where the branch has an honest denominator.
- If the denominator would be misleading or mixed, the atlas shows `N/A`.

## Amount Mode

`Celkem` is built from annual public-budget or public-finance sources loaded into Neon.

Current main source families:

- Schools: MŠMT finance allocations, founder support, EU projects, and derived school expenditure marts.
- Health: Monitor MF for MZ/KHS/public entities, CZSO healthcare financing aggregates, NZIP activity denominators, and selected provider finance joins.
- Social: MPSV chapter aggregates plus recipient-count datasets where they exist.
- Interior: MV chapter aggregates, police crime statistics, and HZS annual activity statistics.
- Justice: MSp chapter aggregates plus court and prison activity denominators.
- Transport: Monitor MF for MD/SFDI, SFDI project execution CSVs, and annual transport usage proxies.
- Agriculture: Monitor MF for MZe/SZIF administration and SZIF recipient payment lists for subsidy outflows.
- Environment: Monitor MF for MŽP/SFŽP administration and the open SFŽP support registry for support-program drilldowns.

Some links are intentionally synthetic:

- The Czech state often publishes budget totals and activity counts separately.
- The atlas joins them only where the join is explainable.
- Synthetic links remain marked by notes in the UI and usually use `atlas.inferred`.

## Comparative Metrics

The atlas uses one denominator family per branch, not one universal `Kč/osoba/rok`.

Current branch metrics:

- Schools: `Kč/žák/rok`
- Police: `Kč/registrovaný skutek`
- Fire rescue: `Kč/zásah`
- Courts: `Kč/vyřízenou věc`
- Prison service: `Kč/vězněnou osobu/rok`
- Pensions and selected social benefits: `Kč/příjemce/...`
- Rail: `Kč/cestujícího`
- Motorway passenger-car branch: `Kč/prodanou známku`
- Heavy-vehicle toll branch: `Kč/zpoplatněné vozidlo`
- Transport investor/project drilldown: `Kč/akci`
- Agriculture area-linked subsidies: `Kč/ha`
- Agriculture other subsidy families: `Kč/příjemce dotace`
- Environment support programs: `Kč/příjemce podpory`

Agriculture caveat:

- the MZe subsidy branch is now split into metric families
- area-linked supports use LPIS hectares matched to the area-family recipient cohort
- livestock, investment, and mixed residual subsidy families still use unique annual subsidy recipients published by SZIF
- technical-assistance payments to `MZe` and `SZIF` themselves are excluded from the subsidy denominator and amount, so the branch reflects subsidy recipients rather than administration financing

Environment caveat:

- the MŽP branch currently uses Monitor MF to get actual annual resort and SFŽP expense totals
- the support-program and recipient drilldowns are then allocated by shares from the open SFŽP support registry
- this is a strong public reconstruction of the support structure, but not a published annual SFŽP accounting split by program family
- `SFZP podpory` therefore has a real top-level amount and a real recipient metric, while lower family and recipient amounts are explained synthetic allocations

Why the transport branch is mixed:

- Rail has a defensible passenger denominator.
- Motorways have different public usage signals for cars and trucks.
- Other road infrastructure does not have one honest common denominator in the current open-data stack.

## Freshness

Freshness differs by source family.

- Budget chapters and Monitor MF: usually annual closed-year values.
- Activity denominators: often annual statistics that can lag budget totals by one year.
- Some comparative transport and health metrics therefore intentionally use the latest available official year and say so in the UI.

Current repo policy:

- Keep Neon as a curated warehouse, not a raw archive.
- Load annual or low-granularity marts into Neon.
- Keep downloaded raw snapshots local or outside Git.

## Why The UI Panel Exists

The in-app `Metodika` panel is the user-facing explanation layer.

It shows:

- what `Celkem` means in the active view
- which comparison metrics are active
- which source families are present in the active graph
- freshness notes
- branch-specific caveats for inferred or lagged flows

That panel is meant to reduce metric confusion when moving across ministries and drilldowns.
