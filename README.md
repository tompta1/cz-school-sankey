# Český rozpočtový atlas — Interaktivní Sankey vizualizace státního rozpočtu ČR

**Live:** [cz-school-sankey.vercel.app](https://cz-school-sankey.vercel.app)

An open-source, interactive Sankey diagram of the Czech state budget — tracking every major crown from the national treasury through fourteen ministry chapters down to individual schools, hospitals, farms, infrastructure projects, and aid recipients. Built for journalists, researchers, policy analysts, and curious citizens who want to understand where public money actually goes.

---

## Co atlas zobrazuje / What the atlas shows

The atlas covers the **unified Czech state budget** (`Státní rozpočet ČR`) for fiscal years **2024** and **2025** (coverage varies by chapter — see details below). Every flow in the diagram is sourced from a named, publicly available dataset. Amounts that cannot be directly observed are labelled as *inferred* and the methodology is documented in the reference panel next to the chart.

The root view shows the flow `Stát → všechny resortní kapitoly → dílčí větve`. Clicking on any highlighted node drills into that chapter's own Sankey — revealing sub-branches, programmes, regions, or individual recipients depending on what open data exist for that ministry.

### Chapters currently in the atlas

| Chapter | Czech name | Drilldown depth | Years |
|---|---|---|---|
| **MŠMT** | Ministerstvo školství, mládeže a tělovýchovy | State → MŠMT → Region → Founder → School → Cost bucket | 2024, 2025 |
| **MPSV** | Ministerstvo práce a sociálních věcí | State → MPSV → Benefit group | 2024 |
| **MV** | Ministerstvo vnitra | State → MV → Police / HZS → Region → Crime class | 2024, 2025 |
| **MSp** | Ministerstvo spravedlnosti | State → MSp → Courts / Prison / Prosecutors | 2024 |
| **MD** | Ministerstvo dopravy + SFDI | State → MD → Rail / Road vignette / Road toll / SFDI investor → Project | 2024, 2025 |
| **MZe** | Ministerstvo zemědělství + SZIF | State → MZe → Subsidy family / Admin → Subsidy recipient | 2024 |
| **MŽP** | Ministerstvo životního prostředí + SFŽP | State → MŽP → SFŽP support / Admin → Support recipient | 2024, 2025 |
| **MMR** | Ministerstvo pro místní rozvoj | State → MMR → IROP regional / Housing → Region → Recipient | 2024, 2025 |
| **MPO** | Ministerstvo průmyslu a obchodu | State → MPO → OP TAK support → Region → Recipient | 2024, 2025 |
| **MK** | Ministerstvo kultury | State → MK → Heritage (PZAD) / Culture → Programme → Region / Recipient | 2024, 2025 |
| **MZV** | Ministerstvo zahraničních věcí | State → MZV → Foreign service / Development aid / Humanitarian aid → Country → Project | 2024 |
| **MO** | Ministerstvo obrany | State → MO → Program financing / Personnel / Other operating | 2024, 2025 |
| **MF** | Ministerstvo financí | State → MF → Tax admin (GFŘ) / Customs (GŘC) / Ministry core | 2024 |
| **MZ** | Ministerstvo zdravotnictví | State → MZ → Hospitals by owner / ZZS / Public health / Outpatient | 2024, 2025 |

---

## Datové zdroje / Data sources

Every node and link in the atlas is backed by one of the sources below. The in-app reference panel shows exactly which sources are active for the currently displayed graph.

### Státní rozpočet a Monitor MF

**Monitor státní pokladny** (`monitor.statnipokladna.gov.cz`) is the primary budget data source for the top layer of almost every ministry chapter. It provides annual realised expenditure (`vydaje`) and cost (`naklady`) at the level of individual state-budget chapters and their subordinate organisational units, identified by their IČO. The atlas queries the `/api/ukazatele` endpoint for each relevant IČO and uses the December period snapshot (`YY12`) as the final annual figure.

Ministries sourced directly from Monitor MF:

| Ministry | IČOs queried |
|---|---|
| MD — Ministerstvo dopravy | `66003008` |
| SFDI — Státní fond dopravní infrastruktury | `70856508` |
| MZe — Ministerstvo zemědělství | `00020478` |
| SZIF — Státní zemědělský intervenční fond (admin) | `48133981` |
| MŽP — Ministerstvo životního prostředí | `00164801` |
| SFŽP — Státní fond životního prostředí | `00020729` |
| MMR — Ministerstvo pro místní rozvoj | `66002222` |
| MPO — Ministerstvo průmyslu a obchodu | `47609109` |
| MK — Ministerstvo kultury | `00023671` |
| MZV — Ministerstvo zahraničních věcí | `45769851` |
| MO — Ministerstvo obrany | `60162694` (+ several subordinate units) |
| MF — Ministerstvo financí | `00006947` |
| GFŘ — Generální finanční ředitelství | `72080043` |
| GŘC — Generální ředitelství cel | `71214011` |
| MZ — Ministerstvo zdravotnictví | `00024341` |

---

### Školství (MŠMT)

**MŠMT „Podrobný rozpis rozpočtu" XLSX** — published annually at `msmt.cz`. One row per school with per-tariff-band pedagogical salary allocations, non-pedagogical salaries, ONIV (other non-investment expenditure), and operational grants. The ETL summates salary columns across school types (MŠ, ZŠ, ŠD, SŠ, KN, VOŠ, ZUŠ) into two buckets: pedagogical wages and non-pedagogical wages.

**DotaceEU „Seznam operací"** — EU structural-fund project list from `dotaceeu.cz`, matched to schools by IČO. Projects with no matching school IČO are excluded.

**ARES** (Administrativní registr ekonomických subjektů) — REST API for resolving official legal names, municipality, and region for every IČO. Results are cached locally.

**Years:** 2024, 2025.

**Per-unit metric:** **Kč/žák/rok** — annual expenditure per pupil. Pupil counts come from the MŠMT XLSX itself (capacity field) or from the school entity register.

---

### Sociální věci (MPSV)

**MPSV kapitolní agregace** — manually curated annual breakdown of the MPSV chapter into four blocks: důchody (pensions), ostatní dávky (other benefits), péče (care services), and správa (administration). Sourced from the official MPSV budget and final account documents.

**ČSSZ a MPSV: počty příjemců** — annual or December-state recipient counts for each benefit type, sourced from ČSSZ open data (`data.cssz.cz`) and MPSV publications.

**Years:** 2024.

**Per-unit metrics (by node):**
- Důchody → **Kč/příjemce důchodu/rok**
- Podpora v nezaměstnanosti → **Kč/příjemce podpory/rok**
- Příspěvek na péči → **Kč/příjemce příspěvku/rok**
- Náhradní výživné → **Kč/příjemce dávky/rok**
- Mixed or administration nodes → no per-unit metric (labelled as non-normalizable)

---

### Bezpečnost a vnitro (MV)

**MV kapitolní bezpečnostní agregace** — annual breakdown of the MV chapter into: Policie ČR, Hasičský záchranný sbor (HZS), MV social benefits, and residual administration. Derived from final budget accounts.

**Policie ČR veřejné kriminální statistiky** — registered crime acts (`registrované skutky`) broken down by region and crime class. Used as the denominator for the police branch and as the basis for regional drilldown.

**HZS statistická ročenka zásahů** — annual HZS intervention statistics by region. Used as the denominator for the fire-rescue branch.

**Years:** 2024, 2025.

**Per-unit metrics (by node):**
- Policie ČR → **Kč/registrovaný skutek** (region- and crime-class-level)
- HZS → **Kč/zásah** (region-level)
- MV social / administration nodes → no per-unit metric

---

### Justice (MSp)

**MSp kapitolní justiční agregace** — breakdown of the Ministry of Justice chapter into: soudy (courts), Vězeňská služba (prison service), státní zastupitelství (state prosecutors), and residual blocks. Sourced from the official budget final accounts.

**Soudní a vězeňské výkonové statistiky** — annual statistics on resolved court cases and average daily inmate population, published in Ministry of Justice annual reports.

**Years:** 2024.

**Per-unit metrics (by node):**
- Soudy → **Kč/vyřízenou věc**
- Vězeňská služba → **Kč/vězněnou osobu/rok**
- Prosecutors and residual nodes → no per-unit metric

---

### Doprava (MD + SFDI)

**Monitor MF: MD a SFDI** — annual realised expenditure of Ministerstvo dopravy (ICO `66003008`) and Státní fond dopravní infrastruktury (ICO `70856508`).

**SFDI projektové čerpání** — open CSV of SFDI-funded infrastructure projects with investor, project name, and drawn amount, published at `kz.sfdi.cz`. Used to build the SFDI investor → project drilldown.

**Dopravní výkonové proxy metriky** — three separate annual series:
- Rail: annual passenger count in rail public transport (source: SŽDC/Správa železnic annual report)
- Vignette: annual count of sold electronic motorway vignettes (source: CENDIS / SDA annual publication)
- Toll: annual count of registered toll-paying heavy vehicles (source: CCS / toll operator reports)

**Years:** 2024, 2025.

**Per-unit metrics (by node):**
- Železnice → **Kč/cestujícího**
- Dálnice — osobní auta → **Kč/prodanou dálniční známku**
- Dálnice — těžká vozidla → **Kč/zpoplatněné vozidlo**
- SFDI investor / projekt → **Kč/akci** (project count)

---

### Zemědělství (MZe + SZIF)

**Monitor MF: MZe a SZIF správa** — annual expenditure of Ministerstvo zemědělství (IČO `00020478`) and the administrative costs of SZIF (IČO `48133981`). Forms the administrative layer of the agriculture branch.

**SZIF seznamy příjemců dotací** — open CSV recipient lists for EU-funded and national agricultural subsidies, published at `szif.gov.cz`. Each row contains recipient name, IČO, subsidy measure, and paid amount. The atlas groups measures into *subsidy families* (area-based, livestock, investment, other) and uses them for the recipient-level drilldown.

**MZe pLPIS výměra uživatelů** — aggregated LPIS land-block area per user from a dated public LPIS export, cross-referenced with the WFS layer to resolve user identities. Used exclusively as the denominator for the area-family subsidy branch. This is the best available public hectare proxy but is not an official closed annual statement of supported hectares.

**Years:** 2024.

**Per-unit metrics (by node):**
- Plošné dotace (area-family) → **Kč/ha** (LPIS-matched hectares)
- Recipient dotace (other families) → **Kč/příjemce dotace** (unique SZIF recipient count)
- Admin nodes → no per-unit metric

---

### Životní prostředí (MŽP + SFŽP)

**Monitor MF: MŽP a SFŽP** — annual expenditure of Ministerstvo životního prostředí (IČO `00164801`) and Státní fond životního prostředí (IČO `00020729`).

**SFŽP aktivní registr podpor** — continuously updated register of active SFŽP grants published at `otevrenadata.sfzp.cz`. Each row contains recipient name, municipality, programme, grant amount, paid amount, and decision date. The atlas creates an annual snapshot based on the decision year and uses it for the support-family → programme → recipient drilldown.

**Years:** 2024, 2025.

**Per-unit metric:** **Kč/příjemce podpory** — unique recipient count from the SFŽP registry. Applied to SFŽP support branches; the MŽP administrative residual does not carry a per-unit metric.

---

### Místní rozvoj (MMR + IROP)

**MMR otevřené rozpočtové ukazatele** — official CSV of MMR budget indicators by expenditure block, published as open data at `mmr.gov.cz`. Used to split the MMR chapter into regional development (IROP), housing, and residual administration.

**DotaceEU / IROP: seznam operací příjemců** — monthly workbook of IROP-funded projects with project name, recipient, IČO, region, and allocated eligible expenditure, published at `dotaceeu.cz`. The atlas uses December snapshots. Enables the regional → recipient drilldown inside the IROP branch.

**Years:** 2024, 2025.

**Per-unit metric:** **Kč/příjemce podpory** — unique recipient IČO count from the IROP operations workbook. Applied to the IROP regional and housing branches.

---

### Průmysl a obchod (MPO + OP TAK)

**Monitor MF: MPO** — annual expenditure of Ministerstvo průmyslu a obchodu (IČO `47609109`).

**DotaceEU / OP TAK: seznam operací příjemců** — monthly workbook of OP TAK (Operational Programme Technologies and Applications for Competitiveness, 2021–2027) projects with project name, recipient, IČO, region, and allocated eligible expenditure. December snapshots. Enables the region → recipient drilldown inside the OP TAK support branch.

**Years:** 2024, 2025.

**Per-unit metric:** **Kč/příjemce podpory** — unique recipient IČO count from the OP TAK operations workbook. Applied to the OP TAK support branch.

---

### Kultura (MK)

**Monitor MF: MK** — annual expenditure of Ministerstvo kultury (IČO `00023671`).

**MK závěrečný účet kapitoly 334** — manually curated selection of large budget line items from the official MK final account, specifically film incentives (`filmové pobídky`) and church restitution support (`církevní podpora`). These are large unmixed blocks that would otherwise remain hidden in the residual.

**MK zveřejněné výsledky dotačních programů** — recipient-level results of selected MK grant programmes, in particular:
- *Kulturní aktivity pro spolky v muzejnictví* (culture-museums programme)
- *Kulturní aktivity v oblasti umění* and related cultural activity grants
Published as result lists after each grant round.

**MK PZAD souhrnné tabulky** — official summary tables of the Programme for the Rescue of Architectural Heritage (Program záchrany architektonického dědictví) with allocation and recipient counts by region. Published as a summary PDF after the grant round. Enables the regional drilldown inside the PZAD heritage branch without requiring a recipient-level breakdown that MK does not openly publish.

**Years:** 2024, 2025.

**Per-unit metric:** **Kč/příjemce podpory** — applied to programme branches where recipient-level data are published (culture-museums, PZAD regional). Film incentive and church restitution nodes do not carry a per-unit metric.

---

### Zahraniční věci (MZV)

**Monitor MF: MZV** — annual expenditure of Ministerstvo zahraničních věcí (IČO `45769851`).

**MZV Česká diplomacie 2024** — official annual publication of the Czech diplomatic network, available as a PDF at `mzv.gov.cz`. The ETL extracts the total count of foreign posts (`zastupitelské úřady a úřady v zahraničí`) as the denominator for the foreign-service branch.

**MZV a ČRA výroční přehledy rozvojových a humanitárních projektů** — official annual workbooks of Czech development cooperation (ODA) and humanitarian aid projects, published by MZV and the Czech Development Agency (ČRA). Each row contains country, sector, recipient organisation, project name, planned amount, and actual drawn amount. The atlas separates the development and humanitarian branches and enables the country → project drilldown for both.

**Years:** 2024.

**Per-unit metrics (by node):**
- Zahraniční služba → **Kč/zastupitelský úřad** (total foreign post count from Česká diplomacie 2024)
- Rozvojová pomoc → **Kč/projekt** (project count from ODA workbook)
- Humanitární pomoc → **Kč/projekt** (project count from humanitarian workbook)

---

### Obrana (MO)

**Monitor MF: MO** — annual expenditure of Ministerstvo obrany. Due to the organisational structure of the defence chapter, the ETL aggregates expenditure across the ministry and its subordinate units.

**MO Fakta a trendy 2025** — official publication of the Ministry of Defence (`mocr.mo.gov.cz`) containing a multi-year summary table of three expenditure categories: Programové financování a modernizace (programme financing and modernisation), Osobní mandatorní výdaje (mandatory personnel expenditure), and Ostatní běžné výdaje (other current expenditure). The category amounts are proportionally scaled to the total Monitor MF volume to ensure consistency.

**MO počty vojáků z povolání** — official table of professional soldier (`voják z povolání`) headcounts from the same Fakta a trendy 2025 publication. Used as the denominator for all defence nodes.

**Years:** 2024, 2025.

**Per-unit metric:** **Kč/vojáka z povolání/rok** — applied uniformly to the MO ministry node and all three category branches. This is the most legible public denominator available; MO does not publish open operational performance data that would allow a more granular unit.

---

### Finance (MF)

**Monitor MF: MF, GFŘ, GŘC** — annual expenditure of three separate state-budget organisational units that together constitute the Ministry of Finance chapter:
- Ministerstvo financí — vlastní aparát (IČO `00006947`): the ministry proper, ~2.9 billion CZK in 2024
- Generální finanční ředitelství / Finanční správa (IČO `72080043`): tax administration body overseeing ~14 000 staff, ~12.7 billion CZK in 2024
- Generální ředitelství cel / Celní správa (IČO `71214011`): customs administration, ~6.4 billion CZK in 2024

All three are queried individually from Monitor MF, giving directly observed branch amounts — no scaling or estimation is required, unlike ministries with a single reporting IČO.

**Finanční správa ČR výroční zpráva — počet daňových subjektů** — annual count of registered tax entities (`registrované daňové subjekty`) from the Financial Administration annual report, published at `financnisprava.cz`. Approximately 3.617 million in 2024.

**Years:** 2024.

**Per-unit metric:** **Kč/daňový subjekt** — applied to the MF ministry node and all three sub-branches. The registered tax entity count is the most legible and consistently published public denominator for tax administration; it is available annually in the Financial Administration annual report.

---

### Zdravotnictví (MZ)

**Monitor MF: MZ a rozpočtové entity hygieny** — annual expenditure of Ministerstvo zdravotnictví (IČO `00024341`) and subordinate entities including regional hygiene stations (KHS) and national public health institutes.

**Monitor MF: zdravotní pojišťovny** — annual financial statements of public health insurers queried from Monitor MF by their IČOs. Used to derive the public-insurance aggregate for the hospital and outpatient branches.

**NZIP A038: zdravotnická záchranná služba** — annual aggregated performance data for the Emergency Medical Service (ZZS) from the National Health Information Portal (`nzip.cz`), including total interventions, patients, and emergency calls. Used as the denominator for the ZZS branch.

**ČSÚ ZDR02: financování zdravotnictví** — National Health Accounts dataset from the Czech Statistical Office (`data.csu.gov.cz`), covering healthcare financing by provider type and financing type, including the outpatient sector. Used for the ambulatory / outpatient branch aggregate where provider-level finance data are not available.

**ÚZIS Monitor: výkazy zdravotnických zařízení** — annual financial statements of individual healthcare providers (IČO-level) from the Institute of Health Information and Statistics (ÚZIS), accessed via Monitor MF. Used to build the hospital owner-type (region / municipality / state / unverified) breakdown and the ZZS branch.

**Years:** 2024, 2025.

**Per-unit metrics (by node):**
- Nemocnice → no per-unit metric at the top level (mixed owner types); per-owner drilldown uses cost per institution
- ZZS → denominator is total annual ZZS interventions (**Kč/zásah**)
- Ambulantní péče → no per-unit metric (ČSÚ ZDR02 does not publish a clean unit count at the atlas level)
- Veřejné zdravotnictví → no per-unit metric

---

## Architektura / Architecture

```
Browser (React + ECharts)
        │
        ▼
GitHub Pages  ──── static bundle ────▶  Vercel Edge Functions (/api/atlas/*)
                                                │
                                                ▼
                                       Neon Postgres (PostgreSQL 17)
                                       ┌──────────────────────────┐
                                       │  meta.*  – ETL lineage   │
                                       │  raw.*   – ingested data  │
                                       │  core.*  – normalised     │
                                       │  mart.*  – query-ready    │
                                       └──────────────────────────┘
                                                ▲
                                        GitHub Actions ETL
```

- **Frontend**: React 19 + TypeScript, ECharts Sankey component, served as a static bundle from GitHub Pages.
- **API**: Vercel serverless functions (`api/atlas/[resource].ts`) — read-only, one function per ministry resource.
- **Database**: Neon serverless PostgreSQL 17 hosted in `aws-eu-central-1`. Four schemas: `meta` (ETL lineage and dataset releases), `raw` (ingested source data), `core` (normalised shared entities), `mart` (denormalised latest views consumed by the API).
- **ETL**: Python 3.11+ scripts in `etl/`, one subdirectory per domain. Each domain has a `fetch_*.py` (download) and is loaded via a shared `load_*_raw.py` loader that registers dataset releases in `meta.dataset_release`.

### Database schema conventions

Every raw table follows the same pattern:

```sql
create table raw.<domain>_<entity> (
  raw_id             bigserial primary key,
  dataset_release_id bigint references meta.dataset_release,
  reporting_year     integer,
  -- domain-specific columns --
  payload            jsonb,          -- full source row as JSON
  loaded_at          timestamptz
);

create or replace view mart.<domain>_<entity>_latest as
select distinct on (reporting_year, <natural_key>)
  ...
from raw.<domain>_<entity> r
join meta.dataset_release d using (dataset_release_id)
order by reporting_year, <natural_key>, d.snapshot_label desc, r.raw_id desc;
```

This `distinct on` pattern ensures that re-running an ETL load always reflects the latest snapshot without requiring destructive deletes of historical data.

---

## ETL pipeline

```
etl/
├── mf/                         # Ministerstvo financí
│   ├── fetch_budget_entities.py    # Monitor MF → 3 IČOs (MF, GFŘ, GŘC)
│   └── fetch_activity_metrics.py   # Finanční správa VZ → tax subject count
├── mo/                         # Ministerstvo obrany
│   ├── fetch_budget_entities.py    # Monitor MF → MO entities
│   ├── fetch_budget_aggregates.py  # PDF: Fakta a trendy 2025 → category amounts
│   └── fetch_personnel_metrics.py  # PDF: Fakta a trendy 2025 → soldier count
├── mzv/                        # Ministerstvo zahraničních věcí
│   ├── fetch_budget_entities.py    # Monitor MF → MZV
│   ├── fetch_diplomatic_metrics.py # PDF: Česká diplomacie 2024 → post count
│   └── fetch_aid_operations.py     # XLSX: ODA + humanitarian project workbooks
├── mk/                         # Ministerstvo kultury
│   ├── fetch_budget_entities.py
│   ├── fetch_budget_aggregates.py  # PDF: závěrečný účet kap. 334
│   ├── fetch_support_awards.py     # XLSX/web: dotační výsledkové listy
│   └── fetch_region_metrics.py     # PDF: PZAD souhrnné tabulky
├── mpo/                        # Ministerstvo průmyslu a obchodu
│   ├── fetch_budget_entities.py
│   └── fetch_optak_operations.py   # XLSX: DotaceEU OP TAK seznam operací
├── mmr/                        # Ministerstvo pro místní rozvoj
│   ├── fetch_budget_aggregates.py  # CSV: MMR otevřená data
│   └── fetch_irop_operations.py    # XLSX: DotaceEU IROP seznam operací
├── environment/                # MŽP + SFŽP
│   ├── fetch_budget_entities.py
│   └── fetch_sfzp_supports.py      # API: SFŽP aktivní registr podpor
├── agriculture/                # MZe + SZIF
│   ├── fetch_budget_entities.py
│   ├── fetch_szif_payments.py      # CSV: SZIF seznamy příjemců
│   └── fetch_lpis_user_area.py     # WFS/LPIS: výměra uživatelů
├── transport/                  # MD + SFDI
│   ├── fetch_budget_entities.py
│   ├── fetch_sfdi_projects.py      # CSV: SFDI projektové čerpání
│   └── fetch_activity_metrics.py   # Various: rail/vignette/toll metrics
├── mv/                         # Ministerstvo vnitra
│   ├── fetch_budget_aggregates.py
│   ├── fetch_police_crime.py       # CSV: Policie ČR kriminální statistiky
│   └── fetch_fire_rescue.py        # PDF/CSV: HZS ročenka zásahů
├── justice/                    # Ministerstvo spravedlnosti
│   ├── fetch_budget_aggregates.py
│   └── fetch_activity_aggregates.py
├── social/                     # MPSV
│   ├── fetch_mpsv_aggregates.py
│   └── fetch_recipient_metrics.py  # ČSSZ open data
├── health/                     # MZ + pojišťovny
│   ├── fetch_monitor_indicators.py # Monitor MF: zdravotní entity
│   ├── fetch_provider_sites.py     # NRPZS: registr poskytovatelů
│   ├── fetch_claims_*.py           # VZP/pojišťovny: výkazy péče
│   ├── fetch_financing_aggregates.py  # ČSÚ ZDR02
│   └── fetch_zzs_activity.py       # NZIP A038
├── load_mf_raw.py
├── load_mo_raw.py
├── load_mzv_raw.py
├── load_mk_raw.py
├── load_mpo_raw.py
├── load_mmr_raw.py
├── load_environment_raw.py
├── load_agriculture_raw.py
├── load_transport_raw.py
├── load_mv_raw.py
├── load_justice_raw.py
├── load_social_raw.py
└── load_health_raw.py
```

### Running a domain ETL locally

```bash
# Example: MF (Ministerstvo financí)
cd etl/mf
python3 fetch_budget_entities.py --year 2024
python3 fetch_activity_metrics.py --year 2024
cd ../..
python3 etl/load_mf_raw.py --database-url "$DATABASE_URL"

# Example: MO (Ministerstvo obrany)
cd etl/mo
python3 fetch_budget_entities.py --year 2024 --year 2025
python3 fetch_budget_aggregates.py --year 2024 --year 2025
python3 fetch_personnel_metrics.py --year 2024 --year 2025
cd ../..
python3 etl/load_mo_raw.py --database-url "$DATABASE_URL"
```

The Neon CLI is the recommended way to obtain the connection string in CI:

```bash
npx neonctl connection-string --project-id <project-id> | \
  xargs -I{} python3 etl/load_mf_raw.py --database-url {}
```

---

## Srovnávací metriky / Per-unit metrics

Toggling **"Na jednotku"** in the chart switches all node weights and link widths from absolute CZK amounts to a normalised per-unit value, enabling meaningful cross-ministry comparison. The denominator used for each branch is shown in the in-app reference panel.

| Metric | Denominator | Applies to |
|---|---|---|
| Kč/žák/rok | Pupil count (MŠMT XLSX) | All school-finance flows |
| Kč/příjemce důchodu/rok | Pension recipient count (ČSSZ) | Pensions branch |
| Kč/příjemce podpory/rok | Unemployment benefit recipients | Unemployment branch |
| Kč/příjemce příspěvku/rok | Care-allowance recipients | Care allowance branch |
| Kč/příjemce dávky/rok | Substitute alimony recipients | Substitute alimony branch |
| Kč/registrovaný skutek | Registered crime acts (Policie ČR) | Police branch & regional drilldown |
| Kč/zásah | HZS interventions | Fire-rescue branch & regional drilldown |
| Kč/vyřízenou věc | Resolved court cases (MSp statistics) | Courts branch |
| Kč/vězněnou osobu/rok | Average daily inmate population | Prison service branch |
| Kč/cestujícího | Rail passengers (Správa železnic) | Rail infrastructure branch |
| Kč/prodanou dálniční známku | Annual vignette sales | Road vignette branch |
| Kč/zpoplatněné vozidlo | Toll-registered heavy vehicles | Road toll branch |
| Kč/akci | SFDI project count | SFDI investor / project drilldown |
| Kč/příjemce dotace | Unique SZIF recipients | Agriculture subsidy branches |
| Kč/ha | LPIS-matched hectares (pLPIS) | Area-based subsidy branch |
| Kč/příjemce podpory | Unique SFŽP recipient count | SFŽP support branches |
| Kč/příjemce podpory | Unique IROP recipient IČO count | MMR IROP branches |
| Kč/příjemce podpory | Unique OP TAK recipient IČO count | MPO OP TAK branch |
| Kč/příjemce podpory | Unique MK grant recipient count | MK programme branches |
| Kč/zastupitelský úřad | Foreign post count (Česká diplomacie 2024) | MZV foreign service branch |
| Kč/projekt | ODA / humanitarian project count (MZV/ČRA workbooks) | MZV aid branches |
| Kč/vojáka z povolání/rok | Professional soldier headcount (MO Fakta a trendy 2025) | All MO branches |
| Kč/daňový subjekt | Registered tax entity count (Finanční správa VZ) | All MF branches |

Flows for which no defensible per-unit denominator exists (mixed administrative residuals, inferred synthetic links) are excluded from normalisation and rendered at their absolute CZK weight when per-unit mode is active.

---

## Vyhledávání / Search

The search bar at the top of the atlas supports full-text, diacritic-insensitive search across:
- All 8 000+ school entities (by name or IČO)
- All health providers in the ÚZIS / Monitor MF directory (by name or IČO)

Selecting a result navigates directly to that entity's drilldown view regardless of the current atlas level.

---

## Vývoj / Development

### Prerequisites

- Node.js ≥ 22
- Python ≥ 3.11 with `psycopg`, `openpyxl`, `pypdf`, `requests`
- Neon Postgres instance (or any PostgreSQL 17-compatible database)
- Vercel CLI for local API development

### Quick start

```bash
npm install
cp .env.vercel.health .env.local   # or set DATABASE_URL manually
vercel dev                          # starts frontend + /api/* on localhost:3000
```

### Tests

```bash
npm test              # vitest unit tests
npm run smoke:prod    # smoke-check the live production API
```

The smoke test suite (`scripts/smoke-atlas-api.mjs`) verifies that every atlas resource endpoint returns the expected node and link structure for year 2024, including all drilldown levels. It runs on every GitHub Actions push and nightly against the production URL.

### Build & deploy

```bash
npm run build         # tsc + vite build → dist/
vercel --prod         # deploy to production
```

Database schema is applied by running `db/schema.sql` against the Neon instance. All statements use `create table if not exists` and `create or replace view`, making the schema idempotent:

```bash
npx neonctl connection-string --project-id <id> | \
  xargs -I{} python3 -c "
import psycopg, sys
with psycopg.connect(sys.argv[1]) as c:
    c.execute(open('db/schema.sql').read()); c.commit()
" {}
```

---

## CI/CD

| Workflow | Trigger | What it does |
|---|---|---|
| `ci.yml` | Push / PR | TypeScript check, Vite build, vitest unit tests |
| `smoke-production.yml` | Push + nightly | Smoke-tests the live production API endpoints |
| `refresh-neon-school-data.yml` | Manual / quarterly | Re-runs the school ETL and reloads Neon |
| `deploy-pages.yml` | Push to master | Builds and deploys the static frontend to GitHub Pages |

---

## Licence

MIT — see [LICENSE](LICENSE).

Data sources are published by Czech public authorities under the conditions described in the in-app reference panel for each dataset. The atlas does not modify source data; it aggregates, joins, and normalises it for visualisation purposes.
