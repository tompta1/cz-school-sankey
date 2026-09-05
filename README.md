# Kam šla moje daňová koruna?

Interaktivní Sankey diagram českých veřejných výdajů. Sleduje peníze od státního rozpočtu přes resorty k programům, regionům a tam, kde existují otevřená data, až ke konkrétním institucím nebo příjemcům.

**Aplikace:** [cz-school-sankey.vercel.app](https://cz-school-sankey.vercel.app)

Výchozí rok je **2025**. Rok 2024 zůstává pro srovnání a pro oblasti, kde ještě není dostupný stejně podrobný rok 2025. Absolutní částky jsou primární pohled; srovnávací režim používá pro každou větev její vlastní označenou jednotku. Nepodložené poměry jsou `N/A`.

## Co je kde

| Cesta | Obsah | Proč existuje |
|---|---|---|
| `src/` | React/Vite aplikace, Sankey graf a klientské řazení | Uživatelské rozhraní |
| `api/` | Vercel read-only API nad mart vrstvou | Malé odpovědi pro jednotlivé drilldowny |
| `etl/` | Python fetchery, loadery a transformace po doménách | Opakovatelný sběr a publikace dat |
| `etl/data/raw/` | Sledované malé snapshoty a lokální zdrojové soubory | Reprodukovatelnost bez ukládání velkých upstreamů do Gitu |
| `db/` | PostgreSQL schéma, migrace a lokální nástroje | Neon warehouse `meta`, `raw`, `core`, `mart` |
| `.github/workflows/` | CI, ETL, deploy a smoke testy | Bezobslužný produkční tok |
| `scripts/` | DQ, smoke a lokální DB utility | Ověření produkce a reconciliation |

## Pokrytí

| Doména | Interní kód | Detail | Dostupné roky |
|---|---|---|---|
| Školství | `school` | kraj → zřizovatel → škola → náklad | 2024, 2025 |
| Zdraví | `health` | nemocnice, ZZS, veřejné zdraví, ambulance | 2024, 2025; výkony nemocnic do 2024 |
| Sociální věci | `social` | skupiny dávek | rozpočet a čtyři srovnávací údaje 2024–2025 |
| Spravedlnost | `justice` | soudy, vězeňství, zastupitelství | rozpočet a stavy vězňů 2024–2025; soudní výkon 2024 |
| Zemědělství | `agriculture` | typ podpory → příjemce | rozpočet 2024–2025; detail hlavně 2024 |
| Životní prostředí | `environment` | SFŽP program → příjemce | 2024, 2025 |
| Regionální rozvoj | `mmr` / `regions` | IROP → kraj → příjemce | 2024, 2025 |
| Průmysl a obchod | `mpo` / `business` | OP TAK → kraj → příjemce | 2024, 2025 |
| Kultura | `mk` / `culture` | vybrané programy → kraj/příjemce | rozpočet 2024–2025; detail hlavně 2024 |
| Zahraničí | `mzv` / `foreign` | služba, pomoc → země → projekt | rozpočet 2024–2025; detail hlavně 2024 |
| Doprava | `transport` | druh infrastruktury → investor → akce | 2024, 2025 |
| Vnitro | `mv` / `internal` | Policie, HZS → kraj | rozpočet 2024–2025; výkon hlavně 2024 |
| Finance | `mf` / `finance` | MF, GFŘ, GŘC | 2024, 2025 |
| Obrana | `mo` / `defense` | programové, osobní a běžné výdaje | 2024, 2025 |

## Datové zdroje

Aplikace v panelu **Zdroje** ukazuje odkazy použité v právě otevřeném grafu. Hlavní upstreamy jsou:

| Oblast | Co používáme | URL |
|---|---|---|
| Státní rozpočet | závěrečný účet MF, sešit G | [mf.gov.cz](https://mf.gov.cz/cs/rozpoctova-politika/statni-rozpocet/plneni-statniho-rozpoctu) |
| Resorty a veřejné organizace | realizované výdaje a náklady podle IČO | [Monitor státní pokladny](https://monitor.statnipokladna.gov.cz) |
| Školy | rozpis rozpočtu a registr škol | [MŠMT](https://www.msmt.cz/vzdelavani/skolstvi-v-cr/statistika-skolstvi) |
| Zřizovatelé škol | FIN 2-12 M a účetní výkazy škol | [Monitor MF](https://monitor.statnipokladna.gov.cz) |
| EU projekty škol a IROP | seznamy operací | [DotaceEU](https://www.dotaceeu.cz/cs/informace-o-cerpani/seznamy-prijemcu) |
| OP TAK | seznam operací a příjemců | [DotaceEU](https://www.dotaceeu.cz/cs/statistiky-a-analyzy/seznam-operaci-%28prijemcu%29) |
| Identita organizací | názvy a sídla podle IČO | [ARES API](https://ares.gov.cz/swagger-ui/) |
| Nemocniční výkony | NRHZS podle IČO | [ÚZIS](https://datanzis.uzis.gov.cz/data/NR-04-NRHZS/NR-04-02/) |
| Ambulantní financování | národní zdravotní účty ZDR02 | [ČSÚ](https://data.csu.gov.cz/opendata/sady/ZDR02/distribuce/csv) |
| Záchranná služba | výkaz A038 | [NZIP](https://www.nzip.cz/data/1802-vykaz-a038-zdravotnicka-zachranna-sluzba-datovy-souhrn) |
| Sociální dávky | počty příjemců | [ČSSZ](https://data.cssz.cz/web/otevrena-data/), [MPSV](https://data.mpsv.cz) |
| Policie | registrované skutky KRI10 | [ČSÚ](https://data.csu.gov.cz/opendata/sady/KRI10/distribuce/csv) |
| HZS | zásahy podle krajů | [HZS](https://hzscr.gov.cz/hasicien/ViewFile.aspx?docid=22436114) |
| Justice | rozpočet a soudní data | [závěrečný účet 2024](https://msp.gov.cz/documents/d/msp/zaverecny-ucet-kapitoly-za-rok-2024-pdf), [ukazatele 2025](https://msp.gov.cz/documents/d/msp/zavazne-ukazatele-2025-pdf), [soudní data 2024](https://msp.gov.cz/documents/d/msp/data_soudy_2024-xlsm) |
| Doprava | čerpání projektů SFDI | [SFDI](https://sfdi.gov.cz) |
| Dopravní výkon | cestující, známky a mýtná vozidla | [SYDOS](https://www.sydos.cz/cs/rocenka-2024), [eDalnice](https://edalnice.cz), [CzechToll](https://www.czechtoll.cz) |
| Zemědělské podpory | příjemci SZIF | [SZIF](https://szif.gov.cz/cs/seznam-prijemcu-dotaci) |
| Zemědělská plocha | výměra uživatelů LPIS | [MZe pLPIS](https://mze.gov.cz/public/app/eagriapp/LpisData/Cr.aspx) |
| Životní prostředí | registr podpor SFŽP | [SFŽP](https://otevrenadata.sfzp.cz/) |
| MMR | rozpočtová otevřená data | [MMR](https://mmr.gov.cz/cs/ministerstvo/urad/povinne-zverejnene-informace/otevrena-data-mmr) |
| Kultura | závěrečný účet a výsledky podpor | [MK](https://mk.gov.cz) |
| Zahraniční pomoc | výroční projektové přehledy | [MZV](https://mzv.gov.cz/jnp/cz/zahranicni_vztahy/rozvojova_spoluprace/koncepce_publikace/vyrocni_prehledy/prehled_rozvojove_spoluprace_a_2.html) |
| Obrana | Fakta a trendy | [MO](https://mocr.mo.gov.cz/finance-a-zakazky/resortni-rozpocet/1resortni-rozpocet--263042/) |
| Správa daní | výroční zprávy Finanční správy | [Finanční správa](https://www.financnisprava.cz/cs/financni-sprava/zpravy-a-analyzy/vyrocni-zpravy) |

## Srovnávací režim

| Větev | Jednotka |
|---|---|
| školy | Kč/žák/rok |
| veřejné nemocnice | Kč/vykázaný výkon |
| ZZS | Kč/výjezd |
| důchody a vybrané dávky | Kč/příjemce/rok |
| Policie / HZS | Kč/registrovaný skutek / zásah |
| soudy / vězeňství | Kč/vyřízenou věc / vězněnou osobu |
| železnice / známky / mýto | Kč/cestujícího / známku / vozidlo |
| plošné zemědělské podpory | Kč/ha |
| ostatní dohledatelné podpory | Kč/příjemce |
| obrana / finanční správa | Kč/vojáka / daňový subjekt |
| MPO, MZV a nepodložené smíšené větve | `N/A` |

Jednotky nejsou totožné výsledkové ukazatele. Slouží k orientačnímu srovnání intenzity uvnitř označených větví, ne k hodnocení jejich společenské hodnoty.

## Architektura

```text
Browser
  └─ Vercel: Vite frontend + /api/atlas/*
       └─ Neon PostgreSQL
            ├─ meta  release a lineage
            ├─ raw   malé zdrojové snapshoty
            ├─ core  sdílené entity
            └─ mart  query-ready pohledy

GitHub Actions
  ├─ refresh-neon-domains.yml
  ├─ deploy-vercel-production.yml
  └─ smoke-production.yml
```

Produkční sled je záměrně rozdělený na `ETL → Neon verification → Vercel deploy → production smoke`. GitHub Pages není produkční API cíl.

## Lokální vývoj

Požadavky: Node.js 22+, Python 3.11+ a PostgreSQL 17 nebo Neon.

```bash
npm install
npm run dev
npm test
npm run build
```

Pro lokální API nastavte `DATABASE_URL` a spusťte `vercel dev`. Lokální databázi lze spravovat přes:

```bash
npm run db:up
npm run db:apply:schema
npm run db:down
```

## ETL a produkce

Hlavní vstup je ruční i plánovaný workflow `.github/workflows/refresh-neon-domains.yml`. Přátelské názvy domén (`regions`, `business`, `culture`, `foreign`, `internal`, `finance`, `defense`) se mapují na interní kódy (`mmr`, `mpo`, `mk`, `mzv`, `mv`, `mf`, `mo`). Školství zůstává ruční, protože používá sledované roční workbooky a evidence zřizovatelů.

GitHub konfigurace:

- secret `NEON_DATABASE_URL`
- secret `VERCEL_TOKEN`
- variable `PRODUCTION_API_BASE_URL`
- variables `VERCEL_ORG_ID` a `VERCEL_PROJECT_ID`, případně `.vercel/project.json`

Neon free-tier ochrana kontroluje velikost před loadem, používá výchozí limit **440 MB**, po ověření maže nahrazené releasy a spouští běžný `VACUUM (ANALYZE)`. Velké upstream soubory se nesmí ukládat celé do Neon.

Ověření:

```bash
npm test
npm run build
npm run dq:top-level
npm run smoke:prod
```

## Licence

MIT. Zdrojová data zůstávají pod podmínkami příslušných veřejných institucí.
