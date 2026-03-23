import type { ApiGraph, SankeyLink } from '../types';

import { metricDescriptorsForLinks, type MetricDescriptor } from './metricReference';

export interface DatasetReference {
  datasetKey: string;
  title: string;
  description: string;
  freshness: string;
  rationale: string;
  url?: string;
}

export interface AtlasReferenceSummary {
  metrics: MetricDescriptor[];
  datasets: DatasetReference[];
  notes: string[];
  inferredFlowCount: number;
}

const DATASET_REFERENCES: Record<string, DatasetReference> = {
  'api.aggregated': {
    datasetKey: 'api.aggregated',
    title: 'Školský warehouse atlasu',
    description: 'Agregované školské toky nad lokálně připraveným warehouse modelem MŠMT, zřizovatelů a školských výdajů.',
    freshness: 'Ruční roční ETL. Aktuálně jsou v atlasu nahrané roky 2024 a 2025.',
    rationale: 'Dává konzistentní resortní finanční obraz od státního rozpočtu po školy.',
  },
  aggregated: {
    datasetKey: 'aggregated',
    title: 'Školský warehouse atlasu',
    description: 'Agregované školské toky nad lokálně připraveným warehouse modelem MŠMT, zřizovatelů a školských výdajů.',
    freshness: 'Ruční roční ETL. Aktuálně jsou v atlasu nahrané roky 2024 a 2025.',
    rationale: 'Dává konzistentní resortní finanční obraz od státního rozpočtu po školy.',
  },
  health_mz_budget_entities: {
    datasetKey: 'health_mz_budget_entities',
    title: 'Monitor MF: MZ a rozpočtové entity hygieny',
    description: 'Rozpočtové a nákladové údaje ministerstva zdravotnictví, KHS a dalších veřejně zdravotních institucí z Monitoru MF.',
    freshness: 'Roční stav po uzávěrce rozpočtového roku. V atlasu jsou nahrané roky 2024 a 2025.',
    rationale: 'Je to nejpřímější oficiální zdroj pro top-level veřejné zdravotnictví a hygienu.',
    url: 'https://monitor.statnipokladna.gov.cz',
  },
  health_financing_aggregates: {
    datasetKey: 'health_financing_aggregates',
    title: 'ČSÚ ZDR02: financování zdravotnictví',
    description: 'Národní zdravotnické účty podle typu financování a typu poskytovatele, včetně ambulantní péče.',
    freshness: 'Roční dataset s typickým zpožděním. V atlasu se pro některé větve používá poslední dostupný oficiální rok.',
    rationale: 'Pro ambulantní agregace je to nejbezpečnější veřejný zdroj bez předstírání provider-level financí.',
    url: 'https://data.csu.gov.cz/opendata/sady/ZDR02/distribuce/csv',
  },
  health_zzs_activity_aggregates: {
    datasetKey: 'health_zzs_activity_aggregates',
    title: 'NZIP A038: zdravotnická záchranná služba',
    description: 'Agregované výkony a pacienti zdravotnické záchranné služby.',
    freshness: 'Roční výkazová data. V atlasu je denominator zatím použit hlavně na vrcholu větve ZZS.',
    rationale: 'Je to nejbližší oficiální objemový ukazatel k přednemocniční neodkladné péči.',
    url: 'https://www.nzip.cz/data/1802-vykaz-a038-zdravotnicka-zachranna-sluzba-datovy-souhrn',
  },
  social_mpsv_aggregates: {
    datasetKey: 'social_mpsv_aggregates',
    title: 'MPSV: kapitolní sociální agregace',
    description: 'Ruční resortní rozpad výdajových bloků MPSV na důchody, dávky, péči a správu.',
    freshness: 'Roční rozpočtová / závěrečná data. Detailní rozpad je zatím nahraný hlavně pro 2024.',
    rationale: 'Umožňuje čitelný rozpad velkého ministerského bloku na skutečné sociální programy.',
  },
  social_recipient_metrics: {
    datasetKey: 'social_recipient_metrics',
    title: 'ČSSZ a MPSV: počty příjemců dávek',
    description: 'Počty příjemců důchodů, podpor a vybraných dávek použité jako denominátory.',
    freshness: 'Roční nebo prosincové stavové statistiky. Nepokrývá všechny sociální programy.',
    rationale: 'Srovnávací metrika se používá jen u těch dávek, kde existuje obhajitelný počet příjemců.',
    url: 'https://data.cssz.cz/web/otevrena-data/',
  },
  mv_budget_aggregates: {
    datasetKey: 'mv_budget_aggregates',
    title: 'MV: kapitolní bezpečnostní agregace',
    description: 'Rozpad kapitoly MV na Policii, HZS, sociální dávky MV a ostatní správu.',
    freshness: 'Roční závěrečné a rozpočtové údaje. 2025 má zatím hrubší rozpad než 2024.',
    rationale: 'Zajišťuje konzistentní horní vrstvu pro bezpečnostní resort.',
  },
  mv_police_crime_aggregates: {
    datasetKey: 'mv_police_crime_aggregates',
    title: 'Policie ČR / veřejné kriminální statistiky',
    description: 'Registrované skutky po krajích a třídách kriminality.',
    freshness: 'Roční kriminalitní statistiky. Aktuální drilldown je založen na 2024.',
    rationale: 'Je to nejstabilnější regionální objemový ukazatel pro policejní větev.',
  },
  mv_fire_rescue_activity_aggregates: {
    datasetKey: 'mv_fire_rescue_activity_aggregates',
    title: 'HZS: statistická ročenka zásahů',
    description: 'Počty zásahů HZS na národní a krajské úrovni.',
    freshness: 'Roční ročenka HZS. V atlasu je denominator zaveden pro 2024.',
    rationale: 'Zásahy jsou oficiální provozní jednotka, která dává smysl pro HZS.',
  },
  justice_budget_aggregates: {
    datasetKey: 'justice_budget_aggregates',
    title: 'MSp: kapitolní justiční agregace',
    description: 'Rozpad ministerstva spravedlnosti na soudy, vězeňství, státní zastupitelství a další bloky.',
    freshness: 'Roční závěrečné / rozpočtové údaje. Detailnější rozpad je zatím k dispozici zejména pro 2024.',
    rationale: 'Umožňuje zobrazit justici jako několik odlišných provozních systémů místo jedné krabice.',
  },
  justice_activity_aggregates: {
    datasetKey: 'justice_activity_aggregates',
    title: 'Soudní a vězeňské výkonové statistiky',
    description: 'Vyřízené soudní věci a průměrné stavy vězněných osob použité jako denominátory.',
    freshness: 'Roční resortní statistiky, zatím hlavně pro 2024.',
    rationale: 'Jde o nejbližší obhajitelné výkonové jmenovatele pro soudy a vězeňství.',
  },
  transport_budget_entities: {
    datasetKey: 'transport_budget_entities',
    title: 'Monitor MF: MD a SFDI',
    description: 'Roční rozpočtové a nákladové údaje Ministerstva dopravy a SFDI.',
    freshness: 'Roční účetní data. V atlasu jsou nahrané roky 2024 a 2025.',
    rationale: 'Tvoří horní finanční obal dopravního resortu.',
    url: 'https://monitor.statnipokladna.gov.cz',
  },
  transport_sfdi_projects: {
    datasetKey: 'transport_sfdi_projects',
    title: 'SFDI: projektové čerpání',
    description: 'Otevřené CSV projektů a čerpání, které se používá pro větve investorů a konkrétních akcí.',
    freshness: 'Roční snapshot projektu/čerpání. V atlasu jsou nahrané roky 2024 a 2025.',
    rationale: 'Je to jediný detailní veřejný zdroj, který dovoluje dopravní drilldown po investorech a akcích.',
    url: 'https://kz.sfdi.cz',
  },
  transport_activity_metrics: {
    datasetKey: 'transport_activity_metrics',
    title: 'Dopravní výkonové proxy metriky',
    description: 'Roční počty cestujících, prodaných známek a registrovaných mýtných vozidel pro srovnávací metriky v dopravě.',
    freshness: 'Roční oficiální publikace; některé denominatorové řady mohou zaostávat za rozpočtovým rokem o jeden rok.',
    rationale: 'Oddělují železnici, osobní dálniční provoz a těžká vozidla bez předstírání jednotné dopravní metriky.',
  },
  agriculture_budget_entities: {
    datasetKey: 'agriculture_budget_entities',
    title: 'Monitor MF: MZe a SZIF správa',
    description: 'Roční rozpočtové a nákladové údaje Ministerstva zemědělství a provozu SZIF.',
    freshness: 'Roční účetní data z Monitoru MF. V atlasu slouží jako správní vrstva resortu.',
    rationale: 'Oddělují provozní správu resortu od skutečně vyplacených zemědělských dotací.',
    url: 'https://monitor.statnipokladna.gov.cz',
  },
  agriculture_szif_payments: {
    datasetKey: 'agriculture_szif_payments',
    title: 'SZIF: seznamy příjemců dotací',
    description: 'Otevřené CSV seznamů příjemců dotací z fondů EU a z národních zdrojů včetně identifikace příjemce, opatření a vyplacené částky.',
    freshness: 'Uzavřené fiskální roky EU mají samostatné roční soubory; atlas zatím používá uzavřený rok 2024.',
    rationale: 'Je to nejpřímější veřejný zdroj pro dotační větev MZe. Atlas z něj skládá rodiny opatření a používá jej pro fallback metriku Kč/příjemce tam, kde není k dispozici věcnější jednotka.',
    url: 'https://szif.gov.cz/cs/seznam-prijemcu-dotaci',
  },
  agriculture_lpis_user_area: {
    datasetKey: 'agriculture_lpis_user_area',
    title: 'MZe pLPIS: výměra uživatelů',
    description: 'Agregovaná výměra dílů půdních bloků podle uživatele z datovaného veřejného exportu LPIS, spojená s veřejnou WFS vrstvou kvůli identifikaci uživatele.',
    freshness: 'Atlas používá konkrétní datovaný export LPIS DPB a doplňuje k němu jména uživatelů z aktuální veřejné WFS vrstvy. Jde o nejlepší veřejně dostupný hektarový proxy, ale ne o oficiální uzavřený výkaz podporovaných hektarů.',
    rationale: 'U největších plošných opatření je hektar nejbližší věcný jmenovatel. Atlas jej používá jen pro area-family větev a jen nad uživateli, které se podaří spárovat s příjemcovskou vrstvou SZIF.',
    url: 'https://mze.gov.cz/public/app/eagriapp/LpisData/Cr.aspx',
  },
  environment_budget_entities: {
    datasetKey: 'environment_budget_entities',
    title: 'Monitor MF: MŽP a SFŽP',
    description: 'Roční rozpočtové a nákladové údaje Ministerstva životního prostředí a Státního fondu životního prostředí.',
    freshness: 'Roční účetní data. V atlasu jsou nahrané roky 2024 a 2025.',
    rationale: 'Tvoří skutečný výdajový obal environmentální větve, ze kterého se odděluje fondová podpora SFŽP.',
    url: 'https://monitor.statnipokladna.gov.cz',
  },
  environment_sfzp_supports: {
    datasetKey: 'environment_sfzp_supports',
    title: 'SFŽP: aktivní registr podpor',
    description: 'Otevřený registr aktivních podpor SFŽP s příjemcem, obcí, částkou podpory, vyplacenou částkou a datem podpisu rozhodnutí.',
    freshness: 'Průběžně aktualizovaný registr. Atlas z něj vytváří roční snapshot podle roku podpisu rozhodnutí.',
    rationale: 'Pro první iteraci MŽP je to nejpraktičtější veřejný recipient-level zdroj. Používá se pro recipient counts a pro programové share při rozdělení skutečných výdajů SFŽP.',
    url: 'https://otevrenadata.sfzp.cz/',
  },
  mmr_budget_aggregates: {
    datasetKey: 'mmr_budget_aggregates',
    title: 'MMR: otevřené rozpočtové ukazatele',
    description: 'Oficiální CSV rozpočtových ukazatelů MMR po hlavních výdajových blocích.',
    freshness: 'Roční otevřený CSV export. V atlasu jsou zatím nahrané roky 2024 a 2025.',
    rationale: 'Je to nejjednodušší a nejpřímější otevřený top-level zdroj pro větev MMR.',
    url: 'https://mmr.gov.cz/cs/ministerstvo/urad/povinne-zverejnene-informace/otevrena-data-mmr',
  },
  mmr_irop_operations: {
    datasetKey: 'mmr_irop_operations',
    title: 'DotaceEU / IROP: seznam operací příjemců',
    description: 'Měsíční workbook operací s názvem projektu, příjemcem, IČ, krajem a přidělenými způsobilými výdaji.',
    freshness: 'Průběžně aktualizovaný měsíční export. Atlas používá prosincové snapshoty pro 2024 a 2025.',
    rationale: 'U MMR je to nejsilnější recipient-level otevřený zdroj, který umožňuje drilldown do kraje a IČO bez vymýšlení neveřejných vazeb.',
    url: 'https://www.dotaceeu.cz/cs/informace-o-cerpani/seznamy-prijemcu',
  },
  mpo_budget_entities: {
    datasetKey: 'mpo_budget_entities',
    title: 'Monitor MF: MPO',
    description: 'Roční rozpočtové a nákladové údaje Ministerstva průmyslu a obchodu.',
    freshness: 'Roční účetní data. V atlasu jsou nahrané roky 2024 a 2025.',
    rationale: 'Dává skutečný horní výdajový obal pro MPO, i když detailní recipient-level drilldown je zatím dostupný jen pro dotační část.',
    url: 'https://monitor.statnipokladna.gov.cz',
  },
  mpo_optak_operations: {
    datasetKey: 'mpo_optak_operations',
    title: 'DotaceEU / OP TAK: seznam operací příjemců',
    description: 'Měsíční workbook 2021+ programů s projektem, příjemcem, IČ, krajem a přidělenými způsobilými výdaji. Atlas z něj používá program OP TAK.',
    freshness: 'Průběžně aktualizovaný měsíční export. Atlas používá prosincové snapshoty pro 2024 a 2025.',
    rationale: 'Je to nejsilnější veřejný recipient-level zdroj pro dotační větev MPO a umožňuje drilldown až do IČO.',
    url: 'https://www.dotaceeu.cz/cs/statistiky-a-analyzy/seznam-operaci-%28prijemcu%29',
  },
  mk_budget_entities: {
    datasetKey: 'mk_budget_entities',
    title: 'Monitor MF: MK',
    description: 'Roční rozpočtové a nákladové údaje Ministerstva kultury.',
    freshness: 'Roční účetní data. V atlasu jsou zatím nahrané roky 2024 a 2025.',
    rationale: 'Dávají skutečný horní výdajový obal pro resort kultury, i když detailní recipient-level drilldown je zatím dostupný jen pro vybrané programy.',
    url: 'https://monitor.statnipokladna.gov.cz',
  },
  mk_budget_aggregates: {
    datasetKey: 'mk_budget_aggregates',
    title: 'MK: závěrečný účet kapitoly 334',
    description: 'Ruční výběr velkých rozpočtových položek MK z oficiálního závěrečného účtu, zejména filmové pobídky a církevní podpora.',
    freshness: 'Roční uzavřený dokument. Detailní agregace jsou zatím nahrané pro 2024.',
    rationale: 'Oddělují velké nemíchané bloky, které by jinak zůstaly schované v resortním reziduu.',
    url: 'https://mk.gov.cz',
  },
  mk_support_awards: {
    datasetKey: 'mk_support_awards',
    title: 'MK: zveřejněné výsledky dotačních programů',
    description: 'Recipient-level výsledky vybraných programů MK, zejména kulturních aktivit pro spolky v muzejnictví a kulturních aktivit v památkové péči.',
    freshness: 'Roční výsledkové dokumenty po dotačním řízení. V atlasu je zatím nahraný rok 2024.',
    rationale: 'Jde o přesně zveřejněné programové seznamy příjemců; atlas je používá jen tam, kde ministerstvo skutečně publikuje recipient-level výsledky.',
    url: 'https://mk.gov.cz',
  },
  mk_region_metrics: {
    datasetKey: 'mk_region_metrics',
    title: 'MK: PZAD souhrnné tabulky',
    description: 'Oficiální souhrnné tabulky Programu záchrany architektonického dědictví s alokací a počty příjemců podle krajů.',
    freshness: 'Roční souhrnný PDF dokument. V atlasu je zatím nahraný rok 2024.',
    rationale: 'Umožňuje pravdivý regionální drilldown PZAD bez vymýšlení recipient-level rozkladu tam, kde není otevřeně zveřejněný.',
    url: 'https://mk.gov.cz',
  },
  mzv_budget_entities: {
    datasetKey: 'mzv_budget_entities',
    title: 'Monitor MF: Ministerstvo zahraničních věcí',
    description: 'Roční rozpočtové a nákladové údaje kapitoly MZV z Monitoru MF.',
    freshness: 'Roční účetní data. V atlasu jsou nahrané roky 2024 a 2025.',
    rationale: 'Dávají skutečný horní výdajový obal pro MZV, nad který se pak navazují explicitní aid/workbook vrstvy.',
    url: 'https://monitor.statnipokladna.gov.cz',
  },
  mzv_diplomatic_metrics: {
    datasetKey: 'mzv_diplomatic_metrics',
    title: 'MZV: Česká diplomacie 2024',
    description: 'Oficiální přehled zahraniční sítě MZV včetně počtu zastupitelských úřadů a jejich typů.',
    freshness: 'Roční publikační přehled. V atlasu je zatím použit rok 2024 jako poslední dostupná oficiální síťová struktura.',
    rationale: 'Je to nejčitelnější veřejný denominator pro zahraniční službu, protože MZV nepublikuje provozní výkon po ambasádách jako otevřená data.',
    url: 'https://mzv.gov.cz/jnp/cz/zahranicni_vztahy/vyrocni_zpravy_a_dokumenty/publikace_ceska_diplomacie_2024.html',
  },
  mzv_aid_operations: {
    datasetKey: 'mzv_aid_operations',
    title: 'MZV a ČRA: výroční přehledy rozvojových a humanitárních projektů',
    description: 'Oficiální workbooky s projekty zahraniční rozvojové spolupráce a humanitární pomoci včetně země realizace, realizátora a skutečného čerpání.',
    freshness: 'Roční uzavřené přehledy. V atlasu je zatím nahraný rok 2024.',
    rationale: 'Jde o nejsilnější veřejný projektový zdroj pro aid větev MZV; umožňuje oddělit rozvojovou a humanitární pomoc bez vymýšlení neveřejných vazeb.',
    url: 'https://mzv.gov.cz/jnp/cz/zahranicni_vztahy/rozvojova_spoluprace/koncepce_publikace/vyrocni_prehledy/prehled_rozvojove_spoluprace_a_2.html',
  },
  mo_budget_entities: {
    datasetKey: 'mo_budget_entities',
    title: 'Monitor MF: Ministerstvo obrany',
    description: 'Roční rozpočtové a nákladové údaje kapitoly Ministerstva obrany z Monitoru MF.',
    freshness: 'Roční účetní data. V atlasu jsou nahrané roky 2024 a 2025.',
    rationale: 'Dávají skutečný horní výdajový obal obranné kapitoly, na který se navazuje jednoduchý oficiální rozpad MO.',
    url: 'https://monitor.statnipokladna.gov.cz',
  },
  mo_budget_aggregates: {
    datasetKey: 'mo_budget_aggregates',
    title: 'MO: Fakta a trendy 2025',
    description: 'Oficiální tabulka základních oblastí výdajů kapitoly MO: Programové financování, Osobní mandatorní výdaje a Ostatní běžné výdaje.',
    freshness: 'Publikační přehled s historickou tabulkou. V atlasu se používají roky 2024 a 2025.',
    rationale: 'Je to nejjednodušší a oficiálně publikovaný rozpad obranné kapitoly na tři srozumitelné bloky bez potřeby vymýšlet neveřejné podvětve.',
    url: 'https://mocr.mo.gov.cz/finance-a-zakazky/resortni-rozpocet/-resortni-rozpocet-254181/',
  },
  mo_personnel_metrics: {
    datasetKey: 'mo_personnel_metrics',
    title: 'MO: počty vojáků z povolání',
    description: 'Oficiální tabulka počtů vojáků z povolání z publikace Fakta a trendy 2025.',
    freshness: 'Publikační přehled. V atlasu se používají roky 2024 a 2025.',
    rationale: 'Počet vojáků z povolání je první obhajitelný veřejný denominator pro jednoduchou obrannou větev; MO nepublikuje otevřená výkonová data, která by dovolovala pravdivější jednotku pro celý resort.',
    url: 'https://mocr.mo.gov.cz/finance-a-zakazky/resortni-rozpocet/-resortni-rozpocet-254181/',
  },
  atlas_inferred: {
    datasetKey: 'atlas.inferred',
    title: 'Odvozené atlasové alokace',
    description: 'Syntetické nebo odvozené toky vytvořené z více oficiálních zdrojů, nikoli přímo reportované jako jeden údaj.',
    freshness: 'Vznikají při každém přepočtu atlasu nad aktuálně nahranými daty.',
    rationale: 'Jsou potřeba tam, kde stát publikuje jen oddělené rozpočtové a výkonové vrstvy, ale ne jejich přímé spojení.',
  },
};

function lookupDatasetReference(datasetKey: string): DatasetReference {
  if (DATASET_REFERENCES[datasetKey]) return DATASET_REFERENCES[datasetKey];
  if (datasetKey === 'atlas.inferred') return DATASET_REFERENCES.atlas_inferred;
  return {
    datasetKey,
    title: datasetKey,
    description: 'Zdroj použitý v aktivním grafu, pro který ještě není doplněná ručně psaná metodická karta.',
    freshness: 'Řiďte se poznámkami u konkrétní větve.',
    rationale: 'Položka je stále zobrazena, aby bylo zřejmé, odkud graf bere data.',
  };
}

function collectNotes(links: SankeyLink[]): string[] {
  const seen = new Set<string>();
  const notes: string[] = [];

  for (const link of links) {
    const note = link.note?.trim();
    if (!note) continue;
    if (seen.has(note)) continue;
    seen.add(note);
    notes.push(note);
  }

  notes.sort((a, b) => {
    const priority = (value: string) =>
      value.includes('poslední dostupný') || value.includes('syntet') || value.includes('rozdělen') ? 0 : 1;
    return priority(a) - priority(b) || a.localeCompare(b, 'cs');
  });

  return notes.slice(0, 10);
}

export function buildAtlasReferenceSummary(
  graph: ApiGraph,
  perUnit: boolean,
  fallbackPerUnitLabel: string,
  fallbackCountLabel: string,
): AtlasReferenceSummary {
  const datasetKeys = [...new Set(graph.links.map((link) => link.sourceDataset).filter(Boolean))];
  const metrics = perUnit
    ? metricDescriptorsForLinks(graph.links, fallbackPerUnitLabel, fallbackCountLabel)
    : [];

  return {
    metrics,
    datasets: datasetKeys.map(lookupDatasetReference).sort((a, b) => a.title.localeCompare(b.title, 'cs')),
    notes: collectNotes(graph.links),
    inferredFlowCount: graph.links.filter((link) => link.certainty === 'inferred').length,
  };
}
