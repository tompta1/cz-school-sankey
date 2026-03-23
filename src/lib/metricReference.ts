import type { SankeyLink } from '../types';

import { normalizationGroup } from './sankeyOrdering';

export interface MetricDescriptor {
  group: string | null;
  perUnitLabel: string;
  countLabel: string;
  title: string;
  description: string;
  rationale: string;
}

const FALLBACK_DESCRIPTOR: MetricDescriptor = {
  group: null,
  perUnitLabel: 'srovnávací jednotku',
  countLabel: 'srovnávacích jednotek',
  title: 'Srovnávací jednotka',
  description: 'Poměrová metrika používá denominátor specifický pro daný resort nebo tok.',
  rationale: 'Používá se jen tam, kde existuje obhajitelný početní jmenovatel a nepůsobí zavádějícím dojmem.',
};

const METRIC_DESCRIPTORS: Record<string, MetricDescriptor> = {
  school_pupil: {
    group: 'school_pupil',
    perUnitLabel: 'žáka/rok',
    countLabel: 'žáků',
    title: 'Kč/žák/rok',
    description: 'Roční školské výdaje vztažené k počtu žáků v dané části toku.',
    rationale: 'U školství je počet žáků nejstabilnější a nejlépe čitelný výkonový jmenovatel.',
  },
  police_registered_case: {
    group: 'police_registered_case',
    perUnitLabel: 'registrovaný skutek',
    countLabel: 'registrovaných skutků',
    title: 'Kč/registrovaný skutek',
    description: 'Výdaje Policie ČR vztažené k počtu registrovaných skutků.',
    rationale: 'Na policejní větvi je to nejlépe dostupný oficiální a regionálně členěný objemový ukazatel.',
  },
  fire_rescue_intervention: {
    group: 'fire_rescue_intervention',
    perUnitLabel: 'zásah',
    countLabel: 'zásahů',
    title: 'Kč/zásah',
    description: 'Výdaje HZS vztažené k počtu zásahů.',
    rationale: 'HZS publikuje zásahy konzistentně po krajích a letech, proto fungují jako srovnávací jednotka.',
  },
  justice_resolved_case: {
    group: 'justice_resolved_case',
    perUnitLabel: 'vyřízenou věc',
    countLabel: 'vyřízených věcí',
    title: 'Kč/vyřízenou věc',
    description: 'Výdaje soudů vztažené k počtu vyřízených věcí v hlavních agendách.',
    rationale: 'Vyřízené věci lépe popisují soudní výkon než prostý počet institucí nebo zaměstnanců.',
  },
  justice_inmate: {
    group: 'justice_inmate',
    perUnitLabel: 'vězněnou osobu/rok',
    countLabel: 'vězněných osob',
    title: 'Kč/vězněnou osobu/rok',
    description: 'Výdaje vězeňské služby vztažené k průměrnému dennímu stavu vězněných osob.',
    rationale: 'Průměrný denní stav je standardní provozní jmenovatel pro vězeňství.',
  },
  social_pension_recipient: {
    group: 'social_pension_recipient',
    perUnitLabel: 'příjemce důchodu/rok',
    countLabel: 'příjemců důchodu',
    title: 'Kč/příjemce důchodu/rok',
    description: 'Objem důchodových výdajů vztažený k počtu příjemců důchodu.',
    rationale: 'U důchodů existuje přímo odpovídající počet příjemců, takže metrika je interpretovatelná.',
  },
  social_unemployment_recipient: {
    group: 'social_unemployment_recipient',
    perUnitLabel: 'příjemce podpory/rok',
    countLabel: 'příjemců podpory',
    title: 'Kč/příjemce podpory/rok',
    description: 'Podpory v nezaměstnanosti vztažené k počtu podpořených osob.',
    rationale: 'Na této větvi je počet příjemců nejbližší odpovídající výdajové položce.',
  },
  social_care_recipient: {
    group: 'social_care_recipient',
    perUnitLabel: 'příjemce příspěvku/rok',
    countLabel: 'příjemců příspěvku',
    title: 'Kč/příjemce příspěvku/rok',
    description: 'Příspěvek na péči vztažený k počtu příjemců.',
    rationale: 'Metrika funguje jen na konkrétní dávce, ne na smíšených sociálních koších.',
  },
  social_substitute_alimony_recipient: {
    group: 'social_substitute_alimony_recipient',
    perUnitLabel: 'příjemce dávky/rok',
    countLabel: 'příjemců dávky',
    title: 'Kč/příjemce dávky/rok',
    description: 'Náhradní výživné vztažené k počtu příjemců dávky.',
    rationale: 'U této úzké dávky je počet příjemců přímo spojený s výplatou.',
  },
  agriculture_subsidy_recipient: {
    group: 'agriculture_subsidy_recipient',
    perUnitLabel: 'příjemce dotace',
    countLabel: 'příjemců dotace',
    title: 'Kč/příjemce dotace',
    description: 'Zemědělské dotace přes SZIF vztažené k počtu jedinečných příjemců s kladnou čistou částkou ve fiskálním roce.',
    rationale: 'Počet zveřejněných příjemců SZIF je nejbližší obhajitelný denominátor pro dotační větev MZe bez předstírání obecného počtu všech zemědělců.',
  },
  agriculture_area_hectare: {
    group: 'agriculture_area_hectare',
    perUnitLabel: 'ha',
    countLabel: 'ha LPIS',
    title: 'Kč/ha',
    description: 'Plošné a krajinné podpory SZIF vztažené k hektarům LPIS spárovaným s area-family příjemci.',
    rationale: 'U největších plošných opatření je hektar nejbližší věcný jmenovatel; atlas jej používá jen pro area-family větev a nepřenáší ho do jiných zemědělských podpor.',
  },
  environment_support_recipient: {
    group: 'environment_support_recipient',
    perUnitLabel: 'příjemce podpory',
    countLabel: 'příjemců podpory',
    title: 'Kč/příjemce podpory',
    description: 'Podpory SFŽP jsou porovnávány proti počtu jedinečných příjemců v otevřeném registru podpor za rok podpisu rozhodnutí.',
    rationale: 'U první iterace MŽP je počet příjemců nejbližší obhajitelný jmenovatel; atlas tím neříká nic o dopadu na emise či odpady, pouze o šíři dotační distribuce.',
  },
  mmr_support_recipient: {
    group: 'mmr_support_recipient',
    perUnitLabel: 'příjemce podpory',
    countLabel: 'příjemců podpory',
    title: 'Kč/příjemce podpory',
    description: 'Projektově podložené větve MMR jsou porovnávány proti počtu jedinečných příjemců IROP v otevřeném seznamu operací.',
    rationale: 'Recipient-level IČO je v IROP workbooku zveřejněné přímo, takže jde o nejčistší první denominator bez předstírání počtu obyvatel, bytů nebo projektových uživatelů.',
  },
  mpo_support_recipient: {
    group: 'mpo_support_recipient',
    perUnitLabel: 'příjemce podpory',
    countLabel: 'příjemců podpory',
    title: 'Kč/příjemce podpory',
    description: 'Podpory MPO jsou v první iteraci porovnávány proti počtu jedinečných příjemců OP TAK v otevřeném seznamu operací.',
    rationale: 'U MPO je recipient-level IČO v OP TAK workbooku veřejné a stabilní, takže jde o nejlepší první jmenovatel bez předstírání výrobních, exportních nebo energetických efektů.',
  },
  mk_support_recipient: {
    group: 'mk_support_recipient',
    perUnitLabel: 'příjemce podpory',
    countLabel: 'příjemců podpory',
    title: 'Kč/příjemce podpory',
    description: 'Programově podložené větve MK jsou porovnávány proti počtu zveřejněných příjemců podpory v konkrétním dotačním programu.',
    rationale: 'U první iterace MK je to jediný obhajitelný jmenovatel, protože ministerstvo zveřejňuje jen vybrané výsledkové seznamy a agregace, ne jednotný recipient-level rozpad celé kapitoly.',
  },
  transport_rail_passenger: {
    group: 'transport_rail_passenger',
    perUnitLabel: 'cestujícího',
    countLabel: 'cestujících',
    title: 'Kč/cestujícího',
    description: 'Železniční infrastruktura vztažená k počtu cestujících v železniční osobní dopravě.',
    rationale: 'U železnice existuje silný vztah mezi sítí a veřejně reportovaným objemem cestujících.',
  },
  transport_vignette_sale: {
    group: 'transport_vignette_sale',
    perUnitLabel: 'prodanou známku',
    countLabel: 'prodaných známek',
    title: 'Kč/prodanou známku',
    description: 'Dálniční větev pro osobní auta vztažená k počtu prodaných elektronických dálničních známek.',
    rationale: 'Pro osobní auta je to nejlepší oficiální proxy využití dálniční sítě dostupná v otevřených datech.',
  },
  transport_toll_vehicle: {
    group: 'transport_toll_vehicle',
    perUnitLabel: 'zpoplatněné vozidlo',
    countLabel: 'zpoplatněných vozidel',
    title: 'Kč/zpoplatněné vozidlo',
    description: 'Mýtná větev těžkých vozidel vztažená k počtu registrovaných vozidel v mýtném systému.',
    rationale: 'U těžkých vozidel je počet registrovaných mýtných vozidel nejpřímější veřejně dostupný systémový jmenovatel.',
  },
  transport_project_count: {
    group: 'transport_project_count',
    perUnitLabel: 'akci',
    countLabel: 'akcí',
    title: 'Kč/akci',
    description: 'Investor a projekt v dopravě jsou v drilldownu porovnávány podle počtu akcí.',
    rationale: 'Na detailní úrovni už nejsou k dispozici věrohodné provozní denominátory, proto se vracíme k projektové granularitě.',
  },
};

export function metricDescriptorForGroup(
  group: string | null,
  fallbackPerUnitLabel: string,
  fallbackCountLabel: string,
): MetricDescriptor {
  if (group && METRIC_DESCRIPTORS[group]) {
    return METRIC_DESCRIPTORS[group];
  }

  return {
    ...FALLBACK_DESCRIPTOR,
    perUnitLabel: fallbackPerUnitLabel,
    countLabel: fallbackCountLabel,
    title: `Kč/${fallbackPerUnitLabel}`,
  };
}

export function metricDescriptorForLink(
  link: SankeyLink,
  fallbackPerUnitLabel: string,
  fallbackCountLabel: string,
): MetricDescriptor {
  const group =
    normalizationGroup(link) ??
    (link.flowType === 'mv_budget_group' && link.target === 'security:police'
      ? 'police_registered_case'
      : null) ??
    (link.flowType === 'mv_budget_group' && link.target === 'security:fire-rescue'
      ? 'fire_rescue_intervention'
      : null);

  return metricDescriptorForGroup(group, fallbackPerUnitLabel, fallbackCountLabel);
}

export function metricDescriptorsForLinks(
  links: SankeyLink[],
  fallbackPerUnitLabel: string,
  fallbackCountLabel: string,
): MetricDescriptor[] {
  const seen = new Set<string>();
  const descriptors: MetricDescriptor[] = [];

  for (const link of links) {
    const descriptor = metricDescriptorForLink(link, fallbackPerUnitLabel, fallbackCountLabel);
    const key = descriptor.group ?? descriptor.title;
    if (seen.has(key)) continue;
    seen.add(key);
    descriptors.push(descriptor);
  }

  return descriptors;
}
