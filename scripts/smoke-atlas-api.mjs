#!/usr/bin/env node

const baseUrl = (process.env.ATLAS_API_BASE_URL || 'https://cz-school-sankey.vercel.app').replace(/\/$/, '');

async function fetchJson(path) {
  const response = await fetch(`${baseUrl}${path}`, {
    headers: { accept: 'application/json' },
  });

  if (!response.ok) {
    throw new Error(`${path} -> ${response.status}`);
  }

  return response.json();
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function main() {
  const years = await fetchJson('/api/atlas/years');
  assert(years.domain === 'atlas', 'atlas years endpoint returned unexpected domain');

  const overview2024 = await fetchJson('/api/atlas/overview?year=2024&metric=cost');
  assert(Array.isArray(overview2024.nodes) && overview2024.nodes.length > 20, 'overview 2024 has too few nodes');
  assert(
    overview2024.links.some((link) => link.target === 'transport:ministry:md'),
    'overview 2024 is missing transport root link',
  );
  assert(
    overview2024.links.some((link) => link.target === 'agriculture:ministry:mze'),
    'overview 2024 is missing agriculture root link',
  );
  assert(
    overview2024.links.some((link) => link.target === 'environment:ministry:mzp'),
    'overview 2024 is missing environment root link',
  );
  assert(
    overview2024.links.some((link) => link.target === 'mmr:ministry:mmr'),
    'overview 2024 is missing MMR root link',
  );
  assert(
    overview2024.links.some((link) => link.target === 'mpo:ministry:mpo'),
    'overview 2024 is missing MPO root link',
  );
  assert(
    overview2024.links.some((link) => link.target === 'mk:ministry:mk'),
    'overview 2024 is missing MK root link',
  );

  const transport2024 = await fetchJson('/api/atlas/transport?year=2024');
  assert(
    transport2024.nodes.some((node) => node.id === 'transport:sfdi:rail' && node.metadata?.capacity),
    'transport 2024 is missing rail denominator',
  );
  assert(
    transport2024.nodes.some((node) => node.id === 'transport:sfdi:roads-vignette' && node.metadata?.capacity),
    'transport 2024 is missing vignette denominator',
  );
  assert(
    transport2024.nodes.some((node) => node.id === 'transport:sfdi:roads-toll' && node.metadata?.capacity),
    'transport 2024 is missing toll denominator',
  );

  const mv2024 = await fetchJson('/api/atlas/mv?year=2024');
  assert(
    mv2024.nodes.some((node) => node.id === 'security:police'),
    'MV 2024 is missing police branch',
  );

  const justice2024 = await fetchJson('/api/atlas/justice?year=2024');
  assert(
    justice2024.nodes.some((node) => node.id === 'justice:courts'),
    'justice 2024 is missing courts branch',
  );

  const agriculture2024 = await fetchJson('/api/atlas/agriculture?year=2024');
  assert(
    agriculture2024.nodes.some((node) => node.id === 'agriculture:subsidy:total'),
    'agriculture 2024 is missing subsidy root',
  );

  const agricultureSubsidy2024 = await fetchJson('/api/atlas/agriculture?year=2024&nodeId=agriculture:subsidy:total');
  assert(
    agricultureSubsidy2024.nodes.some((node) => node.id === 'agriculture:subsidy:family:area' && node.metadata?.capacity),
    'agriculture 2024 is missing area-family hectare denominator',
  );

  const agricultureAdmin2024 = await fetchJson('/api/atlas/agriculture?year=2024&nodeId=agriculture:admin');
  assert(
    agricultureAdmin2024.nodes.some((node) => node.id.startsWith('agriculture:admin-entity:')),
    'agriculture 2024 is missing admin entity drilldown',
  );

  const environment2024 = await fetchJson('/api/atlas/environment?year=2024');
  assert(
    environment2024.nodes.some((node) => node.id === 'environment:sfzp:support' && node.metadata?.capacity),
    'environment 2024 is missing SFZP support branch',
  );

  const environmentSupport2024 = await fetchJson('/api/atlas/environment?year=2024&nodeId=environment:sfzp:support');
  assert(
    environmentSupport2024.nodes.some((node) => node.id.startsWith('environment:family:')),
    'environment 2024 is missing support-family drilldown',
  );

  const mmr2024 = await fetchJson('/api/atlas/mmr?year=2024');
  assert(
    mmr2024.nodes.some((node) => node.id === 'mmr:branch:regional' && node.metadata?.capacity),
    'MMR 2024 is missing recipient-backed regional branch',
  );

  const mmrRegional2024 = await fetchJson('/api/atlas/mmr?year=2024&nodeId=mmr:branch:regional');
  assert(
    mmrRegional2024.nodes.some((node) => node.id.startsWith('mmr:region:')),
    'MMR 2024 is missing regional drilldown',
  );

  const mpo2024 = await fetchJson('/api/atlas/mpo?year=2024');
  assert(
    mpo2024.nodes.some((node) => node.id === 'mpo:optak:support' && node.metadata?.capacity),
    'MPO 2024 is missing OP TAK support branch',
  );

  const mpoSupport2024 = await fetchJson('/api/atlas/mpo?year=2024&nodeId=mpo:optak:support');
  assert(
    mpoSupport2024.nodes.some((node) => node.id.startsWith('mpo:recipient:')),
    'MPO 2024 is missing recipient drilldown',
  );

  const mk2024 = await fetchJson('/api/atlas/mk?year=2024');
  assert(
    mk2024.nodes.some((node) => node.id === 'mk:support:heritage' && node.metadata?.capacity),
    'MK 2024 is missing heritage branch',
  );

  const mkHeritage2024 = await fetchJson('/api/atlas/mk?year=2024&nodeId=mk:support:heritage');
  assert(
    mkHeritage2024.nodes.some((node) => node.id.startsWith('mk:region:')),
    'MK 2024 is missing PZAD regional drilldown',
  );

  console.log(`Atlas smoke checks passed for ${baseUrl}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
