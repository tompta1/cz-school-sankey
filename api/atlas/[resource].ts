import {
  getAtlasAgricultureGraph,
  getAtlasHealthGraph,
  getAtlasJusticeGraph,
  getAtlasMvGraph,
  getAtlasOverview,
  getAtlasTransportGraph,
  getAtlasYears,
  searchAtlasEntities,
} from '../_lib/atlas.js';
import { badRequest, json, methodNotAllowed, notFound, preflight } from '../_lib/http.js';
import type { ApiRequest, ApiResponse } from '../_lib/server.js';

export default async function handler(req: ApiRequest, res: ApiResponse) {
  if (preflight(req, res)) return;
  if (req.method !== 'GET') return methodNotAllowed(res);

  const resource = String(req.query.resource || '').trim();

  if (resource === 'years') {
    const years = await getAtlasYears();
    return json(res, 200, { domain: 'atlas', years });
  }

  if (resource === 'overview') {
    const year = Number(req.query.year);
    if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');
    const metric = typeof req.query.metric === 'string' ? req.query.metric.trim() : 'cost';
    if (metric !== 'cost') return badRequest(res, 'Invalid metric');

    const overview = await getAtlasOverview(year);
    if (!overview) return notFound(res, `No unified atlas data for year ${year}`);
    return json(res, 200, overview);
  }

  if (resource === 'health') {
    const year = Number(req.query.year);
    if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');
    const nodeId = typeof req.query.nodeId === 'string' ? req.query.nodeId.trim() : null;
    const offset = Math.max(0, Number(req.query.offset || 0));
    if (!Number.isInteger(offset)) return badRequest(res, 'Invalid offset');

    const graph = await getAtlasHealthGraph(year, nodeId, offset);
    if (!graph) return notFound(res, `No health atlas data for year ${year}`);
    return json(res, 200, graph);
  }

  if (resource === 'mv') {
    const year = Number(req.query.year);
    if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');
    const nodeId = typeof req.query.nodeId === 'string' ? req.query.nodeId.trim() : null;

    const graph = await getAtlasMvGraph(year, nodeId);
    if (!graph) return notFound(res, `No MV atlas data for year ${year}`);
    return json(res, 200, graph);
  }

  if (resource === 'justice') {
    const year = Number(req.query.year);
    if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');
    const nodeId = typeof req.query.nodeId === 'string' ? req.query.nodeId.trim() : null;

    const graph = await getAtlasJusticeGraph(year, nodeId);
    if (!graph) return notFound(res, `No justice atlas data for year ${year}`);
    return json(res, 200, graph);
  }

  if (resource === 'transport') {
    const year = Number(req.query.year);
    if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');
    const nodeId = typeof req.query.nodeId === 'string' ? req.query.nodeId.trim() : null;

    const graph = await getAtlasTransportGraph(year, nodeId);
    if (!graph) return notFound(res, `No transport atlas data for year ${year}`);
    return json(res, 200, graph);
  }

  if (resource === 'agriculture') {
    const year = Number(req.query.year);
    if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');
    const nodeId = typeof req.query.nodeId === 'string' ? req.query.nodeId.trim() : null;
    const offset = Math.max(0, Number(req.query.offset || 0));
    if (!Number.isInteger(offset)) return badRequest(res, 'Invalid offset');

    const graph = await getAtlasAgricultureGraph(year, nodeId, offset);
    if (!graph) return notFound(res, `No agriculture atlas data for year ${year}`);
    return json(res, 200, graph);
  }

  if (resource === 'search') {
    const year = Number(req.query.year);
    const q = String(req.query.q || '').trim();
    const limit = Math.min(Math.max(Number(req.query.limit || 8), 1), 25);

    if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');
    if (q.length < 2) return badRequest(res, 'Search query must be at least 2 characters');

    const results = await searchAtlasEntities(year, q, limit);
    return json(res, 200, { year, q, results });
  }

  return notFound(res, `Unknown atlas resource: ${resource}`);
}
