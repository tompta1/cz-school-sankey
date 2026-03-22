import { badRequest, json, methodNotAllowed, notFound, preflight } from '../_lib/http.js';
import type { ApiRequest, ApiResponse } from '../_lib/server.js';
import {
  getSchoolEuGraph,
  getSchoolFounderGraph,
  getSchoolFounderTypeGraph,
  getSchoolNodeGraph,
  getSchoolOverviewGraph,
  getSchoolRegionGraph,
} from '../_lib/school.js';

export default async function handler(req: ApiRequest, res: ApiResponse) {
  if (preflight(req, res)) return;
  if (req.method !== 'GET') return methodNotAllowed(res);

  const resource = String(req.query.resource || '').trim();
  const year = Number(req.query.year);
  const offset = Number(req.query.offset || 0);

  if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');

  if (resource === 'overview') {
    const graph = await getSchoolOverviewGraph(year);
    if (!graph) return notFound(res, `No school data for year ${year}`);
    return json(res, 200, graph);
  }

  if (resource === 'eu') {
    const graph = await getSchoolEuGraph(year);
    if (!graph) return notFound(res, `No EU data for ${year}`);
    return json(res, 200, graph);
  }

  if (resource === 'region') {
    const region = String(req.query.region || '').trim();
    if (!region) return badRequest(res, 'Missing region');
    const graph = await getSchoolRegionGraph(year, region, offset);
    if (!graph) return notFound(res, `No region data for ${region} in ${year}`);
    return json(res, 200, graph);
  }

  if (resource === 'founders') {
    const founderType = String(req.query.founderType || '');
    if (founderType !== 'kraj' && founderType !== 'obec') {
      return badRequest(res, 'founderType must be kraj or obec');
    }
    const graph = await getSchoolFounderTypeGraph(year, founderType, offset);
    if (!graph) return notFound(res, `No founder data for ${founderType} in ${year}`);
    return json(res, 200, graph);
  }

  if (resource === 'founder') {
    const founderId = String(req.query.founderId || '').trim();
    if (!founderId) return badRequest(res, 'Missing founderId');
    const graph = await getSchoolFounderGraph(year, founderId, offset);
    if (!graph) return notFound(res, `No founder data for ${founderId} in ${year}`);
    return json(res, 200, graph);
  }

  if (resource === 'node') {
    const nodeId = String(req.query.nodeId || '').trim();
    if (!nodeId) return badRequest(res, 'Missing nodeId');
    const graph = await getSchoolNodeGraph(year, nodeId, offset);
    if (!graph) return notFound(res, `No node data for ${nodeId} in ${year}`);
    return json(res, 200, graph);
  }

  return notFound(res, `Unknown graph resource: ${resource}`);
}
