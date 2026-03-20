import { badRequest, json, methodNotAllowed, notFound } from '../_lib/http.js';
import { getSchoolRegionGraph } from '../_lib/school.js';

export default async function handler(req, res) {
  if (req.method !== 'GET') return methodNotAllowed(res);

  const year = Number(req.query.year);
  const region = String(req.query.region || '').trim();
  const offset = Number(req.query.offset || 0);

  if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');
  if (!region) return badRequest(res, 'Missing region');

  const graph = await getSchoolRegionGraph(year, region, offset);
  if (!graph) return notFound(res, `No region data for ${region} in ${year}`);

  return json(res, 200, graph);
}
