import { badRequest, json, methodNotAllowed, notFound, preflight } from '../_lib/http.js';
import { getSchoolFounderGraph } from '../_lib/school.js';

export default async function handler(req, res) {
  if (preflight(req, res)) return;
  if (req.method !== 'GET') return methodNotAllowed(res);

  const year = Number(req.query.year);
  const founderId = String(req.query.founderId || '').trim();
  const offset = Number(req.query.offset || 0);

  if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');
  if (!founderId) return badRequest(res, 'Missing founderId');

  const graph = await getSchoolFounderGraph(year, founderId, offset);
  if (!graph) return notFound(res, `No founder data for ${founderId} in ${year}`);

  return json(res, 200, graph);
}
