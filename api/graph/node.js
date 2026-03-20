import { badRequest, json, methodNotAllowed, notFound } from '../_lib/http.js';
import { getSchoolNodeGraph } from '../_lib/school.js';

export default async function handler(req, res) {
  if (req.method !== 'GET') return methodNotAllowed(res);

  const year = Number(req.query.year);
  const nodeId = String(req.query.nodeId || '').trim();
  const offset = Number(req.query.offset || 0);

  if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');
  if (!nodeId) return badRequest(res, 'Missing nodeId');

  const graph = await getSchoolNodeGraph(year, nodeId, offset);
  if (!graph) return notFound(res, `No node data for ${nodeId} in ${year}`);

  return json(res, 200, graph);
}
