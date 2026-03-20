import { badRequest, json, methodNotAllowed, notFound, preflight } from '../_lib/http.js';
import { getSchoolEuGraph } from '../_lib/school.js';

export default async function handler(req, res) {
  if (preflight(req, res)) return;
  if (req.method !== 'GET') return methodNotAllowed(res);

  const year = Number(req.query.year);
  if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');

  const graph = await getSchoolEuGraph(year);
  if (!graph) return notFound(res, `No EU data for ${year}`);

  return json(res, 200, graph);
}
