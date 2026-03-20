import { badRequest, json, methodNotAllowed, notFound, preflight } from '../_lib/http.js';
import { getSchoolOverviewGraph } from '../_lib/school.js';

export default async function handler(req, res) {
  if (preflight(req, res)) return;
  if (req.method !== 'GET') return methodNotAllowed(res);

  const year = Number(req.query.year);
  if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');

  const graph = await getSchoolOverviewGraph(year);
  if (!graph) return notFound(res, `No school data for year ${year}`);

  return json(res, 200, graph);
}
