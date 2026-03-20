import { badRequest, json, methodNotAllowed, notFound, preflight } from '../_lib/http.js';
import { getSchoolFounderTypeGraph } from '../_lib/school.js';

export default async function handler(req, res) {
  if (preflight(req, res)) return;
  if (req.method !== 'GET') return methodNotAllowed(res);

  const year = Number(req.query.year);
  const founderType = String(req.query.founderType || '');
  const offset = Number(req.query.offset || 0);

  if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');
  if (founderType !== 'kraj' && founderType !== 'obec') {
    return badRequest(res, 'founderType must be kraj or obec');
  }

  const graph = await getSchoolFounderTypeGraph(year, founderType, offset);
  if (!graph) return notFound(res, `No founder data for ${founderType} in ${year}`);

  return json(res, 200, graph);
}
