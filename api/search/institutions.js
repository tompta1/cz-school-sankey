import { badRequest, json, methodNotAllowed, preflight } from '../_lib/http.js';
import { searchInstitutions } from '../_lib/school.js';

export default async function handler(req, res) {
  if (preflight(req, res)) return;
  if (req.method !== 'GET') return methodNotAllowed(res);

  const year = Number(req.query.year);
  const q = String(req.query.q || '').trim();
  const limit = Math.min(Number(req.query.limit || 8), 25);

  if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');
  if (q.length < 2) return badRequest(res, 'Search query must be at least 2 characters');

  const institutions = await searchInstitutions(year, q, limit);
  return json(res, 200, { year, q, institutions });
}
