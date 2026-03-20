import { badRequest, json, methodNotAllowed, notFound } from './_lib/http.js';
import { getSchoolSummary } from './_lib/school.js';

export default async function handler(req, res) {
  if (req.method !== 'GET') return methodNotAllowed(res);

  const year = Number(req.query.year);
  if (!Number.isInteger(year)) return badRequest(res, 'Missing or invalid year');

  const summary = await getSchoolSummary(year);
  if (!summary) return notFound(res, `No school data for year ${year}`);

  return json(res, 200, summary);
}
