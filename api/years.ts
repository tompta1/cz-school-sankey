import { preflight, methodNotAllowed, json } from './_lib/http.js';
import type { ApiRequest, ApiResponse } from './_lib/server.js';
import { getAvailableYears } from './_lib/school.js';

export default async function handler(req: ApiRequest, res: ApiResponse) {
  if (preflight(req, res)) return;
  if (req.method !== 'GET') return methodNotAllowed(res);
  const years = await getAvailableYears();
  return json(res, 200, { domain: 'school', years });
}
