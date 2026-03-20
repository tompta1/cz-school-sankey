import { methodNotAllowed, json } from './_lib/http.js';
import { getAvailableYears } from './_lib/school.js';

export default async function handler(req, res) {
  if (req.method !== 'GET') return methodNotAllowed(res);
  const years = await getAvailableYears();
  return json(res, 200, { domain: 'school', years });
}
