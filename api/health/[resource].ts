import { json, methodNotAllowed, notFound, preflight } from '../_lib/http.js';
import type { ApiRequest, ApiResponse } from '../_lib/server.js';
import { getAvailableHealthYears, getHealthProviders, getHealthSummary } from '../_lib/health.js';

export default async function handler(req: ApiRequest, res: ApiResponse) {
  if (preflight(req, res)) return;
  if (req.method !== 'GET') return methodNotAllowed(res);

  const resource = String(req.query.resource || '').trim();

  if (resource === 'summary') {
    return json(res, 200, await getHealthSummary());
  }

  if (resource === 'years') {
    return json(res, 200, { domain: 'health', years: await getAvailableHealthYears() });
  }

  if (resource === 'providers') {
    const q = String(req.query.q || '');
    const region = String(req.query.region || '');
    const hospitalOnly = String(req.query.hospitalOnly || '').toLowerCase() === 'true';
    const limit = Math.min(Math.max(Number(req.query.limit || 50), 1), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);
    const providers = await getHealthProviders({ q, region, hospitalOnly, limit, offset });
    return json(res, 200, { providers, limit, offset });
  }

  return notFound(res, `Unknown health resource: ${resource}`);
}
