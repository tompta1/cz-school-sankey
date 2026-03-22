import { badRequest, json, methodNotAllowed, notFound, preflight } from '../../_lib/http.js';
import type { ApiRequest, ApiResponse } from '../../_lib/server.js';
import { getHealthPayerMonthly, getHealthProviderYearly } from '../../_lib/health.js';

export default async function handler(req: ApiRequest, res: ApiResponse) {
  if (preflight(req, res)) return;
  if (req.method !== 'GET') return methodNotAllowed(res);

  const resource = String(req.query.resource || '').trim();
  const year = req.query.year == null ? null : Number(req.query.year);
  if (req.query.year != null && !Number.isInteger(year)) {
    return badRequest(res, 'Invalid year');
  }

  if (resource === 'payers') {
    const payerCode = String(req.query.payerCode || '');
    const limit = Math.min(Math.max(Number(req.query.limit || 500), 1), 5000);
    const rows = await getHealthPayerMonthly({ year, payerCode, limit });
    return json(res, 200, { rows });
  }

  if (resource === 'providers') {
    const providerIco = String(req.query.providerIco || req.query.provider_ico || '');
    const limit = Math.min(Math.max(Number(req.query.limit || 500), 1), 5000);
    const rows = await getHealthProviderYearly({ year, providerIco, limit });
    return json(res, 200, { rows });
  }

  return notFound(res, `Unknown health activity resource: ${resource}`);
}
