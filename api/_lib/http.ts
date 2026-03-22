import type { ApiRequest, ApiResponse } from './server.js';

export function applyCors(res: ApiResponse) {
  res.setHeader('access-control-allow-origin', '*');
  res.setHeader('access-control-allow-methods', 'GET, OPTIONS');
  res.setHeader('access-control-allow-headers', 'Content-Type');
}

export function preflight(req: ApiRequest, res: ApiResponse) {
  applyCors(res);
  if (req.method === 'OPTIONS') {
    res.statusCode = 204;
    res.end();
    return true;
  }
  return false;
}

export function json(res: ApiResponse, statusCode: number, payload: unknown) {
  applyCors(res);
  res.statusCode = statusCode;
  res.setHeader('content-type', 'application/json; charset=utf-8');
  res.end(JSON.stringify(payload));
}

export function methodNotAllowed(res: ApiResponse, allowed: string[] = ['GET']) {
  applyCors(res);
  res.setHeader('allow', allowed.join(', '));
  return json(res, 405, { error: 'Method not allowed' });
}

export function badRequest(res: ApiResponse, message: string) {
  return json(res, 400, { error: message });
}

export function notFound(res: ApiResponse, message: string) {
  return json(res, 404, { error: message });
}
