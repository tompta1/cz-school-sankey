export function json(res, statusCode, payload) {
  res.statusCode = statusCode;
  res.setHeader('content-type', 'application/json; charset=utf-8');
  res.end(JSON.stringify(payload));
}

export function methodNotAllowed(res, allowed = ['GET']) {
  res.setHeader('allow', allowed.join(', '));
  return json(res, 405, { error: 'Method not allowed' });
}

export function badRequest(res, message) {
  return json(res, 400, { error: message });
}

export function notFound(res, message) {
  return json(res, 404, { error: message });
}
