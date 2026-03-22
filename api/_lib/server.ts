import type { IncomingMessage, ServerResponse } from 'node:http';

export type QueryValue = string | string[] | undefined;

export type ApiRequest = IncomingMessage & {
  method?: string;
  query: Record<string, QueryValue>;
};

export type ApiResponse = ServerResponse<IncomingMessage>;
