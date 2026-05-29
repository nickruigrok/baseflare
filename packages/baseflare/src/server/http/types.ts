import type { ActionCtx } from "../functions/types";

export type HttpMethod =
  | "GET"
  | "POST"
  | "PUT"
  | "PATCH"
  | "DELETE"
  | "OPTIONS"
  | "HEAD";

export type HttpActionHandler = (
  ctx: ActionCtx,
  request: Request
) => Response | Promise<Response>;

export interface HttpAction {
  readonly handler: HttpActionHandler;
  readonly type: "httpAction";
}

export interface HttpRouteConfig {
  handler: HttpAction;
  method: string;
  path: string;
}

export interface HttpPrefixRouteConfig {
  handler: HttpAction;
  method: string;
  pathPrefix: string;
}
