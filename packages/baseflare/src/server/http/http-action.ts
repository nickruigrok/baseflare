import type { HttpAction, HttpActionHandler } from "./types";

/**
 * Wraps a raw `(ctx, request) => Response` handler for registration on the
 * HTTP router. Runs with action semantics: no direct database access, use
 * `ctx.runQuery` / `ctx.runMutation`.
 */
export function httpAction(handler: HttpActionHandler): HttpAction {
  return {
    type: "httpAction",
    handler,
  };
}
