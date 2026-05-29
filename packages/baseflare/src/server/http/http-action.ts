import type { HttpAction, HttpActionHandler } from "./types";

export function httpAction(handler: HttpActionHandler): HttpAction {
  return {
    type: "httpAction",
    handler,
  };
}
