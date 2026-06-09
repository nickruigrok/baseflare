import { BaseflareError, SchemaError, ValidationError } from "baseflare/values";

import type {
  ActionDefinition,
  MutationDefinition,
  QueryDefinition,
} from "../functions/types";

import {
  jsonResult,
  NotFoundRuntimeError,
  PermissionDeniedRuntimeError,
  RuntimeError,
  toErrorResponse,
  ValidationRuntimeError,
} from "./errors";
import {
  createActionContext,
  executeActionDefinition,
  executeMutationDefinition,
  executeQueryDefinition,
} from "./execution";
import { createFunctionIndex } from "./function-index";
import { assertJsonBounds } from "./json-bounds";
import { getRequestLogFields, logRuntimeEvent } from "./logging";
import { routeRealtimeSubscribe } from "./realtime/connection-do";
import { createRealtimeMutationNotifier } from "./realtime/outbox";
import { configureRealtimeRuntime } from "./realtime/shared";
import { readRequestBodyText } from "./request-body";
import type {
  BaseflareExecutionContext,
  BaseflareManifest,
  BaseflareRuntimeEnv,
  ExportedHandler,
} from "./types";

const MAX_RPC_BODY_BYTES = 1024 * 1024;

function getRouteName(pathname: string, prefix: string): string | null {
  if (!pathname.startsWith(prefix)) {
    return null;
  }

  const name = pathname.slice(prefix.length);
  if (name.length === 0) {
    return null;
  }

  try {
    return decodeURIComponent(name);
  } catch {
    throw new ValidationRuntimeError("RPC route name is malformed");
  }
}

const RPC_ROUTES = [
  { kind: "query", label: "Query", prefix: "/api/query/" },
  { kind: "mutation", label: "Mutation", prefix: "/api/mutation/" },
  { kind: "action", label: "Action", prefix: "/api/action/" },
] as const;

function getRpcMethodError(pathname: string): string | null {
  const route = RPC_ROUTES.find((rpcRoute) =>
    pathname.startsWith(rpcRoute.prefix)
  );
  return route ? `${route.label} RPC requests must use POST` : null;
}

async function parseRpcBodyArgs(request: Request): Promise<unknown> {
  const bodyText = await readRequestBodyText(request, MAX_RPC_BODY_BYTES);
  if (bodyText.trim() === "") {
    throw new ValidationRuntimeError(
      'RPC request bodies must be shaped as {"args": ...}'
    );
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(bodyText) as unknown;
  } catch (error) {
    throw new ValidationRuntimeError(
      error instanceof Error
        ? `Invalid RPC request JSON: ${error.message}`
        : "Invalid RPC request JSON"
    );
  }
  assertJsonBounds(parsed, "RPC request body");

  if (
    typeof parsed !== "object" ||
    parsed === null ||
    Array.isArray(parsed) ||
    !("args" in parsed) ||
    Object.keys(parsed).length !== 1
  ) {
    throw new ValidationRuntimeError(
      'RPC request bodies must contain only {"args": ...}'
    );
  }

  return parsed.args;
}

function corsHeadersForRequest(
  request: Request,
  manifest: BaseflareManifest
): Headers | null {
  const cors = manifest.config?.cors;
  if (!cors) {
    return null;
  }

  const origin = request.headers.get("origin");
  if (!(origin && cors.origins.includes(origin))) {
    return null;
  }

  const headers = new Headers({
    "Access-Control-Allow-Origin": origin,
    Vary: "Origin",
  });
  const requestedMethods = request.headers.get("access-control-request-method");
  if (requestedMethods) {
    headers.set("Access-Control-Allow-Methods", requestedMethods);
  }
  const requestedHeaders = request.headers.get(
    "access-control-request-headers"
  );
  if (requestedHeaders) {
    headers.set("Access-Control-Allow-Headers", requestedHeaders);
  }
  if (cors.maxAge !== undefined) {
    headers.set("Access-Control-Max-Age", String(cors.maxAge));
  }

  return headers;
}

function withCorsHeaders(
  response: Response,
  request: Request,
  manifest: BaseflareManifest
): Response {
  const corsHeaders = corsHeadersForRequest(request, manifest);
  if (!corsHeaders) {
    return response;
  }

  const headers = new Headers(response.headers);
  for (const [key, value] of corsHeaders) {
    headers.set(key, value);
  }

  const webSocket = (response as Response & { readonly webSocket?: WebSocket })
    .webSocket;
  const init: ResponseInit & { webSocket?: WebSocket } = {
    headers,
    status: response.status,
    statusText: response.statusText,
  };
  if (webSocket) {
    init.webSocket = webSocket;
  }

  return new Response(response.body, init);
}

function handleCorsPreflight(
  request: Request,
  manifest: BaseflareManifest
): Response | null {
  if (request.method !== "OPTIONS" || !manifest.config?.cors) {
    return null;
  }

  const headers = corsHeadersForRequest(request, manifest);
  return new Response(null, {
    headers: headers ?? undefined,
    status: 204,
  });
}

function assertRealtimeSubscribeOrigin(
  request: Request,
  url: URL,
  manifest: BaseflareManifest
): void {
  if (url.pathname !== "/api/subscribe") {
    return;
  }

  const origin = request.headers.get("origin");
  if (!origin) {
    return;
  }

  const allowedOrigins = manifest.config?.cors?.origins;
  if (
    allowedOrigins ? allowedOrigins.includes(origin) : origin === url.origin
  ) {
    return;
  }

  throw new PermissionDeniedRuntimeError(
    "Realtime subscription origin is not allowed"
  );
}

function createInvocationOptions(
  env: BaseflareRuntimeEnv,
  manifest: BaseflareManifest,
  request: Request,
  ctx: BaseflareExecutionContext,
  functionIndex: ReturnType<typeof createFunctionIndex>
) {
  return {
    database: env.APP_DB,
    executionContext: ctx,
    functionIndex,
    requestHeaders: request.headers,
    realtime: createRealtimeMutationNotifier(env, ctx),
    rules: manifest.rules,
    schema: manifest.schema,
  };
}

async function handleRpcRequest(
  request: Request,
  url: URL,
  env: BaseflareRuntimeEnv,
  manifest: BaseflareManifest,
  ctx: BaseflareExecutionContext,
  functionIndex: ReturnType<typeof createFunctionIndex>
): Promise<Response | null> {
  for (const route of RPC_ROUTES) {
    const name = getRouteName(url.pathname, route.prefix);
    if (!name) {
      continue;
    }

    if (request.method !== "POST") {
      return null;
    }

    const entry = functionIndex.getByName(route.kind, name, "public");
    if (!entry) {
      throw new NotFoundRuntimeError(`${route.label} "${name}" was not found`);
    }

    const args = await parseRpcBodyArgs(request);
    const options = createInvocationOptions(
      env,
      manifest,
      request,
      ctx,
      functionIndex
    );
    if (route.kind === "query") {
      return jsonResult(
        await executeQueryDefinition(
          entry.definition as QueryDefinition,
          options,
          args
        )
      );
    }

    const invocationOptions = { ...options, invocationName: entry.name };
    return jsonResult(
      route.kind === "mutation"
        ? await executeMutationDefinition(
            entry.definition as MutationDefinition,
            invocationOptions,
            args
          )
        : await executeActionDefinition(
            entry.definition as ActionDefinition,
            invocationOptions,
            args
          )
    );
  }

  return null;
}

function handleCustomHttpRequest(
  request: Request,
  url: URL,
  env: BaseflareRuntimeEnv,
  manifest: BaseflareManifest,
  ctx: BaseflareExecutionContext,
  functionIndex: ReturnType<typeof createFunctionIndex>
): Promise<Response> | Response | null {
  const handler = manifest.http?.lookup(request.method, url.pathname);
  if (!handler) {
    return null;
  }

  const actionContext = createActionContext(
    createInvocationOptions(env, manifest, request, ctx, functionIndex)
  );

  return handler(actionContext, request);
}

async function routeRequest(
  request: Request,
  env: BaseflareRuntimeEnv,
  ctx: BaseflareExecutionContext,
  manifest: BaseflareManifest,
  functionIndex: ReturnType<typeof createFunctionIndex>,
  realtimeRuntimeId: string
): Promise<Response> {
  let url: URL;
  try {
    url = new URL(request.url);
  } catch {
    throw new ValidationRuntimeError("Request URL is malformed");
  }
  assertRealtimeSubscribeOrigin(request, url, manifest);

  return (
    (await routeRealtimeSubscribe(request, env, realtimeRuntimeId)) ??
    (await handleRpcRequest(request, url, env, manifest, ctx, functionIndex)) ??
    (await handleCustomHttpRequest(
      request,
      url,
      env,
      manifest,
      ctx,
      functionIndex
    )) ??
    (() => {
      const rpcMethodError = getRpcMethodError(url.pathname);
      if (request.method !== "POST" && rpcMethodError) {
        throw new ValidationRuntimeError(rpcMethodError);
      }

      throw new NotFoundRuntimeError(
        `Route "${request.method} ${url.pathname}" was not found`
      );
    })()
  );
}

/**
 * Builds the environment Worker `fetch` handler from a Baseflare manifest:
 * RPC routes, the realtime WebSocket endpoint, custom HTTP routes, and CORS.
 * Consumed by the CLI-generated worker entry via `baseflare/runtime`; never
 * runs schema DDL — schema application is deploy-owned.
 */
export function createWorker<
  TEnv extends BaseflareRuntimeEnv = BaseflareRuntimeEnv,
>(manifest: BaseflareManifest): ExportedHandler<TEnv> {
  const functionIndex = createFunctionIndex(manifest);
  const realtimeRuntimeId = configureRealtimeRuntime({
    functionIndex,
    rules: manifest.rules,
    schema: manifest.schema,
  });

  return {
    async fetch(request, env, ctx) {
      try {
        const preflightResponse = handleCorsPreflight(request, manifest);
        if (preflightResponse) {
          return preflightResponse;
        }

        const response = await routeRequest(
          request,
          env,
          ctx,
          manifest,
          functionIndex,
          realtimeRuntimeId
        );
        return withCorsHeaders(response, request, manifest);
      } catch (error) {
        if (
          !(
            error instanceof RuntimeError ||
            error instanceof BaseflareError ||
            error instanceof ValidationError ||
            error instanceof SchemaError
          )
        ) {
          logRuntimeEvent("error", "runtime.unexpected_error", {
            ...getRequestLogFields(request),
            errorName: error instanceof Error ? error.name : typeof error,
          });
        }

        return withCorsHeaders(toErrorResponse(error), request, manifest);
      }
    },
  };
}
