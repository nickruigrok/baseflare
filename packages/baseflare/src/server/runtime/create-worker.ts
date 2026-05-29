import { BaseflareError, SchemaError, ValidationError } from "baseflare/values";

import type {
  ActionDefinition,
  MutationDefinition,
  QueryDefinition,
} from "../functions/types";

import {
  jsonResult,
  NotFoundRuntimeError,
  PayloadTooLargeRuntimeError,
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
import { getRequestLogFields, logRuntimeEvent } from "./logging";
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

async function readRequestBodyText(
  request: Request,
  maxBytes: number
): Promise<string> {
  if (!request.body) {
    return "";
  }

  const reader = request.body.getReader();
  const decoder = new TextDecoder();
  let totalBytes = 0;
  let bodyText = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }

    totalBytes += value.byteLength;
    if (totalBytes > maxBytes) {
      throw new PayloadTooLargeRuntimeError();
    }

    bodyText += decoder.decode(value, { stream: true });
  }

  bodyText += decoder.decode();
  return bodyText;
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
    rules: manifest.rules,
    schema: manifest.schema,
  };
}

async function handleQueryRequest(
  request: Request,
  url: URL,
  env: BaseflareRuntimeEnv,
  manifest: BaseflareManifest,
  ctx: BaseflareExecutionContext,
  functionIndex: ReturnType<typeof createFunctionIndex>
): Promise<Response | null> {
  const queryName = getRouteName(url.pathname, "/api/query/");
  if (!queryName) {
    return null;
  }

  if (request.method !== "POST") {
    throw new ValidationRuntimeError("Query RPC requests must use POST");
  }

  const entry = functionIndex.getByName("query", queryName, "public");
  if (!entry) {
    throw new NotFoundRuntimeError(`Query "${queryName}" was not found`);
  }

  const args = await parseRpcBodyArgs(request);
  const result = await executeQueryDefinition(
    entry.definition as QueryDefinition,
    createInvocationOptions(env, manifest, request, ctx, functionIndex),
    args
  );

  return jsonResult(result);
}

async function handleMutationRequest(
  request: Request,
  url: URL,
  env: BaseflareRuntimeEnv,
  manifest: BaseflareManifest,
  ctx: BaseflareExecutionContext,
  functionIndex: ReturnType<typeof createFunctionIndex>
): Promise<Response | null> {
  const mutationName = getRouteName(url.pathname, "/api/mutation/");
  if (!mutationName) {
    return null;
  }

  if (request.method !== "POST") {
    throw new ValidationRuntimeError("Mutation RPC requests must use POST");
  }

  const entry = functionIndex.getByName("mutation", mutationName, "public");
  if (!entry) {
    throw new NotFoundRuntimeError(`Mutation "${mutationName}" was not found`);
  }

  const args = await parseRpcBodyArgs(request);
  const result = await executeMutationDefinition(
    entry.definition as MutationDefinition,
    {
      ...createInvocationOptions(env, manifest, request, ctx, functionIndex),
      invocationName: entry.name,
    },
    args
  );

  return jsonResult(result);
}

async function handleActionRequest(
  request: Request,
  url: URL,
  env: BaseflareRuntimeEnv,
  manifest: BaseflareManifest,
  ctx: BaseflareExecutionContext,
  functionIndex: ReturnType<typeof createFunctionIndex>
): Promise<Response | null> {
  const actionName = getRouteName(url.pathname, "/api/action/");
  if (!actionName) {
    return null;
  }

  if (request.method !== "POST") {
    throw new ValidationRuntimeError("Action RPC requests must use POST");
  }

  const entry = functionIndex.getByName("action", actionName, "public");
  if (!entry) {
    throw new NotFoundRuntimeError(`Action "${actionName}" was not found`);
  }

  const args = await parseRpcBodyArgs(request);
  const result = await executeActionDefinition(
    entry.definition as ActionDefinition,
    {
      ...createInvocationOptions(env, manifest, request, ctx, functionIndex),
      invocationName: entry.name,
    },
    args
  );

  return jsonResult(result);
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
  functionIndex: ReturnType<typeof createFunctionIndex>
): Promise<Response> {
  const url = new URL(request.url);

  return (
    (await handleQueryRequest(
      request,
      url,
      env,
      manifest,
      ctx,
      functionIndex
    )) ??
    (await handleMutationRequest(
      request,
      url,
      env,
      manifest,
      ctx,
      functionIndex
    )) ??
    (await handleActionRequest(
      request,
      url,
      env,
      manifest,
      ctx,
      functionIndex
    )) ??
    (await handleCustomHttpRequest(
      request,
      url,
      env,
      manifest,
      ctx,
      functionIndex
    )) ??
    (() => {
      throw new NotFoundRuntimeError(
        `Route "${request.method} ${url.pathname}" was not found`
      );
    })()
  );
}

export function createWorker<
  TEnv extends BaseflareRuntimeEnv = BaseflareRuntimeEnv,
>(manifest: BaseflareManifest): ExportedHandler<TEnv> {
  const functionIndex = createFunctionIndex(manifest);

  return {
    async fetch(request, env, ctx) {
      try {
        return await routeRequest(request, env, ctx, manifest, functionIndex);
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

        return toErrorResponse(error);
      }
    },
  };
}
