import {
  type AnyValidator,
  type ObjectOutput,
  type ValidatorShape,
  v,
} from "baseflare/values";

import type { DatabaseReader } from "../db/reader";
import type { DatabaseWriter } from "../db/writer";

type MaybePromise<TValue> = TValue | Promise<TValue>;

export interface Auth {
  getUserIdentity(): MaybePromise<unknown>;
}

export interface StorageReader {
  getUrl(id: string): MaybePromise<string | null>;
}

export interface StorageWriter extends StorageReader {
  delete(id: string): MaybePromise<void>;
  generateUploadUrl(): MaybePromise<string>;
}

export interface StorageActionWriter extends StorageWriter {
  store(blob: Blob): MaybePromise<string>;
}

export interface Scheduler {
  runAfter(
    delayMs: number,
    ref: FunctionReference<unknown, unknown>,
    args: unknown
  ): Promise<string>;
  runAt(
    timestamp: number,
    ref: FunctionReference<unknown, unknown>,
    args: unknown
  ): Promise<string>;
}

/** Context passed to query handlers: permission-checked reads only. */
export interface QueryCtx {
  auth: Auth;
  db: DatabaseReader;
  storage: StorageReader;
}

/**
 * Context passed to mutation handlers. All `db` writes commit atomically when
 * the handler returns; on a write conflict the whole handler re-runs.
 */
export interface MutationCtx extends QueryCtx {
  db: DatabaseWriter;
  /** Runs a query inside this mutation's transaction (sees pending writes). */
  runQuery<TArgs, TResult>(
    ref: FunctionReference<TArgs, TResult>,
    args: TArgs
  ): Promise<TResult>;
  scheduler: Scheduler;
  storage: StorageWriter;
}

/**
 * Context passed to action handlers. Actions have no direct `db` access and
 * are never retried; database work goes through `runQuery`/`runMutation`.
 */
export interface ActionCtx {
  auth: Auth;
  runAction<TArgs, TResult>(
    ref: FunctionReference<TArgs, TResult>,
    args: TArgs
  ): Promise<TResult>;
  /**
   * Runs a mutation. Each call is its own atomic transaction with independent
   * conflict retries — multi-write workflows that must be atomic belong inside
   * one mutation, not across several calls.
   */
  runMutation<TArgs, TResult>(
    ref: FunctionReference<TArgs, TResult>,
    args: TArgs
  ): Promise<TResult>;
  runQuery<TArgs, TResult>(
    ref: FunctionReference<TArgs, TResult>,
    args: TArgs
  ): Promise<TResult>;
  scheduler: Scheduler;
  storage: StorageActionWriter;
}

export type FunctionKind = "query" | "mutation" | "action";
export type FunctionVisibility = "public" | "internal";

export interface FunctionReference<TArgs, TResult> {
  readonly __baseflareArgs?: TArgs;
  readonly __baseflareResult?: TResult;
}

export type HandlerResult<TReturns extends AnyValidator | undefined> =
  TReturns extends AnyValidator ? ReturnType<TReturns["validate"]> : unknown;

export interface BaseFunctionDefinition<
  TKind extends FunctionKind,
  TVisibility extends FunctionVisibility,
  TArgs extends ValidatorShape,
  TReturns extends AnyValidator | undefined,
  TCtx,
> extends FunctionReference<ObjectOutput<TArgs>, HandlerResult<TReturns>> {
  readonly args: TArgs;
  readonly argsValidator: ReturnType<typeof v.object<TArgs>>;
  readonly handler: (
    ctx: TCtx,
    args: ObjectOutput<TArgs>
  ) => MaybePromise<HandlerResult<TReturns>>;
  readonly kind: TKind;
  readonly returns?: TReturns;
  validateArgs(args: unknown): ObjectOutput<TArgs>;
  validateReturn(value: unknown): HandlerResult<TReturns>;
  readonly visibility: TVisibility;
}

export interface FunctionDefinitionConfig<
  TArgs extends ValidatorShape,
  TReturns extends AnyValidator | undefined,
  TCtx,
> {
  args: TArgs;
  handler: (
    ctx: TCtx,
    args: ObjectOutput<TArgs>
  ) => MaybePromise<HandlerResult<TReturns>>;
  returns?: TReturns;
}

export type QueryDefinition<
  TArgs extends ValidatorShape = ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
> = BaseFunctionDefinition<"query", "public", TArgs, TReturns, QueryCtx>;

export type MutationDefinition<
  TArgs extends ValidatorShape = ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
> = BaseFunctionDefinition<"mutation", "public", TArgs, TReturns, MutationCtx>;

export type ActionDefinition<
  TArgs extends ValidatorShape = ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
> = BaseFunctionDefinition<"action", "public", TArgs, TReturns, ActionCtx>;

export type InternalQueryDefinition<
  TArgs extends ValidatorShape = ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
> = BaseFunctionDefinition<"query", "internal", TArgs, TReturns, QueryCtx>;

export type InternalMutationDefinition<
  TArgs extends ValidatorShape = ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
> = BaseFunctionDefinition<
  "mutation",
  "internal",
  TArgs,
  TReturns,
  MutationCtx
>;

export type InternalActionDefinition<
  TArgs extends ValidatorShape = ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
> = BaseFunctionDefinition<"action", "internal", TArgs, TReturns, ActionCtx>;

export function defineFunction<
  TKind extends FunctionKind,
  TVisibility extends FunctionVisibility,
  TArgs extends ValidatorShape,
  TReturns extends AnyValidator | undefined,
  TCtx,
>(
  kind: TKind,
  visibility: TVisibility,
  config: FunctionDefinitionConfig<TArgs, TReturns, TCtx>
): BaseFunctionDefinition<TKind, TVisibility, TArgs, TReturns, TCtx> {
  const argsValidator = v.object(config.args);

  return {
    kind,
    visibility,
    args: config.args,
    argsValidator,
    returns: config.returns,
    handler: config.handler,
    validateArgs(args: unknown) {
      return argsValidator.validate(args);
    },
    validateReturn(value: unknown) {
      if (!config.returns) {
        return value as HandlerResult<TReturns>;
      }

      return config.returns.validate(value) as HandlerResult<TReturns>;
    },
  };
}
