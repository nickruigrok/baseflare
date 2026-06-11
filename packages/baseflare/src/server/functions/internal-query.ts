import type { AnyValidator, ValidatorShape } from "baseflare/values";

import {
  defineFunction,
  type FunctionDefinitionConfig,
  type InternalQueryDefinition,
  type QueryCtx,
} from "./types";

/**
 * Defines a server-only query: never exposed over RPC, callable only through
 * `ctx.runQuery` from other functions.
 */
export function internalQuery<
  TArgs extends ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
>(
  config: FunctionDefinitionConfig<TArgs, TReturns, QueryCtx>
): InternalQueryDefinition<TArgs, TReturns> {
  return defineFunction("query", "internal", config);
}
