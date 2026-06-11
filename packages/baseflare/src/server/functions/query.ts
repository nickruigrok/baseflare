import type { AnyValidator, ValidatorShape } from "baseflare/values";

import {
  defineFunction,
  type FunctionDefinitionConfig,
  type QueryCtx,
  type QueryDefinition,
} from "./types";

/**
 * Defines a public read-only function, callable from clients and subscribable
 * in realtime. Args are validated against `config.args`; the handler reads
 * through `ctx.db` with permission rules applied to every document.
 */
export function query<
  TArgs extends ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
>(
  config: FunctionDefinitionConfig<TArgs, TReturns, QueryCtx>
): QueryDefinition<TArgs, TReturns> {
  return defineFunction("query", "public", config);
}
