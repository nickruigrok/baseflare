import type { AnyValidator, ValidatorShape } from "baseflare/values";

import {
  defineFunction,
  type FunctionDefinitionConfig,
  type InternalMutationDefinition,
  type MutationCtx,
} from "./types";

/**
 * Defines a server-only mutation: never exposed over RPC, callable only
 * through `ctx.runMutation` from other functions. Same atomicity and retry
 * semantics as `mutation`.
 */
export function internalMutation<
  TArgs extends ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
>(
  config: FunctionDefinitionConfig<TArgs, TReturns, MutationCtx>
): InternalMutationDefinition<TArgs, TReturns> {
  return defineFunction("mutation", "internal", config);
}
