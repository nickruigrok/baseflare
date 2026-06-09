import type { AnyValidator, ValidatorShape } from "baseflare/values";

import {
  defineFunction,
  type FunctionDefinitionConfig,
  type MutationCtx,
  type MutationDefinition,
} from "./types";

/**
 * Defines a public write function, callable from clients. The handler runs as
 * one atomic transaction with optimistic concurrency control: on conflict the
 * whole handler is retried, so it must be free of external side effects (use
 * actions for those).
 */
export function mutation<
  TArgs extends ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
>(
  config: FunctionDefinitionConfig<TArgs, TReturns, MutationCtx>
): MutationDefinition<TArgs, TReturns> {
  return defineFunction("mutation", "public", config);
}
