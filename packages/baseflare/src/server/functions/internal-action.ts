import type { AnyValidator, ValidatorShape } from "baseflare/values";

import {
  type ActionCtx,
  defineFunction,
  type FunctionDefinitionConfig,
  type InternalActionDefinition,
} from "./types";

/**
 * Defines a server-only action: never exposed over RPC, callable only through
 * `ctx.runAction` from other actions.
 */
export function internalAction<
  TArgs extends ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
>(
  config: FunctionDefinitionConfig<TArgs, TReturns, ActionCtx>
): InternalActionDefinition<TArgs, TReturns> {
  return defineFunction("action", "internal", config);
}
