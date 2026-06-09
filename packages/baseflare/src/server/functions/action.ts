import type { AnyValidator, ValidatorShape } from "baseflare/values";

import {
  type ActionCtx,
  type ActionDefinition,
  defineFunction,
  type FunctionDefinitionConfig,
} from "./types";

/**
 * Defines a public function for side effects (fetch, email, AI calls, ...).
 * Actions have no direct database access; they read and write through
 * `ctx.runQuery` / `ctx.runMutation` and are never retried by the runtime.
 */
export function action<
  TArgs extends ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
>(
  config: FunctionDefinitionConfig<TArgs, TReturns, ActionCtx>
): ActionDefinition<TArgs, TReturns> {
  return defineFunction("action", "public", config);
}
