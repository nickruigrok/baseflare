import type { AnyValidator, ValidatorShape } from "baseflare/values";

import {
  type ActionCtx,
  type ActionDefinition,
  defineFunction,
  type FunctionDefinitionConfig,
} from "./types";

export function action<
  TArgs extends ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
>(
  config: FunctionDefinitionConfig<TArgs, TReturns, ActionCtx>
): ActionDefinition<TArgs, TReturns> {
  return defineFunction("action", "public", config);
}
