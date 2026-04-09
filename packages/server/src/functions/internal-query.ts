import type { AnyValidator, ValidatorShape } from "@baseflare/values";

import {
  defineFunction,
  type FunctionDefinitionConfig,
  type InternalQueryDefinition,
  type QueryCtx,
} from "./types";

export function internalQuery<
  TArgs extends ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
>(
  config: FunctionDefinitionConfig<TArgs, TReturns, QueryCtx>
): InternalQueryDefinition<TArgs, TReturns> {
  return defineFunction("query", "internal", config);
}
