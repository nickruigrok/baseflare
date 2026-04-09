import type { AnyValidator, ValidatorShape } from "@baseflare/values";

import {
  defineFunction,
  type FunctionDefinitionConfig,
  type QueryCtx,
  type QueryDefinition,
} from "./types";

export function query<
  TArgs extends ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
>(
  config: FunctionDefinitionConfig<TArgs, TReturns, QueryCtx>
): QueryDefinition<TArgs, TReturns> {
  return defineFunction("query", "public", config);
}
