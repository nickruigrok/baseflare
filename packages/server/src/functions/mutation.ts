import type { AnyValidator, ValidatorShape } from "@baseflare/values";

import {
  defineFunction,
  type FunctionDefinitionConfig,
  type MutationCtx,
  type MutationDefinition,
} from "./types";

export function mutation<
  TArgs extends ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
>(
  config: FunctionDefinitionConfig<TArgs, TReturns, MutationCtx>
): MutationDefinition<TArgs, TReturns> {
  return defineFunction("mutation", "public", config);
}
