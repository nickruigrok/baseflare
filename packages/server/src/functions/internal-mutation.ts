import type { AnyValidator, ValidatorShape } from "@baseflare/values";

import {
  defineFunction,
  type FunctionDefinitionConfig,
  type InternalMutationDefinition,
  type MutationCtx,
} from "./types";

export function internalMutation<
  TArgs extends ValidatorShape,
  TReturns extends AnyValidator | undefined = undefined,
>(
  config: FunctionDefinitionConfig<TArgs, TReturns, MutationCtx>
): InternalMutationDefinition<TArgs, TReturns> {
  return defineFunction("mutation", "internal", config);
}
