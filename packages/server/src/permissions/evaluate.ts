import type { EvaluationInput, Rules } from "./types";

export async function evaluate(
  rules: Rules,
  input: EvaluationInput
): Promise<boolean> {
  const tableRules = rules[input.tableName];
  if (!tableRules) {
    return false;
  }

  switch (input.operation) {
    case "read":
      return (
        (await tableRules.read?.({ ctx: input.ctx, doc: input.doc })) ?? false
      );
    case "insert":
      return (
        (await tableRules.insert?.({ ctx: input.ctx, value: input.value })) ??
        false
      );
    case "update":
      return (
        (await tableRules.update?.({
          ctx: input.ctx,
          existingDoc: input.existingDoc,
          value: input.value,
        })) ?? false
      );
    case "delete":
      return (
        (await tableRules.delete?.({
          ctx: input.ctx,
          existingDoc: input.existingDoc,
        })) ?? false
      );
    default:
      return false;
  }
}
