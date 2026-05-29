import { evaluate } from "../permissions/evaluate";
import type { Rules } from "../permissions/types";

import { PermissionDeniedRuntimeError } from "./errors";
import { logRuntimeEvent } from "./logging";

export async function canReadDocument(
  rules: Rules | undefined,
  tableName: string,
  ctx: unknown,
  doc: Record<string, unknown>
): Promise<boolean> {
  if (!rules) {
    return false;
  }

  return await evaluate(rules, {
    tableName,
    operation: "read",
    ctx,
    doc,
  });
}

export async function assertCanInsert(
  rules: Rules | undefined,
  tableName: string,
  ctx: unknown,
  value: Record<string, unknown>
): Promise<void> {
  if (!rules) {
    logDeniedWrite("insert", tableName, "missing_rules");
    throw new PermissionDeniedRuntimeError();
  }

  const allowed = await evaluate(rules, {
    tableName,
    operation: "insert",
    ctx,
    value,
  });

  if (!allowed) {
    logDeniedWrite("insert", tableName, "rule_denied");
    throw new PermissionDeniedRuntimeError();
  }
}

export async function assertCanUpdate(
  rules: Rules | undefined,
  tableName: string,
  ctx: unknown,
  existingDoc: Record<string, unknown>,
  value: Record<string, unknown>
): Promise<void> {
  if (!rules) {
    logDeniedWrite("update", tableName, "missing_rules");
    throw new PermissionDeniedRuntimeError();
  }

  const allowed = await evaluate(rules, {
    tableName,
    operation: "update",
    ctx,
    existingDoc,
    value,
  });

  if (!allowed) {
    logDeniedWrite("update", tableName, "rule_denied");
    throw new PermissionDeniedRuntimeError();
  }
}

export async function assertCanDelete(
  rules: Rules | undefined,
  tableName: string,
  ctx: unknown,
  existingDoc: Record<string, unknown>
): Promise<void> {
  if (!rules) {
    logDeniedWrite("delete", tableName, "missing_rules");
    throw new PermissionDeniedRuntimeError();
  }

  const allowed = await evaluate(rules, {
    tableName,
    operation: "delete",
    ctx,
    existingDoc,
  });

  if (!allowed) {
    logDeniedWrite("delete", tableName, "rule_denied");
    throw new PermissionDeniedRuntimeError();
  }
}

function logDeniedWrite(
  operation: "delete" | "insert" | "update",
  tableName: string,
  reason: "missing_rules" | "rule_denied"
): void {
  logRuntimeEvent("warn", "permission.write_denied", {
    operation,
    reason,
    tableName,
  });
}
