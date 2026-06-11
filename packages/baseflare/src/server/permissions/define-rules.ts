import type { Rules } from "./types";

/**
 * Defines deny-by-default permission rules. Access is granted only by an
 * explicit rule: tables without rules — and operations without a rule — are
 * denied for every caller.
 */
export function defineRules<TRules extends Rules>(rules: TRules): TRules {
  return rules;
}
