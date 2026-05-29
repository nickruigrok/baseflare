import type { Rules } from "./types";

export function defineRules<TRules extends Rules>(rules: TRules): TRules {
  return rules;
}
