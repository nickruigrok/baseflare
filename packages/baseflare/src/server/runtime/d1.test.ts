import { describe, expect, it } from "vitest";

import type { Schema } from "../schema/types";
import { assertKnownTable, assertWithinScanBudget } from "./d1";

describe("D1 runtime helpers", () => {
  it("looks up schema tables by own property only", () => {
    const schema = {
      tables: {},
      toCreateStatements: () => [],
    } as unknown as Schema;

    expect(() => assertKnownTable(schema, "constructor")).toThrow(
      'Unknown table "constructor"'
    );
  });

  it("fails clearly when internal scan budgets are exceeded", () => {
    expect(() => assertWithinScanBudget(20_001, 0)).toThrow(
      "Query exceeded the internal scan budget"
    );
    expect(() => assertWithinScanBudget(1, 5_000_001)).toThrow(
      "Query exceeded the internal scan budget"
    );
  });
});
