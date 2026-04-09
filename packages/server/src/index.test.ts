import { describe, expect, it } from "vitest";

import {
  action,
  createQueryBuilder,
  defineConfig,
  defineRules,
  defineSchema,
  defineTable,
  httpAction,
  httpRouter,
  mutation,
  query,
  validateInsertData,
  validatePatchData,
  validateReplaceData,
} from "./index";

describe("@baseflare/server", () => {
  it("exports the phase 1 public API surface", () => {
    expect(defineSchema).toBeTypeOf("function");
    expect(defineTable).toBeTypeOf("function");
    expect(defineConfig).toBeTypeOf("function");
    expect(createQueryBuilder).toBeTypeOf("function");
    expect(query).toBeTypeOf("function");
    expect(mutation).toBeTypeOf("function");
    expect(action).toBeTypeOf("function");
    expect(defineRules).toBeTypeOf("function");
    expect(httpAction).toBeTypeOf("function");
    expect(httpRouter).toBeTypeOf("function");
    expect(validateInsertData).toBeTypeOf("function");
    expect(validateReplaceData).toBeTypeOf("function");
    expect(validatePatchData).toBeTypeOf("function");
  });
});
