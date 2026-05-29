import { generateId, v } from "@baseflare/values";
import { describe, expect, it } from "vitest";

import { defineTable } from "../schema/define-table";
import {
  validateInsertData,
  validatePatchData,
  validateReplaceData,
} from "./write-validation";

const RESERVED_FIELD_ERROR_PATTERN = /reserved field/;
const DOCUMENT_TEXT_ERROR_PATTERN = /document\.text/;

describe("write validation", () => {
  it("validates insert data, applies defaults, and rejects reserved fields", () => {
    const todos = defineTable({
      text: v.string(),
      completed: v.boolean().default(false),
    });

    expect(validateInsertData(todos, { text: "ship it" })).toEqual({
      text: "ship it",
      completed: false,
    });

    expect(() =>
      validateInsertData(todos, { _id: generateId(), text: "ship it" })
    ).toThrow(RESERVED_FIELD_ERROR_PATTERN);
  });

  it("validates replace data against the full schema", () => {
    const todos = defineTable({
      text: v.string(),
      completed: v.boolean().default(false),
    });

    expect(validateReplaceData(todos, { text: "rewrite docs" })).toEqual({
      text: "rewrite docs",
      completed: false,
    });

    expect(() => validateReplaceData(todos, { completed: true })).toThrow(
      DOCUMENT_TEXT_ERROR_PATTERN
    );
  });

  it("applies shallow patch semantics, removes undefined fields, and ignores orphaned current fields", () => {
    const todos = defineTable({
      text: v.string(),
      completed: v.boolean().default(false),
      assignee: v.optional(v.id("users")),
    });

    const assignee = generateId();
    const patched = validatePatchData(
      todos,
      {
        _id: generateId(),
        _createdAt: Date.now(),
        text: "draft",
        completed: true,
        assignee,
        legacyField: "orphaned",
      },
      {
        text: "published",
        assignee: undefined,
      }
    );

    expect(patched).toEqual({
      text: "published",
      completed: true,
    });
  });

  it("patches unrelated fields on documents missing a newly-required field", () => {
    const todos = defineTable({
      text: v.string(),
      priority: v.string(),
    });

    // `priority` was added later; this old document predates it.
    const patched = validatePatchData(
      todos,
      { _id: generateId(), _createdAt: Date.now(), text: "old" },
      { text: "updated" }
    );

    expect(patched).toEqual({ text: "updated" });
  });

  it("rejects patch fields not in the schema", () => {
    const todos = defineTable({ text: v.string() });

    expect(() =>
      validatePatchData(
        todos,
        { _id: generateId(), _createdAt: Date.now(), text: "old" },
        { nope: 1 }
      )
    ).toThrow(/not allowed by the schema/);

    expect(() =>
      validatePatchData(
        todos,
        { _id: generateId(), _createdAt: Date.now(), text: "old" },
        { nope: undefined }
      )
    ).toThrow(/not allowed by the schema/);
  });
});
