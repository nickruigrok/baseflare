import { v } from "@baseflare/values";

import type { TableDefinition } from "../schema/types";

const RESERVED_DOCUMENT_FIELDS = new Set(["_id", "_createdAt"]);

export type DocumentData = Record<string, unknown>;
export type DocumentPatch = Record<string, unknown>;

function assertPlainObject(
  value: unknown,
  label: string
): asserts value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
}

function assertNoReservedFields(
  value: Record<string, unknown>,
  label: string
): void {
  for (const key of Object.keys(value)) {
    if (RESERVED_DOCUMENT_FIELDS.has(key)) {
      throw new Error(`${label} cannot include reserved field "${key}"`);
    }
  }
}

function createDocumentValidator(table: TableDefinition) {
  return v.object(table.fields);
}

function pickSchemaFields(
  table: TableDefinition,
  value: Record<string, unknown>
): DocumentData {
  const result: DocumentData = {};

  for (const field of Object.keys(table.fields)) {
    if (field in value) {
      result[field] = value[field];
    }
  }

  return result;
}

export function validateInsertData(
  table: TableDefinition,
  data: DocumentData
): DocumentData {
  assertPlainObject(data, "Insert data");
  assertNoReservedFields(data, "Insert data");
  return createDocumentValidator(table).validate(
    data,
    "document"
  ) as DocumentData;
}

export function validateReplaceData(
  table: TableDefinition,
  data: DocumentData
): DocumentData {
  assertPlainObject(data, "Replace data");
  assertNoReservedFields(data, "Replace data");
  return createDocumentValidator(table).validate(
    data,
    "document"
  ) as DocumentData;
}

export function validatePatchData(
  table: TableDefinition,
  current: DocumentData,
  patch: DocumentPatch
): DocumentData {
  assertPlainObject(current, "Current document");
  assertPlainObject(patch, "Patch data");
  assertNoReservedFields(patch, "Patch data");

  const next = pickSchemaFields(table, current);

  for (const [key, value] of Object.entries(patch)) {
    if (value === undefined) {
      delete next[key];
      continue;
    }

    next[key] = value;
  }

  return createDocumentValidator(table).validate(
    next,
    "document"
  ) as DocumentData;
}
