import { describe, expect, it } from "vitest";

import {
  buildCursorPredicate,
  decodeCursor,
  encodeCursor,
  type OrderSpec,
} from "./cursor";

const ID_ASC: OrderSpec = { field: "_id", direction: "asc" };
const PRIORITY_ASC: OrderSpec = { field: "priority", direction: "asc" };
const PRIORITY_DESC: OrderSpec = { field: "priority", direction: "desc" };

describe("cursor encode/decode", () => {
  it("round-trips a field-ordered cursor", () => {
    const cursor = encodeCursor(PRIORITY_ASC, { _id: "id-1", priority: 5 });
    const decoded = decodeCursor(cursor, PRIORITY_ASC);
    expect(decoded).toEqual({
      orderField: "priority",
      orderDirection: "asc",
      id: "id-1",
      v: 5,
    });
  });

  it("normalizes a missing ordered field to null", () => {
    const cursor = encodeCursor(PRIORITY_ASC, { _id: "id-1" });
    expect(decodeCursor(cursor, PRIORITY_ASC).v).toBeNull();
  });

  it("rejects non-scalar ordered field values", () => {
    expect(() =>
      encodeCursor(PRIORITY_ASC, { _id: "id-1", priority: { nested: true } })
    ).toThrow(/must be a string, number, boolean, or null/);
    expect(() =>
      encodeCursor(PRIORITY_ASC, {
        _id: "id-1",
        priority: new Uint8Array([1]),
      })
    ).toThrow(/must be a string, number, boolean, or null/);
  });

  it("rejects malformed cursors", () => {
    expect(() => decodeCursor("!!!not-base64-json", ID_ASC)).toThrow(
      /Invalid pagination cursor/
    );
    const wrongShape = Buffer.from(JSON.stringify({ id: "x" })).toString(
      "base64"
    );
    expect(() => decodeCursor(wrongShape, ID_ASC)).toThrow(
      /Invalid pagination cursor/
    );
  });

  it("rejects malformed cursor values", () => {
    const idCursorWithValue = Buffer.from(
      JSON.stringify({
        orderField: "_id",
        orderDirection: "asc",
        id: "id-1",
        v: 1,
      })
    ).toString("base64");
    const fieldCursorWithoutValue = Buffer.from(
      JSON.stringify({
        orderField: "priority",
        orderDirection: "asc",
        id: "id-1",
      })
    ).toString("base64");
    const fieldCursorWithObjectValue = Buffer.from(
      JSON.stringify({
        orderField: "priority",
        orderDirection: "asc",
        id: "id-1",
        v: { nested: true },
      })
    ).toString("base64");

    expect(() => decodeCursor(idCursorWithValue, ID_ASC)).toThrow(
      /must not include "v"/
    );
    expect(() => decodeCursor(fieldCursorWithoutValue, PRIORITY_ASC)).toThrow(
      /must include "v"/
    );
    expect(() =>
      decodeCursor(fieldCursorWithObjectValue, PRIORITY_ASC)
    ).toThrow(/must be a string, number, boolean, or null/);
  });

  it("rejects cursors that do not match the query ordering", () => {
    const cursor = encodeCursor(PRIORITY_ASC, { _id: "id-1", priority: 5 });
    expect(() => decodeCursor(cursor, ID_ASC)).toThrow(
      /does not match the query ordering/
    );
  });
});

describe("buildCursorPredicate", () => {
  it("builds an _id predicate for default ordering", () => {
    expect(
      buildCursorPredicate(ID_ASC, {
        orderField: "_id",
        orderDirection: "asc",
        id: "id-1",
      })
    ).toEqual({ sql: "_id > ?", params: ["id-1"] });
  });

  it("builds null-aware ascending predicates", () => {
    expect(
      buildCursorPredicate(PRIORITY_ASC, {
        orderField: "priority",
        orderDirection: "asc",
        id: "id-1",
        v: null,
      })
    ).toEqual({
      sql: "((json_extract(_data, '$.priority') IS NULL AND _id > ?) OR json_extract(_data, '$.priority') IS NOT NULL)",
      params: ["id-1"],
    });

    expect(
      buildCursorPredicate(PRIORITY_ASC, {
        orderField: "priority",
        orderDirection: "asc",
        id: "id-1",
        v: 5,
      })
    ).toEqual({
      sql: "(json_extract(_data, '$.priority') > ? OR (json_extract(_data, '$.priority') = ? AND _id > ?))",
      params: [5, 5, "id-1"],
    });

    expect(
      buildCursorPredicate(PRIORITY_ASC, {
        orderField: "priority",
        orderDirection: "asc",
        id: "id-1",
        v: "medium",
      })
    ).toEqual({
      sql: "(json_extract(_data, '$.priority') > ? OR (json_extract(_data, '$.priority') = ? AND _id > ?))",
      params: ["medium", "medium", "id-1"],
    });

    expect(
      buildCursorPredicate(PRIORITY_ASC, {
        orderField: "priority",
        orderDirection: "asc",
        id: "id-1",
        v: true,
      })
    ).toEqual({
      sql: "(json_extract(_data, '$.priority') > ? OR (json_extract(_data, '$.priority') = ? AND _id > ?))",
      params: [1, 1, "id-1"],
    });
  });

  it("builds null-aware descending predicates", () => {
    expect(
      buildCursorPredicate(PRIORITY_DESC, {
        orderField: "priority",
        orderDirection: "desc",
        id: "id-1",
        v: 5,
      })
    ).toEqual({
      sql: "(json_extract(_data, '$.priority') < ? OR (json_extract(_data, '$.priority') = ? AND _id < ?) OR json_extract(_data, '$.priority') IS NULL)",
      params: [5, 5, "id-1"],
    });

    expect(
      buildCursorPredicate(PRIORITY_DESC, {
        orderField: "priority",
        orderDirection: "desc",
        id: "id-1",
        v: null,
      })
    ).toEqual({
      sql: "(json_extract(_data, '$.priority') IS NULL AND _id < ?)",
      params: ["id-1"],
    });
  });
});
