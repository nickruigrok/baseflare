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
const CURSOR_ID = "019078e5-d29f-7b00-8000-1a2b3c4d5e6f";

describe("cursor encode/decode", () => {
  it("round-trips a field-ordered cursor", () => {
    const cursor = encodeCursor(PRIORITY_ASC, { _id: CURSOR_ID, priority: 5 });
    const decoded = decodeCursor(cursor, PRIORITY_ASC);
    expect(decoded).toEqual({
      orderField: "priority",
      orderDirection: "asc",
      id: CURSOR_ID,
      v: 5,
    });
  });

  it("normalizes a missing ordered field to null", () => {
    const cursor = encodeCursor(PRIORITY_ASC, { _id: CURSOR_ID });
    expect(decodeCursor(cursor, PRIORITY_ASC).v).toBeNull();
  });

  it("rejects non-scalar ordered field values", () => {
    expect(() =>
      encodeCursor(PRIORITY_ASC, { _id: CURSOR_ID, priority: { nested: true } })
    ).toThrow(/must be a string, number, boolean, or null/);
    expect(() =>
      encodeCursor(PRIORITY_ASC, {
        _id: CURSOR_ID,
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
        id: CURSOR_ID,
        v: 1,
      })
    ).toString("base64");
    const fieldCursorWithoutValue = Buffer.from(
      JSON.stringify({
        orderField: "priority",
        orderDirection: "asc",
        id: CURSOR_ID,
      })
    ).toString("base64");
    const fieldCursorWithObjectValue = Buffer.from(
      JSON.stringify({
        orderField: "priority",
        orderDirection: "asc",
        id: CURSOR_ID,
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
    const cursor = encodeCursor(PRIORITY_ASC, { _id: CURSOR_ID, priority: 5 });
    expect(() => decodeCursor(cursor, ID_ASC)).toThrow(
      /does not match the query ordering/
    );
  });

  it("rejects forged cursors without a UUIDv7 document id", () => {
    const forged = Buffer.from(
      JSON.stringify({
        orderField: "_id",
        orderDirection: "asc",
        id: "1 OR 1=1",
      })
    ).toString("base64");
    expect(() => decodeCursor(forged, ID_ASC)).toThrow(
      /Invalid pagination cursor/
    );
  });

  it("drops unknown fields from decoded cursors", () => {
    const padded = Buffer.from(
      JSON.stringify({
        orderField: "_id",
        orderDirection: "asc",
        id: CURSOR_ID,
        injected: "extra",
      })
    ).toString("base64");
    expect(decodeCursor(padded, ID_ASC)).toEqual({
      id: CURSOR_ID,
      orderDirection: "asc",
      orderField: "_id",
    });
  });
});

describe("buildCursorPredicate", () => {
  it("builds an _id predicate for default ordering", () => {
    expect(
      buildCursorPredicate(ID_ASC, {
        orderField: "_id",
        orderDirection: "asc",
        id: CURSOR_ID,
      })
    ).toEqual({ sql: "_id > ?", params: [CURSOR_ID] });
  });

  it("builds null-aware ascending predicates", () => {
    expect(
      buildCursorPredicate(PRIORITY_ASC, {
        orderField: "priority",
        orderDirection: "asc",
        id: CURSOR_ID,
        v: null,
      })
    ).toEqual({
      sql: "((json_extract(_data, '$.priority') IS NULL AND _id > ?) OR json_extract(_data, '$.priority') IS NOT NULL)",
      params: [CURSOR_ID],
    });

    expect(
      buildCursorPredicate(PRIORITY_ASC, {
        orderField: "priority",
        orderDirection: "asc",
        id: CURSOR_ID,
        v: 5,
      })
    ).toEqual({
      sql: "(json_extract(_data, '$.priority') > ? OR (json_extract(_data, '$.priority') = ? AND _id > ?))",
      params: [5, 5, CURSOR_ID],
    });

    expect(
      buildCursorPredicate(PRIORITY_ASC, {
        orderField: "priority",
        orderDirection: "asc",
        id: CURSOR_ID,
        v: "medium",
      })
    ).toEqual({
      sql: "(json_extract(_data, '$.priority') > ? OR (json_extract(_data, '$.priority') = ? AND _id > ?))",
      params: ["medium", "medium", CURSOR_ID],
    });

    expect(
      buildCursorPredicate(PRIORITY_ASC, {
        orderField: "priority",
        orderDirection: "asc",
        id: CURSOR_ID,
        v: true,
      })
    ).toEqual({
      sql: "(json_extract(_data, '$.priority') > ? OR (json_extract(_data, '$.priority') = ? AND _id > ?))",
      params: [1, 1, CURSOR_ID],
    });
  });

  it("builds null-aware descending predicates", () => {
    expect(
      buildCursorPredicate(PRIORITY_DESC, {
        orderField: "priority",
        orderDirection: "desc",
        id: CURSOR_ID,
        v: 5,
      })
    ).toEqual({
      sql: "(json_extract(_data, '$.priority') < ? OR (json_extract(_data, '$.priority') = ? AND _id < ?) OR json_extract(_data, '$.priority') IS NULL)",
      params: [5, 5, CURSOR_ID],
    });

    expect(
      buildCursorPredicate(PRIORITY_DESC, {
        orderField: "priority",
        orderDirection: "desc",
        id: CURSOR_ID,
        v: null,
      })
    ).toEqual({
      sql: "(json_extract(_data, '$.priority') IS NULL AND _id < ?)",
      params: [CURSOR_ID],
    });
  });
});
