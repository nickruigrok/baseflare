import { describe, expect, it } from "vitest";

import { createQueryBuilder } from "./query-builder";

const UNIQUE_DOCUMENT_ERROR_PATTERN = /Expected exactly one document/;

describe("createQueryBuilder", () => {
  it("produces json_extract SQL for equality filters", () => {
    const query = createQueryBuilder("todos")
      .filter({ completed: false })
      .order("desc")
      .limit(10) as ReturnType<typeof createQueryBuilder>;

    expect(query.toSQL()).toEqual({
      sql: "SELECT _id, _data FROM todos WHERE json_extract(_data, '$.completed') = ? ORDER BY _id DESC LIMIT ?",
      params: [0, 10],
    });
  });

  it("supports unique, count, and pagination against an executor", async () => {
    const documents = [
      { _id: "019078e5-d29f-7000-8000-000000000001", text: "a" },
      { _id: "019078e5-d29f-7000-8000-000000000002", text: "b" },
      { _id: "019078e5-d29f-7000-8000-000000000003", text: "c" },
    ];

    const query = createQueryBuilder("todos", {
      collect(builtQuery) {
        const limit = builtQuery.params.at(-1);
        return Promise.resolve(
          typeof limit === "number" ? documents.slice(0, limit) : documents
        );
      },
      count() {
        return Promise.resolve(documents.length);
      },
    });

    const secondDocument = documents[1];
    expect(secondDocument).toBeDefined();

    await expect(query.unique()).rejects.toThrow(UNIQUE_DOCUMENT_ERROR_PATTERN);
    await expect(query.count()).resolves.toBe(3);
    await expect(query.take(2)).resolves.toHaveLength(2);
    await expect(
      query.paginate({ numItems: 2, cursor: null })
    ).resolves.toEqual({
      page: documents.slice(0, 2),
      isDone: false,
      continueCursor: secondDocument?._id,
    });
  });
});
