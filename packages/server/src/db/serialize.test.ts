import { generateId } from "@baseflare/values";
import { describe, expect, it } from "vitest";

import { deserialize } from "./deserialize";
import { serialize } from "./serialize";

describe("document serialization", () => {
  it("stores only _data and restores _id + _createdAt on deserialize", () => {
    const id = generateId();
    const bytes = new Uint8Array([1, 2, 3]);
    const serialized = serialize({
      _id: id,
      _createdAt: new Date(0),
      text: "hello",
      bytes,
    });

    expect(serialized).toEqual({
      _data: JSON.stringify({
        text: "hello",
        bytes: { __baseflare_bytes: Buffer.from(bytes).toString("base64") },
      }),
    });

    const deserialized = deserialize({ _id: id, _data: serialized._data });
    expect(deserialized._id).toBe(id);
    expect(deserialized._createdAt).toBeInstanceOf(Date);
    expect(deserialized.text).toBe("hello");
    expect(deserialized.bytes).toEqual(bytes);
  });
});
