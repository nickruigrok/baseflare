import { generateId } from "baseflare/values";
import { describe, expect, it } from "vitest";

import { deserialize } from "./deserialize";
import { serialize } from "./serialize";

describe("document serialization", () => {
  it("stores only _data and restores _id + _createdAt on deserialize", () => {
    const id = generateId();
    const bytes = new Uint8Array([1, 2, 3]);
    const serialized = serialize({
      _id: id,
      _createdAt: 0,
      text: "hello",
      bytes,
    });

    expect(serialized).toEqual({
      _data: JSON.stringify({
        text: "hello",
        bytes: { $bytes: Buffer.from(bytes).toString("base64") },
      }),
    });

    const deserialized = deserialize({ _id: id, _data: serialized._data });
    expect(deserialized._id).toBe(id);
    expect(typeof deserialized._createdAt).toBe("number");
    expect(deserialized.text).toBe("hello");
    expect(deserialized.bytes).toEqual(bytes);
  });

  it("escapes user keys that would collide with the bytes marker", () => {
    const id = generateId();
    const serialized = serialize({
      $bytes: "i am a real string field",
      data: new Uint8Array([9, 9]),
      $weird: 1,
    });

    const deserialized = deserialize({ _id: id, _data: serialized._data });
    expect(deserialized.$bytes).toBe("i am a real string field");
    expect(deserialized.data).toEqual(new Uint8Array([9, 9]));
    expect(deserialized.$weird).toBe(1);
  });
});
