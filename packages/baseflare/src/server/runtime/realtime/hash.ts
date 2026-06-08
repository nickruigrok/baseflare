const HEX_BYTE_LENGTH = 2;

export async function sha256Hex(input: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(input)
  );
  return Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(HEX_BYTE_LENGTH, "0")
  ).join("");
}
