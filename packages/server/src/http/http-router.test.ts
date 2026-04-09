import { describe, expect, it } from "vitest";

import { httpAction } from "./http-action";
import { HttpRouter, httpRouter } from "./http-router";

describe("HttpRouter", () => {
  it("prefers exact matches over prefix matches", async () => {
    const router = httpRouter();
    const prefixResponse = new Response("prefix");
    const exactResponse = new Response("exact");

    router.routeWithPrefix({
      pathPrefix: "/api/v1/",
      method: "GET",
      handler: httpAction(async () => prefixResponse),
    });
    router.route({
      path: "/api/v1/users",
      method: "GET",
      handler: httpAction(async () => exactResponse),
    });

    const exactHandler = router.lookup("GET", "/api/v1/users");
    const prefixHandler = router.lookup("GET", "/api/v1/projects");

    expect(exactHandler).not.toBeNull();
    expect(prefixHandler).not.toBeNull();
    await expect(
      exactHandler?.({} as never, new Request("https://example.com"))
    ).resolves.toBe(exactResponse);
    await expect(
      prefixHandler?.({} as never, new Request("https://example.com"))
    ).resolves.toBe(prefixResponse);
    expect(router.lookup("POST", "/missing")).toBeNull();
    expect(router).toBeInstanceOf(HttpRouter);
  });
});
