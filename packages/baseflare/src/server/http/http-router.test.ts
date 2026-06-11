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

  it("matches prefix routes on path segment boundaries", () => {
    const router = httpRouter();
    const adminHandler = httpAction(async () => new Response("admin"));

    router.routeWithPrefix({
      handler: adminHandler,
      method: "GET",
      pathPrefix: "/admin",
    });

    expect(router.lookup("GET", "/admin")).toBe(adminHandler.handler);
    expect(router.lookup("GET", "/admin/users")).toBe(adminHandler.handler);
    expect(router.lookup("GET", "/administer")).toBeNull();
  });
});
