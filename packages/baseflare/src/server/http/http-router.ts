import type {
  HttpActionHandler,
  HttpPrefixRouteConfig,
  HttpRouteConfig,
} from "./types";

function normalizeMethod(method: string): string {
  return method.toUpperCase();
}

function assertPath(path: string, label: string): void {
  if (!path.startsWith("/")) {
    throw new Error(`${label} must start with "/"`);
  }
}

/**
 * Routes custom HTTP requests to `httpAction` handlers. Exact paths win over
 * prefixes; among matching prefixes, the longest wins.
 */
export class HttpRouter {
  private readonly exactRoutes = new Map<string, HttpActionHandler>();
  private readonly prefixRoutes: Array<{
    key: string;
    method: string;
    pathPrefix: string;
    handler: HttpActionHandler;
  }> = [];

  /** Registers a handler for one exact `method` + `path` pair. */
  route(config: HttpRouteConfig): void {
    assertPath(config.path, "Route path");
    const method = normalizeMethod(config.method);
    const key = `${method}:${config.path}`;

    if (this.exactRoutes.has(key)) {
      throw new Error(`Route "${method} ${config.path}" is already registered`);
    }

    this.exactRoutes.set(key, config.handler.handler);
  }

  /** Registers a handler for every path under `pathPrefix`. */
  routeWithPrefix(config: HttpPrefixRouteConfig): void {
    assertPath(config.pathPrefix, "Route prefix");
    const method = normalizeMethod(config.method);
    const key = `${method}:${config.pathPrefix}`;

    if (this.prefixRoutes.some((route) => route.key === key)) {
      throw new Error(
        `Route prefix "${method} ${config.pathPrefix}" is already registered`
      );
    }

    this.prefixRoutes.push({
      key,
      method,
      pathPrefix: config.pathPrefix,
      handler: config.handler.handler,
    });
  }

  /** Resolves the handler for a request, or null when no route matches. */
  lookup(method: string, path: string): HttpActionHandler | null {
    const normalizedMethod = normalizeMethod(method);
    const exactMatch = this.exactRoutes.get(`${normalizedMethod}:${path}`);
    if (exactMatch) {
      return exactMatch;
    }

    const matchingPrefixes = this.prefixRoutes
      .filter(
        (route) =>
          route.method === normalizedMethod &&
          matchesPathPrefix(path, route.pathPrefix)
      )
      .sort((left, right) => right.pathPrefix.length - left.pathPrefix.length);

    return matchingPrefixes[0]?.handler ?? null;
  }
}

function matchesPathPrefix(path: string, prefix: string): boolean {
  return (
    path === prefix ||
    path.startsWith(prefix.endsWith("/") ? prefix : `${prefix}/`)
  );
}

/** Creates the router exported from `baseflare/http.ts` for custom HTTP endpoints. */
export function httpRouter(): HttpRouter {
  return new HttpRouter();
}
