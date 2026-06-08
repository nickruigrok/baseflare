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

export class HttpRouter {
  private readonly exactRoutes = new Map<string, HttpActionHandler>();
  private readonly prefixRoutes: Array<{
    key: string;
    method: string;
    pathPrefix: string;
    handler: HttpActionHandler;
  }> = [];

  route(config: HttpRouteConfig): void {
    assertPath(config.path, "Route path");
    const method = normalizeMethod(config.method);
    const key = `${method}:${config.path}`;

    if (this.exactRoutes.has(key)) {
      throw new Error(`Route "${method} ${config.path}" is already registered`);
    }

    this.exactRoutes.set(key, config.handler.handler);
  }

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

export function httpRouter(): HttpRouter {
  return new HttpRouter();
}
