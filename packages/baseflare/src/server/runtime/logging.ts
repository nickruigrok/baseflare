export type RuntimeLogLevel = "error" | "info" | "warn";

export function getRequestLogFields(request: Request): {
  readonly method: string;
  readonly pathname: string;
} {
  try {
    const url = new URL(request.url);
    return { method: request.method, pathname: url.pathname };
  } catch {
    return { method: request.method, pathname: "<malformed>" };
  }
}

export function logRuntimeEvent(
  level: RuntimeLogLevel,
  event: string,
  fields: Record<string, unknown> = {}
): void {
  const payload = {
    component: "baseflare-runtime",
    event,
    ...fields,
  };

  if (level === "error") {
    console.error("baseflare-runtime", payload);
    return;
  }

  if (level === "warn") {
    console.warn("baseflare-runtime", payload);
    return;
  }

  console.info("baseflare-runtime", payload);
}

export function emitRuntimeMetric(
  name: string,
  value: number,
  tags: Record<string, string | boolean | number>
): void {
  logRuntimeEvent("info", "runtime.metric", { metric: name, tags, value });
}
