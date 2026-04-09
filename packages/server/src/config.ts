const COMPATIBILITY_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

const CONFIG_KEYS = new Set([
  "project",
  "functions",
  "external",
  "cors",
  "limits",
  "middleware",
  "worker",
]);

const CORS_KEYS = new Set(["origins", "maxAge"]);
const LIMIT_KEYS = new Set(["maxQueryResults", "maxUploadSize"]);
const WORKER_KEYS = new Set(["compatibilityDate", "compatibilityFlags"]);

export interface BaseflareCorsConfig {
  maxAge?: number;
  origins: readonly string[];
}

export interface BaseflareLimitsConfig {
  maxQueryResults?: number;
  maxUploadSize?: string;
}

export interface BaseflareWorkerConfig {
  compatibilityDate?: string;
  compatibilityFlags?: readonly string[];
}

export interface BaseflareConfig {
  cors?: BaseflareCorsConfig;
  external?: readonly string[];
  functions?: string;
  limits?: BaseflareLimitsConfig;
  middleware?: readonly unknown[];
  project: string;
  worker?: BaseflareWorkerConfig;
}

function assertPlainObject(
  value: unknown,
  label: string
): asserts value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
}

function assertAllowedKeys(
  value: Record<string, unknown>,
  label: string,
  allowedKeys: ReadonlySet<string>
): void {
  for (const key of Object.keys(value)) {
    if (!allowedKeys.has(key)) {
      throw new Error(`Unknown ${label} option "${key}"`);
    }
  }
}

function assertStringArray(values: readonly string[], label: string): void {
  for (const value of values) {
    if (typeof value !== "string" || value.length === 0) {
      throw new Error(`${label} entries must be non-empty strings`);
    }
  }
}

function validateCorsConfig(cors: BaseflareCorsConfig): void {
  assertPlainObject(cors, "cors");
  assertAllowedKeys(cors, "cors", CORS_KEYS);
  assertStringArray(cors.origins, "cors.origins");

  if (
    cors.maxAge !== undefined &&
    (!Number.isInteger(cors.maxAge) || cors.maxAge < 0)
  ) {
    throw new Error("cors.maxAge must be a non-negative integer");
  }
}

function validateLimitsConfig(limits: BaseflareLimitsConfig): void {
  assertPlainObject(limits, "limits");
  assertAllowedKeys(limits, "limits", LIMIT_KEYS);
  const maxQueryResults = limits.maxQueryResults;
  const maxUploadSize = limits.maxUploadSize;

  if (
    maxQueryResults !== undefined &&
    (typeof maxQueryResults !== "number" ||
      !Number.isInteger(maxQueryResults) ||
      maxQueryResults <= 0)
  ) {
    throw new Error("limits.maxQueryResults must be a positive integer");
  }

  if (
    maxUploadSize !== undefined &&
    (typeof maxUploadSize !== "string" || maxUploadSize.length === 0)
  ) {
    throw new Error("limits.maxUploadSize must be a non-empty string");
  }
}

function validateWorkerConfig(worker: BaseflareWorkerConfig): void {
  assertPlainObject(worker, "worker");
  assertAllowedKeys(worker, "worker", WORKER_KEYS);
  const compatibilityDate = worker.compatibilityDate;
  const compatibilityFlags = worker.compatibilityFlags;

  if (
    compatibilityDate !== undefined &&
    (typeof compatibilityDate !== "string" ||
      !COMPATIBILITY_DATE_PATTERN.test(compatibilityDate))
  ) {
    throw new Error("worker.compatibilityDate must use the YYYY-MM-DD format");
  }

  if (compatibilityFlags !== undefined) {
    if (!Array.isArray(compatibilityFlags)) {
      throw new Error("worker.compatibilityFlags must be an array");
    }

    assertStringArray(compatibilityFlags, "worker.compatibilityFlags");
  }
}

export function defineConfig(config: BaseflareConfig): BaseflareConfig {
  assertPlainObject(config, "config");
  assertAllowedKeys(config, "config", CONFIG_KEYS);

  if (
    typeof config.project !== "string" ||
    config.project.trim().length === 0
  ) {
    throw new Error("config.project must be a non-empty string");
  }

  if (
    config.functions !== undefined &&
    (typeof config.functions !== "string" ||
      config.functions.trim().length === 0)
  ) {
    throw new Error("config.functions must be a non-empty string");
  }

  if (config.external !== undefined) {
    assertStringArray(config.external, "config.external");
  }

  if (config.cors !== undefined) {
    validateCorsConfig(config.cors);
  }

  if (config.limits !== undefined) {
    validateLimitsConfig(config.limits);
  }

  if (config.middleware !== undefined && !Array.isArray(config.middleware)) {
    throw new Error("config.middleware must be an array");
  }

  if (config.worker !== undefined) {
    validateWorkerConfig(config.worker);
  }

  return config;
}
