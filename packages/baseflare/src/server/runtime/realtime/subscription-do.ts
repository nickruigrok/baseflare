import type { QueryDefinition } from "../../functions/types";
import { InternalRuntimeError, ValidationRuntimeError } from "../errors";
import { executeQueryDefinition } from "../execution";
import { logRuntimeEvent } from "../logging";
import type { D1Database, DurableObjectStub, RuntimeDatabase } from "../types";
import { RealtimeActiveQueryStore } from "./active-query-store";
import {
  createRealtimeOutboxResponseEvents,
  deleteRealtimeOutboxEventsBefore,
  fetchRealtimeOutboxEventById,
  fetchRealtimeOutboxEventSequenceById,
  fetchRealtimeOutboxEvents,
  fetchRealtimeOutboxHistoryGap,
  hasRealtimeOutboxEvents,
} from "./outbox";
import { RealtimeRegistrationStore } from "./registration-store";
import {
  createFullRealtimeAffectedTargets,
  createRealtimeAffectedTargets,
  createRealtimeDependencySet,
  createRegistrationKey,
  getRealtimeRegistrationHomeRouteTarget,
  getRealtimeShardGenerationIdFromName,
  getRealtimeSubscriptionShardName,
  isZeroRealtimeVersionSnapshot,
  parseRealtimeDependencySetValue,
  parseRealtimePartitionId,
  parseRealtimeVersionSnapshotValue,
  serializeRealtimeDependencySet,
  serializeRealtimeVersionSnapshot,
} from "./routing";
import {
  evaluateRealtimeAutoscaling,
  fetchActiveRealtimeShardGeneration,
  fetchOldestRealtimeShardCursor,
  fetchRealtimeVersionSnapshot,
  recordRealtimeShardCursor,
} from "./shards";
import {
  chunkRealtimeDeliveries,
  configuredRealtimeRuntimes,
  emitOutboxLagMetric,
  emitRealtimeMetric,
  getEpoch,
  getOptionalSequence,
  getOptionalStringField,
  getStringField,
  jsonResponse,
  readJsonObject,
} from "./shared";
import type {
  PendingRealtimeDelivery,
  RealtimeAffectedTargets,
  RealtimeDeliveryGroup,
  RealtimeDeliveryResult,
  RealtimeDependencySet,
  RealtimeDurableObjectState,
  RealtimeMetricSource,
  RealtimeObjectEnv,
  RealtimePressureSnapshot,
  RealtimeRegistration,
  RealtimeSequencedOutboxEvent,
  RealtimeVersionSnapshot,
  StoredRealtimeActiveQuery,
  StoredRealtimeRegistration,
} from "./types";
import {
  DEFAULT_REALTIME_SHARD_GENERATION,
  JSON_HEADERS,
  REALTIME_ACTIVE_QUERIES_METRIC,
  REALTIME_BACKPRESSURE_METRIC,
  REALTIME_CATCH_UP_EVENT_LIMIT,
  REALTIME_DELIVERY_BATCHES_METRIC,
  REALTIME_LEASE_MS,
  REALTIME_NOTIFY_EVENT_LOOKUP_ATTEMPTS,
  REALTIME_NOTIFY_EVENT_LOOKUP_RETRY_DELAY_MS,
  REALTIME_OUTBOX_CLEANUP_INTERVAL_MS,
  REALTIME_OUTBOX_CLEANUP_LIMIT,
  REALTIME_OUTBOX_CLEANUPS_METRIC,
  REALTIME_OUTBOX_RETENTION_MS,
  REALTIME_PENDING_WORK_LIMIT,
  REALTIME_QUERY_EXECUTIONS_SAVED_METRIC,
  REALTIME_RE_EVALUATIONS_METRIC,
  REALTIME_REEVALUATION_CONCURRENCY,
} from "./types";

interface RealtimeShardContext {
  readonly generationId: number;
  readonly shardName: string;
}

interface RealtimeEvaluationResult {
  readonly dependencies: RealtimeDependencySet;
  readonly result: unknown;
  readonly resultJson: string;
  readonly versionSnapshot: RealtimeVersionSnapshot;
}

export class RealtimeSubscriptionDO {
  private readonly database: D1Database;
  private readonly env: RealtimeObjectEnv;
  private readonly activeQueryStore: RealtimeActiveQueryStore;
  private readonly registrationStore: RealtimeRegistrationStore;
  private readonly state: RealtimeDurableObjectState;
  private readonly pendingNotifyEventIds = new Set<string>();
  private readonly reEvaluatingActiveQueries = new Set<string>();
  private readonly reEvaluatingRegistrations = new Set<string>();
  private deliveryBatchAttemptsSinceAutoscale = 0;
  private deliveryBatchFailuresSinceAutoscale = 0;
  private lastAutoscaleEvaluationAt = 0;
  private lastDeliveryBatchLatencyMs = 0;
  private lastOutboxCleanupAt = -REALTIME_OUTBOX_CLEANUP_INTERVAL_MS;
  private lastOutboxLagMs = 0;
  private lastProcessedOutboxSequence: number | null = null;
  private lastReEvaluationLatencyMs = 0;
  private shardGenerationId = DEFAULT_REALTIME_SHARD_GENERATION.generationId;
  private shardName = getRealtimeSubscriptionShardName();

  constructor(
    state: RealtimeDurableObjectState | null,
    env: RealtimeObjectEnv
  ) {
    this.database = env.APP_DB;
    this.env = env;
    this.state = state ?? {};
    this.activeQueryStore = new RealtimeActiveQueryStore(this.state);
    this.registrationStore = new RealtimeRegistrationStore(
      this.state,
      this.activeQueryStore
    );
  }

  async alarm(): Promise<void> {
    await this.loadRealtimeState();
    await this.registrationStore.cleanupExpired();
    const cleanupSucceeded = await this.cleanupRealtimeOutbox();
    if (!cleanupSucceeded || (await this.shouldScheduleOutboxCleanupAlarm())) {
      await this.scheduleOutboxCleanupAlarm({ replace: true });
    }
  }

  async fetch(request: Request): Promise<Response> {
    await this.loadRealtimeState();
    const url = new URL(request.url);
    if (request.method !== "POST") {
      return new Response("Not found", { status: 404 });
    }

    if (url.pathname === "/register") {
      return await this.handleRegister(request);
    }

    if (url.pathname === "/adopt-registration") {
      return await this.handleAdoptRegistration(request);
    }

    if (url.pathname === "/activate-registration") {
      return await this.handleActivateRegistration(request);
    }

    if (url.pathname === "/unregister") {
      return await this.handleUnregister(request);
    }

    if (url.pathname === "/notify") {
      return await this.handleNotify(request);
    }

    if (url.pathname === "/catch-up") {
      return await this.handleCatchUp(request);
    }

    if (url.pathname === "/registrations") {
      return jsonResponse({
        lastProcessedOutboxSequence: this.lastProcessedOutboxSequence,
        registrations: this.registrationStore.values(),
        shardName: this.shardName,
      });
    }

    return new Response("Not found", { status: 404 });
  }

  private async loadRealtimeState(): Promise<void> {
    await this.activeQueryStore.loadOnce();
    await this.registrationStore.loadOnce();
  }

  private async handleRegister(request: Request): Promise<Response> {
    const body = await readJsonObject(request);
    this.setCurrentShardName(getOptionalStringField(body, "shardName"));
    await this.scheduleOutboxCleanupAlarm();
    const registration = this.parseRegistration(body);
    const registrationKey = createRegistrationKey(
      registration.connectionKey,
      registration.subscriptionId
    );
    const storedRegistration: StoredRealtimeRegistration = {
      ...registration,
      dependencies: parseRealtimeDependencySetValue(body.dependencies),
      lastResultJson:
        typeof body.lastResultJson === "string"
          ? body.lastResultJson
          : undefined,
      versionSnapshot: parseRealtimeVersionSnapshotValue(body.versionSnapshot),
    };
    const existing = this.registrationStore.get(registrationKey);
    if (!existing || registration.epoch >= existing.epoch) {
      await this.registrationStore.upsert(registrationKey, storedRegistration);
    }
    await this.maybeEvaluateAutoscaling();
    return jsonResponse({ ok: true });
  }

  private async handleAdoptRegistration(request: Request): Promise<Response> {
    const body = await readJsonObject(request);
    this.setCurrentShardName(getOptionalStringField(body, "shardName"));
    await this.scheduleOutboxCleanupAlarm();
    const registration = this.parseRegistration(body);
    const storedRegistration: StoredRealtimeRegistration = {
      ...registration,
      dependencies: parseRealtimeDependencySetValue(body.dependencies),
      lastResultJson:
        typeof body.lastResultJson === "string"
          ? body.lastResultJson
          : undefined,
      movePending: true,
      versionSnapshot: parseRealtimeVersionSnapshotValue(body.versionSnapshot),
    };
    const registrationKey = createRegistrationKey(
      storedRegistration.connectionKey,
      storedRegistration.subscriptionId
    );
    const existing = this.registrationStore.get(registrationKey);
    if (existing && storedRegistration.epoch < existing.epoch) {
      return jsonResponse({ ok: true });
    }

    await this.registrationStore.upsert(registrationKey, storedRegistration);
    await this.maybeEvaluateAutoscaling();
    return jsonResponse({ ok: true });
  }

  private async handleActivateRegistration(
    request: Request
  ): Promise<Response> {
    const body = await readJsonObject(request);
    this.setCurrentShardName(getOptionalStringField(body, "shardName"));
    const registrationKey = createRegistrationKey(
      getStringField(body, "connectionKey"),
      getStringField(body, "subscriptionId")
    );
    const registration = this.registrationStore.get(registrationKey);
    if (!registration) {
      return jsonResponse({ ok: false }, { status: 404 });
    }

    const activatedRegistration = { ...registration, movePending: false };
    try {
      await this.registrationStore.upsert(
        registrationKey,
        activatedRegistration
      );
    } catch (error) {
      if (registration.movePending) {
        try {
          await this.registrationStore.delete(registrationKey);
        } catch (cleanupError) {
          this.logRegistrationMoveCleanupFailure(registration, this.shardName, {
            errorName:
              cleanupError instanceof Error
                ? cleanupError.name
                : typeof cleanupError,
            sourceRemoved: false,
          });
        }
      }
      throw error;
    }
    await this.maybeEvaluateAutoscaling();
    return jsonResponse({ ok: true });
  }

  private async handleUnregister(request: Request): Promise<Response> {
    const body = await readJsonObject(request);
    await this.registrationStore.delete(
      createRegistrationKey(
        getStringField(body, "connectionKey"),
        getStringField(body, "subscriptionId")
      )
    );
    await this.maybeEvaluateAutoscaling();
    return jsonResponse({ ok: true });
  }

  private async handleNotify(request: Request): Promise<Response> {
    const body = await readJsonObject(request);
    const shardContext = this.setCurrentShardName(
      getOptionalStringField(body, "shardName")
    );
    await this.scheduleOutboxCleanupAlarm();
    const eventId = getStringField(body, "eventId");
    const outboxBookmark = getOptionalStringField(body, "outboxBookmark");
    const backpressureResponse = this.tryReserveNotifyEvent(eventId);
    if (backpressureResponse) {
      if (backpressureResponse.rejected) {
        await this.maybeEvaluateAutoscaling();
      }
      return backpressureResponse.response;
    }

    try {
      const eventLookup = await this.fetchNotifyOutboxEvent(
        eventId,
        outboxBookmark
      );
      if (eventLookup.status === "malformed") {
        logRuntimeEvent(
          "warn",
          "runtime.realtime_notify_malformed_outbox_recovered",
          {
            eventId,
            sequence: eventLookup.sequence,
            shardName: this.shardName,
          }
        );
        const result = await this.reEvaluateActiveRegistrations(
          createFullRealtimeAffectedTargets(eventLookup.sequence),
          "notify",
          eventLookup.database
        );
        await this.advanceLastProcessedOutboxSequence(
          eventLookup.sequence,
          shardContext
        );
        await this.cleanupRealtimeOutbox();
        await this.maybeEvaluateAutoscaling();
        return jsonResponse({ ...result, ok: true });
      }

      if (eventLookup.status !== "found") {
        logRuntimeEvent(
          "error",
          "runtime.realtime_notify_outbox_event_missing",
          {
            eventId,
            shardName: this.shardName,
          }
        );
        return jsonResponse(
          { evaluated: 0, failed: 0, ok: false },
          { status: 503 }
        );
      }

      const { database, event } = eventLookup;
      this.lastOutboxLagMs = emitOutboxLagMetric("notify", [event]);
      const result = await this.reEvaluateActiveRegistrations(
        createRealtimeAffectedTargets([event]),
        "notify",
        database
      );
      // Advance after evaluation so an unexpected evaluation failure cannot
      // make this shard permanently skip the event.
      await this.advanceLastProcessedOutboxSequence(
        event.sequence,
        shardContext
      );
      await this.cleanupRealtimeOutbox();
      await this.maybeEvaluateAutoscaling();
      return jsonResponse({ ...result, ok: true });
    } finally {
      this.pendingNotifyEventIds.delete(eventId);
    }
  }

  private async fetchNotifyOutboxEvent(
    eventId: string,
    outboxBookmark?: string
  ): Promise<
    | {
        readonly database: RuntimeDatabase;
        readonly event: RealtimeSequencedOutboxEvent;
        readonly status: "found";
      }
    | {
        readonly database: RuntimeDatabase;
        readonly sequence: number;
        readonly status: "malformed";
      }
    | { readonly status: "missing" }
  > {
    const database =
      outboxBookmark && this.database.withSession
        ? this.database.withSession(outboxBookmark)
        : this.database;
    for (
      let attempt = 1;
      attempt <= REALTIME_NOTIFY_EVENT_LOOKUP_ATTEMPTS;
      attempt += 1
    ) {
      const event = await fetchRealtimeOutboxEventById(database, eventId);
      if (event) {
        return { database, event, status: "found" };
      }

      const malformedSequence = await fetchRealtimeOutboxEventSequenceById(
        database,
        eventId
      );
      if (malformedSequence !== null) {
        return { database, sequence: malformedSequence, status: "malformed" };
      }

      if (attempt < REALTIME_NOTIFY_EVENT_LOOKUP_ATTEMPTS) {
        await waitForRealtimeNotifyEventLookupRetry();
      }
    }

    return { status: "missing" };
  }

  private async handleCatchUp(request: Request): Promise<Response> {
    const body = await readJsonObject(request);
    const shardContext = this.setCurrentShardName(
      getOptionalStringField(body, "shardName")
    );
    await this.scheduleOutboxCleanupAlarm();
    const afterSequence = getOptionalSequence(
      body.afterSequence,
      "afterSequence"
    );
    const outboxBookmark = getOptionalStringField(body, "outboxBookmark");
    const database =
      outboxBookmark && this.database.withSession
        ? this.database.withSession(outboxBookmark)
        : this.database;
    const historyGap = await fetchRealtimeOutboxHistoryGap(
      database,
      afterSequence
    );
    if (historyGap.hasGap) {
      const recoverySequence = historyGap.latestSequence ?? afterSequence;
      const result = await this.reEvaluateActiveRegistrations(
        createFullRealtimeAffectedTargets(recoverySequence),
        "catch_up",
        database
      );
      await this.advanceLastProcessedOutboxSequence(
        recoverySequence,
        shardContext
      );
      await this.cleanupRealtimeOutbox();
      await this.maybeEvaluateAutoscaling();
      return jsonResponse({
        ...result,
        events: [],
        ok: true,
        recoveredByFullReevaluation: true,
      });
    }

    const catchUp = await fetchRealtimeOutboxEvents(
      database,
      afterSequence,
      this.catchUpEventLimit(body.limit)
    );
    if (catchUp.hasMalformedEvents) {
      const result = await this.reEvaluateActiveRegistrations(
        createFullRealtimeAffectedTargets(catchUp.latestReadSequence),
        "catch_up",
        database
      );
      await this.advanceLastProcessedOutboxSequence(
        catchUp.latestReadSequence,
        shardContext
      );
      await this.cleanupRealtimeOutbox();
      await this.maybeEvaluateAutoscaling();
      return jsonResponse({
        ...result,
        events: createRealtimeOutboxResponseEvents(catchUp.events),
        ok: true,
        recoveredByFullReevaluation: true,
      });
    }

    const { events } = catchUp;
    if (events.length === 0) {
      await this.advanceLastProcessedOutboxSequence(
        afterSequence,
        shardContext
      );
      await this.cleanupRealtimeOutbox();
      await this.maybeEvaluateAutoscaling();
      return jsonResponse({
        evaluated: 0,
        events: createRealtimeOutboxResponseEvents(events),
        failed: 0,
        ok: true,
      });
    }

    this.lastOutboxLagMs = emitOutboxLagMetric("catch_up", events);
    const result = await this.reEvaluateActiveRegistrations(
      createRealtimeAffectedTargets(events),
      "catch_up",
      database
    );
    // Advance after evaluation so an unexpected evaluation failure cannot
    // make this shard permanently skip events.
    await this.advanceLastProcessedOutboxSequence(
      events.at(-1)?.sequence,
      shardContext
    );
    await this.cleanupRealtimeOutbox();
    await this.maybeEvaluateAutoscaling();
    return jsonResponse({
      ...result,
      events: createRealtimeOutboxResponseEvents(events),
      ok: true,
      recoveredByFullReevaluation: false,
    });
  }

  private tryReserveNotifyEvent(
    eventId: string
  ): { readonly rejected: boolean; readonly response: Response } | null {
    if (this.pendingNotifyEventIds.has(eventId)) {
      emitRealtimeMetric(REALTIME_BACKPRESSURE_METRIC, 1, {
        result: "coalesced",
      });
      return {
        rejected: false,
        response: jsonResponse({ evaluated: 0, failed: 0, ok: true }),
      };
    }

    if (this.pendingNotifyEventIds.size >= REALTIME_PENDING_WORK_LIMIT) {
      emitRealtimeMetric(REALTIME_BACKPRESSURE_METRIC, 1, {
        result: "rejected",
      });
      return {
        rejected: true,
        response: jsonResponse(
          { evaluated: 0, failed: 0, ok: false },
          { status: 503 }
        ),
      };
    }

    this.pendingNotifyEventIds.add(eventId);
    return null;
  }

  private async advanceLastProcessedOutboxSequence(
    sequence: number | null | undefined,
    shardContext?: RealtimeShardContext
  ): Promise<void> {
    if (sequence == null) {
      return;
    }

    if (!shardContext) {
      throw new InternalRuntimeError(
        "Realtime shard cursor cannot advance without shard context"
      );
    }

    this.lastProcessedOutboxSequence = Math.max(
      this.lastProcessedOutboxSequence ?? 0,
      sequence
    );
    await recordRealtimeShardCursor(
      this.database,
      shardContext.shardName,
      shardContext.generationId,
      this.lastProcessedOutboxSequence
    );
  }

  private setCurrentShardName(
    shardName: string | undefined
  ): RealtimeShardContext {
    const currentShardName = shardName ?? getRealtimeSubscriptionShardName();
    this.shardName = currentShardName;
    this.shardGenerationId =
      getRealtimeShardGenerationIdFromName(currentShardName);
    return {
      generationId: this.shardGenerationId,
      shardName: this.shardName,
    };
  }

  private async maybeEvaluateAutoscaling(): Promise<void> {
    const now = Date.now();
    if (now - this.lastAutoscaleEvaluationAt < 60_000) {
      return;
    }

    this.lastAutoscaleEvaluationAt = now;
    try {
      await evaluateRealtimeAutoscaling(this.database, {
        now,
        pressure: this.createRealtimePressureSnapshot(),
      });
      this.deliveryBatchAttemptsSinceAutoscale = 0;
      this.deliveryBatchFailuresSinceAutoscale = 0;
    } catch (error) {
      logRuntimeEvent("error", "runtime.realtime_autoscaling_failed", {
        errorName: error instanceof Error ? error.name : typeof error,
      });
    }
  }

  private createRealtimePressureSnapshot(): RealtimePressureSnapshot {
    return {
      activeQueryCount: this.activeQueryStore.size(),
      activeRegistrationCount: this.registrationStore.size(),
      deliveryBatchLatencyMs: this.lastDeliveryBatchLatencyMs,
      failedDeliveryRate:
        this.deliveryBatchAttemptsSinceAutoscale === 0
          ? 0
          : this.deliveryBatchFailuresSinceAutoscale /
            this.deliveryBatchAttemptsSinceAutoscale,
      maxFanoutPerActiveQuery: this.activeQueryStore.maxFanout(),
      outboxLagMs: this.lastOutboxLagMs,
      pendingWorkCount:
        this.pendingNotifyEventIds.size + this.reEvaluatingRegistrations.size,
      reEvaluationLatencyMs: this.lastReEvaluationLatencyMs,
    };
  }

  private parseRegistration(
    body: Record<string, unknown>
  ): RealtimeRegistration {
    return {
      args: body.args ?? {},
      authorizationHeader:
        typeof body.authorizationHeader === "string"
          ? body.authorizationHeader
          : undefined,
      connectionKey: getStringField(body, "connectionKey"),
      connectionName: getStringField(body, "connectionName"),
      epoch: getEpoch(body.epoch),
      leaseExpiresAt:
        typeof body.leaseExpiresAt === "number"
          ? body.leaseExpiresAt
          : Date.now() + REALTIME_LEASE_MS,
      queryName: getStringField(body, "queryName"),
      runtimeId: getStringField(body, "runtimeId"),
      subscriptionId: getStringField(body, "subscriptionId"),
    };
  }

  private async reEvaluateActiveRegistrations(
    targets: RealtimeAffectedTargets,
    source: RealtimeMetricSource,
    database: RuntimeDatabase = this.database
  ): Promise<{
    readonly evaluated: number;
    readonly failed: number;
  }> {
    const startedAt = Date.now();
    let evaluated = 0;
    let failed = 0;
    let skipped = 0;
    const candidateActiveQueries: StoredRealtimeActiveQuery[] = [];
    const pendingDeliveries: PendingRealtimeDelivery[] = [];
    const activeQueryKeys = [...this.activeQueryStore.getRelevantKeys(targets)];
    skipped += Math.max(
      0,
      this.activeQueryStore.size() - activeQueryKeys.length
    );
    let nextActiveQueryIndex = 0;
    const prepareNextActiveQuery = async (): Promise<void> => {
      while (nextActiveQueryIndex < activeQueryKeys.length) {
        const activeQueryKey = activeQueryKeys[nextActiveQueryIndex];
        nextActiveQueryIndex += 1;
        if (activeQueryKey === undefined) {
          failed += 1;
          logRuntimeEvent(
            "error",
            "runtime.realtime_active_query_evaluation_failed",
            {
              errorName: "InternalRuntimeError",
              errorMessage:
                "Realtime active query key was missing during re-evaluation",
            }
          );
          continue;
        }

        const result = await this.prepareActiveQueryForEvaluation(
          activeQueryKey,
          targets,
          database
        );
        evaluated += result.evaluated;
        failed += result.failed;
        skipped += result.skipped;
        if (result.activeQuery) {
          candidateActiveQueries.push(result.activeQuery);
        }
      }
    };
    await Promise.all(
      Array.from(
        {
          length: Math.min(
            REALTIME_REEVALUATION_CONCURRENCY,
            activeQueryKeys.length
          ),
        },
        () => prepareNextActiveQuery()
      )
    );

    const evaluationResult = await this.evaluateActiveQueries(
      candidateActiveQueries,
      targets.sequence,
      database
    );
    evaluated += evaluationResult.evaluated;
    failed += evaluationResult.failed;
    pendingDeliveries.push(...evaluationResult.deliveries);

    const deliveryResult = await this.flushPendingDeliveries(pendingDeliveries);
    evaluated += deliveryResult.evaluated;
    failed += deliveryResult.failed;
    emitRealtimeMetric(REALTIME_RE_EVALUATIONS_METRIC, evaluated, {
      result: "evaluated",
      source,
    });
    emitRealtimeMetric(REALTIME_RE_EVALUATIONS_METRIC, failed, {
      result: "failed",
      source,
    });
    emitRealtimeMetric(REALTIME_RE_EVALUATIONS_METRIC, skipped, {
      result: "skipped",
      source,
    });
    this.lastReEvaluationLatencyMs = Date.now() - startedAt;
    emitRealtimeMetric(
      REALTIME_ACTIVE_QUERIES_METRIC,
      this.activeQueryStore.size(),
      {
        result: "active",
        source,
      }
    );
    emitRealtimeMetric(
      REALTIME_ACTIVE_QUERIES_METRIC,
      this.activeQueryStore.maxFanout(),
      {
        result: "fanout",
        source,
      }
    );

    return { evaluated, failed };
  }

  private async prepareActiveQueryForEvaluation(
    activeQueryKey: string,
    targets: RealtimeAffectedTargets,
    database: RuntimeDatabase
  ): Promise<{
    readonly evaluated: number;
    readonly failed: number;
    readonly activeQuery?: StoredRealtimeActiveQuery;
    readonly skipped: number;
  }> {
    const activeQuery = this.activeQueryStore.get(activeQueryKey);
    if (!activeQuery) {
      return { evaluated: 0, failed: 0, skipped: 0 };
    }

    if (this.isActiveQueryReEvaluationBackedOff(activeQuery)) {
      return { evaluated: 0, failed: 0, skipped: 1 };
    }

    if (this.reEvaluatingActiveQueries.has(activeQueryKey)) {
      return { evaluated: 0, failed: 0, skipped: 1 };
    }

    this.reEvaluatingActiveQueries.add(activeQueryKey);
    let shouldRelease = true;
    try {
      if (!(await this.hasRelevantVersionGap(activeQuery, targets, database))) {
        return { evaluated: 0, failed: 0, skipped: 1 };
      }

      shouldRelease = false;
      return { activeQuery, evaluated: 0, failed: 0, skipped: 0 };
    } catch {
      await this.recoverFailedActiveQueryEvaluation(activeQuery);
      return { evaluated: 0, failed: 1, skipped: 0 };
    } finally {
      if (shouldRelease) {
        this.reEvaluatingActiveQueries.delete(activeQueryKey);
      }
    }
  }

  private async evaluateActiveQueries(
    activeQueries: readonly StoredRealtimeActiveQuery[],
    sequence: number | null,
    database: RuntimeDatabase
  ): Promise<{
    readonly deliveries: PendingRealtimeDelivery[];
    readonly evaluated: number;
    readonly failed: number;
  }> {
    let nextGroupIndex = 0;
    const deliveries: PendingRealtimeDelivery[] = [];
    let evaluated = 0;
    let failed = 0;
    const evaluateNextGroup = async (): Promise<void> => {
      while (nextGroupIndex < activeQueries.length) {
        const activeQuery = activeQueries[nextGroupIndex];
        nextGroupIndex += 1;
        if (!activeQuery) {
          continue;
        }

        const result = await this.evaluateActiveQuery(
          activeQuery,
          sequence,
          database
        );
        evaluated += result.evaluated;
        failed += result.failed;
        deliveries.push(...result.deliveries);
        emitRealtimeMetric(REALTIME_ACTIVE_QUERIES_METRIC, 1, {
          result: result.failed > 0 ? "failed" : "evaluated",
        });
      }
    };

    await Promise.all(
      Array.from(
        {
          length: Math.min(
            REALTIME_REEVALUATION_CONCURRENCY,
            activeQueries.length
          ),
        },
        () => evaluateNextGroup()
      )
    );

    return { deliveries, evaluated, failed };
  }

  private async evaluateActiveQuery(
    activeQuery: StoredRealtimeActiveQuery,
    sequence: number | null,
    database: RuntimeDatabase
  ): Promise<{
    readonly deliveries: PendingRealtimeDelivery[];
    readonly evaluated: number;
    readonly failed: number;
  }> {
    const registrations = this.getActiveQueryRegistrations(activeQuery);
    if (registrations.length === 0) {
      this.releaseActiveQueryEvaluation(activeQuery);
      return { deliveries: [], evaluated: 0, failed: 0 };
    }

    try {
      const evaluation = await this.evaluateActiveQueryDefinition(
        activeQuery,
        database
      );
      await this.activeQueryStore.markEvaluated(
        activeQuery,
        evaluation.resultJson,
        evaluation.dependencies,
        evaluation.versionSnapshot
      );
      const result = await this.createActiveQueryDeliveries(
        activeQuery,
        registrations,
        sequence,
        evaluation
      );
      emitRealtimeMetric(
        REALTIME_QUERY_EXECUTIONS_SAVED_METRIC,
        Math.max(0, registrations.length - 1),
        { result: registrations.length > 1 ? "fanout" : "single" }
      );
      return result;
    } catch (error) {
      await this.recoverFailedActiveQueryEvaluation(activeQuery);
      await Promise.allSettled(
        registrations.map((registration) =>
          this.recoverFailedEvaluationRegistration(registration, error)
        )
      );
      this.releaseActiveQueryEvaluation(activeQuery);
      return {
        deliveries: [],
        evaluated: 0,
        failed: registrations.length,
      };
    }
  }

  private async createActiveQueryDeliveries(
    activeQuery: StoredRealtimeActiveQuery,
    registrations: readonly StoredRealtimeRegistration[],
    sequence: number | null,
    evaluation: RealtimeEvaluationResult
  ): Promise<{
    readonly deliveries: PendingRealtimeDelivery[];
    readonly evaluated: number;
    readonly failed: number;
  }> {
    const deliveries: PendingRealtimeDelivery[] = [];
    let evaluated = 0;
    let failed = 0;
    for (const registration of registrations) {
      const registrationKey = createRegistrationKey(
        registration.connectionKey,
        registration.subscriptionId
      );
      this.reEvaluatingRegistrations.add(registrationKey);
      try {
        if (evaluation.resultJson === registration.lastResultJson) {
          const unchanged =
            await this.handleUnchangedRegistration(registration);
          evaluated += unchanged.evaluated;
          failed += unchanged.failed;
          if (unchanged.active) {
            await this.updateRegistrationDependencies(
              registration,
              evaluation.dependencies,
              evaluation.versionSnapshot
            );
          }
          this.releaseRegistrationEvaluation(registration);
          continue;
        }

        deliveries.push(
          this.createPendingDelivery(registration, sequence, evaluation)
        );
      } catch (error) {
        await this.recoverFailedEvaluationRegistration(registration, error);
        this.releaseRegistrationEvaluation(registration);
        failed += 1;
      }
    }

    this.releaseActiveQueryEvaluation(activeQuery);
    return { deliveries, evaluated, failed };
  }

  private getActiveQueryRegistrations(
    activeQuery: StoredRealtimeActiveQuery
  ): StoredRealtimeRegistration[] {
    const registrations: StoredRealtimeRegistration[] = [];
    for (const registrationKey of activeQuery.memberRegistrationKeys) {
      const registration = this.registrationStore.get(registrationKey);
      if (!registration || registration.movePending) {
        continue;
      }

      if (this.isRegistrationReEvaluationBackedOff(registration)) {
        continue;
      }

      registrations.push(registration);
    }

    return registrations;
  }

  private async recoverFailedEvaluationRegistration(
    registration: StoredRealtimeRegistration,
    error: unknown
  ): Promise<void> {
    try {
      if (!(await this.registrationStore.deleteExpired(registration))) {
        await this.registrationStore.markBackedOff(registration);
      }
    } catch (recoveryError) {
      logRuntimeEvent(
        "error",
        "runtime.realtime_registration_state_update_failed",
        {
          connectionKey: registration.connectionKey,
          errorName:
            recoveryError instanceof Error
              ? recoveryError.name
              : typeof recoveryError,
          queryName: registration.queryName,
          subscriptionId: registration.subscriptionId,
        }
      );
    } finally {
      this.logReEvaluationFailure(registration, error);
    }
  }

  private async recoverFailedActiveQueryEvaluation(
    activeQuery: StoredRealtimeActiveQuery
  ): Promise<void> {
    try {
      await this.activeQueryStore.markBackedOff(activeQuery);
    } catch (recoveryError) {
      logRuntimeEvent(
        "error",
        "runtime.realtime_active_query_state_update_failed",
        {
          activeQueryKey: activeQuery.key,
          errorName:
            recoveryError instanceof Error
              ? recoveryError.name
              : typeof recoveryError,
          queryName: activeQuery.queryName,
        }
      );
    }
  }

  private releaseRegistrationEvaluation(
    registration: StoredRealtimeRegistration
  ): void {
    this.reEvaluatingRegistrations.delete(
      createRegistrationKey(
        registration.connectionKey,
        registration.subscriptionId
      )
    );
  }

  private releaseActiveQueryEvaluation(
    activeQuery: StoredRealtimeActiveQuery
  ): void {
    this.reEvaluatingActiveQueries.delete(activeQuery.key);
  }

  private isRegistrationReEvaluationBackedOff(
    registration: StoredRealtimeRegistration
  ): boolean {
    return (
      typeof registration.reEvaluationRetryAt === "number" &&
      registration.reEvaluationRetryAt > Date.now()
    );
  }

  private isActiveQueryReEvaluationBackedOff(
    activeQuery: StoredRealtimeActiveQuery
  ): boolean {
    return (
      typeof activeQuery.reEvaluationRetryAt === "number" &&
      activeQuery.reEvaluationRetryAt > Date.now()
    );
  }

  private async handleUnchangedRegistration(
    registration: StoredRealtimeRegistration
  ): Promise<{
    readonly active: boolean;
    readonly evaluated: number;
    readonly failed: number;
    readonly skipped: number;
  }> {
    if (registration.leaseExpiresAt > Date.now()) {
      await this.registrationStore.clearBackoff(registration);
      return { active: true, evaluated: 1, failed: 0, skipped: 0 };
    }

    let isConnected: boolean;
    try {
      isConnected = await this.hasActiveConnectionSockets(registration);
    } catch (error) {
      const deleted = await this.registrationStore.deleteExpired(registration);
      if (!deleted) {
        await this.registrationStore.markBackedOff(registration);
      }
      this.logReEvaluationFailure(registration, error);
      return { active: !deleted, evaluated: 0, failed: 1, skipped: 0 };
    }

    if (!isConnected) {
      await this.registrationStore.deleteExpired(registration);
      return { active: false, evaluated: 1, failed: 0, skipped: 0 };
    }

    try {
      await this.registrationStore.renewLease(
        registration,
        Date.now() + REALTIME_LEASE_MS
      );
      return { active: true, evaluated: 1, failed: 0, skipped: 0 };
    } catch (error) {
      await this.registrationStore.markBackedOff(registration);
      this.logReEvaluationFailure(registration, error);
      return { active: true, evaluated: 0, failed: 1, skipped: 0 };
    }
  }

  private async hasActiveConnectionSockets(
    registration: StoredRealtimeRegistration
  ): Promise<boolean> {
    const response = await this.env.REALTIME_CONNECTIONS.get(
      this.env.REALTIME_CONNECTIONS.idFromName(registration.connectionName)
    ).fetch("https://baseflare.internal/has-sockets", {
      body: JSON.stringify({ connectionKey: registration.connectionKey }),
      headers: JSON_HEADERS,
      method: "POST",
    });
    if (!response.ok) {
      throw new InternalRuntimeError(
        `Realtime socket liveness check failed with status ${response.status}`
      );
    }

    const result = (await response.json()) as { connected?: unknown };
    return result.connected === true;
  }

  private async flushPendingDeliveries(
    pendingDeliveries: readonly PendingRealtimeDelivery[]
  ): Promise<{
    readonly evaluated: number;
    readonly failed: number;
  }> {
    const deliveryGroups = this.createPendingDeliveryGroups(pendingDeliveries);
    const groupResults = await Promise.allSettled(
      deliveryGroups.map(async (group) => {
        let evaluated = 0;
        let failed = 0;
        try {
          for (const deliveries of chunkRealtimeDeliveries(group.deliveries)) {
            const result = await this.flushPendingDeliveryGroup({
              ...group,
              deliveries,
            });
            evaluated += result.evaluated;
            failed += result.failed;
          }
        } catch (error) {
          await Promise.allSettled(
            group.deliveries.map((delivery) =>
              this.recoverFailedDeliveryRegistration(delivery, error)
            )
          );
          failed = group.deliveries.length;
        }
        emitRealtimeMetric(REALTIME_DELIVERY_BATCHES_METRIC, 1, {
          result: getRealtimeDeliveryGroupMetricResult(evaluated, failed),
        });

        return { evaluated, failed };
      })
    );

    let evaluated = 0;
    let failed = 0;
    for (const result of groupResults) {
      if (result.status === "fulfilled") {
        evaluated += result.value.evaluated;
        failed += result.value.failed;
      }
    }

    return { evaluated, failed };
  }

  private createPendingDeliveryGroups(
    pendingDeliveries: readonly PendingRealtimeDelivery[]
  ): RealtimeDeliveryGroup[] {
    const deliveryGroups = new Map<string, RealtimeDeliveryGroup>();
    for (const delivery of pendingDeliveries) {
      const groupKey = JSON.stringify([
        delivery.registration.connectionName,
        delivery.registration.connectionKey,
      ]);
      const group = deliveryGroups.get(groupKey) ?? {
        connectionKey: delivery.registration.connectionKey,
        connectionName: delivery.registration.connectionName,
        deliveries: [],
      };
      group.deliveries.push(delivery);
      deliveryGroups.set(groupKey, group);
    }

    return [...deliveryGroups.values()];
  }

  private async flushPendingDeliveryGroup(
    group: RealtimeDeliveryGroup
  ): Promise<{
    readonly evaluated: number;
    readonly failed: number;
  }> {
    const startedAt = Date.now();
    this.deliveryBatchAttemptsSinceAutoscale += 1;
    try {
      let result: RealtimeDeliveryResult;
      try {
        result = await this.deliverPendingGroup(group);
      } catch (error) {
        this.deliveryBatchFailuresSinceAutoscale += 1;
        await Promise.allSettled(
          group.deliveries.map((delivery) =>
            this.recoverFailedDeliveryRegistration(delivery, error)
          )
        );
        this.lastDeliveryBatchLatencyMs = Date.now() - startedAt;
        return { evaluated: 0, failed: group.deliveries.length };
      }

      const groupSubscriptionIds = new Set(
        group.deliveries.map((delivery) => delivery.registration.subscriptionId)
      );
      const deliveredSubscriptions = new Set(
        result.deliveredSubscriptions.filter((subscriptionId) =>
          groupSubscriptionIds.has(subscriptionId)
        )
      );
      const leaseExpiresAt = Date.now() + REALTIME_LEASE_MS;
      const deliveredAll =
        deliveredSubscriptions.size === group.deliveries.length;
      const noTargetSockets = result.delivered === 0;
      if (!(deliveredAll || noTargetSockets)) {
        this.deliveryBatchFailuresSinceAutoscale += 1;
      }

      let evaluated = 0;
      let failed = 0;
      const stateUpdates: Promise<void>[] = [];
      for (const delivery of group.deliveries) {
        if (!deliveredSubscriptions.has(delivery.registration.subscriptionId)) {
          failed += 1;
          await this.handleUndeliveredRegistration(delivery);
          continue;
        }

        try {
          await this.registrationStore.markDelivered(
            delivery.registration,
            delivery.resultJson,
            leaseExpiresAt
          );
          evaluated += 1;
          stateUpdates.push(this.updateDeliveredRegistrationState(delivery));
        } catch (error) {
          failed += 1;
          await this.recoverFailedDeliveryRegistration(delivery, error);
          this.logRegistrationStateUpdateFailure(delivery.registration, error);
        }
      }
      await Promise.allSettled(stateUpdates);
      this.lastDeliveryBatchLatencyMs = Date.now() - startedAt;
      return { evaluated, failed };
    } finally {
      for (const delivery of group.deliveries) {
        this.reEvaluatingRegistrations.delete(
          createRegistrationKey(
            delivery.registration.connectionKey,
            delivery.registration.subscriptionId
          )
        );
      }
    }
  }

  private catchUpEventLimit(limit: unknown): number {
    if (typeof limit !== "number" || !Number.isFinite(limit)) {
      return REALTIME_CATCH_UP_EVENT_LIMIT;
    }

    return Math.max(
      1,
      Math.min(Math.trunc(limit), REALTIME_CATCH_UP_EVENT_LIMIT)
    );
  }

  private async handleUndeliveredRegistration(
    delivery: PendingRealtimeDelivery
  ): Promise<void> {
    try {
      const deleted = await this.registrationStore.deleteExpired(
        delivery.registration
      );
      if (!deleted) {
        await this.registrationStore.markBackedOff(delivery.registration);
      }
    } catch (error) {
      this.logRegistrationStateUpdateFailure(delivery.registration, error);
    }
  }

  private async recoverFailedDeliveryRegistration(
    delivery: PendingRealtimeDelivery,
    deliveryError: unknown
  ): Promise<void> {
    try {
      if (
        !(await this.registrationStore.deleteExpired(delivery.registration))
      ) {
        await this.registrationStore.markBackedOff(delivery.registration);
      }
    } catch (recoveryError) {
      logRuntimeEvent(
        "error",
        "runtime.realtime_registration_state_update_failed",
        {
          connectionKey: delivery.registration.connectionKey,
          errorName:
            recoveryError instanceof Error
              ? recoveryError.name
              : typeof recoveryError,
          queryName: delivery.registration.queryName,
          subscriptionId: delivery.registration.subscriptionId,
        }
      );
    } finally {
      this.logReEvaluationFailure(delivery.registration, deliveryError);
    }
  }

  private async updateDeliveredRegistrationState(
    delivery: PendingRealtimeDelivery
  ): Promise<void> {
    try {
      await this.updateRegistrationDependencies(
        delivery.registration,
        delivery.dependencies,
        delivery.versionSnapshot
      );
    } catch (error) {
      if (
        !(await this.registrationStore.deleteExpired(delivery.registration))
      ) {
        await this.registrationStore.markBackedOff(delivery.registration);
      }
      this.logRegistrationStateUpdateFailure(delivery.registration, error);
    }
  }

  private logRegistrationStateUpdateFailure(
    registration: StoredRealtimeRegistration,
    error: unknown
  ): void {
    logRuntimeEvent(
      "error",
      "runtime.realtime_registration_state_update_failed",
      {
        connectionKey: registration.connectionKey,
        errorName: error instanceof Error ? error.name : typeof error,
        queryName: registration.queryName,
        subscriptionId: registration.subscriptionId,
      }
    );
  }

  private async deliverPendingGroup(
    group: RealtimeDeliveryGroup
  ): Promise<RealtimeDeliveryResult> {
    const deliveryResponse = await this.env.REALTIME_CONNECTIONS.get(
      this.env.REALTIME_CONNECTIONS.idFromName(group.connectionName)
    ).fetch("https://baseflare.internal/deliver", {
      body: JSON.stringify({
        connectionKey: group.connectionKey,
        deliveries: group.deliveries.map((delivery) => delivery.message),
      }),
      headers: JSON_HEADERS,
      method: "POST",
    });
    if (!deliveryResponse.ok) {
      throw new InternalRuntimeError(
        `Realtime delivery failed with status ${deliveryResponse.status}`
      );
    }

    const deliveryResult = (await deliveryResponse.json()) as {
      delivered?: unknown;
      deliveredSubscriptions?: unknown;
    };
    const delivered =
      typeof deliveryResult.delivered === "number"
        ? deliveryResult.delivered
        : 0;
    const deliveredSubscriptions = Array.isArray(
      deliveryResult.deliveredSubscriptions
    )
      ? deliveryResult.deliveredSubscriptions.filter(
          (subscriptionId): subscriptionId is string =>
            typeof subscriptionId === "string"
        )
      : [];
    return {
      delivered,
      deliveredSubscriptions: delivered > 0 ? deliveredSubscriptions : [],
    };
  }

  private logReEvaluationFailure(
    registration: StoredRealtimeRegistration,
    error: unknown
  ): void {
    logRuntimeEvent(
      "error",
      "runtime.realtime_registration_re_evaluation_failed",
      {
        errorMessage: error instanceof Error ? error.message : undefined,
        errorName: error instanceof Error ? error.name : typeof error,
        queryName: registration.queryName,
        subscriptionId: registration.subscriptionId,
      }
    );
  }

  private async cleanupRealtimeOutbox(): Promise<boolean> {
    const now = Date.now();
    if (now - this.lastOutboxCleanupAt < REALTIME_OUTBOX_CLEANUP_INTERVAL_MS) {
      return true;
    }

    this.lastOutboxCleanupAt = now;
    try {
      const protectedSequence = await fetchOldestRealtimeShardCursor(
        this.database
      );
      const deleted = await deleteRealtimeOutboxEventsBefore(
        this.database,
        now - REALTIME_OUTBOX_RETENTION_MS,
        REALTIME_OUTBOX_CLEANUP_LIMIT,
        protectedSequence
      );
      emitRealtimeMetric(REALTIME_OUTBOX_CLEANUPS_METRIC, deleted, {
        result:
          deleted >= REALTIME_OUTBOX_CLEANUP_LIMIT ? "limited" : "cleaned",
      });
      return true;
    } catch (error) {
      logRuntimeEvent("error", "runtime.realtime_outbox_cleanup_failed", {
        errorName: error instanceof Error ? error.name : typeof error,
      });
      return false;
    }
  }

  private async scheduleOutboxCleanupAlarm(
    options: { readonly replace?: boolean } = {}
  ): Promise<void> {
    const storage = this.state.storage;
    if (!storage?.setAlarm) {
      return;
    }

    if (!options.replace) {
      const scheduledTime = await storage.getAlarm?.();
      if (scheduledTime !== null && scheduledTime !== undefined) {
        return;
      }
    }

    await storage.setAlarm(Date.now() + REALTIME_OUTBOX_CLEANUP_INTERVAL_MS);
  }

  private async shouldScheduleOutboxCleanupAlarm(): Promise<boolean> {
    if (this.registrationStore.size() > 0) {
      return true;
    }

    try {
      return await hasRealtimeOutboxEvents(this.database);
    } catch (error) {
      logRuntimeEvent("error", "runtime.realtime_outbox_cleanup_failed", {
        errorName: error instanceof Error ? error.name : typeof error,
      });
      return true;
    }
  }

  private async evaluateActiveQueryDefinition(
    activeQuery: StoredRealtimeActiveQuery,
    database: RuntimeDatabase
  ): Promise<RealtimeEvaluationResult> {
    const runtime = configuredRealtimeRuntimes.get(activeQuery.runtimeId);
    if (!runtime) {
      throw new InternalRuntimeError(
        "Baseflare runtime misconfiguration: realtime query runtime is not configured"
      );
    }

    const entry = runtime.functionIndex.getByName(
      "query",
      activeQuery.queryName,
      "public"
    );
    if (!entry) {
      throw new ValidationRuntimeError(
        `Realtime query "${activeQuery.queryName}" was not found`
      );
    }

    const headers = new Headers();
    if (activeQuery.authorizationHeader) {
      headers.set("authorization", activeQuery.authorizationHeader);
    }

    const { dependencies, readObserver } = createRealtimeDependencySet();
    const result = await executeQueryDefinition(
      entry.definition as QueryDefinition,
      {
        database,
        executionContext: {
          waitUntil() {
            // Realtime DO query execution does not schedule nested background work.
          },
        },
        functionIndex: runtime.functionIndex,
        readObserver,
        requestHeaders: headers,
        rules: runtime.rules,
        schema: runtime.schema,
      },
      activeQuery.args
    );
    const resultJson = JSON.stringify(result);
    const versionSnapshot = await fetchRealtimeVersionSnapshot(
      database,
      dependencies
    );

    return { dependencies, result, resultJson, versionSnapshot };
  }

  private createPendingDelivery(
    registration: StoredRealtimeRegistration,
    sequence: number | null,
    evaluation: RealtimeEvaluationResult
  ): PendingRealtimeDelivery {
    return {
      dependencies: evaluation.dependencies,
      message: {
        result: evaluation.result,
        sequence,
        subscriptionId: registration.subscriptionId,
      },
      registration,
      resultJson: evaluation.resultJson,
      versionSnapshot: evaluation.versionSnapshot,
    };
  }

  private async hasRelevantVersionGap(
    activeQuery: StoredRealtimeActiveQuery,
    targets: RealtimeAffectedTargets,
    database: RuntimeDatabase
  ): Promise<boolean> {
    if (
      targets.all ||
      !activeQuery.dependencies ||
      !activeQuery.versionSnapshot
    ) {
      return true;
    }
    if (
      activeQuery.dependencies.tables.size === 0 &&
      activeQuery.dependencies.partitions.size === 0
    ) {
      return true;
    }
    if (isZeroRealtimeVersionSnapshot(activeQuery.versionSnapshot)) {
      return true;
    }

    const relevant = this.getRelevantVersionGapDependencies(
      activeQuery.dependencies,
      targets
    );
    if (relevant.forceEvaluation) {
      return true;
    }

    const { dependencies: relevantDependencies } = relevant;
    if (
      relevantDependencies.tables.size === 0 &&
      relevantDependencies.partitions.size === 0
    ) {
      return false;
    }

    const currentSnapshot = await fetchRealtimeVersionSnapshot(
      database,
      relevantDependencies
    );
    for (const tableName of relevantDependencies.tables) {
      if (
        currentSnapshot.tables.get(tableName) !==
        activeQuery.versionSnapshot.tables.get(tableName)
      ) {
        return true;
      }
    }

    for (const partitionId of relevantDependencies.partitions) {
      if (
        currentSnapshot.partitions.get(partitionId) !==
        activeQuery.versionSnapshot.partitions.get(partitionId)
      ) {
        return true;
      }
    }

    return false;
  }

  private getRelevantVersionGapDependencies(
    registrationDependencies: RealtimeDependencySet,
    targets: RealtimeAffectedTargets
  ): {
    readonly dependencies: {
      readonly partitions: Set<string>;
      readonly tables: Set<string>;
    };
    readonly forceEvaluation: boolean;
  } {
    const dependencies = {
      partitions: new Set<string>(),
      tables: new Set<string>(),
    };
    for (const tableName of registrationDependencies.tables) {
      if (!targets.tables.has(tableName)) {
        continue;
      }

      dependencies.tables.add(tableName);
    }

    for (const partitionId of registrationDependencies.partitions) {
      const partition = parseRealtimePartitionId(partitionId);
      if (!partition) {
        return { dependencies, forceEvaluation: true };
      }

      if (targets.broadTables.has(partition.tableName)) {
        return { dependencies, forceEvaluation: true };
      }

      if (!targets.partitions.has(partitionId)) {
        continue;
      }

      dependencies.partitions.add(partitionId);
    }

    return { dependencies, forceEvaluation: false };
  }

  private async updateRegistrationDependencies(
    registration: StoredRealtimeRegistration,
    dependencies: RealtimeDependencySet,
    versionSnapshot: RealtimeVersionSnapshot
  ): Promise<void> {
    const registrationKey = createRegistrationKey(
      registration.connectionKey,
      registration.subscriptionId
    );
    const nextRegistration = {
      ...registration,
      dependencies,
      versionSnapshot,
    };
    const targetShardName =
      await this.resolveActiveHomeShardName(nextRegistration);
    if (targetShardName && targetShardName !== this.shardName) {
      await this.migrateRegistrationToHomeShard(
        nextRegistration,
        targetShardName
      );
      return;
    }

    await this.registrationStore.updateSameShardDependencies(
      registrationKey,
      registration,
      dependencies,
      versionSnapshot
    );
  }

  private async resolveActiveHomeShardName(
    registration: StoredRealtimeRegistration
  ): Promise<string | null> {
    const activeGeneration = await fetchActiveRealtimeShardGeneration(
      this.database
    );
    if (activeGeneration.generationId !== this.shardGenerationId) {
      return null;
    }

    return getRealtimeSubscriptionShardName(
      getRealtimeRegistrationHomeRouteTarget(registration.dependencies),
      activeGeneration
    );
  }

  private async migrateRegistrationToHomeShard(
    registration: StoredRealtimeRegistration,
    targetShardName: string
  ): Promise<void> {
    const registrationKey = createRegistrationKey(
      registration.connectionKey,
      registration.subscriptionId
    );
    const targetStub = this.env.REALTIME_SUBSCRIPTIONS.get(
      this.env.REALTIME_SUBSCRIPTIONS.idFromName(targetShardName)
    );
    const adoptionResponse = await targetStub.fetch(
      "https://baseflare.internal/adopt-registration",
      {
        body: JSON.stringify({
          ...registration,
          dependencies: registration.dependencies
            ? serializeRealtimeDependencySet(registration.dependencies)
            : undefined,
          shardName: targetShardName,
          versionSnapshot: registration.versionSnapshot
            ? serializeRealtimeVersionSnapshot(registration.versionSnapshot)
            : undefined,
        }),
        headers: JSON_HEADERS,
        method: "POST",
      }
    );
    if (!adoptionResponse.ok) {
      logRuntimeEvent(
        "error",
        "runtime.realtime_registration_adoption_failed",
        {
          shardName: targetShardName,
          status: adoptionResponse.status,
          subscriptionId: registration.subscriptionId,
        }
      );
      throw new InternalRuntimeError(
        `Realtime registration adoption failed with status ${adoptionResponse.status}`
      );
    }

    let connectionUpdateResponse: Response;
    try {
      connectionUpdateResponse = await this.moveConnectionRegistrationShard(
        registration,
        targetShardName
      );
    } catch (error) {
      await this.rollbackAdoptedRegistration(
        targetStub,
        targetShardName,
        registration
      );
      logRuntimeEvent("error", "runtime.realtime_registration_move_failed", {
        connectionKey: registration.connectionKey,
        errorName: error instanceof Error ? error.name : typeof error,
        shardName: targetShardName,
        sourceRemoved: false,
        subscriptionId: registration.subscriptionId,
      });
      throw new InternalRuntimeError(
        `Realtime registration move failed: ${
          error instanceof Error ? error.message : "unknown error"
        }`
      );
    }
    if (!connectionUpdateResponse.ok) {
      await this.rollbackAdoptedRegistration(
        targetStub,
        targetShardName,
        registration
      );
      logRuntimeEvent("error", "runtime.realtime_registration_move_failed", {
        connectionKey: registration.connectionKey,
        shardName: targetShardName,
        sourceRemoved: false,
        status: connectionUpdateResponse.status,
        subscriptionId: registration.subscriptionId,
      });
      throw new InternalRuntimeError(
        `Realtime registration move failed with status ${connectionUpdateResponse.status}`
      );
    }

    try {
      await this.registrationStore.delete(registrationKey);
    } catch (error) {
      const sourceOwnerRestored =
        await this.restoreSourceRegistrationOwner(registration);
      const targetRollbackSucceeded = await this.rollbackAdoptedRegistration(
        targetStub,
        targetShardName,
        registration
      );
      logRuntimeEvent("error", "runtime.realtime_registration_move_failed", {
        connectionKey: registration.connectionKey,
        errorName: error instanceof Error ? error.name : typeof error,
        shardName: targetShardName,
        sourceRemoved: false,
        sourceOwnerRestored,
        subscriptionId: registration.subscriptionId,
        targetRollbackSucceeded,
      });
      throw new InternalRuntimeError(
        `Realtime registration source delete failed: ${
          error instanceof Error ? error.message : "unknown error"
        }`
      );
    }

    let activationResponse: Response;
    try {
      activationResponse = await targetStub.fetch(
        "https://baseflare.internal/activate-registration",
        {
          body: JSON.stringify({
            connectionKey: registration.connectionKey,
            shardName: targetShardName,
            subscriptionId: registration.subscriptionId,
          }),
          headers: JSON_HEADERS,
          method: "POST",
        }
      );
    } catch (error) {
      const targetRollbackSucceeded = await this.rollbackAdoptedRegistration(
        targetStub,
        targetShardName,
        registration
      );
      const sourceOwnerRestored =
        await this.restoreSourceRegistrationOwner(registration);
      await this.registrationStore.upsert(registrationKey, registration);
      logRuntimeEvent(
        "error",
        "runtime.realtime_registration_activation_failed",
        {
          connectionKey: registration.connectionKey,
          errorName: error instanceof Error ? error.name : typeof error,
          shardName: targetShardName,
          sourceOwnerRestored,
          subscriptionId: registration.subscriptionId,
          targetRollbackSucceeded,
        }
      );
      throw new InternalRuntimeError(
        `Realtime registration activation failed: ${
          error instanceof Error ? error.message : "unknown error"
        }`
      );
    }
    if (!activationResponse.ok) {
      const targetRollbackSucceeded = await this.rollbackAdoptedRegistration(
        targetStub,
        targetShardName,
        registration
      );
      const sourceOwnerRestored =
        await this.restoreSourceRegistrationOwner(registration);
      await this.registrationStore.upsert(registrationKey, registration);
      logRuntimeEvent(
        "error",
        "runtime.realtime_registration_activation_failed",
        {
          connectionKey: registration.connectionKey,
          shardName: targetShardName,
          sourceOwnerRestored,
          status: activationResponse.status,
          subscriptionId: registration.subscriptionId,
          targetRollbackSucceeded,
        }
      );
      throw new InternalRuntimeError(
        `Realtime registration activation failed with status ${activationResponse.status}`
      );
    }
  }

  private async moveConnectionRegistrationShard(
    registration: StoredRealtimeRegistration,
    subscriptionShardName: string
  ): Promise<Response> {
    return await this.env.REALTIME_CONNECTIONS.get(
      this.env.REALTIME_CONNECTIONS.idFromName(registration.connectionName)
    ).fetch("https://baseflare.internal/subscription-moved", {
      body: JSON.stringify({
        connectionKey: registration.connectionKey,
        subscriptionId: registration.subscriptionId,
        subscriptionShardName,
      }),
      headers: JSON_HEADERS,
      method: "POST",
    });
  }

  private async restoreSourceRegistrationOwner(
    registration: StoredRealtimeRegistration
  ): Promise<boolean> {
    try {
      const response = await this.moveConnectionRegistrationShard(
        registration,
        this.shardName
      );
      if (!response.ok) {
        logRuntimeEvent("error", "runtime.realtime_registration_move_failed", {
          connectionKey: registration.connectionKey,
          shardName: this.shardName,
          sourceRemoved: false,
          status: response.status,
          subscriptionId: registration.subscriptionId,
        });
        return false;
      }
      return true;
    } catch (error) {
      logRuntimeEvent("error", "runtime.realtime_registration_move_failed", {
        connectionKey: registration.connectionKey,
        errorName: error instanceof Error ? error.name : typeof error,
        shardName: this.shardName,
        sourceRemoved: false,
        subscriptionId: registration.subscriptionId,
      });
      return false;
    }
  }

  private async rollbackAdoptedRegistration(
    targetStub: DurableObjectStub,
    targetShardName: string,
    registration: StoredRealtimeRegistration
  ): Promise<boolean> {
    try {
      const response = await targetStub.fetch(
        "https://baseflare.internal/unregister",
        {
          body: JSON.stringify({
            connectionKey: registration.connectionKey,
            subscriptionId: registration.subscriptionId,
          }),
          headers: JSON_HEADERS,
          method: "POST",
        }
      );
      if (!response.ok) {
        this.logRegistrationMoveCleanupFailure(registration, targetShardName, {
          sourceRemoved: false,
          status: response.status,
        });
        return false;
      }
      return true;
    } catch (error) {
      this.logRegistrationMoveCleanupFailure(registration, targetShardName, {
        errorName: error instanceof Error ? error.name : typeof error,
        sourceRemoved: false,
      });
      return false;
    }
  }

  private logRegistrationMoveCleanupFailure(
    registration: StoredRealtimeRegistration,
    targetShardName: string,
    detail: {
      readonly errorName?: string;
      readonly sourceRemoved?: boolean;
      readonly status?: number;
    }
  ): void {
    logRuntimeEvent(
      "error",
      "runtime.realtime_registration_move_cleanup_failed",
      {
        connectionKey: registration.connectionKey,
        errorName: detail.errorName,
        shardName: targetShardName,
        sourceRemoved: detail.sourceRemoved,
        status: detail.status,
        subscriptionId: registration.subscriptionId,
      }
    );
  }
}

function waitForRealtimeNotifyEventLookupRetry(): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, REALTIME_NOTIFY_EVENT_LOOKUP_RETRY_DELAY_MS);
  });
}

function getRealtimeDeliveryGroupMetricResult(
  evaluated: number,
  failed: number
): "delivered" | "partial" | "undelivered" {
  if (failed === 0) {
    return "delivered";
  }

  return evaluated === 0 ? "undelivered" : "partial";
}
