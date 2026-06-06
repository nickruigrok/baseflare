import type { QueryDefinition } from "../../functions/types";
import {
  PARTITION_VERSION_TABLE_NAME,
  TABLE_VERSION_TABLE_NAME,
} from "../../schema/types";
import { bindStatement } from "../d1";
import { InternalRuntimeError, ValidationRuntimeError } from "../errors";
import { executeQueryDefinition } from "../execution";
import { logRuntimeEvent } from "../logging";
import type { D1Database, DurableObjectStub, RuntimeDatabase } from "../types";
import {
  createRealtimeOutboxResponseEvents,
  deleteRealtimeOutboxEventsBefore,
  fetchLatestRealtimeOutboxSequence,
  fetchOldestRealtimeOutboxSequence,
  fetchRealtimeOutboxEventById,
  fetchRealtimeOutboxEventRowExists,
  fetchRealtimeOutboxEvents,
} from "./outbox";
import {
  createFullRealtimeAffectedTargets,
  createRealtimeAffectedTargets,
  createRealtimeDependencySet,
  createRegistrationKey,
  getPartitionDependencyTable,
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
  RealtimeMetricSource,
  RealtimeObjectEnv,
  RealtimePartitionTarget,
  RealtimePressureSnapshot,
  RealtimeRegistration,
  RealtimeSequencedOutboxEvent,
  RealtimeVersionSnapshot,
  StoredRealtimeRegistration,
} from "./types";
import {
  DEFAULT_REALTIME_SHARD_GENERATION,
  JSON_HEADERS,
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
  REALTIME_RE_EVALUATIONS_METRIC,
  REALTIME_REEVALUATION_CONCURRENCY,
  REALTIME_REEVALUATION_FAILURE_RETRY_MS,
} from "./types";

export class RealtimeSubscriptionDO {
  private readonly database: D1Database;
  private readonly env: RealtimeObjectEnv;
  private readonly registrations = new Map<
    string,
    StoredRealtimeRegistration
  >();
  private readonly registrationKeysByPartition = new Map<string, Set<string>>();
  private readonly registrationKeysByTable = new Map<string, Set<string>>();
  private readonly registrationKeysWithoutDependencies = new Set<string>();
  private readonly pendingNotifyEventIds = new Set<string>();
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

  constructor(_state: unknown, env: RealtimeObjectEnv) {
    this.database = env.APP_DB;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
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
        registrations: this.getStoredRegistrations(),
        shardName: this.shardName,
      });
    }

    return new Response("Not found", { status: 404 });
  }

  private async handleRegister(request: Request): Promise<Response> {
    const body = await readJsonObject(request);
    this.setCurrentShardName(getOptionalStringField(body, "shardName"));
    const registration = this.parseRegistration(body);
    const registrationKey = createRegistrationKey(
      registration.connectionKey,
      registration.subscriptionId
    );
    const existing = this.registrations.get(registrationKey);
    if (!existing || registration.epoch >= existing.epoch) {
      if (existing) {
        this.removeRegistrationFromIndexes(registrationKey, existing);
      }
      this.registrations.set(registrationKey, registration);
      this.registrationKeysWithoutDependencies.add(registrationKey);
    }
    await this.maybeEvaluateAutoscaling();
    return jsonResponse({ ok: true });
  }

  private async handleAdoptRegistration(request: Request): Promise<Response> {
    const body = await readJsonObject(request);
    this.setCurrentShardName(getOptionalStringField(body, "shardName"));
    const registration = this.parseRegistration(body);
    const storedRegistration: StoredRealtimeRegistration = {
      ...registration,
      dependencies: parseRealtimeDependencySetValue(body.dependencies),
      lastResultJson:
        typeof body.lastResultJson === "string"
          ? body.lastResultJson
          : undefined,
      versionSnapshot: parseRealtimeVersionSnapshotValue(body.versionSnapshot),
    };
    const registrationKey = createRegistrationKey(
      storedRegistration.connectionKey,
      storedRegistration.subscriptionId
    );
    const existing = this.registrations.get(registrationKey);
    if (existing && storedRegistration.epoch < existing.epoch) {
      return jsonResponse({ ok: true });
    }

    if (existing) {
      this.removeRegistrationFromIndexes(registrationKey, existing);
    }
    this.registrations.set(registrationKey, storedRegistration);
    this.addRegistrationToIndexes(registrationKey, storedRegistration);
    await this.maybeEvaluateAutoscaling();
    return jsonResponse({ ok: true });
  }

  private async handleUnregister(request: Request): Promise<Response> {
    const body = await readJsonObject(request);
    this.deleteRegistrationByKey(
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
    this.setCurrentShardName(getOptionalStringField(body, "shardName"));
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
        return jsonResponse({ evaluated: 0, failed: 0, ok: true });
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
      await this.advanceLastProcessedOutboxSequence(event.sequence);
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
    | { readonly status: "malformed" | "missing" }
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

      if (await fetchRealtimeOutboxEventRowExists(database, eventId)) {
        return { status: "malformed" };
      }

      if (attempt < REALTIME_NOTIFY_EVENT_LOOKUP_ATTEMPTS) {
        await waitForRealtimeNotifyEventLookupRetry();
      }
    }

    return { status: "missing" };
  }

  private async handleCatchUp(request: Request): Promise<Response> {
    const body = await readJsonObject(request);
    this.setCurrentShardName(getOptionalStringField(body, "shardName"));
    const afterSequence = getOptionalSequence(
      body.afterSequence,
      "afterSequence"
    );
    const recoveredByFullReevaluation =
      await this.hasRealtimeOutboxHistoryGap(afterSequence);
    if (recoveredByFullReevaluation) {
      const latestSequence = await fetchLatestRealtimeOutboxSequence(
        this.database
      );
      const result = await this.reEvaluateActiveRegistrations(
        createFullRealtimeAffectedTargets(latestSequence),
        "catch_up"
      );
      await this.advanceLastProcessedOutboxSequence(latestSequence);
      await this.cleanupRealtimeOutbox();
      await this.maybeEvaluateAutoscaling();
      return jsonResponse({
        ...result,
        events: [],
        ok: true,
        recoveredByFullReevaluation,
      });
    }

    const catchUp = await fetchRealtimeOutboxEvents(
      this.database,
      afterSequence,
      typeof body.limit === "number"
        ? body.limit
        : REALTIME_CATCH_UP_EVENT_LIMIT
    );
    if (catchUp.hasMalformedEvents) {
      const result = await this.reEvaluateActiveRegistrations(
        createFullRealtimeAffectedTargets(catchUp.latestReadSequence),
        "catch_up"
      );
      await this.advanceLastProcessedOutboxSequence(catchUp.latestReadSequence);
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
      await this.advanceLastProcessedOutboxSequence(afterSequence);
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
      "catch_up"
    );
    // Advance after evaluation so an unexpected evaluation failure cannot
    // make this shard permanently skip events.
    await this.advanceLastProcessedOutboxSequence(events.at(-1)?.sequence);
    await this.cleanupRealtimeOutbox();
    await this.maybeEvaluateAutoscaling();
    return jsonResponse({
      ...result,
      events: createRealtimeOutboxResponseEvents(events),
      ok: true,
      recoveredByFullReevaluation,
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
        response: jsonResponse({ evaluated: 0, failed: 0, ok: true }),
      };
    }

    this.pendingNotifyEventIds.add(eventId);
    return null;
  }

  private async advanceLastProcessedOutboxSequence(
    sequence: number | null | undefined
  ): Promise<void> {
    if (sequence == null) {
      return;
    }

    this.lastProcessedOutboxSequence = Math.max(
      this.lastProcessedOutboxSequence ?? 0,
      sequence
    );
    await recordRealtimeShardCursor(
      this.database,
      this.shardName,
      this.shardGenerationId,
      this.lastProcessedOutboxSequence
    );
  }

  private setCurrentShardName(shardName: string | undefined): void {
    if (!shardName) {
      return;
    }

    this.shardName = shardName;
    this.shardGenerationId = getRealtimeShardGenerationIdFromName(shardName);
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
      activeRegistrationCount: this.registrations.size,
      deliveryBatchLatencyMs: this.lastDeliveryBatchLatencyMs,
      failedDeliveryRate:
        this.deliveryBatchAttemptsSinceAutoscale === 0
          ? 0
          : this.deliveryBatchFailuresSinceAutoscale /
            this.deliveryBatchAttemptsSinceAutoscale,
      outboxLagMs: this.lastOutboxLagMs,
      pendingWorkCount:
        this.pendingNotifyEventIds.size + this.reEvaluatingRegistrations.size,
      reEvaluationLatencyMs: this.lastReEvaluationLatencyMs,
    };
  }

  private async hasRealtimeOutboxHistoryGap(
    afterSequence: number | null
  ): Promise<boolean> {
    if (afterSequence === null) {
      return false;
    }

    const oldestSequence = await fetchOldestRealtimeOutboxSequence(
      this.database
    );
    return oldestSequence !== null && afterSequence + 1 < oldestSequence;
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

  private getStoredRegistrations(): StoredRealtimeRegistration[] {
    return Array.from(this.registrations.values());
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
    const pendingDeliveries: PendingRealtimeDelivery[] = [];
    const registrationKeys = [...this.getRelevantRegistrationKeys(targets)];
    skipped += Math.max(0, this.registrations.size - registrationKeys.length);
    let nextRegistrationIndex = 0;
    const evaluateNextRegistration = async (): Promise<void> => {
      while (nextRegistrationIndex < registrationKeys.length) {
        const registrationKey = registrationKeys[nextRegistrationIndex];
        nextRegistrationIndex += 1;
        const result = await this.tryEvaluateRegistration(
          registrationKey,
          targets,
          database
        );
        evaluated += result.evaluated;
        failed += result.failed;
        skipped += result.skipped;
        if (result.delivery) {
          pendingDeliveries.push(result.delivery);
        }
      }
    };
    await Promise.all(
      Array.from(
        {
          length: Math.min(
            REALTIME_REEVALUATION_CONCURRENCY,
            registrationKeys.length
          ),
        },
        () => evaluateNextRegistration()
      )
    );

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

    return { evaluated, failed };
  }

  private async tryEvaluateRegistration(
    registrationKey: string | undefined,
    targets: RealtimeAffectedTargets,
    database: RuntimeDatabase
  ): Promise<{
    readonly delivery?: PendingRealtimeDelivery;
    readonly evaluated: number;
    readonly failed: number;
    readonly skipped: number;
  }> {
    if (!registrationKey) {
      return { evaluated: 0, failed: 0, skipped: 0 };
    }

    const registration = this.registrations.get(registrationKey);
    if (!registration) {
      return { evaluated: 0, failed: 0, skipped: 0 };
    }

    if (this.isRegistrationReEvaluationBackedOff(registration)) {
      return { evaluated: 0, failed: 0, skipped: 1 };
    }

    if (this.reEvaluatingRegistrations.has(registrationKey)) {
      return { evaluated: 0, failed: 0, skipped: 1 };
    }

    this.reEvaluatingRegistrations.add(registrationKey);
    let shouldRelease = true;
    try {
      if (
        !(await this.hasRelevantVersionGap(registration, targets, database))
      ) {
        return { evaluated: 0, failed: 0, skipped: 1 };
      }

      const delivery = await this.evaluateRegistration(
        registration,
        targets.sequence,
        database
      );
      if (!delivery) {
        this.clearRegistrationReEvaluationBackoff(registration);
        this.deleteExpiredRegistration(registration);
        return { evaluated: 1, failed: 0, skipped: 0 };
      }

      shouldRelease = false;
      return { delivery, evaluated: 0, failed: 0, skipped: 0 };
    } catch (error) {
      if (!this.deleteExpiredRegistration(registration)) {
        this.backOffRegistrationReEvaluation(registration);
      }
      this.logReEvaluationFailure(registration, error);
      return { evaluated: 0, failed: 1, skipped: 0 };
    } finally {
      if (shouldRelease) {
        this.reEvaluatingRegistrations.delete(registrationKey);
      }
    }
  }

  private isRegistrationReEvaluationBackedOff(
    registration: StoredRealtimeRegistration
  ): boolean {
    return (
      typeof registration.reEvaluationRetryAt === "number" &&
      registration.reEvaluationRetryAt > Date.now()
    );
  }

  private backOffRegistrationReEvaluation(
    registration: StoredRealtimeRegistration
  ): void {
    registration.reEvaluationRetryAt =
      Date.now() + REALTIME_REEVALUATION_FAILURE_RETRY_MS;
  }

  private clearRegistrationReEvaluationBackoff(
    registration: StoredRealtimeRegistration
  ): void {
    registration.reEvaluationRetryAt = undefined;
  }

  private async flushPendingDeliveries(
    pendingDeliveries: readonly PendingRealtimeDelivery[]
  ): Promise<{
    readonly evaluated: number;
    readonly failed: number;
  }> {
    const groupResults = await Promise.all(
      this.createPendingDeliveryGroups(pendingDeliveries).map(async (group) => {
        let evaluated = 0;
        let failed = 0;
        for (const deliveries of chunkRealtimeDeliveries(group.deliveries)) {
          const result = await this.flushPendingDeliveryGroup({
            ...group,
            deliveries,
          });
          evaluated += result.evaluated;
          failed += result.failed;
        }
        emitRealtimeMetric(REALTIME_DELIVERY_BATCHES_METRIC, 1, {
          result: failed === 0 ? "delivered" : "undelivered",
        });

        return { evaluated, failed };
      })
    );

    let evaluated = 0;
    let failed = 0;
    for (const result of groupResults) {
      evaluated += result.evaluated;
      failed += result.failed;
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
      const result = await this.deliverPendingGroup(group);
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
      const stateUpdates: Promise<void>[] = [];
      for (const delivery of group.deliveries) {
        if (!deliveredSubscriptions.has(delivery.registration.subscriptionId)) {
          if (!this.deleteExpiredRegistration(delivery.registration)) {
            this.backOffRegistrationReEvaluation(delivery.registration);
          }
          continue;
        }

        delivery.registration.lastResultJson = delivery.resultJson;
        delivery.registration.leaseExpiresAt = leaseExpiresAt;
        this.clearRegistrationReEvaluationBackoff(delivery.registration);
        stateUpdates.push(this.updateDeliveredRegistrationState(delivery));
      }
      await Promise.allSettled(stateUpdates);
      this.lastDeliveryBatchLatencyMs = Date.now() - startedAt;
      return {
        evaluated: deliveredSubscriptions.size,
        failed: group.deliveries.length - deliveredSubscriptions.size,
      };
    } catch (error) {
      this.deliveryBatchFailuresSinceAutoscale += 1;
      for (const delivery of group.deliveries) {
        if (!this.deleteExpiredRegistration(delivery.registration)) {
          this.backOffRegistrationReEvaluation(delivery.registration);
        }
        this.logReEvaluationFailure(delivery.registration, error);
      }
      this.lastDeliveryBatchLatencyMs = Date.now() - startedAt;
      return { evaluated: 0, failed: group.deliveries.length };
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
      if (!this.deleteExpiredRegistration(delivery.registration)) {
        this.backOffRegistrationReEvaluation(delivery.registration);
      }
      logRuntimeEvent(
        "error",
        "runtime.realtime_registration_state_update_failed",
        {
          connectionKey: delivery.registration.connectionKey,
          errorName: error instanceof Error ? error.name : typeof error,
          queryName: delivery.registration.queryName,
          subscriptionId: delivery.registration.subscriptionId,
        }
      );
    }
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

  private async cleanupRealtimeOutbox(): Promise<void> {
    const now = Date.now();
    if (now - this.lastOutboxCleanupAt < REALTIME_OUTBOX_CLEANUP_INTERVAL_MS) {
      return;
    }

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
      this.lastOutboxCleanupAt = now;
    } catch (error) {
      logRuntimeEvent("error", "runtime.realtime_outbox_cleanup_failed", {
        errorName: error instanceof Error ? error.name : typeof error,
      });
    }
  }

  private async evaluateRegistration(
    registration: StoredRealtimeRegistration,
    sequence: number | null,
    database: RuntimeDatabase
  ): Promise<PendingRealtimeDelivery | null> {
    const runtime = configuredRealtimeRuntimes.get(registration.runtimeId);
    if (!runtime) {
      throw new InternalRuntimeError(
        "Baseflare runtime misconfiguration: realtime query runtime is not configured"
      );
    }

    const entry = runtime.functionIndex.getByName(
      "query",
      registration.queryName,
      "public"
    );
    if (!entry) {
      throw new ValidationRuntimeError(
        `Realtime query "${registration.queryName}" was not found`
      );
    }

    const headers = new Headers();
    if (registration.authorizationHeader) {
      headers.set("authorization", registration.authorizationHeader);
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
      registration.args
    );
    const resultJson = JSON.stringify(result);
    const versionSnapshot = await fetchRealtimeVersionSnapshot(
      database,
      dependencies
    );
    if (resultJson === registration.lastResultJson) {
      await this.updateRegistrationDependencies(
        registration,
        dependencies,
        versionSnapshot
      );
      return null;
    }

    return {
      dependencies,
      message: {
        result,
        sequence,
        subscriptionId: registration.subscriptionId,
      },
      registration,
      resultJson,
      versionSnapshot,
    };
  }

  private async hasRelevantVersionGap(
    registration: StoredRealtimeRegistration,
    targets: RealtimeAffectedTargets,
    database: RuntimeDatabase
  ): Promise<boolean> {
    if (
      targets.all ||
      !registration.dependencies ||
      !registration.versionSnapshot
    ) {
      return true;
    }
    if (
      registration.dependencies.tables.size === 0 &&
      registration.dependencies.partitions.size === 0
    ) {
      return true;
    }
    if (isZeroRealtimeVersionSnapshot(registration.versionSnapshot)) {
      return true;
    }

    for (const tableName of registration.dependencies.tables) {
      if (!targets.tables.has(tableName)) {
        continue;
      }

      const currentVersion = await this.fetchTableVersion(database, tableName);
      if (
        currentVersion !== registration.versionSnapshot.tables.get(tableName)
      ) {
        return true;
      }
    }

    for (const partitionId of registration.dependencies.partitions) {
      const partition = parseRealtimePartitionId(partitionId);
      if (!partition) {
        return true;
      }

      if (targets.broadTables.has(partition.tableName)) {
        return true;
      }

      if (!targets.partitions.has(partitionId)) {
        continue;
      }

      const currentVersion = await this.fetchPartitionVersion(
        database,
        partition
      );
      if (
        currentVersion !==
        registration.versionSnapshot.partitions.get(partitionId)
      ) {
        return true;
      }
    }

    return false;
  }

  private async fetchTableVersion(
    database: RuntimeDatabase,
    tableName: string
  ): Promise<number | null> {
    const row = await bindStatement(
      database,
      `SELECT version FROM ${TABLE_VERSION_TABLE_NAME}
       WHERE table_name = ?
       LIMIT 1`,
      [tableName]
    ).first<{ version: number }>();

    return typeof row?.version === "number" ? row.version : null;
  }

  private async fetchPartitionVersion(
    database: RuntimeDatabase,
    partition: RealtimePartitionTarget
  ): Promise<number> {
    const row = await bindStatement(
      database,
      `SELECT version FROM ${PARTITION_VERSION_TABLE_NAME}
       WHERE table_name = ? AND partition_key = ? AND partition_value = ?
       LIMIT 1`,
      [partition.tableName, partition.partitionKey, partition.partitionValue]
    ).first<{ version: number }>();

    return row?.version ?? 0;
  }

  private deleteExpiredRegistration(
    registration: StoredRealtimeRegistration
  ): boolean {
    if (registration.leaseExpiresAt > Date.now()) {
      return false;
    }

    this.deleteRegistrationByKey(
      createRegistrationKey(
        registration.connectionKey,
        registration.subscriptionId
      )
    );
    return true;
  }

  private getRelevantRegistrationKeys(
    targets: RealtimeAffectedTargets
  ): Set<string> {
    if (targets.all) {
      return new Set(this.registrations.keys());
    }

    const registrationKeys = new Set(this.registrationKeysWithoutDependencies);

    for (const tableName of targets.tables) {
      this.addIndexedRegistrationKeys(
        registrationKeys,
        this.registrationKeysByTable.get(tableName)
      );
    }

    for (const partitionId of targets.partitions) {
      this.addIndexedRegistrationKeys(
        registrationKeys,
        this.registrationKeysByPartition.get(partitionId)
      );
    }

    for (const tableName of targets.broadTables) {
      this.addPartitionRegistrationKeysForTable(registrationKeys, tableName);
    }

    return registrationKeys;
  }

  private addPartitionRegistrationKeysForTable(
    target: Set<string>,
    tableName: string
  ): void {
    for (const [partitionId, registrationKeys] of this
      .registrationKeysByPartition) {
      if (getPartitionDependencyTable(partitionId) === tableName) {
        this.addIndexedRegistrationKeys(target, registrationKeys);
      }
    }
  }

  private addIndexedRegistrationKeys(
    target: Set<string>,
    registrationKeys: ReadonlySet<string> | undefined
  ): void {
    if (!registrationKeys) {
      return;
    }

    for (const registrationKey of registrationKeys) {
      target.add(registrationKey);
    }
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
    this.removeRegistrationFromIndexes(registrationKey, registration);
    registration.dependencies = dependencies;
    registration.versionSnapshot = versionSnapshot;
    this.addRegistrationToIndexes(registrationKey, registration);
    await this.migrateRegistrationToHomeShard(registration);
  }

  private addRegistrationToIndexes(
    registrationKey: string,
    registration: StoredRealtimeRegistration
  ): void {
    if (
      !registration.dependencies ||
      (registration.dependencies.tables.size === 0 &&
        registration.dependencies.partitions.size === 0)
    ) {
      this.registrationKeysWithoutDependencies.add(registrationKey);
      return;
    }

    for (const tableName of registration.dependencies.tables) {
      this.addRegistrationToIndex(
        this.registrationKeysByTable,
        tableName,
        registrationKey
      );
    }

    for (const partitionId of registration.dependencies.partitions) {
      this.addRegistrationToIndex(
        this.registrationKeysByPartition,
        partitionId,
        registrationKey
      );
    }
  }

  private async migrateRegistrationToHomeShard(
    registration: StoredRealtimeRegistration
  ): Promise<void> {
    const activeGeneration = await fetchActiveRealtimeShardGeneration(
      this.database
    );
    if (activeGeneration.generationId !== this.shardGenerationId) {
      return;
    }

    const targetShardName = getRealtimeSubscriptionShardName(
      getRealtimeRegistrationHomeRouteTarget(registration.dependencies),
      activeGeneration
    );
    if (targetShardName === this.shardName) {
      return;
    }

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

    const connectionUpdateResponse = await this.env.REALTIME_CONNECTIONS.get(
      this.env.REALTIME_CONNECTIONS.idFromName(registration.connectionName)
    ).fetch("https://baseflare.internal/subscription-moved", {
      body: JSON.stringify({
        connectionKey: registration.connectionKey,
        subscriptionId: registration.subscriptionId,
        subscriptionShardName: targetShardName,
      }),
      headers: JSON_HEADERS,
      method: "POST",
    });
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
      return;
    }

    this.deleteRegistrationByKey(registrationKey);
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

  private addRegistrationToIndex(
    index: Map<string, Set<string>>,
    indexKey: string,
    registrationKey: string
  ): void {
    const registrationKeys = index.get(indexKey) ?? new Set<string>();
    registrationKeys.add(registrationKey);
    index.set(indexKey, registrationKeys);
  }

  private deleteRegistrationByKey(registrationKey: string): void {
    const registration = this.registrations.get(registrationKey);
    if (!registration) {
      return;
    }

    this.removeRegistrationFromIndexes(registrationKey, registration);
    this.registrations.delete(registrationKey);
  }

  private removeRegistrationFromIndexes(
    registrationKey: string,
    registration: StoredRealtimeRegistration
  ): void {
    this.registrationKeysWithoutDependencies.delete(registrationKey);
    if (!registration.dependencies) {
      return;
    }

    for (const tableName of registration.dependencies.tables) {
      this.removeRegistrationFromIndex(
        this.registrationKeysByTable,
        tableName,
        registrationKey
      );
    }

    for (const partitionId of registration.dependencies.partitions) {
      this.removeRegistrationFromIndex(
        this.registrationKeysByPartition,
        partitionId,
        registrationKey
      );
    }
  }

  private removeRegistrationFromIndex(
    index: Map<string, Set<string>>,
    indexKey: string,
    registrationKey: string
  ): void {
    const registrationKeys = index.get(indexKey);
    if (!registrationKeys) {
      return;
    }

    registrationKeys.delete(registrationKey);
    if (registrationKeys.size === 0) {
      index.delete(indexKey);
    }
  }
}

function waitForRealtimeNotifyEventLookupRetry(): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, REALTIME_NOTIFY_EVENT_LOOKUP_RETRY_DELAY_MS);
  });
}
