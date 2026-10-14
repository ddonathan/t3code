import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import {
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ScheduledTaskId,
  ThreadId,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import { idleThreadProjection, liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";

import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as UsageLimitRecoveryWorker from "../orchestration-v2/UsageLimitRecoveryWorker.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ScheduledTasks from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as Scheduler from "./Scheduler.ts";

it.effect.each(["on time", "after restart"])(
  "runs Scheduled Tasks and a persisted limit retry through the same scheduler %s",
  (scenario) =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const resetAt = DateTime.formatIso(DateTime.add(now, { seconds: 60 }));
      const thread: OrchestrationV2ThreadShell = {
        id: ThreadId.make("thread:limited"),
        projectId: ProjectId.make("project:test"),
        title: "Limited",
        providerInstanceId: ProviderInstanceId.make("codex"),
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy: "user",
        creationSource: "web",
        branch: null,
        worktreePath: null,
        lineage: {
          rootThreadId: ThreadId.make("thread:limited"),
          parentThreadId: null,
          relationshipToParent: null,
        },
        forkedFrom: null,
        activeProviderThreadId: null,
        latestRunId: RunId.make("run:limited"),
        latestRunCompletedAt: now,
        activeRunId: null,
        status: "failed",
        lastErrorClass: "usage_limit",
        usageLimitResetAt: resetAt,
        pendingRuntimeRequest: null,
        latestVisibleMessage: null,
        latestUserMessageAt: now,
        hasActionableProposedPlan: false,
        itemCount: 1,
        visibleItemCount: 1,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        pinnedAt: null,
        deletedAt: null,
        limitRecovery: {
          runId: RunId.make("run:limited"),
          resetAt,
          autoResume: true,
          requestId: CommandId.make("recovery:choice"),
        },
      };
      if (scenario === "after restart") {
        yield* TestClock.adjust("65 seconds");
      }
      const current = yield* Ref.make(thread);
      const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2ServerCommand>>([]);
      const receipts = yield* Queue.unbounded<"task" | "retry">();
      const layerDependencies = Layer.mergeAll(
        NodeCrypto.layer,
        Layer.mock(ThreadLaunchService.ThreadLaunchService)({
          launch: () =>
            Queue.offer(receipts, "task").pipe(
              Effect.andThen(Effect.die("fixture dispatch failure")),
            ),
        }),
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          dispatch: (command) =>
            Ref.update(commands, (all) => [...all, command]).pipe(
              Effect.andThen(
                Ref.update(current, (shell) => ({ ...shell, status: "running" as const })),
              ),
              Effect.andThen(Queue.offer(receipts, "retry")),
              Effect.as({ sequence: 1, storedEvents: [] }),
            ),
        }),
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getLimitRecoveryCandidates: () =>
            Ref.get(current).pipe(
              Effect.map((shell) => (shell.status === "failed" ? [shell] : [])),
            ),
        }),
        Layer.mock(ServerSettings.ServerSettingsService)({
          getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
        }),
        Layer.mock(SecretRequests.SecretRequests)({}),
      );
      const layerWorkers = Layer.mergeAll(
        ScheduledTasks.layer,
        UsageLimitRecoveryWorker.layer,
      ).pipe(Layer.provide(layerDependencies), Layer.provide(Scheduler.layer));
      yield* Effect.gen(function* () {
        const tasks = yield* ScheduledTasks.ScheduledTaskService;
        const { task } = yield* tasks.upsert({
          title: "Scheduled work",
          prompt: "Run scheduled work",
          enabled: true,
          schedule: { type: "interval", everyMs: 60_000 },
          projectId: thread.projectId,
          workspaceStrategy: { type: "root" },
          modelSelection: thread.modelSelection,
          runtimeMode: thread.runtimeMode,
          interactionMode: thread.interactionMode,
        });
        const sql = yield* SqlClient.SqlClient;
        yield* sql`UPDATE scheduled_tasks SET next_run_at = ${resetAt} WHERE task_id = ${task.id}`;
        if (scenario === "on time") {
          yield* TestClock.adjust("55 seconds");
          assert.deepEqual(yield* Ref.get(commands), []);
        }
        yield* TestClock.adjust("5 seconds");
        assert.deepEqual([yield* Queue.take(receipts), yield* Queue.take(receipts)].sort(), [
          "retry",
          "task",
        ]);
        const delivered = yield* Ref.get(commands);
        assert.equal(delivered.length, 1);
        expect(delivered[0]).toMatchObject({
          type: "message.dispatch",
          usageLimitContinuationOfRunId: thread.latestRunId,
          usageLimitRecoveryRequestId: thread.limitRecovery!.requestId,
          text: "Continue where you left off.",
        });
      }).pipe(Effect.provide(layerWorkers));
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect(
  "blocked task lifetimes cannot hold the due batch or later polls, overlap themselves, or outlive the service",
  () =>
    Effect.gen(function* () {
      const started = yield* Queue.unbounded<string>();
      const interrupted = yield* Ref.make<ReadonlyArray<string>>([]);
      const dispatches = yield* Ref.make<
        ReadonlyArray<{ threadId: ThreadId; commandId: CommandId; messageId: MessageId }>
      >([]);
      const poll = yield* Queue.unbounded<Effect.Effect<void>>();
      const layerDependencies = Layer.mergeAll(
        NodeCrypto.layer,
        Layer.mock(Scheduler.Scheduler)({
          register: <E, R>(_name: string, runDueWork: Effect.Effect<void, E, R>) =>
            Effect.gen(function* () {
              const context = yield* Effect.context<R>();
              yield* Queue.offer(
                poll,
                runDueWork.pipe(Effect.provideContext(context), Effect.orDie),
              );
            }),
        }),
        Layer.mock(ThreadLaunchService.ThreadLaunchService)({
          launch: (input) => {
            const messageId = input.initialMessage?.messageId;
            if (messageId === undefined)
              return Effect.die("Scheduled launch omitted its message id");
            const threadId = ThreadId.make(`thread:${input.title}`);
            return Ref.update(dispatches, (all) => [
              ...all,
              { threadId, commandId: input.commandId, messageId },
            ]).pipe(
              Effect.andThen(Queue.offer(started, threadId)),
              Effect.as({
                threadId,
                projection: idleThreadProjection(liveThreadShell(threadId)),
                resumed: false,
              }),
            );
          },
        }),
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          sendToThread: (input) =>
            Ref.update(dispatches, (all) => [...all, input]).pipe(
              Effect.andThen(Queue.offer(started, input.threadId)),
              Effect.andThen(
                Effect.never.pipe(
                  Effect.onInterrupt(() =>
                    Ref.update(interrupted, (all) => [...all, input.threadId]),
                  ),
                ),
              ),
            ),
        }),
        Layer.mock(SecretRequests.SecretRequests)({}),
      );
      const layerWorkers = ScheduledTasks.layer.pipe(Layer.provide(layerDependencies));
      const sql = yield* SqlClient.SqlClient;
      yield* Effect.gen(function* () {
        const tasks = yield* ScheduledTasks.ScheduledTaskService;
        const createDue = (name: string) =>
          Effect.gen(function* () {
            const created = yield* tasks.upsert({
              id: ScheduledTaskId.make(`scheduled-task:${name}`),
              title: name,
              prompt: `Run ${name}`,
              enabled: true,
              schedule: { type: "interval", everyMs: 60_000 },
              projectId: ProjectId.make("project:isolation"),
              ...(name.startsWith("blocked-") ? { threadId: ThreadId.make(`thread:${name}`) } : {}),
              workspaceStrategy: { type: "root" },
              modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
              runtimeMode: "full-access",
              interactionMode: "default",
            });
            const now = yield* DateTime.now;
            yield* sql`UPDATE scheduled_tasks SET next_run_at = ${DateTime.formatIso(now)} WHERE task_id = ${created.task.id}`;
          });
        for (const name of ["blocked-1", "blocked-2", "blocked-3", "blocked-4", "healthy-5"]) {
          yield* createDue(name);
        }
        const runPoll = yield* Queue.take(poll);
        // Returning from the real source proves dispatch lifetimes do not retain
        // the Scheduler's source permit. The old four-slot batch cannot return.
        yield* runPoll;
        assert.deepEqual(
          (yield* Effect.forEach([1, 2, 3, 4, 5], () => Queue.take(started))).sort(),
          [
            "thread:blocked-1",
            "thread:blocked-2",
            "thread:blocked-3",
            "thread:blocked-4",
            "thread:healthy-5",
          ],
        );
        const initial = yield* Ref.get(dispatches);
        const blockedIds = initial.filter((input) => input.threadId.startsWith("thread:blocked-"));
        assert.equal(new Set(blockedIds.map((input) => input.commandId)).size, 4);
        assert.equal(new Set(blockedIds.map((input) => input.messageId)).size, 4);
        // Deliberately leave a running row due again; activeRuns must still
        // prevent a second dispatch even if a concurrent writer changes status.
        yield* sql`UPDATE scheduled_tasks SET last_run_status = 'failed' WHERE task_id = 'scheduled-task:blocked-1'`;
        yield* createDue("healthy-future");
        yield* TestClock.adjust("5 seconds");
        yield* runPoll;
        assert.equal(yield* Queue.take(started), "thread:healthy-future");
        yield* runPoll;
        assert.deepEqual(
          (yield* Ref.get(dispatches)).filter((input) =>
            input.threadId.startsWith("thread:blocked-"),
          ),
          blockedIds,
        );
        assert.equal((yield* Ref.get(dispatches)).length, 6);
        const manual = yield* tasks
          .runNow({ id: ScheduledTaskId.make("scheduled-task:blocked-1") })
          .pipe(Effect.flip);
        assert.equal(manual.taskId, "scheduled-task:blocked-1");
        assert.equal((yield* Ref.get(dispatches)).length, 6);
        yield* sql`UPDATE scheduled_tasks SET last_run_status = 'running' WHERE task_id = 'scheduled-task:blocked-1'`;
      }).pipe(Effect.provide(layerWorkers));
      // Service disposal interrupts dispatches; the SQL layer stays open for readback.
      assert.deepEqual((yield* Ref.get(interrupted)).toSorted(), [
        "thread:blocked-1",
        "thread:blocked-2",
        "thread:blocked-3",
        "thread:blocked-4",
      ]);
      const rows = yield* sql<{
        task_id: string;
        last_run_status: string;
        run_count: number;
        next_run_at: string;
      }>`SELECT task_id, last_run_status, run_count, next_run_at FROM scheduled_tasks WHERE task_id LIKE 'scheduled-task:blocked-%' ORDER BY task_id`;
      const healthyRows = yield* sql<{
        last_run_status: string;
        run_count: number;
        next_run_at: string;
      }>`SELECT last_run_status, run_count, next_run_at FROM scheduled_tasks WHERE task_id LIKE 'scheduled-task:healthy-%'`;
      assert.equal(healthyRows.length, 2);
      const shutdownAt = DateTime.toEpochMillis(yield* DateTime.now);
      for (const row of healthyRows) {
        assert.equal(row.last_run_status, "succeeded");
        assert.equal(row.run_count, 1);
        assert.isAbove(DateTime.toEpochMillis(DateTime.makeUnsafe(row.next_run_at)), shutdownAt);
      }
      assert.equal(rows.length, 4);
      for (const row of rows) {
        assert.equal(row.last_run_status, "failed");
        assert.equal(row.run_count, 1);
        assert.isAbove(DateTime.toEpochMillis(DateTime.makeUnsafe(row.next_run_at)), shutdownAt);
      }
      // Rebuild the service at the same clock: interrupted occurrences must not replay.
      yield* Effect.gen(function* () {
        yield* ScheduledTasks.ScheduledTaskService;
        assert.deepEqual(yield* ScheduledTasks.listDueTasks(yield* DateTime.now), []);
        const restartedPoll = yield* Queue.take(poll);
        yield* restartedPoll;
      }).pipe(Effect.provide(layerWorkers));
      assert.equal((yield* Ref.get(dispatches)).length, 6);
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect(
  "forked scheduled workers log defects before markRunning and release their reservation",
  () =>
    Effect.gen(function* () {
      const poll = yield* Deferred.make<Effect.Effect<void>>();
      const logged = yield* Deferred.make<unknown>();
      const defectOnRead = yield* Ref.make(false);
      const dispatched = yield* Deferred.make<void>();
      const warnings: unknown[] = [];
      const fixtureDefect = new Error("fixture origin read defect before markRunning");
      const dependencies = Layer.mergeAll(
        NodeCrypto.layer,
        Layer.succeed(ScheduledTasks.ScheduledTaskWebhookOrigin)(
          Ref.get(defectOnRead).pipe(
            Effect.flatMap((fail) =>
              fail
                ? Ref.set(defectOnRead, false).pipe(Effect.andThen(Effect.die(fixtureDefect)))
                : Effect.succeed({ relayHookBaseUrl: null }),
            ),
          ),
        ),
        Layer.mock(Scheduler.Scheduler)({
          register: <E, R>(_name: string, runDueWork: Effect.Effect<void, E, R>) =>
            Effect.gen(function* () {
              const context = yield* Effect.context<R>();
              yield* Deferred.succeed(
                poll,
                runDueWork.pipe(Effect.provideContext(context), Effect.orDie),
              );
            }),
        }),
        Layer.mock(ThreadLaunchService.ThreadLaunchService)({}),
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          sendToThread: () =>
            Deferred.succeed(dispatched, undefined).pipe(Effect.andThen(Effect.never)),
        }),
        Layer.mock(SecretRequests.SecretRequests)({}),
      );
      yield* Effect.gen(function* () {
        const tasks = yield* ScheduledTasks.ScheduledTaskService;
        const sql = yield* SqlClient.SqlClient;
        const { task } = yield* tasks.upsert({
          title: "Defect before running",
          prompt: "Synthetic task",
          enabled: true,
          schedule: { type: "interval", everyMs: 60_000 },
          projectId: ProjectId.make("project:defect"),
          threadId: ThreadId.make("thread:defect"),
          workspaceStrategy: { type: "root" },
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
          runtimeMode: "full-access",
          interactionMode: "default",
        });
        const now = DateTime.formatIso(yield* DateTime.now);
        yield* sql`UPDATE scheduled_tasks SET next_run_at = ${now} WHERE task_id = ${task.id}`;
        yield* Ref.set(defectOnRead, true);
        const runPoll = yield* Deferred.await(poll);
        yield* runPoll;
        const warning = yield* Deferred.await(logged);
        expect(warning).toEqual([
          "Scheduled task run failed",
          { taskId: task.id, cause: expect.objectContaining({ reasons: expect.any(Array) }) },
        ]);
        expect(warning).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              cause: expect.objectContaining({
                reasons: [expect.objectContaining({ defect: fixtureDefect })],
              }),
            }),
          ]),
        );
        const rows = yield* sql<{
          last_run_status: string;
          run_count: number;
        }>`SELECT last_run_status, run_count FROM scheduled_tasks WHERE task_id = ${task.id}`;
        assert.deepEqual(rows, [{ last_run_status: "never", run_count: 0 }]);
        assert.isFalse(yield* Deferred.isDone(dispatched));
        yield* Ref.set(defectOnRead, false);
        yield* runPoll;
        yield* Deferred.await(dispatched);
      }).pipe(
        Effect.provide([
          ScheduledTasks.layer.pipe(Layer.provide(dependencies)),
          Logger.layer([
            Logger.make(({ message }) => {
              warnings.push(message);
              Deferred.doneUnsafe(logged, Effect.succeed(message));
            }),
          ]),
        ]),
      );
      assert.equal(warnings.length, 1);
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);
