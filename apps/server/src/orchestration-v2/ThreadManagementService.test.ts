import { expect, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  NodeId,
  type OrchestrationV2Command,
  type OrchestrationV2Run,
  type OrchestrationV2CapacityRecovery,
  EventId,
  type OrchestrationV2Subagent,
  type OrchestrationV2ProviderThread,
  ProviderThreadId,
  ProviderDriverKind,
  ProviderSessionId,
  type OrchestrationV2StoredEvent,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadShell,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as LegacyV1ThreadImporter from "./legacy/LegacyV1ThreadImporter.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import { DispatchModeLimit, type DispatchModeRefusal } from "./DispatchModeLimit.ts";
import { idleThreadProjection, liveThreadShell } from "../mcp/McpToolAccess.testkit.ts";

it("stamps authoritative provenance on commands that create threads or messages", () => {
  const command: OrchestrationV2Command = {
    type: "thread.create",
    createdBy: "agent",
    creationSource: "mcp",
    commandId: CommandId.make("command:thread-management:create"),
    threadId: ThreadId.make("thread:thread-management:create"),
    projectId: ProjectId.make("project:thread-management"),
    title: "Thread management",
    modelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5-codex",
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
  };

  expect(
    ThreadManagementService.withCreationProvenance(command, {
      createdBy: "user",
      creationSource: "web",
    }),
  ).toMatchObject({
    createdBy: "user",
    creationSource: "web",
  });
});

it("client provenance cannot grant capacity continuation the agent identity", () => {
  const command: OrchestrationV2Command = {
    type: "message.dispatch",
    createdBy: "agent",
    creationSource: "server",
    commandId: CommandId.make("spoofed-capacity-command"),
    threadId: ThreadId.make("spoofed-capacity-thread"),
    messageId: MessageId.make("spoofed-capacity-message"),
    text: "Continue",
    attachments: [],
    capacityContinuationOfRunId: RunId.make("failed-capacity-run"),
    dispatchMode: { type: "start_immediately" },
  };
  expect(
    ThreadManagementService.withCreationProvenance(command, {
      createdBy: "user",
      creationSource: "server",
    }),
  ).toMatchObject({ createdBy: "user", creationSource: "server" });
});

it.each([undefined, "mcp"] as const)(
  "client intake stamps continuity control provenance over legacy or supplied %s origin",
  (creationSource) => {
    const command: OrchestrationV2Command = {
      type: "thread.continuity.set",
      commandId: CommandId.make("continuity-provenance"),
      threadId: ThreadId.make("continuity-provenance"),
      enabled: false,
      ...(creationSource === undefined ? {} : { creationSource }),
    };
    expect(
      ThreadManagementService.withCreationProvenance(command, {
        createdBy: "user",
        creationSource: "mobile",
      }),
    ).toMatchObject({ creationSource: "mobile" });
  },
);

it("leaves commands that do not create durable authored content unchanged", () => {
  const command: OrchestrationV2Command = {
    type: "run.interrupt",
    commandId: CommandId.make("command:thread-management:interrupt"),
    threadId: ThreadId.make("thread:thread-management:interrupt"),
    runId: RunId.make("run:thread-management:interrupt"),
  };

  expect(
    ThreadManagementService.withCreationProvenance(command, {
      createdBy: "user",
      creationSource: "web",
    }),
  ).toBe(command);
});

it("identifies every existing thread that must be hydrated before dispatch", () => {
  const sourceThreadId = ThreadId.make("thread:thread-management:source");
  const targetThreadId = ThreadId.make("thread:thread-management:target");
  const parentThreadId = ThreadId.make("thread:thread-management:parent");

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make("command:thread-management:create"),
      threadId: targetThreadId,
      projectId: ProjectId.make("project:thread-management"),
      title: "Created thread",
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    }),
  ).toEqual([]);

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "thread.archive",
      commandId: CommandId.make("command:thread-management:archive"),
      threadId: targetThreadId,
    }),
  ).toEqual([targetThreadId]);

  // Read-state commands skip transcript hydration entirely: they fire on
  // every activity bump while a thread is open and never touch messages.
  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "thread.visit",
      commandId: CommandId.make("command:thread-management:visit"),
      threadId: targetThreadId,
      visitedAt: "2026-07-30T00:00:00.000Z",
    }),
  ).toEqual([]);

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "thread.mark-unread",
      commandId: CommandId.make("command:thread-management:mark-unread"),
      threadId: targetThreadId,
    }),
  ).toEqual([]);

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "thread.fork",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make("command:thread-management:fork"),
      sourceThreadId,
      targetThreadId,
      sourcePoint: {
        type: "run",
        runId: RunId.make("run:thread-management:source"),
      },
    }),
  ).toEqual([sourceThreadId]);

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "thread.merge_back",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make("command:thread-management:merge"),
      sourceThreadId,
      targetThreadId,
      sourcePoint: {
        type: "run",
        runId: RunId.make("run:thread-management:source"),
      },
    }),
  ).toEqual([sourceThreadId, targetThreadId]);

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "delegated_task.request",
      createdBy: "agent",
      creationSource: "provider",
      commandId: CommandId.make("command:thread-management:delegate"),
      parentThreadId,
      parentRunId: RunId.make("run:thread-management:parent"),
      parentNodeId: NodeId.make("node:thread-management:parent"),
      task: "Inspect the migration",
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5-codex",
      },
      runtimeMode: "full-access",
      interactionMode: "default",
    }),
  ).toEqual([parentThreadId]);

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "delegated_task.wake-policy",
      commandId: CommandId.make("command:thread-management:wake-policy"),
      parentThreadId,
      taskId: NodeId.make("node:thread-management:delegated"),
      completionWake: "always",
    }),
  ).toEqual([parentThreadId]);

  expect(
    ThreadManagementService.existingThreadIdsForCommand({
      type: "thread.created.record",
      commandId: CommandId.make("command:thread-management:record"),
      parentThreadId,
      parentRunId: RunId.make("run:thread-management:parent"),
      parentNodeId: NodeId.make("node:thread-management:parent"),
      targetThreadId,
      targetRunId: null,
    }),
  ).toEqual([parentThreadId, targetThreadId]);
});

it("derives thread management messages from structural error attributes", () => {
  const projectId = ProjectId.make("project:thread-management:errors");
  const threadId = ThreadId.make("thread:thread-management:errors");
  const runId = RunId.make("run:thread-management:errors");
  const messageId = MessageId.make("message:thread-management:errors");
  const infrastructureCause = new Error("private sqlite detail");

  const threadNotFound = new ThreadManagementService.ThreadManagementThreadNotFoundError({
    projectId,
    threadId,
  });
  expect(threadNotFound).toMatchObject({ projectId, threadId });
  expect(threadNotFound.message).toBe(`Thread ${threadId} was not found in project ${projectId}.`);

  const runNotFound = new ThreadManagementService.ThreadManagementRunNotFoundError({
    threadId,
    runId,
  });
  expect(runNotFound).toMatchObject({ threadId, runId });
  expect(runNotFound.message).toBe(`Run ${runId} does not belong to thread ${threadId}.`);

  const archived = new ThreadManagementService.ThreadManagementThreadArchivedError({
    threadId,
  });
  expect(archived).toMatchObject({ threadId });
  expect(archived.message).toBe(`Thread ${threadId} is archived and cannot receive messages.`);

  const notSteerable = new ThreadManagementService.ThreadManagementNoSteerableRunError({
    threadId,
    mode: "restart",
  });
  expect(notSteerable).toMatchObject({
    threadId,
    mode: "restart",
  });
  expect(notSteerable.message).toBe(
    `Thread ${threadId} has no running turn that can be restarted.`,
  );

  const notInterruptible = new ThreadManagementService.ThreadManagementThreadNotInterruptibleError({
    threadId,
    runId,
  });
  expect(notInterruptible).toMatchObject({ threadId, runId });
  expect(notInterruptible.message).toBe(`Run ${runId} is not currently interruptible.`);

  const listFailure = new ThreadManagementService.ThreadManagementProjectThreadsListError({
    projectId,
    cause: infrastructureCause,
  });
  expect(listFailure).toMatchObject({ projectId, cause: infrastructureCause });
  expect(listFailure.message).toBe(`Unable to list threads in project ${projectId}.`);
  expect(listFailure.message).not.toContain(infrastructureCause.message);

  const durableProjectionFailure =
    new ThreadManagementService.ThreadManagementDurableRunProjectionError({
      threadId,
      messageId,
    });
  expect(durableProjectionFailure).toMatchObject({ threadId, messageId });
  expect(durableProjectionFailure.message).toBe(
    `Message ${messageId} was accepted on thread ${threadId} without a durable run projection.`,
  );
});

const layerReceiptsTest = CommandReceiptStore.layer.pipe(
  Layer.provide(SqlitePersistence.layerMemory),
);

const layerThreadManagementTest = (settingsLayer: ReturnType<typeof ServerSettings.layerTest>) =>
  ThreadManagementService.layer.pipe(
    Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
    Layer.provide(Layer.mock(Orchestrator.OrchestratorV2)({})),
    Layer.provide(settingsLayer),
    Layer.provide(layerReceiptsTest),
  );

it.effect("refuses a new turn when the project effectively disables the instance", () => {
  const projectId = ProjectId.make("project:thread-management:gate-disabled");
  const instanceId = ProviderInstanceId.make("codex_work");
  const commandId = CommandId.make("command:thread-management:gate-disabled");
  const layerTest = layerThreadManagementTest(
    ServerSettings.layerTest({
      providerInstances: { [instanceId]: { driver: "codex", enabled: true } },
      projectSettingsOverrides: {
        [projectId]: { providerInstances: { [instanceId]: { enabled: false } } },
      },
    }),
  );

  return Effect.gen(function* () {
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const error = yield* Effect.flip(
      threads.assertProviderInstanceEnabled({ projectId, commandId, instanceId }),
    );

    expect(error).toBeInstanceOf(
      ThreadManagementService.ThreadManagementProviderInstanceDisabledError,
    );
    expect(error).toMatchObject({ projectId, instanceId });
    expect(error.message).toBe(
      `Provider instance "${instanceId}" is disabled for project ${projectId}.`,
    );
  }).pipe(Effect.provide(layerTest));
});

it.effect("allows a new turn when the project enables an instance the machine disabled", () => {
  const projectId = ProjectId.make("project:thread-management:gate-enabled");
  const instanceId = ProviderInstanceId.make("codex_work");
  const commandId = CommandId.make("command:thread-management:gate-enabled");
  const layerTest = layerThreadManagementTest(
    ServerSettings.layerTest({
      providerInstances: { [instanceId]: { driver: "codex", enabled: false } },
      projectSettingsOverrides: {
        [projectId]: { providerInstances: { [instanceId]: { enabled: true } } },
      },
    }),
  );

  return Effect.gen(function* () {
    const threads = yield* ThreadManagementService.ThreadManagementService;
    yield* threads.assertProviderInstanceEnabled({ projectId, commandId, instanceId });
  }).pipe(Effect.provide(layerTest));
});

it.effect("inherits the machine value when the project has no override", () => {
  const projectId = ProjectId.make("project:thread-management:gate-inherit");
  const instanceId = ProviderInstanceId.make("codex_work");
  const commandId = CommandId.make("command:thread-management:gate-inherit");
  const layerTest = layerThreadManagementTest(
    ServerSettings.layerTest({
      providerInstances: { [instanceId]: { driver: "codex", enabled: false } },
    }),
  );

  return Effect.gen(function* () {
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const error = yield* Effect.flip(
      threads.assertProviderInstanceEnabled({ projectId, commandId, instanceId }),
    );

    expect(error).toBeInstanceOf(
      ThreadManagementService.ThreadManagementProviderInstanceDisabledError,
    );
  }).pipe(Effect.provide(layerTest));
});

const layerDispatchChokepointTest =
  (settingsLayer: ReturnType<typeof ServerSettings.layerTest>) => (dispatched: Array<string>) =>
    ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: () =>
            Effect.succeed({
              thread: {
                id: ThreadId.make("thread:thread-management:chokepoint"),
                projectId: ProjectId.make("project:thread-management:chokepoint"),
                deletedAt: null,
                modelSelection: {
                  instanceId: ProviderInstanceId.make("codex_work"),
                  model: "test",
                },
              },
            } as unknown as OrchestrationV2ThreadProjection),
          dispatch: (command) =>
            Effect.sync(() => {
              dispatched.push(command.type);
              return { sequence: 1, storedEvents: [] } as never;
            }),
        }),
      ),
      Layer.provide(settingsLayer),
      Layer.provideMerge(layerReceiptsTest),
    );

it.effect(
  "the dispatch chokepoint refuses a continuation message.dispatch on a project-disabled instance before reaching the orchestrator",
  () => {
    const projectId = ProjectId.make("project:thread-management:chokepoint");
    const threadId = ThreadId.make("thread:thread-management:chokepoint");
    const instanceId = ProviderInstanceId.make("codex_work");
    const commandId = CommandId.make("command:thread-management:chokepoint-disabled");
    const dispatched: Array<string> = [];
    const layerTest = layerDispatchChokepointTest(
      ServerSettings.layerTest({
        providerInstances: { [instanceId]: { driver: "codex", enabled: true } },
        projectSettingsOverrides: {
          [projectId]: { providerInstances: { [instanceId]: { enabled: false } } },
        },
      }),
    )(dispatched);

    return Effect.gen(function* () {
      const threads = yield* ThreadManagementService.ThreadManagementService;
      const error = yield* Effect.flip(
        threads.dispatch({
          type: "message.dispatch",
          commandId,
          threadId,
          messageId: MessageId.make("message:thread-management:chokepoint-disabled"),
          text: "Background task completed.",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
        }),
      );

      expect(error).toBeInstanceOf(
        ThreadManagementService.ThreadManagementProviderInstanceDisabledError,
      );
      expect(error).toMatchObject({ projectId, instanceId });
      expect(dispatched).toEqual([]);
    }).pipe(Effect.provide(layerTest));
  },
);

it.effect(
  "a replay of an already-accepted command id dispatches despite the instance now being disabled",
  () => {
    const projectId = ProjectId.make("project:thread-management:chokepoint");
    const threadId = ThreadId.make("thread:thread-management:chokepoint");
    const instanceId = ProviderInstanceId.make("codex_work");
    const commandId = CommandId.make("command:thread-management:chokepoint-replay");
    const dispatched: Array<string> = [];
    const layerTest = layerDispatchChokepointTest(
      ServerSettings.layerTest({
        providerInstances: { [instanceId]: { driver: "codex", enabled: true } },
        projectSettingsOverrides: {
          [projectId]: { providerInstances: { [instanceId]: { enabled: false } } },
        },
      }),
    )(dispatched);

    return Effect.gen(function* () {
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      yield* receipts.insertIfAbsent({
        commandId,
        threadId,
        commandType: "message.dispatch",
        acceptedAt: yield* DateTime.now,
        resultSequence: 1,
        status: "accepted",
        error: null,
      });

      const threads = yield* ThreadManagementService.ThreadManagementService;
      yield* threads.dispatch({
        type: "message.dispatch",
        commandId,
        threadId,
        messageId: MessageId.make("message:thread-management:chokepoint-replay"),
        text: "Background task completed.",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "server",
      });

      expect(dispatched).toEqual(["message.dispatch"]);
    }).pipe(Effect.provide(layerTest));
  },
);

it.effect("classifies projection infrastructure failures separately from a missing thread", () => {
  const projectId = ProjectId.make("project:thread-management:projection-failure");
  const threadId = ThreadId.make("thread:thread-management:projection-failure");
  const infrastructureCause = new Error("sqlite read failed");
  const projectionError = new Orchestrator.OrchestratorProjectionError({
    threadId,
    cause: infrastructureCause,
  });
  const layerTest = ThreadManagementService.layer.pipe(
    Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
    Layer.provide(
      Layer.mock(Orchestrator.OrchestratorV2)({
        getThreadProjection: () => Effect.fail(projectionError),
      }),
    ),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(layerReceiptsTest),
  );

  return Effect.gen(function* () {
    const service = yield* ThreadManagementService.ThreadManagementService;
    const error = yield* Effect.flip(service.getProjectThread({ projectId, threadId }));

    expect(error).toBeInstanceOf(ThreadManagementService.ThreadManagementProjectionLoadError);
    expect(error).toMatchObject({
      projectId,
      threadId,
      cause: projectionError,
    });
    expect(error.message).toBe(`Unable to load thread ${threadId} in project ${projectId}.`);
  }).pipe(Effect.provide(layerTest));
});

it.effect("uses thread-not-found only after a projection loads outside the project", () => {
  const projectId = ProjectId.make("project:thread-management:requested");
  const otherProjectId = ProjectId.make("project:thread-management:other");
  const threadId = ThreadId.make("thread:thread-management:wrong-project");
  const projection = {
    thread: {
      id: threadId,
      projectId: otherProjectId,
      deletedAt: null,
    },
  } as OrchestrationV2ThreadProjection;
  const layerTest = ThreadManagementService.layer.pipe(
    Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
    Layer.provide(
      Layer.mock(Orchestrator.OrchestratorV2)({
        getThreadProjection: () => Effect.succeed(projection),
      }),
    ),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(layerReceiptsTest),
  );

  return Effect.gen(function* () {
    const service = yield* ThreadManagementService.ThreadManagementService;
    const error = yield* Effect.flip(service.getProjectThread({ projectId, threadId }));

    expect(error).toBeInstanceOf(ThreadManagementService.ThreadManagementThreadNotFoundError);
    expect(error).toMatchObject({ projectId, threadId });
    expect("cause" in error).toBe(false);
  }).pipe(Effect.provide(layerTest));
});

it.effect("preserves failed legacy materialization when reading checkpoint context", () => {
  const threadId = ThreadId.make("thread:thread-management:checkpoint-import-failure");
  const importError = new LegacyV1ThreadImporter.LegacyV1ThreadImportError({
    threadId,
    operation: "hydrate transcript for",
    cause: new Error("checkpoint import failed"),
  });
  const layerTest = ThreadManagementService.layerWithLegacyImporter.pipe(
    Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getCheckpointContext: () =>
            Effect.succeed({ runs: [], checkpointScopes: [], checkpoints: [] }),
        }),
        Layer.mock(LegacyV1ThreadImporter.LegacyV1ThreadImporter)({
          ensureTranscript: () => Effect.fail(importError),
        }),
      ),
    ),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(layerReceiptsTest),
  );
  return Effect.gen(function* () {
    const service = yield* ThreadManagementService.ThreadManagementService;
    const error = yield* service.getCheckpointContext(threadId).pipe(Effect.flip);
    expect(error).toBeInstanceOf(Orchestrator.OrchestratorProjectionError);
    expect(error).toMatchObject({ threadId, cause: importError });
  }).pipe(Effect.provide(layerTest));
});

function capacityWaitFixture(now: DateTime.Utc) {
  const threadId = ThreadId.make("thread:capacity-wait");
  const shell = liveThreadShell(threadId, { activeRunId: null });
  const run: OrchestrationV2Run = {
    id: RunId.make("run:capacity-wait:failed"),
    threadId,
    ordinal: 1,
    providerInstanceId: shell.providerInstanceId,
    modelSelection: shell.modelSelection,
    providerThreadId: ProviderThreadId.make("provider-thread:capacity-wait"),
    userMessageId: MessageId.make("message:capacity-wait:authority"),
    rootNodeId: null,
    activeAttemptId: null,
    status: "failed",
    requestedAt: now,
    startedAt: now,
    completedAt: now,
    checkpointId: null,
    contextHandoffId: null,
  };
  const recovery: OrchestrationV2CapacityRecovery = {
    generation: CommandId.make("capacity-wait:generation"),
    originalRunId: run.id,
    authorityMessageId: run.userMessageId,
    failedRunId: run.id,
    retryRunId: null,
    providerThreadId: ProviderThreadId.make("provider-thread:capacity-wait"),
    modelSelection: shell.modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    reactionsReleased: false,
    attemptsUsed: 0,
    nextAttemptAt: DateTime.add(now, { seconds: 15 }),
    status: "pending",
    reason: "Waiting for model capacity.",
  };
  return {
    run,
    recovery,
    projection: {
      ...idleThreadProjection(shell),
      thread: { ...idleThreadProjection(shell).thread, capacityRecovery: recovery },
      runs: [run],
    },
  };
}

it.effect.each(["pending", "retrying"] as const)(
  "waitForThread keeps %s owned capacity work inside its original timeout",
  (phase) =>
    Effect.gen(function* () {
      const fixture = capacityWaitFixture(yield* DateTime.now);
      const retry: OrchestrationV2Run = {
        ...fixture.run,
        id: RunId.make("capacity-wait:active-retry"),
        ordinal: 2,
        status: "running",
        completedAt: null,
      };
      const projection =
        phase === "pending"
          ? fixture.projection
          : {
              ...fixture.projection,
              thread: {
                ...fixture.projection.thread,
                capacityRecovery: {
                  ...fixture.recovery,
                  status: "retrying" as const,
                  retryRunId: retry.id,
                  attemptsUsed: 1,
                  nextAttemptAt: null,
                },
              },
              runs: [fixture.run, retry],
            };
      const read = yield* Deferred.make<void>();
      const layer = ThreadManagementService.layer.pipe(
        Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadEventSequence: () => Effect.succeed(0),
            streamStoredEventsFrom: () => Stream.never,
            getThreadRecords: () => Deferred.succeed(read, undefined).pipe(Effect.as(projection)),
          }),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(layerReceiptsTest),
      );
      const service = yield* ThreadManagementService.ThreadManagementService.pipe(
        Effect.provide(layer),
      );
      const fiber = yield* service
        .waitForThread({
          projectId: fixture.projection.thread.projectId,
          threadId: fixture.run.threadId,
          runId: fixture.run.id,
          timeoutMs: 1,
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(read);
      yield* TestClock.adjust(Duration.millis(1));
      const result = yield* Fiber.join(fiber);
      expect(result).toMatchObject({
        status: phase === "pending" ? "waiting" : "running",
        timedOut: true,
        run:
          phase === "pending"
            ? { id: fixture.run.id, status: "failed" }
            : { id: retry.id, status: "running" },
        capacityRecovery: projection.thread.capacityRecovery,
      });
    }),
);

it.effect.each(["no-deadline", "superseded", "inactive-retry", "cancelled"] as const)(
  "waitForThread does not extend a failed run for %s capacity evidence",
  (scenario) =>
    Effect.gen(function* () {
      const fixture = capacityWaitFixture(yield* DateTime.now);
      const later = { ...fixture.run, id: RunId.make("capacity-wait:new-human"), ordinal: 2 };
      const recovery =
        scenario === "no-deadline"
          ? { ...fixture.recovery, nextAttemptAt: null }
          : scenario === "inactive-retry"
            ? { ...fixture.recovery, status: "retrying" as const, retryRunId: later.id }
            : scenario === "cancelled"
              ? { ...fixture.recovery, status: "cancelled" as const }
              : fixture.recovery;
      const projection = {
        ...fixture.projection,
        thread: { ...fixture.projection.thread, capacityRecovery: recovery },
        runs:
          scenario === "superseded" || scenario === "inactive-retry"
            ? [fixture.run, later]
            : [fixture.run],
      };
      const layer = ThreadManagementService.layer.pipe(
        Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadEventSequence: () => Effect.succeed(0),
            streamStoredEventsFrom: () =>
              Stream.die("Invalid recovery must not subscribe or wait."),
            getThreadRecords: () => Effect.succeed(projection),
          }),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(layerReceiptsTest),
      );
      const service = yield* ThreadManagementService.ThreadManagementService.pipe(
        Effect.provide(layer),
      );
      const result = yield* service.waitForThread({
        projectId: projection.thread.projectId,
        threadId: fixture.run.threadId,
        runId: fixture.run.id,
        timeoutMs: 1,
      });
      expect(result).toMatchObject({
        status: "failed",
        timedOut: false,
        run: { id: fixture.run.id, status: "failed" },
      });
    }),
);

it.effect("waitForThread ends a capacity hold when durable cancellation metadata arrives", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const fixture = capacityWaitFixture(now);
    const subscribed = yield* Deferred.make<void>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    let current = fixture.projection;
    const layer = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          streamStoredEventsFrom: (input) =>
            input?.eventType === "thread.metadata-updated"
              ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.fromQueue(events)),
                )
              : Stream.never,
          getThreadRecords: () => Effect.succeed(current),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layer),
    );
    const fiber = yield* service
      .waitForThread({
        projectId: current.thread.projectId,
        threadId: fixture.run.threadId,
        runId: fixture.run.id,
        timeoutMs: 60_000,
      })
      .pipe(Effect.forkChild);
    yield* Deferred.await(subscribed);
    current = {
      ...fixture.projection,
      thread: {
        ...fixture.projection.thread,
        capacityRecovery: { ...fixture.recovery, status: "cancelled", nextAttemptAt: null },
      },
    };
    yield* Queue.offer(events, {
      sequence: 1,
      commandId: null,
      event: {
        id: EventId.make("capacity-wait-cancelled"),
        type: "thread.metadata-updated",
        threadId: fixture.run.threadId,
        occurredAt: now,
        payload: current.thread,
      },
    });
    expect(yield* Fiber.join(fiber)).toMatchObject({
      status: "failed",
      timedOut: false,
      run: { id: fixture.run.id, status: "failed" },
      capacityRecovery: { status: "cancelled" },
    });
  }),
);

it.effect("waitForThread follows an owned capacity retry until that retry actually completes", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const fixture = capacityWaitFixture(now);
    const subscribed = yield* Deferred.make<void>();
    const retryRead = yield* Deferred.make<void>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    let current = fixture.projection;
    const retry: OrchestrationV2Run = {
      ...fixture.run,
      id: RunId.make("run:capacity-wait:retry"),
      ordinal: 2,
      status: "running",
      completedAt: null,
    };
    const recovery = {
      ...fixture.recovery,
      status: "retrying" as const,
      attemptsUsed: 1,
      nextAttemptAt: null,
      retryRunId: retry.id,
    };
    const layer = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          streamStoredEventsFrom: (input) =>
            input?.eventType === "run.updated"
              ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.fromQueue(events)),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.gen(function* () {
              if (current.runs.some((run) => run.id === retry.id && run.status === "running"))
                yield* Deferred.succeed(retryRead, undefined);
              return current;
            }),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layer),
    );
    const fiber = yield* service
      .waitForThread({
        projectId: current.thread.projectId,
        threadId: fixture.run.threadId,
        runId: fixture.run.id,
        timeoutMs: 60_000,
      })
      .pipe(Effect.forkChild);
    yield* Deferred.await(subscribed);
    current = {
      ...fixture.projection,
      thread: { ...fixture.projection.thread, capacityRecovery: recovery },
      runs: [fixture.run, retry],
    };
    yield* Queue.offer(events, {
      sequence: 1,
      commandId: null,
      event: {
        id: EventId.make("capacity-retry-running"),
        type: "run.updated",
        threadId: retry.threadId,
        runId: retry.id,
        occurredAt: now,
        payload: retry,
      },
    });
    yield* Deferred.await(retryRead);
    const completed = { ...retry, status: "completed" as const, completedAt: now };
    current = {
      ...current,
      thread: { ...current.thread, capacityRecovery: { ...recovery, status: "complete" } },
      runs: [fixture.run, completed],
    };
    yield* Queue.offer(events, {
      sequence: 2,
      commandId: null,
      event: {
        id: EventId.make("capacity-retry-completed"),
        type: "run.updated",
        threadId: retry.threadId,
        runId: retry.id,
        occurredAt: now,
        payload: completed,
      },
    });
    expect(yield* Fiber.join(fiber)).toMatchObject({
      status: "completed",
      timedOut: false,
      run: { id: retry.id, status: "completed" },
      capacityRecovery: { status: "complete" },
    });
  }),
);

it.effect.each([
  { finalStatus: "completed" as const, timedOut: false },
  { finalStatus: "failed" as const, timedOut: false },
  { finalStatus: "cancelled" as const, timedOut: false },
  { finalStatus: "interrupted" as const, timedOut: false },
  { finalStatus: "rolled_back" as const, timedOut: false },
  { finalStatus: "running" as const, timedOut: true },
  { finalStatus: "missing" as const },
])("waitForThread timeout final read when selected run is $finalStatus", (scenario) =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-timeout");
    const threadId = ThreadId.make("thread:thread-management:wait-timeout");
    const runId = RunId.make("run:thread-management:wait-timeout");
    const subscribed = yield* Deferred.make<void>();
    let reads = 0;
    const projection = (status: OrchestrationV2Run["status"] | "missing") =>
      ({
        thread: { id: threadId, projectId, deletedAt: null },
        runs: status === "missing" ? [] : [{ id: runId, status }],
      }) as unknown as OrchestrationV2ThreadProjection;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          // No run update arrives, so the timeout path runs while a final
          // projection read can still observe a terminal run.
          streamStoredEventsFrom: () =>
            Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
              Stream.drain,
              Stream.concat(Stream.never),
            ),
          getThreadRecords: () =>
            Effect.sync(() => {
              reads += 1;
              return projection(reads === 1 ? "running" : scenario.finalStatus);
            }),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({
        projectId,
        threadId,
        runId,
        timeoutMs: 1,
      })
      .pipe(Effect.result, Effect.forkChild);
    yield* Deferred.await(subscribed);
    yield* TestClock.adjust(Duration.millis(1));
    const result = yield* Fiber.join(fiber);

    if (scenario.finalStatus === "missing") {
      expect(result._tag).toBe("Failure");
      expect(result).toMatchObject({
        failure: expect.any(ThreadManagementService.ThreadManagementRunNotFoundError),
      });
      expect(result).toMatchObject({
        failure: { threadId, runId },
      });
    } else {
      expect(result._tag).toBe("Success");
      expect(result).toMatchObject({
        success: {
          threadId,
          timedOut: scenario.timedOut,
          run: { id: runId, status: scenario.finalStatus },
        },
      });
    }
  }),
);

it.effect("waitForThread reads the run again only when the run updates", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-event");
    const threadId = ThreadId.make("thread:thread-management:wait-event");
    const runId = RunId.make("run:thread-management:wait-event");
    const subscribed = yield* Deferred.make<void>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    let status: OrchestrationV2Run["status"] = "running";
    let reads = 0;
    const stored = (sequence: number, event: object) =>
      ({ sequence, event: { threadId, ...event } }) as unknown as OrchestrationV2StoredEvent;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          // Only the run.updated stream carries events in this test.
          streamStoredEventsFrom: (input) =>
            input?.eventType === "run.updated"
              ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.fromQueue(events)),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.sync(() => {
              reads += 1;
              return {
                thread: { id: threadId, projectId, deletedAt: null },
                runs: [{ id: runId, status }],
              } as unknown as OrchestrationV2ThreadProjection;
            }),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({ projectId, threadId, runId, timeoutMs: 60 * 60 * 1_000 })
      .pipe(Effect.forkChild);
    yield* Deferred.await(subscribed);
    status = "completed";
    yield* Queue.offer(events, stored(1, { type: "run.updated", payload: { id: "other-run" } }));
    yield* Queue.offer(events, stored(2, { type: "run.updated", payload: { id: runId, status } }));
    const result = yield* Fiber.join(fiber);

    expect(result).toMatchObject({ timedOut: false, run: { id: runId, status: "completed" } });
    // The first read plus one for this run's update. The other run caused none.
    expect(reads).toBe(2);
  }),
);

it.effect.each([
  "running",
  "missing",
  "completed",
  "unsupported",
  "missing-native-id",
  "wrong-thread",
  "no-session",
  "unsupported-runtime",
  "provider-failed",
  "provider-timeout",
  "within-limit",
  "runtime-raised",
  "interaction-raised",
] as const)("stops a persisted workflow with state %s", (state) =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:workflow-stop");
    const subagentId = NodeId.make("node:workflow-stop");
    const runId = RunId.make("run:workflow-stop");
    const providerThreadId = ProviderThreadId.make("provider-thread:workflow-stop");
    const providerSessionId = ProviderSessionId.make("provider-session:workflow-stop");
    const driver = ProviderDriverKind.make("claudeAgent");
    const instanceId = ProviderInstanceId.make("claudeAgent");
    const now = yield* DateTime.now;
    let targetModes: Pick<OrchestrationV2ThreadShell, "runtimeMode" | "interactionMode"> = {
      runtimeMode: "approval-required",
      interactionMode: "plan",
    };
    const task: OrchestrationV2Subagent = {
      id: subagentId,
      threadId,
      runId,
      parentNodeId: NodeId.make("node:workflow-parent"),
      origin: "provider_native",
      createdBy: "agent",
      driver,
      providerInstanceId: instanceId,
      providerThreadId: null,
      childThreadId: null,
      nativeTaskRef:
        state === "missing-native-id"
          ? null
          : {
              driver,
              nativeId: "native-workflow-id",
              strength: "strong",
            },
      workflow: { phases: [], agents: [] },
      prompt: "Run the workflow",
      title: null,
      model: null,
      status: state === "completed" ? "completed" : "running",
      result: null,
      startedAt: now,
      completedAt: null,
      updatedAt: now,
      ...(state === "unsupported" ? { driver: ProviderDriverKind.make("codex") } : {}),
    };
    const providerThread: OrchestrationV2ProviderThread = {
      id: providerThreadId,
      driver,
      providerInstanceId: instanceId,
      providerSessionId,
      appThreadId: state === "wrong-thread" ? ThreadId.make("other-thread") : threadId,
      ownerNodeId: null,
      nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
      nativeConversationHeadRef: null,
      status: "idle",
      firstRunOrdinal: 1,
      lastRunOrdinal: 1,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [],
      contextUsage: null,
      nativeMetadata: null,
      createdAt: now,
      updatedAt: now,
    };
    const calls: Array<{ taskId: string; providerThread: OrchestrationV2ProviderThread }> = [];
    const stopStarted = yield* Deferred.make<void>();
    const runtime = {
      driver,
      providerSessionId,
      instanceId,
      ...(state === "unsupported-runtime"
        ? {}
        : {
            stopTask: (input: (typeof calls)[number]) =>
              state === "provider-failed"
                ? Effect.fail(
                    new ProviderAdapter.ProviderAdapterProtocolError({
                      driver,
                      detail: "private provider detail",
                    }),
                  )
                : state === "provider-timeout"
                  ? Deferred.succeed(stopStarted, undefined).pipe(Effect.andThen(Effect.never))
                  : Effect.sync(() => {
                      calls.push(input);
                    }),
          }),
    } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadRecords: () =>
              Effect.sync(
                () =>
                  ({
                    thread: { id: threadId, deletedAt: null, ...targetModes },
                    subagents: state === "missing" ? [] : [task],
                    runs: [{ id: runId, providerThreadId }],
                    providerThreads: [providerThread],
                  }) as unknown as OrchestrationV2ThreadProjection,
              ),
          }),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
            get: (id) => {
              expect(id).toBe(providerSessionId);
              return Effect.succeed(state === "no-session" ? Option.none() : Option.some(runtime));
            },
          }),
        ),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    yield* Effect.gen(function* () {
      const service = yield* ThreadManagementService.ThreadManagementService;
      const refused = yield* Ref.make<DispatchModeRefusal | undefined>(undefined);
      const stop = service
        .stopWorkflow({ threadId, subagentId })
        .pipe(
          Effect.provideService(
            DispatchModeLimit,
            state === "within-limit" || state === "runtime-raised" || state === "interaction-raised"
              ? { runtimeMode: "approval-required", interactionMode: "plan", refused }
              : undefined,
          ),
          Effect.result,
        );
      const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
      const result =
        state === "runtime-raised" || state === "interaction-raised"
          ? yield* Effect.gen(function* () {
              const started = yield* Deferred.make<void>();
              const pending = yield* executor.withLock(
                threadId,
                Effect.gen(function* () {
                  const fiber = yield* Deferred.succeed(started, undefined).pipe(
                    Effect.andThen(stop),
                    Effect.forkChild,
                  );
                  yield* Deferred.await(started);
                  targetModes =
                    state === "runtime-raised"
                      ? { ...targetModes, runtimeMode: "full-access" }
                      : { ...targetModes, interactionMode: "default" };
                  return fiber;
                }),
              );
              return yield* Fiber.join(pending);
            })
          : state === "provider-timeout"
            ? yield* Effect.gen(function* () {
                const finished = yield* Ref.make(false);
                const pending = yield* stop.pipe(
                  Effect.tap(() => Ref.set(finished, true)),
                  Effect.forkChild,
                );
                yield* Deferred.await(stopStarted);
                yield* TestClock.adjust("15 seconds");
                expect(yield* Ref.get(finished)).toBe(true);
                const result = yield* Fiber.join(pending);
                expect(yield* executor.withLock(threadId, Effect.succeed("lock-released"))).toBe(
                  "lock-released",
                );
                return result;
              })
            : yield* stop;
      if (state === "running" || state === "within-limit") {
        expect(result).toMatchObject({ _tag: "Success" });
        expect(calls).toEqual([{ taskId: "native-workflow-id", providerThread }]);
        expect(task.status).toBe("running");
      } else {
        expect(result).toMatchObject({
          _tag: "Failure",
          failure: {
            _tag: "OrchestrationV2StopWorkflowError",
            threadId,
            subagentId,
            reason:
              state === "missing" || state === "completed"
                ? "not-running"
                : state === "unsupported" || state === "unsupported-runtime"
                  ? "unsupported"
                  : state === "provider-failed" || state === "provider-timeout"
                    ? "stop-failed"
                    : "unavailable",
          },
        });
        expect(calls).toEqual([]);
        if (state === "provider-timeout") {
          expect(result).toMatchObject({ failure: { cause: { _tag: "TimeoutError" } } });
        }
        if (state === "runtime-raised" || state === "interaction-raised") {
          expect(yield* Ref.get(refused)).toEqual({
            threadId,
            ...targetModes,
            mode: state === "runtime-raised" ? "runtime" : "interaction",
          });
        }
      }
    }).pipe(Effect.provide(Layer.merge(layerTest, ThreadCommandExecutor.layer)));
  }),
);

it.effect.each([
  { status: "completed" as const, settles: true },
  { status: "failed" as const, settles: false },
  { status: "interrupted" as const, settles: false },
])("settleAfterRun settles only when the run $status", ({ status, settles }) =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:settle-after-run");
    const threadId = ThreadId.make("thread:thread-management:settle-after-run");
    const runId = RunId.make("run:thread-management:settle-after-run");
    const dispatched: Array<string> = [];
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, projectId, deletedAt: null },
              runs: [{ id: runId, status }],
            } as unknown as OrchestrationV2ThreadProjection),
          dispatch: (command) =>
            Effect.sync(() => {
              dispatched.push(command.type);
              return { sequence: 1, storedEvents: [] } as never;
            }),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );

    yield* service.settleAfterRun({ projectId, threadId, runId });

    expect(dispatched).toEqual(settles ? ["thread.settle"] : []);
  }),
);
