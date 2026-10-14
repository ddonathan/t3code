import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ModelCapacityRecoveryWorker from "./ModelCapacityRecoveryWorker.ts";
import { SourceControlProviderKind } from "@t3tools/contracts";
import { limitRecoveryCommand } from "./UsageLimitRecoveryWorker.ts";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as SourceControlProviderRegistry from "../sourceControl/SourceControlProviderRegistry.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  type ApplicationStoredEvent,
  CheckpointId,
  CheckpointRef,
  CommandId,
  ContextTransferId,
  EventId,
  MessageId,
  NodeId,
  RuntimeRequestId,
  TurnItemId,
  type ModelSelection,
  type OrchestrationV2Run,
  ProjectId,
  type PullRequestDetail,
  type PullRequestComment,
  PullRequestOperationError,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderSessionId,
  ProviderTurnId,
  RunId,
  ScheduledTaskId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as ServerConfig from "../config.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as ProjectEnrichmentService from "../project/ProjectEnrichmentService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as McpSessionRegistryTestkit from "../mcp/McpSessionRegistry.testkit.ts";
import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as LegacyV1ThreadImporter from "./legacy/LegacyV1ThreadImporter.ts";
import * as Orchestrator from "./Orchestrator.ts";
import { ROLLBACK_FAILED_MESSAGE } from "./CheckpointRollbackService.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as ProviderRuntimeRecoveryService from "./ProviderRuntimeRecoveryService.ts";
import { continueRestartedRun } from "./RestartContinuation.ts";
import * as ProjectionMaintenance from "./ProjectionMaintenance.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as PullRequestWatchReactor from "./PullRequestWatchReactor.ts";
import { PullRequestProviderError } from "@t3tools/source-control-core/server/PullRequestProvider";
import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as RuntimeLayer from "./runtimeLayer.ts";
import { shellStreamItemFromThreadShell } from "./ShellStream.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const layerPlatformTest = Layer.merge(
  NodeServices.layer,
  Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
    resolveLink: () => Effect.die("unused title link"),
  }),
);

const layerServerConfig = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-orchestration-v2-runtime-layer-",
});

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;
const alternateInstanceId = ProviderInstanceId.make("codex_alternate");

const layerVcsDriverRegistryTest = VcsDriverRegistry.layer.pipe(
  Layer.provide(VcsProcess.layer),
  Layer.provide(layerServerConfig),
  Layer.provide(layerPlatformTest),
);

const layerCheckpointStoreTest = CheckpointStore.layer.pipe(
  Layer.provide(layerVcsDriverRegistryTest),
);
const layerGitWorkflowTest = Layer.mock(GitWorkflow.GitWorkflowService)({
  pruneWorktrees: () => Effect.void,
  createWorktree: () => Effect.succeed({} as never),
});
const layerProjectServiceTest = Layer.mock(ProjectService.ProjectService)({
  getById: () => Effect.succeed(Option.none()),
});

const driver = ProviderDriverKind.make("codex");
const orchestrationAdapter = {
  instanceId: modelSelection.instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () => Effect.die("sessions are not used by lifecycle tests"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const providerInstance = {
  instanceId: modelSelection.instanceId,
  driverKind: driver,
  continuationIdentity: {
    driverKind: driver,
    continuationKey: "codex:test",
  },
  displayName: "Codex test",
  enabled: true,
  // No supportedRuntimeModes: every runtime mode runs as stored.
  snapshot: { getSnapshot: Effect.succeed({}) } as unknown as ProviderInstance["snapshot"],
  orchestrationAdapter,
  textGeneration: {} as ProviderInstance["textGeneration"],
} satisfies ProviderInstance;
const alternateProviderInstance = {
  ...providerInstance,
  instanceId: alternateInstanceId,
  continuationIdentity: {
    driverKind: driver,
    continuationKey: "codex:test:alternate",
  },
  displayName: "Codex alternate test",
  orchestrationAdapter: {
    ...orchestrationAdapter,
    instanceId: alternateInstanceId,
  },
} satisfies ProviderInstance;

const layerTestProviderInstanceRegistry = Layer.succeed(
  ProviderInstanceRegistry.ProviderInstanceRegistry,
  {
    getInstance: (instanceId) =>
      Effect.succeed(
        [providerInstance, alternateProviderInstance].find(
          (instance) => instanceId === instance.instanceId,
        ),
      ),
    listInstances: Effect.succeed([providerInstance, alternateProviderInstance]),
    listUnavailable: Effect.succeed([]),
    streamChanges: Stream.empty,
    subscribeChanges: Effect.never,
  },
);

/** Seed a project row the way a committed `project.created` event folds into it. */
/** A watched pull request as the host reports it: open, mergeable, with a failing lint check. */
const watchedPullRequestDetail = (input: {
  readonly projectId: ProjectId;
  readonly number: number;
  readonly at: string;
}): PullRequestDetail => ({
  provider: SourceControlProviderKind.make("github"),
  capabilities: {
    diff: true,
    comment: true,
    actions: [],
    mergeMethods: [],
    search: false,
    review: { inlineComment: false, reply: false, resolve: false, verdicts: [] },
    reviewers: { request: false, listCandidates: false },
  },
  viewerPermissions: {
    actions: [],
    comment: true,
    resolve: true,
    verdicts: [],
    requestReviewers: false,
  },
  projectId: input.projectId,
  projectTitle: "Watch wake",
  workspaceRoot: "/workspace/watch",
  repository: "pingdotgg/t3code",
  number: input.number,
  title: "Watched pull request",
  body: "",
  url: `https://github.com/pingdotgg/t3code/pull/${input.number}`,
  author: { login: "agent-user", name: null, avatarUrl: null },
  state: "open",
  isDraft: false,
  mergeability: "mergeable",
  additions: 1,
  deletions: 0,
  changedFiles: 1,
  headBranch: "feature",
  headSha: "abc1234def",
  baseBranch: "main",
  createdAt: input.at,
  updatedAt: input.at,
  mergedAt: null,
  closedAt: null,
  reviewers: [],
  labels: [],
  checks: [{ name: "lint", status: "failure", description: null, url: null }],
  mergeCapabilities: { merge: true, squash: true, rebase: true },
  viewer: "agent-user",
});

const seedProject = (input: {
  readonly projectId: ProjectId;
  readonly title: string;
  readonly workspaceRoot: string;
  readonly defaultModelSelection: ModelSelection | null;
  readonly createdAt: string;
}) =>
  Effect.flatMap(ProjectStore.ProjectStoreV2, (projects) =>
    projects.apply({
      sequence: 0,
      eventId: EventId.make(`seed:${input.projectId}`),
      aggregateKind: "project",
      aggregateId: input.projectId,
      occurredAt: input.createdAt,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
      type: "project.created",
      payload: {
        projectId: input.projectId,
        title: input.title,
        workspaceRoot: input.workspaceRoot,
        defaultModelSelection: input.defaultModelSelection,
        scripts: [],
        createdAt: input.createdAt,
        updatedAt: input.createdAt,
      },
    }),
  );

/** Move a seeded project the way a committed `project.meta-updated` event does. */
const moveProject = (projectId: ProjectId, workspaceRoot: string, updatedAt: string) =>
  Effect.flatMap(ProjectStore.ProjectStoreV2, (projects) =>
    projects.apply({
      sequence: 0,
      eventId: EventId.make(`move:${projectId}:${workspaceRoot}`),
      aggregateKind: "project",
      aggregateId: projectId,
      occurredAt: updatedAt,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
      type: "project.meta-updated",
      payload: { projectId, workspaceRoot, updatedAt },
    }),
  );

const layerTest = Layer.mergeAll(
  RuntimeLayer.layer,
  RuntimeLayer.layerEventSink,
  ProjectStore.layer,
  ProjectionStore.layer,
  EffectOutbox.layer,
  ThreadCommandExecutor.layer,
).pipe(
  Layer.provide(McpSessionRegistryTestkit.layer),
  Layer.provide(McpProviderSessions.layer),
  Layer.provide(SqlitePersistence.layerMemory),
  Layer.provide(layerCheckpointStoreTest),
  Layer.provide(layerServerConfig),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provide(layerTestProviderInstanceRegistry),
  Layer.provide(layerGitWorkflowTest),
  Layer.provide(layerProjectServiceTest),
  Layer.provide(layerPlatformTest),
);

const layerLegacyImportTest = RuntimeLayer.layer.pipe(
  Layer.provide(McpSessionRegistryTestkit.layer),
  Layer.provide(McpProviderSessions.layer),
  Layer.provideMerge(SqlitePersistence.layerMemory),
  Layer.provide(layerCheckpointStoreTest),
  Layer.provide(layerServerConfig),
  Layer.provide(ServerSettings.layerTest()),
  Layer.provide(layerTestProviderInstanceRegistry),
  Layer.provide(layerGitWorkflowTest),
  Layer.provide(layerProjectServiceTest),
  Layer.provide(layerPlatformTest),
);

const layerProjectDeletionTest = Layer.mergeAll(
  RuntimeLayer.layer.pipe(Layer.provide(RuntimeLayer.layerProjectService)),
  RuntimeLayer.layerProjectService,
  RuntimeLayer.layerEventSink,
  ThreadCommandExecutor.layer,
).pipe(
  Layer.provide(
    Layer.mock(ProjectEnrichmentService.ProjectEnrichmentService)({
      peek: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: false,
        }),
      getAvailable: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: false,
        }),
      invalidate: () => Effect.void,
    }),
  ),
  Layer.provide(
    Layer.mock(WorkspacePaths.WorkspacePaths)({
      normalizeWorkspaceRoot: (workspaceRoot) => Effect.succeed(workspaceRoot),
    }),
  ),
  Layer.provide(McpSessionRegistryTestkit.layer),
  Layer.provide(McpProviderSessions.layer),
  Layer.provide(SqlitePersistence.layerMemory),
  Layer.provide(layerCheckpointStoreTest),
  Layer.provide(layerServerConfig),
  Layer.provide(ServerSettings.layerTest()),
  Layer.provide(layerTestProviderInstanceRegistry),
  Layer.provide(layerGitWorkflowTest),
  Layer.provide(layerPlatformTest),
);

it.layer(layerProjectDeletionTest)("project deletion during thread commands", (it) => {
  it.effect("waits for an in-flight thread update before planning deletion", () =>
    Effect.gen(function* () {
      const projects = yield* ProjectService.ProjectService;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
      const projectId = ProjectId.make("runtime-project-delete-concurrent");
      const threadId = ThreadId.make("runtime-thread-delete-concurrent");
      const updateCommandId = CommandId.make("runtime-thread-delete-concurrent-update");
      yield* projects.create({
        commandId: CommandId.make("runtime-project-delete-concurrent-create"),
        projectId,
        title: "Concurrent deletion",
        workspaceRoot: "/work/concurrent-deletion",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("runtime-thread-delete-concurrent-create"),
        createdBy: "user",
        creationSource: "web",
        threadId,
        projectId,
        title: "Original title",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });

      const updateReady = yield* Deferred.make<void>();
      const releaseUpdate = yield* Deferred.make<void>();
      const deletionQueued = yield* Deferred.make<void>();
      const commitCommand = eventSink.commitCommand;
      const withLock = executor.withLock;
      let threadLockRequests = 0;
      const commitSpy = vi
        .spyOn(eventSink, "commitCommand")
        .mockImplementation((input) =>
          input.commandId === updateCommandId
            ? Deferred.succeed(updateReady, undefined).pipe(
                Effect.andThen(Deferred.await(releaseUpdate)),
                Effect.andThen(commitCommand(input)),
              )
            : commitCommand(input),
        );
      const observeLock: ThreadCommandExecutor.ThreadCommandExecutor["Service"]["withLock"] = (
        key,
        effect,
      ) => {
        if (key !== threadId || ++threadLockRequests !== 2) return withLock(key, effect);
        return Deferred.succeed(deletionQueued, undefined).pipe(
          Effect.andThen(withLock(key, effect)),
        );
      };
      const lockSpy = vi.spyOn(executor, "withLock").mockImplementation(observeLock);
      yield* Effect.gen(function* () {
        const updateFiber = yield* orchestrator
          .dispatch({
            type: "thread.metadata.update",
            commandId: updateCommandId,
            threadId,
            title: "Updated before deletion",
          })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.raceFirst(
          Deferred.await(updateReady),
          Fiber.join(updateFiber).pipe(
            Effect.andThen(Effect.die("The update completed before reaching its commit barrier.")),
          ),
        );
        const deleteFiber = yield* projects
          .delete({
            commandId: CommandId.make("runtime-project-delete-concurrent-delete"),
            projectId,
            force: true,
          })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.raceFirst(
          Deferred.await(deletionQueued),
          Fiber.join(deleteFiber).pipe(
            Effect.andThen(Effect.die("Project deletion bypassed the in-flight thread command.")),
          ),
        );
        yield* Deferred.succeed(releaseUpdate, undefined);
        yield* Fiber.join(updateFiber);
        const deletedProject = yield* Fiber.join(deleteFiber);
        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.isNotNull(deletedProject.deletedAt);
        assert.isNotNull(projection.thread.deletedAt);
        assert.equal(projection.thread.title, "Updated before deletion");
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            commitSpy.mockRestore();
            lockSpy.mockRestore();
          }),
        ),
      );
    }),
  );
});

const layerSharedApplicationDataPlaneTest = Layer.mergeAll(
  RuntimeLayer.layer.pipe(Layer.provide(RuntimeLayer.layerProjectService)),
  RuntimeLayer.layerProjectService,
  RuntimeLayer.layerEventSink,
  RuntimeLayer.layerEventInfrastructure,
).pipe(
  Layer.provide(
    Layer.mock(WorkspacePaths.WorkspacePaths)({
      normalizeWorkspaceRoot: (workspaceRoot) => Effect.succeed(workspaceRoot),
    }),
  ),
  Layer.provide(
    Layer.succeed(ProjectEnrichmentService.ProjectEnrichmentService, {
      peek: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: false,
        }),
      request: () => Effect.void,
      getAvailable: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: false,
        }),
      invalidate: () => Effect.void,
      subscribeChanges: Effect.never,
    }),
  ),
  Layer.provide(McpSessionRegistryTestkit.layer),
  Layer.provide(McpProviderSessions.layer),
  Layer.provideMerge(SqlitePersistence.layerMemory),
  Layer.provide(layerCheckpointStoreTest),
  Layer.provide(layerServerConfig),
  Layer.provide(ServerSettings.layerTest()),
  Layer.provide(layerTestProviderInstanceRegistry),
  Layer.provide(layerGitWorkflowTest),
  Layer.provide(layerPlatformTest),
);

it.layer(layerTest)("OrchestrationV2LayerLive", (it) => {
  it.effect("emits model updates separately from provider switches", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-model-selection-events");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-model-selection-events-create"),
        threadId,
        projectId: ProjectId.make("runtime-layer-model-selection-events-project"),
        title: "Model selection events",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });

      const sameInstance = yield* orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("runtime-layer-model-selection-events-update"),
        threadId,
        modelSelection: { ...modelSelection, model: "gpt-5.5" },
      });
      assert.deepEqual(
        sameInstance.storedEvents.map((stored) => stored.event.type),
        ["thread.model-selection-updated"],
      );

      const differentInstance = yield* orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("runtime-layer-model-selection-events-switch"),
        threadId,
        modelSelection: { instanceId: alternateInstanceId, model: "gpt-5.5" },
      });
      assert.deepEqual(
        differentInstance.storedEvents.map((stored) => stored.event.type),
        ["thread.provider-switched"],
      );
    }),
  );

  /**
   * A thread with one ready checkpoint and no queued turn start. Every provider
   * rollback on it fails, so each rollback effect retries until it gives up.
   */
  const seedFailingRollbackThread = (name: string) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threadId = ThreadId.make(name);
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make(`${name}-create`),
        threadId,
        projectId: ProjectId.make(`${name}-project`),
        title: "Rollback failure",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        // Its own path, so other rollback tests keep an isolated worktree.
        worktreePath: `/tmp/t3-${name}`,
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make(`${name}-message`),
        threadId,
        messageId: MessageId.make(`${name}-message`),
        text: "Create the provider thread and checkpoint scope.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
      });
      const scope = (yield* orchestrator.getThreadProjection(threadId)).checkpointScopes[0]!;
      const now = yield* DateTime.now;
      const checkpointId = CheckpointId.make(`${name}-checkpoint`);
      yield* eventSink.write({
        commandId: CommandId.make(`${name}-seed`),
        events: [
          {
            id: EventId.make(`${name}-checkpoint-event`),
            type: "checkpoint.captured",
            threadId,
            occurredAt: now,
            payload: {
              id: checkpointId,
              threadId,
              scopeId: scope.id,
              runId: null,
              nodeId: scope.nodeId,
              parentCheckpointId: null,
              ordinalWithinScope: 0,
              appRunOrdinal: null,
              ref: CheckpointRef.make(`refs/t3/${name}`),
              status: "ready",
              files: [],
              capturedAt: now,
            },
          },
        ],
      });
      // These tests cover rollback only, so drop the first message's start.
      yield* outbox.cancelUnsettled({
        threadId,
        effectTypes: ["provider-turn.start"],
        reason: "not under test",
      });
      return {
        threadId,
        rollback: (commandId: CommandId) =>
          orchestrator.dispatch({
            type: "checkpoint.rollback",
            commandId,
            threadId,
            checkpointId,
            scopeId: scope.id,
            restoreFiles: false,
          }),
      };
    });

  it.effect("projects a rollback that fails every attempt and clears it on the next one", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const { threadId, rollback } = yield* seedFailingRollbackThread("runtime-rollback-failure");

      const rollbackCommandId = CommandId.make("runtime-rollback-failure-rollback");
      yield* rollback(rollbackCommandId);
      // Retries back off on the clock; advance it until the worker gives up.
      for (let attempt = 0; attempt < 5; attempt++) {
        yield* worker.drain();
        yield* TestClock.adjust("30 seconds");
      }

      const [rollbackEffect] = yield* outbox.listByCommandId(rollbackCommandId);
      assert.equal(rollbackEffect?.status, "failed");
      const failed = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(failed.thread.rollbackFailure, {
        requestId: rollbackCommandId,
        message: ROLLBACK_FAILED_MESSAGE,
      });

      yield* rollback(CommandId.make("runtime-rollback-failure-retry"));
      const retried = yield* orchestrator.getThreadProjection(threadId);
      assert.isNull(retried.thread.rollbackFailure);
    }),
  );

  it.effect("ignores a late failure from a rollback that a newer one superseded", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const { threadId, rollback } = yield* seedFailingRollbackThread(
        "runtime-rollback-superseded",
      );

      const olderCommandId = CommandId.make("runtime-rollback-superseded-older");
      yield* rollback(olderCommandId);
      // Spend every attempt but the last.
      for (let attempt = 0; attempt < 4; attempt++) {
        yield* worker.drain();
        yield* TestClock.adjust("30 seconds");
      }
      const newerCommandId = CommandId.make("runtime-rollback-superseded-newer");
      yield* rollback(newerCommandId);
      // The older rollback's last attempt fails after the newer one started.
      yield* worker.drain();

      const [olderEffect] = yield* outbox.listByCommandId(olderCommandId);
      assert.equal(olderEffect?.status, "failed");
      const superseded = yield* orchestrator.getThreadProjection(threadId);
      assert.isNull(superseded.thread.rollbackFailure);

      for (let attempt = 0; attempt < 5; attempt++) {
        yield* TestClock.adjust("30 seconds");
        yield* worker.drain();
      }
      const [newerEffect] = yield* outbox.listByCommandId(newerCommandId);
      assert.equal(newerEffect?.status, "failed");
      const failed = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(failed.thread.rollbackFailure, {
        requestId: newerCommandId,
        message: ROLLBACK_FAILED_MESSAGE,
      });
    }),
  );

  it.effect("rejects non-ready rollback targets before persisting events or effects", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const threadId = ThreadId.make("runtime-rollback-readiness");
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-rollback-readiness-create"),
        threadId,
        projectId: ProjectId.make("runtime-rollback-readiness-project"),
        title: "Rollback readiness",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: process.cwd(),
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-rollback-readiness-message"),
        threadId,
        messageId: MessageId.make("runtime-rollback-readiness-message"),
        text: "Create the provider thread and checkpoint scope.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
      });
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const scope = projection.checkpointScopes[0]!;
      const now = yield* DateTime.now;

      for (const status of ["missing", "error", "stale", "ready"] as const) {
        const checkpointId = CheckpointId.make("runtime-rollback-checkpoint");
        const commandId = CommandId.make(`runtime-rollback-${status}`);
        yield* eventSink.write({
          commandId: CommandId.make(`runtime-rollback-${status}-seed`),
          events: [
            {
              id: EventId.make(`runtime-rollback-${status}-event`),
              type: "checkpoint.captured",
              threadId,
              occurredAt: now,
              payload: {
                id: checkpointId,
                threadId,
                scopeId: scope.id,
                runId: null,
                nodeId: scope.nodeId,
                parentCheckpointId: null,
                ordinalWithinScope: 0,
                appRunOrdinal: null,
                ref: CheckpointRef.make(`refs/t3/runtime-rollback-${status}`),
                status,
                files: [],
                capturedAt: now,
              },
            },
          ],
        });
        const previousSequence = yield* orchestrator.getThreadEventSequence(threadId);
        const rollback = orchestrator.dispatch({
          type: "checkpoint.rollback",
          commandId,
          threadId,
          checkpointId,
          scopeId: scope.id,
        });

        if (status === "ready") {
          const accepted = yield* rollback;
          assert.deepEqual(
            accepted.storedEvents.map((stored) => stored.event.type),
            ["thread.metadata-updated", "checkpoint.rollback-requested"],
          );
          assert.deepEqual(
            (yield* outbox.listByCommandId(commandId)).map((effect) => effect.request.type),
            ["provider-thread.rollback"],
          );
          const path = yield* Path.Path.pipe(Effect.provide(NodeServices.layer));
          yield* orchestrator.dispatch({
            type: "thread.create",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make("runtime-rollback-ancestor-create"),
            threadId: ThreadId.make("runtime-rollback-ancestor"),
            projectId: ProjectId.make("runtime-rollback-readiness-project"),
            title: "Ancestor workspace owner",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: path.dirname(process.cwd()),
          });
          const overlapCommandId = CommandId.make("runtime-rollback-overlap");
          const overlapSequence = yield* orchestrator.getThreadEventSequence(threadId);
          const overlap = yield* orchestrator
            .dispatch({
              type: "checkpoint.rollback",
              commandId: overlapCommandId,
              threadId,
              checkpointId,
              scopeId: scope.id,
            })
            .pipe(Effect.flip);
          assert.match(String(overlap.cause), /isolated worktree/);
          assert.equal(yield* orchestrator.getThreadEventSequence(threadId), overlapSequence);
          assert.deepEqual(yield* outbox.listByCommandId(overlapCommandId), []);
          yield* orchestrator.dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make("runtime-rollback-share"),
            threadId,
            worktreePath: null,
          });
          const sharedCommandId = CommandId.make("runtime-rollback-shared");
          const sequence = yield* orchestrator.getThreadEventSequence(threadId);
          const shared = yield* orchestrator
            .dispatch({
              type: "checkpoint.rollback",
              commandId: sharedCommandId,
              threadId,
              checkpointId,
              scopeId: scope.id,
            })
            .pipe(Effect.flip);
          assert.match(String(shared.cause), /isolated worktree/);
          assert.equal(yield* orchestrator.getThreadEventSequence(threadId), sequence);
          assert.deepEqual(yield* outbox.listByCommandId(sharedCommandId), []);
          const conversationOnly = yield* orchestrator.dispatch({
            type: "checkpoint.rollback",
            commandId: CommandId.make("runtime-rollback-conversation"),
            threadId,
            checkpointId,
            scopeId: scope.id,
            restoreFiles: false,
          });
          assert.deepEqual(
            conversationOnly.storedEvents.map((stored) => stored.event.type),
            ["thread.metadata-updated", "checkpoint.rollback-requested"],
          );
        } else {
          const error = yield* rollback.pipe(Effect.flip);
          assert.instanceOf(error, Orchestrator.OrchestratorDispatchError);
          assert.equal(
            error.cause,
            `Checkpoint ${checkpointId} is ${status} and cannot be restored.`,
          );
          assert.equal(yield* orchestrator.getThreadEventSequence(threadId), previousSequence);
          assert.deepEqual(
            yield* eventSink.readByCommandId({ commandId }).pipe(Stream.runCollect),
            [],
          );
          assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
        }
      }
    }).pipe(Effect.provide(Layer.fresh(layerTest))),
  );

  it.effect("resolves delivery intent against the active run and starts after it completes", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const threadId = ThreadId.make("runtime-delivery-intent");
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-delivery-intent-create"),
        threadId,
        projectId: ProjectId.make("runtime-delivery-intent-project"),
        title: "Delivery intent",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: process.cwd(),
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-delivery-intent-first"),
        threadId,
        messageId: MessageId.make("runtime-delivery-intent-first"),
        text: "Start work.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
      });
      const initial = yield* orchestrator.getThreadProjection(threadId);
      const run = initial.runs[0]!;
      const providerThread = initial.providerThreads[0]!;
      const now = yield* DateTime.now;
      const providerSession = {
        id: providerThread.providerSessionId!,
        driver,
        providerInstanceId: modelSelection.instanceId,
        status: "running" as const,
        cwd: process.cwd(),
        model: modelSelection.model,
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const providerTurn = {
        id: ProviderTurnId.make("runtime-delivery-intent-turn"),
        providerThreadId: providerThread.id,
        nodeId: run.rootNodeId!,
        runAttemptId: run.activeAttemptId,
        nativeTurnRef: null,
        ordinal: 1,
        status: "running" as const,
        startedAt: now,
        completedAt: null,
      };
      yield* eventSink.write({
        commandId: CommandId.make("runtime-delivery-intent-running"),
        events: [
          {
            id: EventId.make("runtime-delivery-intent-run-event"),
            type: "run.updated",
            threadId,
            runId: run.id,
            occurredAt: now,
            payload: { ...run, status: "running", startedAt: now },
          },
          {
            id: EventId.make("runtime-delivery-intent-session-event"),
            type: "provider-session.attached",
            threadId,
            occurredAt: now,
            payload: providerSession,
          },
          {
            id: EventId.make("runtime-delivery-intent-turn-event"),
            type: "provider-turn.updated",
            threadId,
            runId: run.id,
            occurredAt: now,
            payload: providerTurn,
          },
        ],
      });
      const sessionSpy = vi
        .spyOn(sessions, "get")
        .mockReturnValue(
          Effect.succeed(
            Option.some({ providerSession } as ProviderAdapter.ProviderAdapterV2SessionRuntime),
          ),
        );
      yield* Effect.addFinalizer(() => Effect.sync(() => sessionSpy.mockRestore()));

      const steerCommandId = CommandId.make("runtime-delivery-intent-auto");
      const steerMessageId = MessageId.make("runtime-delivery-intent-auto");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: steerCommandId,
        threadId,
        messageId: steerMessageId,
        text: "Include this in the active work.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        deliveryIntent: "auto",
      });
      const steered = yield* orchestrator.getThreadProjection(threadId);
      assert.lengthOf(steered.runs, 1);
      assert.equal(
        steered.messages.find((message) => message.id === steerMessageId)?.runId,
        run.id,
      );
      assert.deepEqual(
        (yield* outbox.listByCommandId(steerCommandId)).map((effect) => effect.request),
        [
          {
            type: "provider-turn.steer",
            providerSessionId: providerSession.id,
            providerThreadId: providerThread.id,
            providerTurnId: providerTurn.id,
            messageId: steerMessageId,
          },
        ],
      );

      yield* eventSink.write({
        commandId: CommandId.make("runtime-delivery-intent-completed"),
        events: [
          {
            id: EventId.make("runtime-delivery-intent-run-completed"),
            type: "run.updated",
            threadId,
            runId: run.id,
            occurredAt: now,
            payload: { ...run, status: "completed", startedAt: now, completedAt: now },
          },
          {
            id: EventId.make("runtime-delivery-intent-turn-completed"),
            type: "provider-turn.updated",
            threadId,
            runId: run.id,
            occurredAt: now,
            payload: { ...providerTurn, status: "completed", completedAt: now },
          },
        ],
      });
      const nextCommandId = CommandId.make("runtime-delivery-intent-next");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: nextCommandId,
        threadId,
        messageId: MessageId.make("runtime-delivery-intent-next"),
        text: "The previous run finished before this arrived.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        deliveryIntent: "restart",
      });
      const restarted = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(
        restarted.runs.map((candidate) => candidate.status),
        ["completed", "starting"],
      );
      assert.deepEqual(
        (yield* outbox.listByCommandId(nextCommandId)).map((effect) => effect.request.type),
        ["provider-turn.start"],
      );
    }),
  );

  it.effect("answers an async question after its provider exits and commits the answer once", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("runtime-async-question");
      const requestId = RuntimeRequestId.make("runtime-async-question-request");
      const nodeId = NodeId.make("runtime-async-question-node");
      const itemId = TurnItemId.make("runtime-async-question-item");
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("runtime-async-question-create"),
        createdBy: "user",
        creationSource: "web",
        threadId,
        projectId: ProjectId.make("runtime-async-question-project"),
        title: "Async question",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: process.cwd(),
      });
      yield* eventSink.write({
        commandId: CommandId.make("runtime-async-question-seed"),
        events: [
          {
            id: EventId.make("runtime-async-question-node-event"),
            type: "node.updated",
            threadId,
            nodeId,
            occurredAt: now,
            payload: {
              id: nodeId,
              threadId,
              runId: null,
              parentNodeId: null,
              rootNodeId: nodeId,
              kind: "user_input_request",
              status: "waiting",
              countsForRun: false,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              runtimeRequestId: requestId,
              checkpointScopeId: null,
              startedAt: now,
              completedAt: null,
            },
          },
          {
            id: EventId.make("runtime-async-question-request-event"),
            type: "runtime-request.updated",
            threadId,
            nodeId,
            occurredAt: now,
            payload: {
              id: requestId,
              nodeId,
              providerTurnId: null,
              nativeRequestRef: null,
              kind: "user_input",
              status: "pending",
              responseCapability: { type: "message" },
              createdAt: now,
              resolvedAt: null,
            },
          },
          {
            id: EventId.make("runtime-async-question-item-event"),
            type: "turn-item.updated",
            threadId,
            nodeId,
            occurredAt: now,
            payload: {
              id: itemId,
              type: "user_input_request",
              threadId,
              runId: null,
              nodeId,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 0,
              status: "waiting",
              title: null,
              startedAt: now,
              completedAt: null,
              updatedAt: now,
              requestId,
              responseMode: "message",
              questions: [{ id: "color", header: "Color", question: "Which color?", options: [] }],
            },
          },
        ],
      });
      const invalid = yield* orchestrator
        .dispatch({
          type: "runtime-request.respond",
          commandId: CommandId.make("runtime-async-question-blank"),
          threadId,
          requestId,
          answers: { color: " " },
        })
        .pipe(Effect.result);
      assert.equal(invalid._tag, "Failure");
      const unanswered = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(unanswered.runtimeRequests[0]?.status, "pending");
      assert.deepEqual(unanswered.messages, []);

      const command = {
        type: "runtime-request.respond" as const,
        commandId: CommandId.make("runtime-async-question-answer"),
        threadId,
        requestId,
        answers: { color: "  Blue  " },
      };
      const accepted = yield* orchestrator.dispatch(command);
      const repeated = yield* orchestrator.dispatch(command);
      assert.equal(repeated.sequence, accepted.sequence);
      const answered = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(answered.runtimeRequests[0]?.status, "resolved");
      assert.deepEqual(answered.runtimeRequests[0]?.answers, command.answers);
      assert.equal(answered.nodes.find((node) => node.id === nodeId)?.status, "completed");
      assert.equal(answered.turnItems.find((item) => item.id === itemId)?.status, "completed");
      const answeredItem = answered.turnItems.find((item) => item.id === itemId);
      assert.equal(answeredItem?.type, "user_input_request");
      if (answeredItem?.type === "user_input_request") {
        assert.deepEqual(answeredItem.questionAnswer, {
          requestId,
          answers: command.answers,
          attachmentsByQuestionId: {},
          questionTextById: { color: "Which color?" },
        });
      }
      assert.equal(answered.messages.length, 1);
      assert.equal(answered.messages[0]?.text, "Which color?\nBlue");
      assert.equal(answered.messages[0]?.role, "user");
      assert.equal(answered.runs.length, 1);

      const duplicate = yield* orchestrator
        .dispatch({
          ...command,
          commandId: CommandId.make("runtime-async-question-duplicate"),
        })
        .pipe(Effect.result);
      assert.equal(duplicate._tag, "Failure");
      assert.equal((yield* orchestrator.getThreadProjection(threadId)).messages.length, 1);
    }),
  );

  it.effect("dismisses message-capable questions directly and while settling", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;

      const seedQuestion = Effect.fn("runtimeLayerTest.seedQuestion")(function* (name: string) {
        const threadId = ThreadId.make(`${name}-thread`);
        const requestId = RuntimeRequestId.make(`${name}-request`);
        const nodeId = NodeId.make(`${name}-node`);
        const itemId = TurnItemId.make(`${name}-item`);
        const now = yield* DateTime.now;
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`${name}-create`),
          createdBy: "user",
          creationSource: "web",
          threadId,
          projectId: ProjectId.make(`${name}-project`),
          title: "Dismissible question",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: process.cwd(),
        });
        yield* eventSink.write({
          commandId: CommandId.make(`${name}-seed`),
          events: [
            {
              id: EventId.make(`${name}-node-event`),
              type: "node.updated",
              threadId,
              nodeId,
              occurredAt: now,
              payload: {
                id: nodeId,
                threadId,
                runId: null,
                parentNodeId: null,
                rootNodeId: nodeId,
                kind: "user_input_request",
                status: "waiting",
                countsForRun: false,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: null,
                runtimeRequestId: requestId,
                checkpointScopeId: null,
                startedAt: now,
                completedAt: null,
              },
            },
            {
              id: EventId.make(`${name}-request-event`),
              type: "runtime-request.updated",
              threadId,
              nodeId,
              occurredAt: now,
              payload: {
                id: requestId,
                nodeId,
                providerTurnId: null,
                nativeRequestRef: null,
                kind: "user_input",
                status: "pending",
                responseCapability: { type: "message" },
                createdAt: now,
                resolvedAt: null,
              },
            },
            {
              id: EventId.make(`${name}-item-event`),
              type: "turn-item.updated",
              threadId,
              nodeId,
              occurredAt: now,
              payload: {
                id: itemId,
                type: "user_input_request",
                threadId,
                runId: null,
                nodeId,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 0,
                status: "waiting",
                title: null,
                startedAt: now,
                completedAt: null,
                updatedAt: now,
                requestId,
                responseMode: "message",
                questions: [{ id: "choice", header: "Choice", question: "Continue?", options: [] }],
              },
            },
          ],
        });
        return { threadId, requestId, nodeId, itemId };
      });

      const dismissed = yield* seedQuestion("runtime-dismiss-question");
      yield* orchestrator.dispatch({
        type: "thread.user-input.dismiss",
        commandId: CommandId.make("runtime-dismiss-question-command"),
        threadId: dismissed.threadId,
        requestId: dismissed.requestId,
      });
      const dismissedProjection = yield* orchestrator.getThreadProjection(dismissed.threadId);
      assert.equal(dismissedProjection.runtimeRequests[0]?.status, "resolved");
      assert.equal(dismissedProjection.runtimeRequests[0]?.decision, "cancel");
      assert.equal(
        dismissedProjection.nodes.find((node) => node.id === dismissed.nodeId)?.status,
        "cancelled",
      );
      assert.equal(
        dismissedProjection.turnItems.find((item) => item.id === dismissed.itemId)?.status,
        "cancelled",
      );
      assert.lengthOf(dismissedProjection.messages, 0);

      const settled = yield* seedQuestion("runtime-settle-question");
      yield* orchestrator.dispatch({
        type: "thread.settle",
        commandId: CommandId.make("runtime-settle-question-command"),
        threadId: settled.threadId,
      });
      const settledProjection = yield* orchestrator.getThreadProjection(settled.threadId);
      assert.equal(settledProjection.thread.settledOverride, "settled");
      assert.equal(settledProjection.runtimeRequests[0]?.status, "resolved");
      assert.equal(settledProjection.runtimeRequests[0]?.decision, "cancel");
      assert.equal(
        settledProjection.nodes.find((node) => node.id === settled.nodeId)?.status,
        "cancelled",
      );
      assert.equal(
        settledProjection.turnItems.find((item) => item.id === settled.itemId)?.status,
        "cancelled",
      );
    }),
  );

  it.effect("merges an explicit provider-finished run while checkpoint capture is pending", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      const projectId = ProjectId.make("runtime-layer-waiting-merge-project");
      const targetThreadId = ThreadId.make("runtime-layer-waiting-merge-target");
      const sourceThreadId = ThreadId.make("runtime-layer-waiting-merge-source");
      const baseRunId = RunId.make("runtime-layer-waiting-merge-base-run");
      const sourceRunId = RunId.make("runtime-layer-waiting-merge-source-run");
      const sourceProviderThreadId = ProviderThreadId.make(
        "runtime-layer-waiting-merge-provider-thread",
      );
      const forkTransferId = ContextTransferId.make("runtime-layer-waiting-merge-fork-transfer");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-waiting-merge-create-target"),
        threadId: targetThreadId,
        projectId,
        title: "Waiting merge target",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const target = yield* orchestrator.getThreadProjection(targetThreadId);

      yield* eventSink.write({
        commandId: CommandId.make("runtime-layer-waiting-merge-seed"),
        events: [
          {
            id: EventId.make("runtime-layer-waiting-merge-source-thread-event"),
            type: "thread.created",
            threadId: sourceThreadId,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: now,
            payload: {
              ...target.thread,
              id: sourceThreadId,
              title: "Waiting merge source",
              activeProviderThreadId: null,
              lineage: {
                parentThreadId: targetThreadId,
                relationshipToParent: "fork",
                rootThreadId: targetThreadId,
              },
              forkedFrom: {
                type: "run",
                threadId: targetThreadId,
                runId: baseRunId,
              },
              createdAt: now,
              updatedAt: now,
            },
          },
          {
            id: EventId.make("runtime-layer-waiting-merge-fork-transfer-event"),
            type: "context-transfer.created",
            threadId: sourceThreadId,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: now,
            payload: {
              id: forkTransferId,
              type: "fork",
              sourceThreadId: targetThreadId,
              targetThreadId: sourceThreadId,
              sourcePoint: { threadId: targetThreadId, runId: baseRunId },
              basePoint: null,
              sourceProviderInstanceId: modelSelection.instanceId,
              targetProviderInstanceId: modelSelection.instanceId,
              targetRunId: null,
              status: "consumed",
              resolution: null,
              createdBy: "user",
              error: null,
              createdAt: now,
              updatedAt: now,
              consumedAt: now,
            },
          },
          {
            id: EventId.make("runtime-layer-waiting-merge-provider-thread-event"),
            type: "provider-thread.updated",
            threadId: sourceThreadId,
            driver,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: now,
            payload: {
              id: sourceProviderThreadId,
              driver,
              providerInstanceId: modelSelection.instanceId,
              providerSessionId: null,
              appThreadId: sourceThreadId,
              ownerNodeId: null,
              nativeThreadRef: {
                driver,
                nativeId: "native-waiting-merge-source",
                strength: "strong",
              },
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: 1,
              lastRunOrdinal: 1,
              handoffIds: [],
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
            },
          },
          {
            id: EventId.make("runtime-layer-waiting-merge-source-run-event"),
            type: "run.created",
            threadId: sourceThreadId,
            runId: sourceRunId,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: now,
            payload: {
              id: sourceRunId,
              threadId: sourceThreadId,
              ordinal: 1,
              providerInstanceId: modelSelection.instanceId,
              modelSelection,
              providerThreadId: sourceProviderThreadId,
              userMessageId: MessageId.make("runtime-layer-waiting-merge-message"),
              rootNodeId: null,
              activeAttemptId: null,
              status: "waiting",
              queuePosition: null,
              requestedAt: now,
              startedAt: now,
              completedAt: null,
              checkpointId: null,
              contextHandoffId: null,
            },
          },
        ],
      });

      yield* orchestrator.dispatch({
        type: "thread.merge_back",
        createdBy: "user",
        creationSource: "mobile",
        commandId: CommandId.make("runtime-layer-waiting-merge"),
        sourceThreadId,
        targetThreadId,
        sourcePoint: { type: "run", runId: sourceRunId },
        createdAt: now,
      });

      const mergedTarget = yield* orchestrator.getThreadProjection(targetThreadId);
      const transfer = mergedTarget.contextTransfers.find(
        (candidate) => candidate.type === "merge_back",
      );
      assert.isDefined(transfer);
      assert.equal(transfer.status, "pending");
      assert.equal(transfer.sourceThreadId, sourceThreadId);
      assert.equal(transfer.targetThreadId, targetThreadId);
      assert.equal(transfer.sourcePoint.runId, sourceRunId);
      assert.isUndefined(transfer.sourcePoint.checkpointId);
      assert.equal(transfer.sourcePoint.providerThreadRef?.nativeId, "native-waiting-merge-source");
      assert.equal(transfer.basePoint?.runId, baseRunId);
      assert.isNull(transfer.error);
    }),
  );
});

it.layer(layerLegacyImportTest)("OrchestrationV2 legacy import", (it) => {
  it.effect("hydrates imported transcripts before commands and propagates hydration failures", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadManagement = yield* ThreadManagementService.ThreadManagementService;
      const metadataThreadId = ThreadId.make("runtime-layer-legacy-metadata-thread");
      const failureThreadId = ThreadId.make("runtime-layer-legacy-failure-thread");
      const projectId = ProjectId.make("runtime-layer-legacy-project");

      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        ) VALUES (
          ${projectId},
          'Legacy project',
          '/tmp/runtime-layer-legacy-project',
          '{"instanceId":"codex","model":"gpt-5.4"}',
          '[]',
          '2026-01-01T00:00:00.000Z',
          '2026-01-04T00:00:00.000Z',
          NULL
        )
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          created_at,
          updated_at,
          archived_at,
          settled_override,
          settled_at,
          deleted_at
        ) VALUES
          (
            ${metadataThreadId},
            ${projectId},
            'Legacy metadata title',
            '{"instanceId":"codex","model":"gpt-5.4"}',
            'full-access',
            'default',
            'main',
            '/tmp/runtime-layer-legacy-project',
            NULL,
            '2026-01-01T00:00:00.000Z',
            '2026-01-04T00:00:00.000Z',
            NULL,
            NULL,
            NULL,
            NULL
          ),
          (
            ${failureThreadId},
            ${projectId},
            'Legacy failure title',
            '{"instanceId":"codex","model":"gpt-5.4"}',
            'full-access',
            'default',
            'main',
            '/tmp/runtime-layer-legacy-project',
            NULL,
            '2026-01-01T00:00:00.000Z',
            '2026-01-04T00:00:00.000Z',
            NULL,
            NULL,
            NULL,
            NULL
          )
      `;
      yield* sql`
        INSERT INTO projection_thread_messages (
          message_id,
          thread_id,
          turn_id,
          role,
          text,
          attachments_json,
          is_streaming,
          created_at,
          updated_at
        ) VALUES
          (
            'message:runtime-layer-legacy:1',
            ${metadataThreadId},
            NULL,
            'user',
            'First imported question',
            '[]',
            0,
            '2026-01-01T01:00:00.000Z',
            '2026-01-01T01:00:00.000Z'
          ),
          (
            'message:runtime-layer-legacy:2',
            ${metadataThreadId},
            NULL,
            'assistant',
            'First imported answer',
            '[]',
            0,
            '2026-01-02T01:00:00.000Z',
            '2026-01-02T01:00:00.000Z'
          ),
          (
            'message:runtime-layer-legacy:3',
            ${metadataThreadId},
            NULL,
            'user',
            'Latest imported question',
            '[]',
            0,
            '2026-01-03T01:00:00.000Z',
            '2026-01-03T01:00:00.000Z'
          ),
          (
            'message:runtime-layer-legacy:failure',
            ${failureThreadId},
            NULL,
            'user',
            'Imported context must load before archive',
            '[]',
            0,
            '2026-01-03T01:00:00.000Z',
            '2026-01-03T01:00:00.000Z'
          )
      `;

      yield* importer.reconcileShells;
      assert.isTrue((yield* maintenance.verify).valid);

      yield* threadManagement.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("runtime-layer-legacy-metadata-update"),
        threadId: metadataThreadId,
        title: "Updated after import",
      });
      const updatedProjection = yield* threadManagement.getThreadProjection(metadataThreadId);
      assert.equal(updatedProjection.thread.title, "Updated after import");
      assert.deepEqual(
        updatedProjection.messages.map((message) => message.text),
        ["First imported question", "First imported answer", "Latest imported question"],
      );

      yield* sql`
        ALTER TABLE projection_thread_messages
        RENAME TO projection_thread_messages_unavailable
      `;
      const { projectionFailure, hydrationFailure } = yield* Effect.all({
        projectionFailure: threadManagement.getThreadProjection(failureThreadId).pipe(Effect.flip),
        hydrationFailure: threadManagement
          .dispatch({
            type: "thread.archive",
            commandId: CommandId.make("runtime-layer-legacy-failed-archive"),
            threadId: failureThreadId,
          })
          .pipe(Effect.flip),
      }).pipe(
        Effect.ensuring(
          sql`
            ALTER TABLE projection_thread_messages_unavailable
            RENAME TO projection_thread_messages
          `.pipe(Effect.orDie),
        ),
      );
      assert.instanceOf(projectionFailure, Orchestrator.OrchestratorProjectionError);
      assert.instanceOf(projectionFailure.cause, LegacyV1ThreadImporter.LegacyV1ThreadImportError);
      assert.instanceOf(hydrationFailure, Orchestrator.OrchestratorDispatchError);
      assert.instanceOf(hydrationFailure.cause, LegacyV1ThreadImporter.LegacyV1ThreadImportError);

      const projectionAfterFailure = yield* orchestrator.getThreadProjection(failureThreadId);
      assert.isNull(projectionAfterFailure.thread.archivedAt);

      yield* threadManagement.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("runtime-layer-legacy-retried-archive"),
        threadId: failureThreadId,
      });
      const projectionAfterRetry = yield* threadManagement.getThreadProjection(failureThreadId);
      assert.isNotNull(projectionAfterRetry.thread.archivedAt);
    }),
  );
});

it.layer(layerTest)("OrchestrationV2LayerLive lifecycle", (it) => {
  it.effect("applies lifecycle commands idempotently and emits archive/removal shell deltas", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-lifecycle-thread");
      const projectId = ProjectId.make("runtime-layer-lifecycle-project");
      const project = {
        projectId,
        title: "Lifecycle project",
        workspaceRoot: "/workspace/project",
        defaultModelSelection: null,
        createdAt: "2026-09-07T00:00:00.000Z",
      } as const;
      yield* seedProject(project);
      const create = {
        type: "thread.create" as const,
        createdBy: "user" as const,
        creationSource: "web" as const,
        commandId: CommandId.make("runtime-layer-lifecycle-create"),
        threadId,
        projectId,
        title: "Lifecycle thread",
        modelSelection,
        runtimeMode: "full-access" as const,
        interactionMode: "default" as const,
        branch: null,
        worktreePath: null,
      };

      const firstCreate = yield* orchestrator.dispatch(create);
      const retriedCreate = yield* orchestrator.dispatch(create);
      assert.equal(retriedCreate.sequence, firstCreate.sequence);
      assert.lengthOf(retriedCreate.storedEvents, 1);

      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("runtime-layer-lifecycle-metadata"),
        threadId,
        title: "Renamed lifecycle thread",
        branch: "feature/v2",
        worktreePath: "/tmp/t3-v2-worktree",
      });
      const staleWorkspaceUpdate = yield* orchestrator
        .dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("runtime-layer-lifecycle-stale-workspace"),
          threadId,
          branch: "feature/stale",
          worktreePath: "/tmp/stale-worktree",
          expectedWorktreePath: null,
        })
        .pipe(Effect.flip);
      assert.instanceOf(staleWorkspaceUpdate, Orchestrator.OrchestratorDispatchError);
      const projectionAfterStaleWorkspaceUpdate = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projectionAfterStaleWorkspaceUpdate.thread.branch, "feature/v2");
      assert.equal(projectionAfterStaleWorkspaceUpdate.thread.worktreePath, "/tmp/t3-v2-worktree");
      const pullRequestSnapshot = yield* orchestrator.getShellSnapshot();
      const pullRequest = {
        projectId,
        repository: "owner/repository",
        number: 24,
        url: "https://github.com/owner/repository/pull/24",
      };
      yield* moveProject(projectId, "/workspace/moved", "2026-09-07T00:01:00.000Z");
      const staleProjectWorkspace = yield* orchestrator
        .dispatch({
          type: "thread.pull-request.sync",
          commandId: CommandId.make("runtime-layer-lifecycle-pr-sync-stale-project-workspace"),
          threadId,
          projectId,
          snapshotSequence: pullRequestSnapshot.snapshotSequence,
          expected: {
            workspaceRoot: "/workspace/project",
            branch: "feature/v2",
            worktreePath: "/tmp/t3-v2-worktree",
            linkedPullRequest: null,
            branchPullRequest: null,
          },
          branchPullRequest: pullRequest,
        })
        .pipe(Effect.flip);
      assert.instanceOf(staleProjectWorkspace, Orchestrator.OrchestratorDispatchError);
      yield* moveProject(projectId, project.workspaceRoot, "2026-09-07T00:02:00.000Z");
      yield* orchestrator.dispatch({
        type: "thread.pull-request.sync",
        commandId: CommandId.make("runtime-layer-lifecycle-pr-sync"),
        threadId,
        projectId,
        snapshotSequence: pullRequestSnapshot.snapshotSequence,
        expected: {
          workspaceRoot: "/workspace/project",
          branch: "feature/v2",
          worktreePath: "/tmp/t3-v2-worktree",
          linkedPullRequest: null,
          branchPullRequest: null,
        },
        branchPullRequest: pullRequest,
      });
      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(threadId)).thread.branchPullRequest,
        pullRequest,
      );
      const stalePullRequestSync = yield* orchestrator
        .dispatch({
          type: "thread.pull-request.sync",
          commandId: CommandId.make("runtime-layer-lifecycle-pr-sync-stale"),
          threadId,
          projectId,
          snapshotSequence: pullRequestSnapshot.snapshotSequence,
          expected: {
            workspaceRoot: "/workspace/project",
            branch: "feature/v2",
            worktreePath: "/tmp/t3-v2-worktree",
            linkedPullRequest: null,
            branchPullRequest: null,
          },
          branchPullRequest: null,
        })
        .pipe(Effect.flip);
      assert.instanceOf(stalePullRequestSync, Orchestrator.OrchestratorDispatchError);
      yield* orchestrator.dispatch({
        type: "thread.runtime-mode.set",
        commandId: CommandId.make("runtime-layer-lifecycle-runtime"),
        threadId,
        runtimeMode: "approval-required",
      });
      yield* orchestrator.dispatch({
        type: "thread.interaction-mode.set",
        commandId: CommandId.make("runtime-layer-lifecycle-interaction"),
        threadId,
        interactionMode: "plan",
      });
      yield* orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("runtime-layer-lifecycle-model"),
        threadId,
        modelSelection: { ...modelSelection, model: "gpt-5.5" },
      });
      yield* orchestrator.dispatch({
        type: "thread.active.reorder",
        commandId: CommandId.make("runtime-layer-lifecycle-active-order"),
        threadId,
        orderKey: "a0",
      });
      assert.equal((yield* orchestrator.getThreadProjection(threadId)).thread.activeOrderKey, "a0");

      // Automatic settlement (#8600): a stale snapshot loses to any change
      // made after it, and a fresh one settles like a user settle would.
      const preAutoProjection = yield* orchestrator.getThreadProjection(threadId);
      const staleAutoSettle = yield* orchestrator
        .dispatch({
          type: "thread.auto-settle",
          commandId: CommandId.make("runtime-layer-lifecycle-auto-settle-stale"),
          threadId,
          snapshotAt: DateTime.makeUnsafe(
            DateTime.toEpochMillis(preAutoProjection.thread.updatedAt) - 1,
          ),
        })
        .pipe(Effect.flip);
      assert.instanceOf(staleAutoSettle, Orchestrator.OrchestratorDispatchError);
      yield* orchestrator.dispatch({
        type: "thread.auto-settle",
        commandId: CommandId.make("runtime-layer-lifecycle-auto-settle"),
        threadId,
        snapshotAt: preAutoProjection.thread.updatedAt,
      });
      const autoSettledProjection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(autoSettledProjection.thread.settledOverride, "settled");
      assert.isNull(autoSettledProjection.thread.activeOrderKey);
      yield* orchestrator.dispatch({
        type: "thread.unsettle",
        commandId: CommandId.make("runtime-layer-lifecycle-auto-unsettle"),
        threadId,
        reason: "user",
      });
      // An explicit un-settle outranks the sweep even with a fresh snapshot.
      const postUnsettleProjection = yield* orchestrator.getThreadProjection(threadId);
      const overriddenAutoSettle = yield* orchestrator
        .dispatch({
          type: "thread.auto-settle",
          commandId: CommandId.make("runtime-layer-lifecycle-auto-settle-overridden"),
          threadId,
          snapshotAt: postUnsettleProjection.thread.updatedAt,
        })
        .pipe(Effect.flip);
      assert.instanceOf(overriddenAutoSettle, Orchestrator.OrchestratorDispatchError);

      yield* orchestrator.dispatch({
        type: "thread.settle",
        commandId: CommandId.make("runtime-layer-lifecycle-settle"),
        threadId,
      });
      const settledProjection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(settledProjection.thread.settledOverride, "settled");
      assert.isNotNull(settledProjection.thread.settledAt);

      yield* orchestrator.dispatch({
        type: "thread.unsettle",
        commandId: CommandId.make("runtime-layer-lifecycle-unsettle"),
        threadId,
        reason: "user",
      });
      const activeProjection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(activeProjection.thread.settledOverride, "active");
      assert.isNull(activeProjection.thread.settledAt);
      assert.isNotNull(activeProjection.thread.unsettledAt);
      const activeShell = (yield* orchestrator.getShellSnapshot()).threads.find(
        (thread) => thread.id === threadId,
      );
      assert.deepEqual(activeShell?.unsettledAt, activeProjection.thread.unsettledAt);

      const archive = yield* orchestrator.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("runtime-layer-lifecycle-archive"),
        threadId,
      });
      const archivedShell = yield* orchestrator.getShellSnapshot();
      assert.notInclude(
        archivedShell.threads.map((thread) => thread.id),
        threadId,
      );
      assert.include(
        archivedShell.archivedThreads.map((thread) => thread.id),
        threadId,
      );
      const activeOnlyShell = yield* orchestrator.getShellSnapshot({ location: "active" });
      assert.notInclude(
        activeOnlyShell.threads.map((thread) => thread.id),
        threadId,
      );
      assert.lengthOf(activeOnlyShell.archivedThreads, 0);
      const archiveOnlyShell = yield* orchestrator.getShellSnapshot({ location: "archive" });
      assert.lengthOf(archiveOnlyShell.threads, 0);
      assert.include(
        archiveOnlyShell.archivedThreads.map((thread) => thread.id),
        threadId,
      );
      assert.deepEqual(
        shellStreamItemFromThreadShell({
          stored: archive.storedEvents[0]!,
          shell: yield* orchestrator.getThreadShell(threadId),
        }),
        {
          kind: "thread.removed",
          sequence: archive.sequence,
          location: "active",
          threadId,
        },
      );

      const remove = yield* orchestrator.dispatch({
        type: "thread.delete",
        commandId: CommandId.make("runtime-layer-lifecycle-delete"),
        threadId,
      });
      const deletedShell = yield* orchestrator.getShellSnapshot();
      assert.notInclude(
        deletedShell.threads.map((thread) => thread.id),
        threadId,
      );
      assert.notInclude(
        deletedShell.archivedThreads.map((thread) => thread.id),
        threadId,
      );
      assert.deepEqual(
        shellStreamItemFromThreadShell({
          stored: remove.storedEvents[0]!,
          shell: yield* orchestrator.getThreadShell(threadId),
        }),
        {
          kind: "thread.removed",
          sequence: remove.sequence,
          location: "active",
          threadId,
        },
      );

      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.thread.title, "Renamed lifecycle thread");
      assert.equal(projection.thread.branch, "feature/v2");
      assert.equal(projection.thread.worktreePath, "/tmp/t3-v2-worktree");
      assert.equal(projection.thread.runtimeMode, "approval-required");
      assert.equal(projection.thread.interactionMode, "plan");
      assert.equal(projection.thread.modelSelection.model, "gpt-5.5");
      assert.isNotNull(projection.thread.archivedAt);
      assert.isNotNull(projection.thread.deletedAt);
    }),
  );

  it.effect("persists linked pull requests through projection rebuilds and unlinking", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const threadId = ThreadId.make("runtime-layer-linked-pull-request-thread");
      const linkedPullRequest = {
        projectId: ProjectId.make("runtime-layer-linked-pull-request-project"),
        repository: "pingdotgg/t3code",
        number: 8160,
        url: "https://github.com/pingdotgg/t3code/pull/8160",
      } as const;

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-linked-pull-request-create"),
        threadId,
        projectId: linkedPullRequest.projectId,
        title: "Linked pull request thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("runtime-layer-linked-pull-request-link"),
        threadId,
        linkedPullRequest,
      });

      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(threadId)).thread.linkedPullRequest,
        linkedPullRequest,
      );
      const linkedShell = yield* orchestrator.getThreadShell(threadId);
      assert.isNotNull(linkedShell);
      assert.deepEqual(linkedShell.linkedPullRequest, linkedPullRequest);

      const rebuilt = yield* maintenance.rebuild;
      assert.isTrue(rebuilt.valid);
      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(threadId)).thread.linkedPullRequest,
        linkedPullRequest,
      );

      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("runtime-layer-linked-pull-request-unlink"),
        threadId,
        linkedPullRequest: null,
      });
      assert.isNull((yield* orchestrator.getThreadProjection(threadId)).thread.linkedPullRequest);
      const unlinkedShell = yield* orchestrator.getThreadShell(threadId);
      assert.isNotNull(unlinkedShell);
      assert.isNull(unlinkedShell.linkedPullRequest);
    }),
  );

  it.effect("keeps the branch pull request when linking another pull request", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const threadId = ThreadId.make("branch-pr-link");
      const projectId = ProjectId.make("branch-pr-project");
      yield* seedProject({
        projectId,
        title: "PR links",
        workspaceRoot: "/workspace/pr-links",
        defaultModelSelection: null,
        createdAt: "2026-09-17T00:00:00.000Z",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("branch-pr-create"),
        threadId,
        projectId,
        title: "PR links",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "feature/pr-links",
        worktreePath: null,
      });
      const snapshot = yield* orchestrator.getShellSnapshot();
      yield* orchestrator.dispatch({
        type: "thread.pull-request.sync",
        commandId: CommandId.make("branch-pr-discover"),
        threadId,
        projectId,
        snapshotSequence: snapshot.snapshotSequence,
        expected: {
          workspaceRoot: "/workspace/pr-links",
          branch: "feature/pr-links",
          worktreePath: null,
          linkedPullRequest: null,
          branchPullRequest: null,
        },
        branchPullRequest: {
          projectId,
          repository: "pingdotgg/t3code",
          number: 1,
          url: "https://github.com/pingdotgg/t3code/pull/1",
        },
      });
      for (const [index, number] of [2, 2, 1, 3].entries()) {
        yield* orchestrator.dispatch({
          type: "thread.pull-request.link",
          commandId: CommandId.make(`branch-pr-link-${index}`),
          threadId,
          host: "GitHub.com",
          repository: "Pingdotgg/T3code",
          number,
          url: `https://github.com/pingdotgg/t3code/pull/${number}`,
          source: "manual",
        });
        assert.deepEqual(
          (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.map((link) => link.number),
          number === 3 ? [1, 2, 3] : [1, 2],
        );
      }
      yield* orchestrator.dispatch({
        type: "thread.pull-request.unlink",
        commandId: CommandId.make("branch-pr-unlink"),
        threadId,
        host: "github.com",
        repository: "pingdotgg/t3code",
        number: 1,
      });
      yield* orchestrator.dispatch({
        type: "thread.pull-request.link",
        commandId: CommandId.make("branch-pr-link-after-unlink"),
        threadId,
        host: "github.com",
        repository: "pingdotgg/t3code",
        number: 4,
        url: "https://github.com/pingdotgg/t3code/pull/4",
        source: "manual",
      });
      assert.isTrue((yield* maintenance.rebuild).valid);
      assert.deepEqual(
        (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.map((link) => link.number),
        [2, 3, 4],
      );
    }),
  );

  it.effect("retains multiple pull requests and dismissed stack members through rebuilds", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const threadId = ThreadId.make("runtime-multiple-pull-requests");
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("multi-pr-create"),
        threadId,
        projectId: ProjectId.make("multi-pr-project"),
        title: "Stack",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const key = { host: "GitHub.com", repository: "Pingdotgg/T3code" };
      for (const number of [1, 2]) {
        yield* orchestrator.dispatch({
          type: "thread.pull-request.link",
          commandId: CommandId.make(`multi-pr-link-${number}`),
          threadId,
          ...key,
          number,
          url: `https://github.com/pingdotgg/t3code/pull/${number}`,
          source: number === 1 ? "manual" : "stack",
        });
      }
      const linked = yield* orchestrator.getThreadShell(threadId);
      assert.deepEqual(
        linked?.pullRequests?.map(({ host, repository, number }) => ({ host, repository, number })),
        [1, 2].map((number) => ({ host: "github.com", repository: "pingdotgg/t3code", number })),
      );
      yield* orchestrator.dispatch({
        type: "thread.pull-request.unlink",
        commandId: CommandId.make("multi-pr-dismiss"),
        threadId,
        ...key,
        number: 2,
      });
      yield* orchestrator.dispatch({
        type: "thread.pull-request.unlink",
        commandId: CommandId.make("multi-pr-unlink"),
        threadId,
        ...key,
        number: 1,
      });
      assert.deepEqual(
        (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.map(({ number, source }) => ({
          number,
          source,
        })),
        [{ number: 2, source: "stack-dismissed" }],
      );
      yield* orchestrator.dispatch({
        type: "thread.pull-request.link",
        commandId: CommandId.make("multi-pr-rediscover"),
        threadId,
        ...key,
        number: 2,
        url: "https://github.com/pingdotgg/t3code/pull/2",
        source: "stack",
      });
      assert.equal(
        (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.source,
        "stack-dismissed",
      );
      assert.isTrue((yield* maintenance.rebuild).valid);
      assert.equal(
        (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.source,
        "stack-dismissed",
      );
      yield* orchestrator.dispatch({
        type: "thread.pull-request.link",
        commandId: CommandId.make("multi-pr-restore"),
        threadId,
        ...key,
        number: 2,
        url: "https://github.com/pingdotgg/t3code/pull/2",
        source: "manual",
      });
      assert.equal(
        (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.source,
        "manual",
      );
    }),
  );

  it.effect("starts, records, and stops a pull request watch", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const threadId = ThreadId.make("runtime-pull-request-watch");
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("pr-watch-create"),
        threadId,
        projectId: ProjectId.make("pr-watch-project"),
        title: "Watch",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const key = { host: "github.com", repository: "pingdotgg/t3code", number: 7 };
      const url = "https://github.com/pingdotgg/t3code/pull/7";
      const watchOf = Effect.map(
        orchestrator.getThreadShell(threadId),
        (thread) => thread?.pullRequests?.[0]?.watch,
      );

      // Watching an unlinked pull request links it in the same command.
      yield* orchestrator.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make("pr-watch-start"),
        threadId,
        ...key,
        watching: true,
        link: { url, source: "agent" },
      });
      assert.equal(
        (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.source,
        "agent",
      );
      const started = yield* watchOf;
      assert.isDefined(started);
      if (started === undefined) return;

      // A legacy client re-linking the same pull request keeps its watch.
      yield* orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("pr-watch-legacy-relink"),
        threadId,
        linkedPullRequest: { projectId: ProjectId.make("pr-watch-project"), ...key, url },
      });
      assert.deepEqual(yield* watchOf, started);

      const recorded = { ...started, headSha: "abc123", failedChecks: ["lint"], wakes: 1 };
      yield* orchestrator.dispatch({
        type: "thread.pull-request-watch.sync",
        commandId: CommandId.make("pr-watch-record"),
        threadId,
        ...key,
        startedAt: started.startedAt,
        watch: recorded,
      });
      assert.deepEqual(yield* watchOf, recorded);
      assert.isTrue((yield* maintenance.rebuild).valid);
      assert.deepEqual(yield* watchOf, recorded);

      yield* orchestrator.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make("pr-watch-stop"),
        threadId,
        ...key,
        watching: false,
      });
      // A wake read before the stop must neither wake the agent nor bring the watch back.
      const late = yield* orchestrator
        .dispatch({
          type: "thread.pull-request-watch.sync",
          commandId: CommandId.make("pr-watch-late-record"),
          threadId,
          ...key,
          startedAt: started.startedAt,
          watch: { ...recorded, wakes: 2 },
          wake: {
            messageId: MessageId.make("pr-watch-late-wake"),
            text: "Update",
            notification: { source: { kind: "monitor" }, outcome: "updated", summary: "#7" },
          },
        })
        .pipe(Effect.flip);
      assert.equal(late._tag, "OrchestratorDispatchError");
      assert.isUndefined(yield* watchOf);
      const { messages } = yield* orchestrator.getThreadRecords(threadId, ["messages"]);
      assert.deepEqual(messages, []);
    }),
  );

  it.effect.each(["manual", "automatic"])("settling ends every pull request watch: %s", (mode) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const threadId = ThreadId.make(`pr-watch-settle-${mode}`);
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make(`pr-watch-settle-create-${mode}`),
        threadId,
        projectId: ProjectId.make("pr-watch-settle-project"),
        title: "Settle watched PRs",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const key = { host: "github.com", repository: "pingdotgg/t3code" };
      for (const number of [7, 8]) {
        yield* orchestrator.dispatch({
          type: "thread.pull-request.watch",
          commandId: CommandId.make(`pr-watch-settle-start-${mode}-${number}`),
          threadId,
          ...key,
          number,
          watching: true,
          link: { url: `https://github.com/pingdotgg/t3code/pull/${number}`, source: "agent" },
        });
      }
      const before = yield* orchestrator.getThreadShell(threadId);
      const started = before?.pullRequests?.[0]?.watch;
      assert.isDefined(started);
      if (started === undefined || before === null) return;
      yield* orchestrator.dispatch({
        ...(mode === "manual"
          ? { type: "thread.settle" as const }
          : { type: "thread.auto-settle" as const, snapshotAt: before.updatedAt }),
        commandId: CommandId.make(`pr-watch-settle-${mode}`),
        threadId,
      });
      const expected = before.pullRequests?.map(({ watch: _watch, ...link }) => link);
      assert.deepEqual((yield* orchestrator.getThreadShell(threadId))?.pullRequests, expected);
      assert.isTrue((yield* maintenance.rebuild).valid);
      assert.deepEqual((yield* orchestrator.getThreadShell(threadId))?.pullRequests, expected);

      for (const wake of [
        undefined,
        {
          messageId: MessageId.make(`pr-watch-settle-late-wake-${mode}`),
          text: "Update",
          notification: {
            source: { kind: "monitor" as const },
            outcome: "updated" as const,
            summary: "#7",
          },
        },
      ]) {
        const late = yield* orchestrator
          .dispatch({
            type: "thread.pull-request-watch.sync",
            commandId: CommandId.make(`pr-watch-settle-late-${mode}-${wake !== undefined}`),
            threadId,
            ...key,
            number: 7,
            startedAt: started.startedAt,
            watch: { ...started, headSha: "late", wakes: 1 },
            ...(wake === undefined ? {} : { wake }),
          })
          .pipe(Effect.flip);
        assert.equal(late._tag, "OrchestratorDispatchError");
      }
      const restart = {
        type: "thread.pull-request.watch" as const,
        threadId,
        ...key,
        number: 7,
        watching: true,
      };
      const blocked = yield* orchestrator
        .dispatch({
          ...restart,
          commandId: CommandId.make(`pr-watch-settle-blocked-${mode}`),
        })
        .pipe(Effect.flip);
      assert.equal(blocked._tag, "OrchestratorDispatchError");
      assert.deepEqual((yield* orchestrator.getThreadRecords(threadId, ["messages"])).messages, []);
      yield* orchestrator.dispatch({
        type: "thread.unsettle",
        commandId: CommandId.make(`pr-watch-settle-unsettle-${mode}`),
        threadId,
        reason: "user",
      });
      yield* orchestrator.dispatch({
        ...restart,
        commandId: CommandId.make(`pr-watch-settle-restart-${mode}`),
      });
      assert.isDefined((yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.watch);
    }),
  );

  it.effect("archiving ends watches, and an archived thread cannot start one", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-pull-request-watch-archive");
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("pr-watch-archive-create"),
        threadId,
        projectId: ProjectId.make("pr-watch-archive-project"),
        title: "Archive watched PR",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const watch = {
        type: "thread.pull-request.watch" as const,
        threadId,
        host: "github.com",
        repository: "pingdotgg/t3code",
        number: 7,
        watching: true,
        link: { url: "https://github.com/pingdotgg/t3code/pull/7", source: "agent" as const },
      };
      const watched = Effect.map(
        orchestrator.getThreadShell(threadId),
        (thread) => thread?.pullRequests?.[0]?.watch !== undefined,
      );
      yield* orchestrator.dispatch({
        ...watch,
        commandId: CommandId.make("pr-watch-archive-start"),
      });
      assert.isTrue(yield* watched);

      yield* orchestrator.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("pr-watch-archive"),
        threadId,
      });
      assert.isFalse(yield* watched);
      const refused = yield* orchestrator
        .dispatch({ ...watch, commandId: CommandId.make("pr-watch-archive-restart") })
        .pipe(Effect.flip);
      assert.equal(refused._tag, "OrchestratorDispatchError");

      yield* orchestrator.dispatch({
        type: "thread.unarchive",
        commandId: CommandId.make("pr-watch-unarchive"),
        threadId,
      });
      assert.isFalse(yield* watched);
    }),
  );

  it.effect("ends a watch it cannot read, and tells the agent", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-pull-request-watch-unreadable");
      const projectId = ProjectId.make("pr-watch-unreadable-project");
      yield* seedProject({
        projectId,
        title: "Watch unreadable",
        workspaceRoot: "/workspace/watch-unreadable",
        defaultModelSelection: null,
        createdAt: "2026-10-01T00:00:00.000Z",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("pr-watch-unreadable-create"),
        threadId,
        projectId,
        title: "Watch unreadable",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      yield* orchestrator.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make("pr-watch-unreadable-start"),
        threadId,
        host: "github.com",
        repository: "pingdotgg/t3code",
        number: 8,
        watching: true,
        link: { url: "https://github.com/pingdotgg/t3code/pull/8", source: "agent" },
      });
      const reactor = yield* PullRequestWatchReactor.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            NodeServices.layer,
            Layer.mock(PullRequestService.PullRequestService)({
              detail: () => Effect.die("host unreachable"),
              activity: () => Effect.die("host unreachable"),
            }),
          ),
        ),
      );
      for (let pass = 0; pass < 15; pass += 1) yield* reactor.sweep;

      const thread = yield* orchestrator.getThreadShell(threadId);
      assert.isUndefined(thread?.pullRequests?.[0]?.watch);
      const { messages } = yield* orchestrator.getThreadRecords(threadId, ["messages"]);
      assert.deepEqual(
        messages.flatMap((message) => message.notification?.summary ?? []),
        ["#8: stopped watching, could not read it"],
      );
    }),
  );

  it.effect(
    "reads a watched pull request once a pass, waits out rate limits, and skips quiet passes",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const projectId = ProjectId.make("pr-watch-shared-project");
        yield* seedProject({
          projectId,
          title: "Watch shared",
          workspaceRoot: "/workspace/watch-shared",
          defaultModelSelection: null,
          createdAt: "2026-10-01T00:00:00.000Z",
        });
        const threadIds = ["one", "two"].map((name) =>
          ThreadId.make(`runtime-pull-request-watch-shared-${name}`),
        );
        const watchFrom = (threadId: ThreadId) =>
          Effect.gen(function* () {
            yield* orchestrator.dispatch({
              type: "thread.create",
              createdBy: "user",
              creationSource: "web",
              commandId: CommandId.make(`create:${threadId}`),
              threadId,
              projectId,
              title: "Watch shared",
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: null,
              worktreePath: null,
            });
            yield* orchestrator.dispatch({
              type: "thread.pull-request.watch",
              commandId: CommandId.make(`watch:${threadId}`),
              threadId,
              host: "github.com",
              repository: "pingdotgg/t3code",
              number: 9,
              watching: true,
              link: { url: "https://github.com/pingdotgg/t3code/pull/9", source: "agent" },
            });
          });
        for (const threadId of threadIds) yield* watchFrom(threadId);
        const rateLimited = new PullRequestOperationError({
          operation: "getChangeRequest",
          detail: "github requests are paused until the rate limit resets",
          cause: new PullRequestProviderError({
            provider: SourceControlProviderKind.make("github"),
            operation: "getChangeRequest",
            reason: "rate-limited",
            detail: "paused",
          }),
        });
        let host: "rate-limited" | "open" | "closed" = "rate-limited";
        let reads = 0;
        const reactor = yield* PullRequestWatchReactor.make.pipe(
          Effect.provide(
            Layer.mergeAll(
              NodeServices.layer,
              Layer.mock(PullRequestService.PullRequestService)({
                detail: () =>
                  Effect.suspend(() => {
                    reads += 1;
                    return host === "rate-limited"
                      ? Effect.fail(rateLimited)
                      : Effect.succeed({
                          ...watchedPullRequestDetail({
                            projectId,
                            number: 9,
                            at: "2026-10-02T12:00:00.000Z",
                          }),
                          state: host,
                          checks: [
                            { name: "lint", status: "success", description: null, url: null },
                          ],
                        });
                  }),
                activity: () =>
                  host === "rate-limited"
                    ? Effect.fail(rateLimited)
                    : Effect.succeed({
                        comments: [],
                        commentCount: 0,
                        commentsTruncated: false,
                        reviewThreads: [],
                        commits: [],
                      }),
              }),
            ),
          ),
        );
        const summaries = (threadId: ThreadId) =>
          Effect.map(orchestrator.getThreadRecords(threadId, ["messages"]), ({ messages }) =>
            messages.flatMap((message) => message.notification?.summary ?? []),
          );
        const watching = (threadId: ThreadId) =>
          Effect.map(
            orchestrator.getThreadShell(threadId),
            (thread) => thread?.pullRequests?.[0]?.watch !== undefined,
          );

        // However long the host stays rate limited, the watch waits instead of giving up, and
        // both threads share one read a pass.
        for (let pass = 0; pass < 20; pass += 1) yield* reactor.sweep;
        assert.equal(reads, 20);
        for (const threadId of threadIds) {
          assert.isTrue(yield* watching(threadId));
          assert.deepEqual(yield* summaries(threadId), []);
        }

        host = "open";
        yield* reactor.sweep;
        assert.equal(reads, 21);
        for (const threadId of threadIds) {
          assert.deepEqual(yield* summaries(threadId), ["#9: checks passed"]);
        }

        // Nothing in flight and nothing moved, so the next pass spends no request.
        yield* reactor.sweep;
        assert.equal(reads, 21);

        // A new watch on the quiet pull request takes its first look on the next pass.
        const late = ThreadId.make("runtime-pull-request-watch-shared-late");
        yield* watchFrom(late);
        yield* reactor.sweep;
        assert.equal(reads, 22);
        assert.deepEqual(yield* summaries(late), ["#9: checks passed"]);
        for (const threadId of threadIds) {
          assert.deepEqual(yield* summaries(threadId), ["#9: checks passed"]);
        }
        threadIds.push(late);

        // The quiet reread finds the pull request closed, ends both watches, and says so.
        host = "closed";
        yield* TestClock.adjust("10 minutes");
        yield* reactor.sweep;
        assert.equal(reads, 23);
        for (const threadId of threadIds) {
          assert.isFalse(yield* watching(threadId));
          assert.deepEqual(yield* summaries(threadId), [
            "#9: checks passed",
            "#9: closed, stopped watching",
          ]);
        }
      }),
  );

  it.effect("reports how long an ended watch was quiet", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-pull-request-watch-ended");
      const projectId = ProjectId.make("pr-watch-ended-project");
      yield* seedProject({
        projectId,
        title: "Watch ended",
        workspaceRoot: "/workspace/watch-ended",
        defaultModelSelection: null,
        createdAt: "2026-10-01T00:00:00.000Z",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("pr-watch-ended-create"),
        threadId,
        projectId,
        title: "Watch ended",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const key = { host: "github.com", repository: "pingdotgg/t3code", number: 11 };
      const watching = (on: boolean, id: string) =>
        orchestrator.dispatch({
          type: "thread.pull-request.watch",
          commandId: CommandId.make(`pr-watch-ended-${id}`),
          threadId,
          ...key,
          watching: on,
          link: { url: "https://github.com/pingdotgg/t3code/pull/11", source: "agent" },
        });
      yield* watching(true, "start");

      let headSha = "aaaaaaa";
      const ended: Array<unknown> = [];
      const capture = Logger.make(({ message }) => {
        const [text, fields] = Array.isArray(message) ? message : [message];
        if (text === "pull request watch ended") ended.push(fields);
      });
      const reactor = yield* PullRequestWatchReactor.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            NodeServices.layer,
            Layer.mock(PullRequestService.PullRequestService)({
              detail: () =>
                Effect.sync(() => ({
                  ...watchedPullRequestDetail({ projectId, number: key.number, at: "2026-10-02" }),
                  headSha,
                  checks: [],
                })),
              activity: () =>
                Effect.succeed({
                  comments: [],
                  commentCount: 0,
                  commentsTruncated: false,
                  reviewThreads: [],
                  commits: [],
                }),
            }),
          ),
        ),
      );
      const sweep = reactor.sweep.pipe(Effect.provide(Logger.layer([capture])));

      // The first read learns the head. A push 6 hours later, then 2 quiet hours.
      yield* sweep;
      yield* TestClock.adjust("6 hours");
      headSha = "bbbbbbb";
      yield* sweep;
      yield* TestClock.adjust("2 hours");
      yield* sweep;
      yield* watching(false, "stop");
      // The end is reported once.
      yield* sweep;
      yield* sweep;

      assert.deepEqual(ended, [
        {
          threadId,
          pullRequest: "github.com/pingdotgg/t3code#11",
          reason: "stopped",
          minutes: 480,
          quietMinutes: 120,
          longestQuietMinutes: 360,
          wakes: 0,
          reads: 3,
          partial: false,
        },
      ]);
    }),
  );

  it.effect("with a host fingerprint, a watch reads only what moved", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projectId = ProjectId.make("pr-watch-fingerprint-project");
      const threadId = ThreadId.make("runtime-pull-request-watch-fingerprint");
      yield* seedProject({
        projectId,
        title: "Watch fingerprint",
        workspaceRoot: "/workspace/watch-fingerprint",
        defaultModelSelection: null,
        createdAt: "2026-10-01T00:00:00.000Z",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("pr-watch-fingerprint-create"),
        threadId,
        projectId,
        title: "Watch fingerprint",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      yield* orchestrator.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make("pr-watch-fingerprint-start"),
        threadId,
        host: "github.com",
        repository: "pingdotgg/t3code",
        number: 11,
        watching: true,
        link: { url: "https://github.com/pingdotgg/t3code/pull/11", source: "agent" },
      });

      let fingerprint: { status: string; remarks: string } | "rate-limited" = {
        status: "OPEN pending",
        remarks: "0",
      };
      type CheckStatus = "pending" | "success" | "failure";
      // What the host says now, and what a read answers: a read right after a cache fill can
      // still answer from before a change until the cache is invalidated.
      let checks: { lint: CheckStatus; test: CheckStatus } = { lint: "pending", test: "pending" };
      let cachedChecks: typeof checks | null = null;
      let comments: Array<PullRequestComment> = [];
      let detailReads = 0;
      let activityReads = 0;
      const reactor = yield* PullRequestWatchReactor.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            NodeServices.layer,
            Layer.mock(PullRequestService.PullRequestService)({
              watchFingerprint: () =>
                Effect.suspend(() =>
                  fingerprint === "rate-limited"
                    ? Effect.fail(
                        new PullRequestOperationError({
                          operation: "watchFingerprint",
                          detail: "paused",
                          cause: new PullRequestProviderError({
                            provider: SourceControlProviderKind.make("github"),
                            operation: "getChangeRequestWatchFingerprint",
                            reason: "rate-limited",
                            detail: "paused",
                          }),
                        }),
                      )
                    : Effect.succeed(fingerprint),
                ),
              invalidate: () =>
                Effect.sync(() => {
                  cachedChecks = null;
                }),
              detail: () =>
                Effect.sync(() => {
                  detailReads += 1;
                  const answer = cachedChecks ?? checks;
                  cachedChecks = null;
                  return {
                    ...watchedPullRequestDetail({
                      projectId,
                      number: 11,
                      at: "2026-10-02T12:00:00.000Z",
                    }),
                    checks: Object.entries(answer).map(([name, status]) => ({
                      name,
                      status,
                      description: null,
                      url: null,
                    })),
                  };
                }),
              activity: () =>
                Effect.sync(() => {
                  activityReads += 1;
                  return {
                    comments,
                    commentCount: comments.length,
                    commentsTruncated: false,
                    reviewThreads: [],
                    commits: [],
                  };
                }),
            }),
          ),
        ),
      );
      const reads = () => ({ detail: detailReads, activity: activityReads });
      const summaries = Effect.map(
        orchestrator.getThreadRecords(threadId, ["messages"]),
        ({ messages }) => messages.flatMap((message) => message.notification?.summary ?? []),
      );

      // The first look reads everything.
      yield* reactor.sweep;
      assert.deepEqual(reads(), { detail: 1, activity: 1 });

      // While checks run, the cheap detail is read every pass: check counts by state cannot
      // tell which check finished, so a failure can arrive with the fingerprint unchanged.
      checks = { lint: "failure", test: "pending" };
      yield* reactor.sweep;
      assert.deepEqual(reads(), { detail: 2, activity: 1 });
      assert.deepEqual(yield* summaries, ["#11: checks failed"]);

      // When the fingerprint moves, a cached answer from before the move is not trusted.
      yield* TestClock.adjust("1 millis");
      cachedChecks = checks;
      checks = { lint: "failure", test: "success" };
      fingerprint = { status: "OPEN settled", remarks: "0" };
      yield* reactor.sweep;
      assert.deepEqual(reads(), { detail: 3, activity: 1 });

      // Nothing in flight and nothing moved, so the passes read nothing.
      for (let pass = 0; pass < 5; pass += 1) yield* reactor.sweep;
      assert.deepEqual(reads(), { detail: 3, activity: 1 });

      // A new comment moves the remarks, which take the activity read too.
      yield* TestClock.adjust("1 millis");
      comments = [
        {
          id: "comment-1",
          kind: "issue-comment",
          author: { login: "reviewer", name: null, avatarUrl: null },
          body: "Please rename this.",
          createdAt: "2999-01-01T00:00:00.000Z",
          url: null,
          path: null,
          reviewState: null,
        },
      ];
      fingerprint = { status: "OPEN settled", remarks: "1" };
      yield* reactor.sweep;
      assert.deepEqual(reads(), { detail: 4, activity: 2 });
      assert.deepEqual(yield* summaries, ["#11: checks failed", "#11: new comments"]);

      // While the host is rate limited, a pass reads nothing and the watch stays on.
      fingerprint = "rate-limited";
      yield* reactor.sweep;
      assert.deepEqual(reads(), { detail: 4, activity: 2 });
      assert.isDefined((yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.watch);

      // Edits inside review threads leave no trace in the fingerprint, so the activity is read
      // again after half an hour regardless.
      fingerprint = { status: "OPEN settled", remarks: "1" };
      yield* TestClock.adjust("30 minutes");
      yield* reactor.sweep;
      assert.deepEqual(reads(), { detail: 5, activity: 3 });
      assert.deepEqual(yield* summaries, ["#11: checks failed", "#11: new comments"]);
    }),
  );

  it.effect.each([
    "single page",
    "paginated",
    "page failure",
    "repeated cursor",
    "missing comments",
    "thread list truncated",
  ])("wakes a watched thread once: %s", (mode) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make(`runtime-pull-request-watch-wake-${mode}`);
      const projectId = ProjectId.make(`pr-watch-wake-project-${mode}`);
      yield* seedProject({
        projectId,
        title: "Watch wake",
        workspaceRoot: "/workspace/watch",
        defaultModelSelection: null,
        createdAt: "2026-10-01T00:00:00.000Z",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make(`pr-watch-wake-create-${mode}`),
        threadId,
        projectId,
        title: "Watch wake",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const key = { host: "github.com", repository: "pingdotgg/t3code", number: 7 };
      const url = "https://github.com/pingdotgg/t3code/pull/7";
      yield* orchestrator.dispatch({
        type: "thread.pull-request.link",
        commandId: CommandId.make(`pr-watch-wake-link-${mode}`),
        threadId,
        ...key,
        url,
        source: "agent",
      });
      yield* orchestrator.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make(`pr-watch-wake-start-${mode}`),
        threadId,
        ...key,
        watching: true,
      });

      const at = "2026-10-02T12:00:00.000Z";
      const detail = watchedPullRequestDetail({ projectId, number: key.number, at });
      const initialWatch = (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.watch;
      assert.isDefined(initialWatch);
      const remark: PullRequestComment = {
        id: "review-1",
        kind: "review-comment",
        author: { login: "reviewer", name: null, avatarUrl: null },
        body: "One more thing.",
        createdAt: "2999-01-01T00:00:03.000Z",
        url: null,
        path: "src/index.ts",
        reviewState: null,
      };
      const firstTen = Array.from({ length: 10 }, (_, index) => ({
        ...remark,
        id: `old-${index}`,
        createdAt: "1900-01-01T00:00:00.000Z",
      }));
      const eleventh = {
        ...remark,
        id: "reply-11",
        body: "Eleventh reply.",
        createdAt: "2999-01-01T00:00:01.000Z",
      };
      const twelfth = {
        ...remark,
        id: "reply-12",
        body: "Twelfth reply.",
        createdAt: "2999-01-01T00:00:02.000Z",
      };
      const incomplete = mode !== "single page" && mode !== "paginated";
      let recovering = false;
      let pagesRead = 0;
      const reactor = yield* PullRequestWatchReactor.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            NodeServices.layer,
            Layer.mock(PullRequestService.PullRequestService)({
              detail: () => Effect.succeed(detail),
              activity: () =>
                Effect.succeed({
                  comments:
                    mode === "single page"
                      ? [remark]
                      : [...firstTen, { ...remark, kind: "issue-comment" }],
                  commentCount: mode === "single page" ? 1 : 13,
                  commentsTruncated: mode !== "single page",
                  reviewThreadsTruncated: mode === "thread list truncated" && !recovering,
                  reviewThreads:
                    mode === "single page"
                      ? []
                      : [
                          {
                            id: "review-thread",
                            path: "src/index.ts",
                            line: 1,
                            side: "right",
                            isResolved: false,
                            isOutdated: false,
                            comments: firstTen,
                            commentCount: 12,
                            nextCommentsCursor: "after-10",
                          },
                        ],
                  commits: [],
                }),
              threadComments: (input) => {
                pagesRead += 1;
                assert.equal(input.threadId, "review-thread");
                if (input.cursor === "after-10") {
                  return Effect.succeed({ comments: [eleventh], nextCursor: "after-11" });
                }
                assert.equal(input.cursor, "after-11");
                if (!recovering && mode === "page failure") {
                  return Effect.fail(
                    new PullRequestOperationError({
                      operation: "threadComments",
                      detail: "Page unavailable",
                    }),
                  );
                }
                if (!recovering && mode === "repeated cursor") {
                  return Effect.succeed({ comments: [eleventh], nextCursor: "after-11" });
                }
                if (!recovering && mode === "missing comments") {
                  return Effect.succeed({ comments: [], nextCursor: null });
                }
                // Overlapping pages must not report the same reply twice.
                return Effect.succeed({ comments: [eleventh, twelfth], nextCursor: null });
              },
            }),
          ),
        ),
      );
      if (incomplete) {
        yield* reactor.sweep;
        const held = (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.watch;
        assert.equal(held?.remarksThrough, initialWatch?.remarksThrough);
        assert.deepEqual(held?.remarkIds, initialWatch?.remarkIds);
        assert.deepEqual(held?.failedChecks, ["lint"]);
        const { messages } = yield* orchestrator.getThreadRecords(threadId, ["messages"]);
        assert.deepEqual(
          messages.map((message) => message.notification?.summary),
          ["#7: checks failed"],
        );
        // Keep the two notifications ordered independently of their random message IDs.
        yield* TestClock.adjust("1 millis");
        recovering = true;
      }
      yield* reactor.sweep;
      // A thread whose count has not moved is not paged again.
      const pagesBefore = pagesRead;
      yield* reactor.sweep;
      assert.equal(pagesRead, pagesBefore);

      const { messages } = yield* orchestrator.getThreadRecords(threadId, ["messages"]);
      assert.deepEqual(
        messages.flatMap((message) =>
          message.notification === undefined ? [] : [message.notification.summary],
        ),
        incomplete
          ? ["#7: checks failed", "#7: new comments"]
          : ["#7: checks failed, new comments"],
      );
      if (mode !== "single page") {
        const wake = messages.at(-1);
        assert.include(wake?.text ?? "", "3 new comments");
        assert.include(wake?.text ?? "", "Eleventh reply.");
        assert.include(wake?.text ?? "", "Twelfth reply.");
      }
      const watch = (yield* orchestrator.getThreadShell(threadId))?.pullRequests?.[0]?.watch;
      assert.equal(watch?.remarksThrough, remark.createdAt);
      assert.deepEqual(watch?.remarkIds, [remark.id]);
      assert.deepEqual(
        { headSha: watch?.headSha, failedChecks: watch?.failedChecks, wakes: watch?.wakes },
        { headSha: "abc1234def", failedChecks: ["lint"], wakes: incomplete ? 1 : 0 },
      );
    }),
  );

  it.effect("persists rejected command receipts across retries", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const command = {
        type: "thread.archive" as const,
        commandId: CommandId.make("runtime-layer-rejected-command"),
        threadId: ThreadId.make("runtime-layer-missing-thread"),
      };

      const first = yield* orchestrator.dispatch(command).pipe(Effect.flip);
      const retry = yield* orchestrator.dispatch(command).pipe(Effect.flip);

      assert.equal(first._tag, "OrchestratorProjectionError");
      assert.equal(retry._tag, "OrchestratorCommandPreviouslyRejectedError");
    }),
  );

  it.effect(
    "admits restart continuations once and rejects a stale continuation behind newer work",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make("runtime-layer-restart-continuation");
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("restart-create"),
          threadId,
          projectId: ProjectId.make("restart-project"),
          title: "Restart",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: "/tmp/runtime-layer-restart",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make("restart-user-message"),
          threadId,
          messageId: MessageId.make("restart-user-message"),
          text: "Original work",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "start_immediately" },
        });
        const original = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        const now = yield* DateTime.now;
        yield* eventSink.commitCommand({
          commandId: CommandId.make("restart-cancel"),
          threadId,
          commandType: "provider-runtime.reconcile",
          acceptedAt: now,
          events: [
            {
              id: EventId.make("restart-cancel-event"),
              type: "run.updated",
              threadId,
              runId: original.id,
              occurredAt: now,
              payload: { ...original, status: "cancelled", completedAt: now },
            },
          ],
          effects: [],
        });
        const command = {
          type: "message.dispatch" as const,
          createdBy: "agent" as const,
          creationSource: "server" as const,
          commandId: CommandId.make("restart-automatic-message"),
          threadId,
          messageId: MessageId.make("restart-automatic-message"),
          text: "Continue where you left off.",
          notification: {
            source: { kind: "system" as const },
            outcome: "updated" as const,
            summary: "T3 Code restarted and resumed this turn",
            detail: "Continue where you left off.",
          },
          attachments: [],
          modelSelection,
          dispatchMode: { type: "start_immediately" as const },
          restartContinuationOfRunId: original.id,
        };
        yield* orchestrator.dispatch(command);
        yield* orchestrator.dispatch(command);
        const admitted = yield* orchestrator.getThreadProjection(threadId);
        assert.lengthOf(admitted.runs, 2);
        assert.equal(admitted.runs[1]?.restartContinuationOfRunId, original.id);
        // The timeline shows a work log row; the prompt stays in its detail.
        const continuationItems = admitted.turnItems.filter(
          (item) => item.runId === admitted.runs[1]?.id,
        );
        assert.isFalse(continuationItems.some((item) => item.type === "user_message"));
        assert.deepInclude(
          continuationItems.flatMap((item) =>
            item.type === "notification" ? [[item.summary, item.detail]] : [],
          ),
          ["T3 Code restarted and resumed this turn", "Continue where you left off."],
        );
        // A differently identified stale delivery still must not create another run.
        yield* orchestrator.dispatch({
          ...command,
          commandId: CommandId.make("restart-stale-race"),
          messageId: MessageId.make("restart-stale-race"),
        });
        const raced = yield* orchestrator.getThreadProjection(threadId);
        assert.lengthOf(raced.runs, 2);
        assert.isFalse(raced.messages.some((message) => message.id === "restart-stale-race"));
      }),
  );

  it.effect.each(["failed", "completed", "waiting", "cancelled"] as const)(
    "does not admit a restart continuation of a %s run with only lost background work",
    (status) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make(`runtime-layer-restart-${status}-source`);
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(`restart-${status}-create`),
          threadId,
          projectId: ProjectId.make("restart-project"),
          title: "Restart",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: "/tmp/runtime-layer-restart-failed",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(`restart-${status}-user-message`),
          threadId,
          messageId: MessageId.make(`restart-${status}-user-message`),
          text: "Original work",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "start_immediately" },
        });
        const original = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        const now = yield* DateTime.now;
        yield* eventSink.commitCommand({
          commandId: CommandId.make(`restart-${status}-reconcile`),
          threadId,
          commandType: "provider-runtime.reconcile",
          acceptedAt: now,
          events: [
            {
              id: EventId.make(`restart-${status}-run`),
              type: "run.updated",
              threadId,
              runId: original.id,
              occurredAt: now,
              payload: { ...original, status, completedAt: now },
            },
            {
              id: EventId.make(`restart-${status}-work`),
              type: "run.background-work-cancelled",
              threadId,
              runId: original.id,
              occurredAt: now,
              payload: {
                runId: original.id,
                restartCancelledBackgroundWork: [{ kind: "shell", label: "sleep 25" }],
              },
            },
          ],
          effects: [],
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "agent",
          creationSource: "server",
          commandId: CommandId.make(`restart-${status}-continuation`),
          threadId,
          messageId: MessageId.make(`restart-${status}-continuation`),
          text: "Note: the T3 server restarted.",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "start_immediately" },
          restartContinuationOfRunId: original.id,
        });
        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.lengthOf(projection.runs, 1);
        assert.equal(projection.runs[0]?.status, status);
      }),
  );

  it.effect("rejects settling a thread while a run is active", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-active-settle-thread");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-active-settle-create"),
        threadId,
        projectId: ProjectId.make("runtime-layer-active-settle-project"),
        title: "Active settle",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: "/tmp/runtime-layer-active-settle",
      });
      yield* orchestrator.dispatch({
        type: "thread.settle",
        commandId: CommandId.make("runtime-layer-active-settle-initial"),
        threadId,
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-active-settle-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-active-settle-message"),
        text: "Keep this run active.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });

      const nonEmptyClaim = yield* orchestrator
        .dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("runtime-layer-active-settle-empty-claim"),
          threadId,
          worktreePath: "/tmp/reassigned-after-message",
          expectedEmpty: true,
        })
        .pipe(Effect.flip);
      assert.instanceOf(nonEmptyClaim, Orchestrator.OrchestratorDispatchError);

      const error = yield* orchestrator
        .dispatch({
          type: "thread.settle",
          commandId: CommandId.make("runtime-layer-active-settle"),
          threadId,
        })
        .pipe(Effect.flip);

      assert.equal(error._tag, "OrchestratorDispatchError");
      assert.equal(error.cause, `Thread ${threadId} is still running. Stop it before settling.`);
      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.runs[0]?.status, "starting");
      assert.isNull(projection.thread.settledOverride);
      assert.isNull(projection.thread.settledAt);
      assert.isNotNull(projection.thread.unsettledAt);
    }),
  );

  it.effect("settles a thread its own agent settled once the turn completes", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const threadManagement = yield* ThreadManagementService.ThreadManagementService;
      const projectId = ProjectId.make("runtime-layer-settle-after-run-project");
      const threadId = ThreadId.make("runtime-layer-settle-after-run-thread");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-settle-after-run-create"),
        threadId,
        projectId,
        title: "Settle after run",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: "/tmp/runtime-layer-settle-after-run",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-settle-after-run-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-settle-after-run-message"),
        text: "Fix it and then settle this thread.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });
      const run = (yield* orchestrator.getThreadProjection(threadId)).runs[0];
      if (run === undefined) return yield* Effect.die(new Error("Run missing."));

      const settled = yield* orchestrator
        .streamStoredEventsFrom({ threadId, afterSequence: 0, eventType: "thread.settled" })
        .pipe(Stream.runHead, Effect.forkChild);
      const result = yield* threadManagement.settleThread({
        threadId,
        commandId: CommandId.make("runtime-layer-settle-after-run-settle"),
        byOwnAgent: true,
      });
      assert.deepEqual(result, { settlesWhenTurnEnds: true });
      assert.isNull((yield* orchestrator.getThreadProjection(threadId)).thread.settledOverride);
      const now = yield* DateTime.now;
      yield* eventSink.write({
        commandId: CommandId.make("runtime-layer-settle-after-run-completed"),
        events: [
          {
            id: EventId.make("runtime-layer-settle-after-run-completed"),
            type: "run.updated",
            threadId,
            runId: run.id,
            occurredAt: now,
            payload: { ...run, status: "completed", startedAt: now, completedAt: now },
          },
        ],
      });
      yield* Fiber.join(settled);

      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(projection.thread.settledOverride, "settled");
    }),
  );

  it.effect("settles past held automatic runs but not held user messages", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("runtime-layer-settle-automatic-queued");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("settle-automatic-create"),
        threadId,
        projectId: ProjectId.make("settle-automatic-project"),
        title: "Settle automatic queued",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: "/tmp/runtime-layer-settle-automatic",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("settle-automatic-active"),
        threadId,
        messageId: MessageId.make("settle-automatic-active"),
        text: "Active",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });
      const queueMessage = (id: string, automatic: boolean) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: automatic ? "agent" : "user",
          creationSource: automatic ? "provider" : "web",
          ...(automatic
            ? {
                notification: {
                  source: { kind: "background_task" as const },
                  outcome: "updated" as const,
                  summary: "Background activity updated",
                },
              }
            : {}),
          commandId: CommandId.make(id),
          threadId,
          messageId: MessageId.make(id),
          text: id,
          attachments: [],
          modelSelection,
          dispatchMode: { type: "queue_after_active" },
        });
      yield* queueMessage("settle-automatic-notification", true);

      // Simulate a restart: the active run ends and recovery holds the queue.
      const holdQueueAfterRestart = (commandId: string) =>
        Effect.gen(function* () {
          const projection = yield* orchestrator.getThreadProjection(threadId);
          const now = yield* DateTime.now;
          yield* eventSink.commitCommand({
            commandId: CommandId.make(commandId),
            threadId,
            commandType: "provider-runtime.reconcile",
            acceptedAt: now,
            events: projection.runs
              .filter((run) => run.status === "starting" || run.status === "queued")
              .map((run) => ({
                id: EventId.make(`${commandId}:${run.id}`),
                type: "run.updated" as const,
                threadId,
                runId: run.id,
                occurredAt: now,
                payload:
                  run.status === "queued"
                    ? { ...run, queueHeld: true }
                    : { ...run, status: "cancelled" as const, completedAt: now },
              })),
            effects: [],
          });
        });
      yield* holdQueueAfterRestart("settle-automatic-restart");

      yield* orchestrator.dispatch({
        type: "thread.settle",
        commandId: CommandId.make("settle-automatic-settle"),
        threadId,
      });
      const settled = yield* orchestrator.getThreadProjection(threadId);
      assert.isNotNull(settled.thread.settledAt);
      assert.isTrue(settled.runs.every((run) => run.status === "cancelled"));

      // A held message the user typed still blocks settling.
      yield* orchestrator.dispatch({
        type: "thread.unsettle",
        commandId: CommandId.make("settle-automatic-unsettle"),
        threadId,
        reason: "user",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("settle-automatic-active-2"),
        threadId,
        messageId: MessageId.make("settle-automatic-active-2"),
        text: "Active again",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });
      yield* queueMessage("settle-automatic-user-queued", false);
      yield* holdQueueAfterRestart("settle-automatic-restart-2");
      const error = yield* orchestrator
        .dispatch({
          type: "thread.settle",
          commandId: CommandId.make("settle-automatic-settle-2"),
          threadId,
        })
        .pipe(Effect.flip);
      assert.equal(error._tag, "OrchestratorDispatchError");
      assert.equal(
        error.cause,
        `Thread ${threadId} has a queued message. Send it or remove it from the queue before settling.`,
      );
    }),
  );

  it.effect("cancels queued work when a thread is archived", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-archive-queued-thread");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-archive-queued-create"),
        threadId,
        projectId: ProjectId.make("runtime-layer-archive-queued-project"),
        title: "Archive queued work",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: "/tmp/runtime-layer-archive-queued",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-archive-queued-active-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-archive-queued-active-message"),
        text: "Keep the provider occupied.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-archive-queued-next-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-archive-queued-next-message"),
        text: "Do not run this after archive.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "queue_after_active" },
      });

      const beforeArchive = yield* orchestrator.getThreadProjection(threadId);
      const activeRun = beforeArchive.runs.find((run) => run.status === "starting");
      const queuedRun = beforeArchive.runs.find((run) => run.status === "queued");
      assert.isDefined(activeRun);
      assert.isDefined(queuedRun);

      yield* orchestrator.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("runtime-layer-archive-queued-archive"),
        threadId,
      });

      const archived = yield* orchestrator.getThreadProjection(threadId);
      assert.isNotNull(archived.thread.archivedAt);
      assert.equal(archived.runs.find((run) => run.id === queuedRun.id)?.status, "cancelled");
      assert.equal(
        archived.attempts.find((attempt) => attempt.runId === queuedRun.id)?.status,
        "cancelled",
      );
      assert.equal(archived.nodes.find((node) => node.runId === queuedRun.id)?.status, "cancelled");
      assert.equal(yield* orchestrator.resumeQueuedRuns, 0);

      const promoteError = yield* orchestrator
        .dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("runtime-layer-archive-queued-promote"),
          threadId,
          queuedRunId: queuedRun.id,
          targetRunId: activeRun.id,
        })
        .pipe(Effect.flip);
      assert.equal(promoteError._tag, "OrchestratorDispatchError");

      const afterPromotion = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(afterPromotion.runs.find((run) => run.id === queuedRun.id)?.status, "cancelled");
    }),
  );

  it.effect.each([false, true])(
    "promotes only one queued run after each terminal run (notification: %s)",
    (automatic) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make(`runtime-layer-serialized-queue-thread-${automatic}`);

        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(`runtime-layer-serialized-queue-create-${automatic}`),
          threadId,
          projectId: ProjectId.make(`runtime-layer-serialized-queue-project-${automatic}`),
          title: "Serialized queue",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: process.cwd(),
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(`runtime-layer-serialized-queue-active-${automatic}`),
          threadId,
          messageId: MessageId.make(`runtime-layer-serialized-queue-active-${automatic}`),
          text: "Active",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "start_immediately" },
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: automatic ? "agent" : "user",
          creationSource: automatic ? "provider" : "web",
          ...(automatic
            ? {
                notification: {
                  source: { kind: "monitor" as const },
                  outcome: "updated" as const,
                  summary: "Monitor updated",
                  detail: "Build is green",
                },
              }
            : {}),
          commandId: CommandId.make(`runtime-layer-serialized-queue-first-${automatic}`),
          threadId,
          messageId: MessageId.make(`runtime-layer-serialized-queue-first-${automatic}`),
          text: "First queued",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "queue_after_active" },
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(`runtime-layer-serialized-queue-second-${automatic}`),
          threadId,
          messageId: MessageId.make(`runtime-layer-serialized-queue-second-${automatic}`),
          text: "Second queued",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "queue_after_active" },
        });

        const before = yield* orchestrator.getThreadProjection(threadId);
        const activeRun = before.runs.find((run) => run.status === "starting");
        const queuedRuns = before.runs
          .filter((run) => run.status === "queued")
          .toSorted((left, right) => left.ordinal - right.ordinal);
        const firstQueuedRun = queuedRuns[0];
        const secondQueuedRun = queuedRuns[1];
        assert.isDefined(activeRun);
        assert.isDefined(firstQueuedRun);
        assert.isDefined(secondQueuedRun);
        assert.isFalse(
          before.turnItems.some(
            (item) =>
              item.type === "user_message" &&
              (item.messageId === firstQueuedRun.userMessageId ||
                item.messageId === secondQueuedRun.userMessageId),
          ),
          "queued messages must not exist as turn items before dispatch",
        );

        const promotedRunIds = yield* Queue.unbounded<RunId>();
        const afterSequence = yield* orchestrator.getThreadEventSequence(threadId);
        yield* eventSink.stream({ threadId, afterSequence }).pipe(
          Stream.runForEach((stored) =>
            stored.event.type === "run.updated" && stored.event.payload.status === "starting"
              ? Queue.offer(promotedRunIds, stored.event.payload.id)
              : Effect.void,
          ),
          Effect.forkScoped,
        );
        yield* Effect.yieldNow;

        const activeCompletedAt = yield* DateTime.now;
        yield* eventSink.write({
          events: [
            {
              id: EventId.make(`runtime-layer-serialized-queue-active-completed-${automatic}`),
              type: "run.updated",
              threadId,
              runId: activeRun.id,
              ...(activeRun.rootNodeId === null ? {} : { nodeId: activeRun.rootNodeId }),
              providerInstanceId: activeRun.providerInstanceId,
              occurredAt: activeCompletedAt,
              payload: {
                ...activeRun,
                status: "completed",
                completedAt: activeCompletedAt,
              },
            },
          ],
        });

        assert.equal(yield* Queue.take(promotedRunIds), firstQueuedRun.id);
        const afterFirstPromotion = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          afterFirstPromotion.runs.find((run) => run.id === firstQueuedRun.id)?.status,
          "starting",
        );
        assert.equal(
          afterFirstPromotion.runs.find((run) => run.id === secondQueuedRun.id)?.status,
          "queued",
        );
        const promotedMessageItem = afterFirstPromotion.turnItems.find(
          (item) =>
            item.runId === firstQueuedRun.id &&
            (item.type === "user_message" || item.type === "notification"),
        );
        assert.isDefined(promotedMessageItem);
        if (automatic) {
          assert.equal(promotedMessageItem.type, "notification");
          assert.equal(
            afterFirstPromotion.messages.find(
              (message) => message.id === firstQueuedRun.userMessageId,
            )?.text,
            "First queued",
          );
          assert.equal(
            afterFirstPromotion.messages.find(
              (message) => message.id === firstQueuedRun.userMessageId,
            )?.notification?.summary,
            "Monitor updated",
          );
          assert.isFalse(
            afterFirstPromotion.turnItems.some(
              (item) =>
                item.type === "user_message" && item.messageId === firstQueuedRun.userMessageId,
            ),
          );
        } else {
          assert.equal(promotedMessageItem.type, "user_message");
        }
        assert.isTrue(
          promotedMessageItem.startedAt !== null &&
            DateTime.toEpochMillis(promotedMessageItem.startedAt) >=
              DateTime.toEpochMillis(activeCompletedAt),
        );

        const promotedFirst = afterFirstPromotion.runs.find((run) => run.id === firstQueuedRun.id);
        assert.isDefined(promotedFirst);
        const firstCompletedAt = yield* DateTime.now;
        yield* eventSink.write({
          events: [
            {
              id: EventId.make(`runtime-layer-serialized-queue-first-completed-${automatic}`),
              type: "run.updated",
              threadId,
              runId: promotedFirst.id,
              ...(promotedFirst.rootNodeId === null ? {} : { nodeId: promotedFirst.rootNodeId }),
              providerInstanceId: promotedFirst.providerInstanceId,
              occurredAt: firstCompletedAt,
              payload: {
                ...promotedFirst,
                status: "completed",
                completedAt: firstCompletedAt,
              },
            },
          ],
        });

        assert.equal(yield* Queue.take(promotedRunIds), secondQueuedRun.id);
        const afterSecondPromotion = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          afterSecondPromotion.runs.find((run) => run.id === firstQueuedRun.id)?.status,
          "completed",
        );
        assert.equal(
          afterSecondPromotion.runs.find((run) => run.id === secondQueuedRun.id)?.status,
          "starting",
        );
      }),
  );

  it.effect("starts a wake's work clock from the run that ran before it", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("runtime-layer-wake-work-start-thread");
      const messageId = (key: string) => MessageId.make(`runtime-layer-wake-work-start-${key}`);

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-wake-work-start-create"),
        threadId,
        projectId: ProjectId.make("runtime-layer-wake-work-start-project"),
        title: "Wake work start",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: process.cwd(),
      });
      const dispatch = (key: string, wake: boolean) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: wake ? "agent" : "user",
          creationSource: wake ? "provider" : "web",
          ...(wake
            ? {
                notification: {
                  source: { kind: "background_task" as const },
                  outcome: "updated" as const,
                  summary: "Background activity updated",
                },
              }
            : {}),
          commandId: CommandId.make(`runtime-layer-wake-work-start-${key}`),
          threadId,
          messageId: messageId(key),
          text: key,
          attachments: [],
          modelSelection,
          dispatchMode:
            key === "prompt" ? { type: "start_immediately" } : { type: "queue_after_active" },
        });
      const runFor = (key: string) =>
        Effect.map(orchestrator.getThreadProjection(threadId), ({ runs }) => {
          const run = runs.find((candidate) => candidate.userMessageId === messageId(key));
          assert.isDefined(run);
          return run;
        });
      yield* dispatch("prompt", false);
      yield* dispatch("queued", false);
      yield* dispatch("early-wake", true);
      // The early wake now runs ahead of the older queued prompt, as a
      // delegated result does when it jumps the queue.
      yield* orchestrator.dispatch({
        type: "queued-run.reorder",
        commandId: CommandId.make("runtime-layer-wake-work-start-reorder"),
        threadId,
        runId: (yield* runFor("queued")).id,
        beforeRunId: null,
      });
      yield* dispatch("late-wake", true);
      // A queued wake has no clock yet: what runs before it is still unknown.
      assert.isUndefined((yield* runFor("early-wake")).workStartedAt);

      const startedRunIds = yield* Queue.unbounded<RunId>();
      const afterSequence = yield* orchestrator.getThreadEventSequence(threadId);
      yield* eventSink.stream({ threadId, afterSequence }).pipe(
        Stream.runForEach((stored) =>
          stored.event.type === "run.updated" && stored.event.payload.status === "starting"
            ? Queue.offer(startedRunIds, stored.event.payload.id)
            : Effect.void,
        ),
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;

      const now = yield* DateTime.now;
      // Runs start and settle the way the provider would report them.
      const settle = (key: string, startedAt: DateTime.Utc) =>
        Effect.gen(function* () {
          const run = yield* runFor(key);
          yield* eventSink.write({
            events: [
              {
                id: EventId.make(`runtime-layer-wake-work-start-${key}-completed`),
                type: "run.updated",
                threadId,
                runId: run.id,
                ...(run.rootNodeId === null ? {} : { nodeId: run.rootNodeId }),
                providerInstanceId: run.providerInstanceId,
                occurredAt: startedAt,
                payload: { ...run, status: "completed", startedAt, completedAt: startedAt },
              },
            ],
          });
        });
      const millis = (value: DateTime.Utc | undefined) =>
        value === undefined ? undefined : DateTime.toEpochMillis(value);

      yield* settle("prompt", now);
      assert.equal(yield* Queue.take(startedRunIds), (yield* runFor("early-wake")).id);
      assert.equal(millis((yield* runFor("early-wake")).workStartedAt), millis(now));

      yield* settle("early-wake", DateTime.add(now, { seconds: 1 }));
      assert.equal(yield* Queue.take(startedRunIds), (yield* runFor("queued")).id);
      assert.isUndefined((yield* runFor("queued")).workStartedAt);

      // The queued prompt starts long after it was requested; the wake after it
      // counts from that start, not from the request.
      const queuedStartedAt = DateTime.add(now, { minutes: 10 });
      yield* settle("queued", queuedStartedAt);
      assert.equal(yield* Queue.take(startedRunIds), (yield* runFor("late-wake")).id);
      assert.equal(millis((yield* runFor("late-wake")).workStartedAt), millis(queuedStartedAt));
    }),
  );

  it.effect.each(["usage_limit", "provider_error"] as const)(
    "handles a queued message after a %s failure",
    (failureClass) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make(`runtime-layer-failed-queue-${failureClass}`);

        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(`${threadId}:create`),
          threadId,
          projectId: ProjectId.make(`${threadId}:project`),
          title: "Failed queue",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: process.cwd(),
        });
        for (const index of [0, 1]) {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make(`${threadId}:message:${index}`),
            threadId,
            messageId: MessageId.make(`${threadId}:message:${index}`),
            text: index === 0 ? "Active" : "Queued",
            attachments: [],
            modelSelection,
            dispatchMode: { type: index === 0 ? "start_immediately" : "queue_after_active" },
          });
        }

        const before = yield* orchestrator.getThreadProjection(threadId);
        const activeRun = before.runs.find((run) => run.status === "starting");
        const queuedRun = before.runs.find((run) => run.status === "queued");
        assert.isDefined(activeRun);
        assert.isDefined(queuedRun);
        assert.isNotNull(activeRun.rootNodeId);

        const promotedRunIds = yield* Queue.unbounded<RunId>();
        const heldRunIds = yield* Queue.unbounded<RunId>();
        const afterSequence = yield* orchestrator.getThreadEventSequence(threadId);
        yield* eventSink.stream({ threadId, afterSequence }).pipe(
          Stream.runForEach((stored) =>
            stored.event.type !== "run.updated"
              ? Effect.void
              : stored.event.payload.status === "starting"
                ? Queue.offer(promotedRunIds, stored.event.payload.id)
                : stored.event.payload.queueHeld === true
                  ? Queue.offer(heldRunIds, stored.event.payload.id)
                  : Effect.void,
          ),
          Effect.forkScoped,
        );
        yield* Effect.yieldNow;

        const now = yield* DateTime.now;
        yield* eventSink.write({
          events: [
            {
              id: EventId.make(`${threadId}:error`),
              type: "turn-item.updated",
              threadId,
              runId: activeRun.id,
              nodeId: activeRun.rootNodeId,
              providerInstanceId: activeRun.providerInstanceId,
              occurredAt: now,
              payload: {
                id: TurnItemId.make(`${threadId}:error`),
                type: "error",
                threadId,
                runId: activeRun.id,
                nodeId: activeRun.rootNodeId,
                providerThreadId: activeRun.providerThreadId,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 2,
                status: "failed",
                title: "Provider failure",
                startedAt: now,
                completedAt: now,
                updatedAt: now,
                failure: {
                  class: failureClass,
                  message: "Provider failed.",
                  code: "provider_failed",
                  retryable: null,
                  ...(failureClass === "usage_limit"
                    ? { resetAt: DateTime.formatIso(DateTime.add(now, { hours: 1 })) }
                    : {}),
                },
              },
            },
            {
              id: EventId.make(`${threadId}:failed`),
              type: "run.updated",
              threadId,
              runId: activeRun.id,
              nodeId: activeRun.rootNodeId,
              providerInstanceId: activeRun.providerInstanceId,
              occurredAt: now,
              payload: { ...activeRun, status: "failed", completedAt: now },
            },
          ],
        });

        if (failureClass === "provider_error") {
          assert.equal(yield* Queue.take(heldRunIds), queuedRun.id);
          const held = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(held.runs.find((run) => run.id === queuedRun.id)?.status, "queued");
          yield* orchestrator.dispatch({
            type: "queue.resume",
            commandId: CommandId.make(`${threadId}:resume`),
            threadId,
          });
          assert.equal(yield* Queue.take(promotedRunIds), queuedRun.id);
          return;
        }
        yield* orchestrator.resumeQueuedRuns;
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(after.runs.find((run) => run.id === queuedRun.id)?.status, "queued");
        assert.isFalse(after.turnItems.some((item) => item.runId === queuedRun.id));
        const shell = (yield* orchestrator.getShellSnapshot()).threads.find(
          (thread) => thread.id === threadId,
        );
        assert.equal(shell?.latestRunId, activeRun.id);
        assert.equal(shell?.status, "failed");
        assert.equal(shell?.lastErrorClass, "usage_limit");
      }),
  );

  it.effect("keeps the queue after a user interrupts the active run", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-interrupted-queue");
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make(`${threadId}:create`),
        threadId,
        projectId: ProjectId.make(`${threadId}:project`),
        title: "Interrupted queue",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: process.cwd(),
      });
      for (const [index, text] of ["Active", "Queued"].entries()) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(`${threadId}:message:${index}`),
          threadId,
          messageId: MessageId.make(`${threadId}:message:${index}`),
          text,
          attachments: [],
          modelSelection,
          dispatchMode: { type: index === 0 ? "start_immediately" : "queue_after_active" },
        });
      }
      const before = yield* orchestrator.getThreadProjection(threadId);
      const activeRun = before.runs[0]!;
      const queuedRun = before.runs[1]!;
      yield* orchestrator.dispatch({
        type: "run.interrupt",
        commandId: CommandId.make(`${threadId}:interrupt`),
        threadId,
        runId: activeRun.id,
        holdQueue: true,
      });

      yield* orchestrator.resumeQueuedRuns;
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(after.runs.find((run) => run.id === activeRun.id)?.status, "interrupted");
      assert.equal(after.runs.find((run) => run.id === queuedRun.id)?.status, "queued");
      assert.isTrue(after.runs.find((run) => run.id === queuedRun.id)?.queueHeld);
      assert.isFalse(after.turnItems.some((item) => item.runId === queuedRun.id));
    }),
  );

  it.effect.each(["startup", "shutdown"] as const)(
    "preserves and holds queued messages across %s until explicitly resumed",
    (trigger) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const recovery = yield* ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService;
        const threadId = ThreadId.make(`queue-hold-${trigger}`);
        yield* orchestrator.dispatch({
          type: "thread.create",
          createdBy: "user",
          creationSource: "web",
          commandId: CommandId.make(`${threadId}:create`),
          threadId,
          projectId: ProjectId.make(`${threadId}:project`),
          title: "Recover queue",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: process.cwd(),
        });
        for (const [index, text] of ["Active", "First queued", "Second queued"].entries()) {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make(`${threadId}:message:${index}`),
            threadId,
            messageId: MessageId.make(`${threadId}:message:${index}`),
            text,
            attachments: [],
            modelSelection,
            dispatchMode: { type: index === 0 ? "start_immediately" : "queue_after_active" },
          });
        }
        const before = yield* orchestrator.getThreadProjection(threadId);
        const queued = before.runs.filter((run) => run.status === "queued");
        assert.equal(queued.length, 2);
        yield* recovery.reconcile(trigger);
        // A second boot must preserve the hold, even when only queued work remains.
        yield* recovery.reconcile("startup");
        const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
        assert.isTrue((yield* maintenance.rebuild).valid);
        assert.equal(yield* orchestrator.resumeQueuedRuns, 0);
        const held = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          held.runs.map((run) => run.status),
          ["cancelled", "queued", "queued"],
        );
        for (const run of queued) {
          assert.deepEqual(
            held.runs.find((row) => row.id === run.id),
            { ...run, queueHeld: true },
          );
          assert.deepEqual(
            held.messages.find((row) => row.id === run.userMessageId),
            before.messages.find((row) => row.id === run.userMessageId),
          );
          assert.equal(
            held.attempts.find((row) => row.id === run.activeAttemptId)?.status,
            "pending",
          );
          assert.equal(held.nodes.find((row) => row.id === run.rootNodeId)?.status, "pending");
        }
        // Editing and reordering are allowed without releasing the hold.
        const first = queued[0]!;
        const second = queued[1]!;
        yield* orchestrator.dispatch({
          type: "queued-run.edit",
          commandId: CommandId.make(`${threadId}:edit`),
          threadId,
          runId: second.id,
          text: "Edited second message",
        });
        yield* orchestrator.dispatch({
          type: "queued-run.reorder",
          commandId: CommandId.make(`${threadId}:reorder`),
          threadId,
          runId: second.id,
          beforeRunId: first.id,
        });
        assert.equal(yield* orchestrator.resumeQueuedRuns, 0);
        const resume = {
          type: "queue.resume" as const,
          commandId: CommandId.make(`${threadId}:resume`),
          threadId,
        };
        yield* orchestrator.dispatch(resume);
        yield* orchestrator.dispatch(resume);
        const resumed = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(resumed.runs.find((run) => run.id === second.id)?.status, "starting");
        assert.equal(resumed.runs.find((run) => run.id === first.id)?.status, "queued");
        assert.isFalse(resumed.runs.some((run) => run.status === "queued" && run.queueHeld));
        assert.equal(
          resumed.messages.find((row) => row.id === second.userMessageId)?.text,
          "Edited second message",
        );
        assert.equal(resumed.runs.length, 3, "resume retries must not duplicate messages or runs");
      }),
  );

  it.effect("edits and removes queued runs", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make("runtime-layer-queued-edit-thread");

      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-queued-edit-create"),
        threadId,
        projectId: ProjectId.make("runtime-layer-queued-edit-project"),
        title: "Edit queued work",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: "/tmp/runtime-layer-queued-edit",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-queued-edit-active-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-queued-edit-active-message"),
        text: "Keep the provider occupied.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-queued-edit-queued-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-queued-edit-queued-message"),
        text: "Original queued text.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "queue_after_active" },
      });

      const before = yield* orchestrator.getThreadProjection(threadId);
      const queuedRun = before.runs.find((run) => run.status === "queued");
      assert.isDefined(queuedRun);

      yield* orchestrator.dispatch({
        type: "queued-run.edit",
        commandId: CommandId.make("runtime-layer-queued-edit-edit"),
        threadId,
        runId: queuedRun.id,
        text: "Updated queued text.",
      });

      const afterEdit = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(
        afterEdit.messages.find((message) => message.id === queuedRun.userMessageId)?.text,
        "Updated queued text.",
      );
      const editedItem = afterEdit.turnItems.find(
        (item) => item.type === "user_message" && item.messageId === queuedRun.userMessageId,
      );
      assert.isUndefined(editedItem, "editing queue state must not create a timeline turn item");

      yield* orchestrator.dispatch({
        type: "queued-run.edit",
        commandId: CommandId.make("runtime-layer-queued-edit-attachments"),
        threadId,
        runId: queuedRun.id,
        text: "Updated queued text with an attachment.",
        attachments: [
          {
            type: "image",
            id: "runtime-layer-queued-edit-attachment",
            name: "screenshot.png",
            mimeType: "image/png",
            sizeBytes: 128,
          },
        ],
      });
      const afterAttachmentEdit = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(
        afterAttachmentEdit.messages
          .find((message) => message.id === queuedRun.userMessageId)
          ?.attachments.map((attachment) => attachment.id),
        ["runtime-layer-queued-edit-attachment"],
      );

      yield* orchestrator.dispatch({
        type: "queued-run.edit",
        commandId: CommandId.make("runtime-layer-queued-edit-text-only"),
        threadId,
        runId: queuedRun.id,
        text: "Text-only edit keeps attachments.",
      });
      const afterTextOnlyEdit = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(
        afterTextOnlyEdit.messages
          .find((message) => message.id === queuedRun.userMessageId)
          ?.attachments.map((attachment) => attachment.id),
        ["runtime-layer-queued-edit-attachment"],
        "an edit without attachments must leave the stored attachments untouched",
      );

      const emptyEditError = yield* orchestrator
        .dispatch({
          type: "queued-run.edit",
          commandId: CommandId.make("runtime-layer-queued-edit-empty"),
          threadId,
          runId: queuedRun.id,
          text: "   ",
        })
        .pipe(Effect.flip);
      assert.equal(emptyEditError._tag, "OrchestratorCommandRejectedError");

      yield* orchestrator.dispatch({
        type: "queued-run.cancel",
        commandId: CommandId.make("runtime-layer-queued-edit-cancel"),
        threadId,
        runId: queuedRun.id,
      });

      const afterCancel = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(afterCancel.runs.find((run) => run.id === queuedRun.id)?.status, "cancelled");
      assert.equal(
        afterCancel.attempts.find((attempt) => attempt.runId === queuedRun.id)?.status,
        "cancelled",
      );
      assert.equal(
        afterCancel.nodes.find((node) => node.runId === queuedRun.id)?.status,
        "cancelled",
      );
      assert.isFalse(
        afterCancel.visibleTurnItems.some(
          (row) => row.item.type === "user_message" && row.item.runId === queuedRun.id,
        ),
        "removed queued message must not surface as a transcript row",
      );
      assert.equal(yield* orchestrator.resumeQueuedRuns, 0);

      const cancelAgainError = yield* orchestrator
        .dispatch({
          type: "queued-run.cancel",
          commandId: CommandId.make("runtime-layer-queued-edit-cancel-again"),
          threadId,
          runId: queuedRun.id,
        })
        .pipe(Effect.flip);
      assert.equal(cancelAgainError._tag, "OrchestratorDispatchError");
    }),
  );
});

it.layer(layerSharedApplicationDataPlaneTest)("pending provider interruption", (it) => {
  it.effect("interrupts a pending provider start without launching provider work", () =>
    Effect.gen(function* () {
      const projects = yield* ProjectService.ProjectService;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadManagement = yield* ThreadManagementService.ThreadManagementService;
      const effectWorker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const projectId = ProjectId.make("runtime-layer-pending-interrupt-project");
      const threadId = ThreadId.make("runtime-layer-pending-interrupt-thread");

      yield* projects.create({
        commandId: CommandId.make("runtime-layer-pending-interrupt-project-create"),
        projectId,
        title: "Pending interrupt project",
        workspaceRoot: "/tmp/runtime-layer-pending-interrupt-project",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-pending-interrupt-create"),
        threadId,
        projectId,
        title: "Pending interrupt",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-pending-interrupt-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-pending-interrupt-message"),
        text: "Do not reach the provider.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });

      const starting = yield* orchestrator.getThreadProjection(threadId);
      const run = starting.runs[0];
      assert.isDefined(run);
      assert.equal(run.status, "starting");

      const interrupt = yield* threadManagement.interruptThread({
        projectId,
        commandId: CommandId.make("runtime-layer-pending-interrupt-command"),
        threadId,
        runId: run.id,
        reason: "Cancelled before provider start",
      });
      assert.equal(interrupt.type, "interrupt_requested");

      const interrupted = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(interrupted.runs[0]?.status, "interrupted");
      assert.equal(interrupted.attempts[0]?.status, "interrupted");
      assert.equal(
        interrupted.nodes.find((node) => node.kind === "root_turn")?.status,
        "interrupted",
      );
      assert.deepEqual(
        interrupted.turnItems.filter((item) => item.runId === run.id).map((item) => item.type),
        ["user_message", "run_interrupt_request", "run_interrupt_result"],
      );
      assert.deepEqual(interrupted.providerTurns, []);
      assert.isFalse(yield* effectWorker.runOnce);
    }),
  );
});

it.layer(layerTest)("snooze queued runs", (it) => {
  it.effect.each(["notification", "delegated-completion", "user"] as const)(
    "only user messages block snooze with a held %s run",
    (kind) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make(`snooze-queued:${kind}`);
        const snoozedUntil = "2099-07-25T09:00:00.000Z";
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`create:${threadId}`),
          threadId,
          projectId: ProjectId.make("snooze-queued-project"),
          title: "Snooze with held queued work",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: "/tmp/snooze-queued",
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`active:${threadId}`),
          threadId,
          messageId: MessageId.make(`active:${threadId}`),
          text: "Active run",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const activeRun = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        const now = yield* DateTime.now;
        if (kind === "delegated-completion") {
          yield* eventSink.write({
            events: [
              {
                id: EventId.make(`completion:${threadId}`),
                type: "run.updated",
                threadId,
                runId: activeRun.id,
                occurredAt: now,
                payload: {
                  ...activeRun,
                  delegatedCompletion: {
                    disposition: "open",
                    nextGeneration: 2,
                    delivery: {
                      messageId: MessageId.make(`queued:${threadId}`),
                      generation: 1,
                      taskIds: [NodeId.make("snooze-queued-task")],
                    },
                  },
                },
              },
            ],
          });
        }
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`queued:${threadId}`),
          threadId,
          messageId: MessageId.make(`queued:${threadId}`),
          text: "Queued work",
          attachments: [],
          modelSelection,
          dispatchMode: { type: "queue_after_active" },
          createdBy: kind === "user" ? "user" : "agent",
          creationSource: kind === "user" ? "web" : "server",
          ...(kind === "notification"
            ? {
                notification: {
                  source: { kind: "background_task" as const },
                  outcome: "updated" as const,
                  summary: "Background activity updated",
                },
              }
            : kind === "delegated-completion"
              ? {
                  delegatedCompletion: {
                    parentRunId: activeRun.id,
                    generation: 1,
                    taskIds: [NodeId.make("snooze-queued-task")],
                  },
                }
              : {}),
        });
        // Recovery holds the queue while the thread's active run ends.
        const beforeRecovery = yield* orchestrator.getThreadProjection(threadId);
        yield* eventSink.commitCommand({
          commandId: CommandId.make(`recovery:${threadId}`),
          threadId,
          commandType: "provider-runtime.reconcile",
          acceptedAt: now,
          events: beforeRecovery.runs.map((run) => ({
            id: EventId.make(`recovery:${run.id}`),
            type: "run.updated" as const,
            threadId,
            runId: run.id,
            occurredAt: now,
            payload:
              run.status === "queued"
                ? { ...run, queueHeld: true }
                : { ...run, status: "completed" as const, completedAt: now },
          })),
          effects: [],
        });
        const beforeSnooze = yield* orchestrator.getThreadProjection(threadId);
        assert.isTrue(beforeSnooze.runs.some((run) => run.status === "queued" && run.queueHeld));
        const snooze = orchestrator.dispatch({
          type: "thread.snooze",
          commandId: CommandId.make(`snooze:${threadId}`),
          threadId,
          snoozedUntil,
        });
        if (kind === "user") {
          const error = yield* snooze.pipe(Effect.flip);
          assert.equal(error._tag, "OrchestratorDispatchError");
          assert.equal(error.cause, `Thread ${threadId} has a queued run and cannot be snoozed.`);
          assert.isNull((yield* orchestrator.getThreadProjection(threadId)).thread.snoozedUntil);
        } else {
          yield* snooze;
          const snoozed = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(DateTime.formatIso(snoozed.thread.snoozedUntil!), snoozedUntil);
          assert.deepEqual(snoozed.runs, beforeSnooze.runs);
          yield* orchestrator.dispatch({
            type: "thread.unsnooze",
            commandId: CommandId.make(`unsnooze:${threadId}`),
            threadId,
            reason: "user",
          });
          const awake = yield* orchestrator.getThreadProjection(threadId);
          assert.isNull(awake.thread.snoozedUntil);
          assert.deepEqual(awake.runs, beforeSnooze.runs);
        }
      }),
  );
});

it.layer(layerSharedApplicationDataPlaneTest)("snooze projection", (it) => {
  it.effect("carries snooze state through the V2 shell projection", () =>
    Effect.gen(function* () {
      const projects = yield* ProjectService.ProjectService;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projectId = ProjectId.make("runtime-layer-snoozed-project");
      const threadId = ThreadId.make("runtime-layer-snoozed-thread");
      const snoozedUntil = "2099-07-25T09:00:00.000Z";

      yield* projects.create({
        commandId: CommandId.make("runtime-layer-snoozed-project-create"),
        projectId,
        title: "Snoozed shell projection",
        workspaceRoot: "/tmp/runtime-layer-snoozed-project",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-snoozed-thread-create"),
        threadId,
        projectId,
        title: "Snoozed thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      yield* orchestrator.dispatch({
        type: "thread.snooze",
        commandId: CommandId.make("runtime-layer-snoozed-thread-snooze"),
        threadId,
        snoozedUntil,
      });

      const firstProjection = yield* orchestrator.getThreadProjection(threadId);
      const firstSnoozedAt = firstProjection.thread.snoozedAt;
      const firstUpdatedAt = firstProjection.thread.updatedAt;
      assert.isNotNull(firstSnoozedAt);

      yield* orchestrator.dispatch({
        type: "thread.snooze",
        commandId: CommandId.make("runtime-layer-snoozed-thread-snooze-again"),
        threadId,
        snoozedUntil,
      });

      const shell = yield* orchestrator.getShellSnapshot();
      const thread = shell.threads.find((candidate) => candidate.id === threadId);
      assert.isDefined(thread);
      assert.equal(DateTime.formatIso(thread.snoozedUntil!), snoozedUntil);
      assert.deepEqual(thread.snoozedAt, firstSnoozedAt);
      assert.deepEqual(thread.updatedAt, firstUpdatedAt);

      yield* orchestrator.dispatch({
        type: "thread.unsnooze",
        commandId: CommandId.make("runtime-layer-snoozed-thread-wake"),
        threadId,
        reason: "user",
      });
      const projections = yield* ProjectionStore.ProjectionStoreV2.pipe(
        Effect.provide(ProjectionStore.layer),
      );
      const [candidate] = yield* projections.getSettlementCandidates(threadId);
      assert.deepEqual(candidate?.lastSnoozeWakeAt, yield* DateTime.now);
      yield* orchestrator.dispatch({
        type: "thread.snooze",
        commandId: CommandId.make("runtime-layer-snoozed-thread-snooze-after-wake"),
        threadId,
        snoozedUntil,
      });

      yield* orchestrator.dispatch({
        type: "message.dispatch",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-snoozed-message"),
        threadId,
        messageId: MessageId.make("runtime-layer-snoozed-message"),
        text: "Wake this thread.",
        attachments: [],
        modelSelection,
        dispatchMode: { type: "start_immediately" },
      });

      const awakened = yield* orchestrator.getThreadProjection(threadId);
      assert.isNull(awakened.thread.snoozedUntil);
      assert.isNull(awakened.thread.snoozedAt);
    }),
  );
});

it.layer(layerSharedApplicationDataPlaneTest)("visited projection", (it) => {
  it.effect("carries the visited watermark through the V2 shell projection", () =>
    Effect.gen(function* () {
      const projects = yield* ProjectService.ProjectService;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projectId = ProjectId.make("runtime-layer-visited-project");
      const threadId = ThreadId.make("runtime-layer-visited-thread");
      const visitedAt = "2026-07-24T01:00:00.000Z";

      yield* projects.create({
        commandId: CommandId.make("runtime-layer-visited-project-create"),
        projectId,
        title: "Visited shell projection",
        workspaceRoot: "/tmp/runtime-layer-visited-project",
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-visited-thread-create"),
        threadId,
        projectId,
        title: "Visited thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const created = yield* orchestrator.getThreadProjection(threadId);
      assert.isNull(created.thread.lastVisitedAt);
      const createdUpdatedAt = created.thread.updatedAt;

      yield* TestClock.adjust("1 second");
      yield* orchestrator.dispatch({
        type: "thread.visit",
        commandId: CommandId.make("runtime-layer-visited-thread-visit"),
        threadId,
        visitedAt,
      });
      const visited = yield* orchestrator.getThreadProjection(threadId);
      assert.isNotNull(visited.thread.lastVisitedAt);
      assert.equal(DateTime.formatIso(visited.thread.lastVisitedAt!), visitedAt);
      // Visiting records read state, not activity: updatedAt must not move.
      assert.deepEqual(visited.thread.updatedAt, createdUpdatedAt);

      // Monotonic: an older watermark (a replay or a stale device) cannot
      // rewind the marker.
      yield* orchestrator.dispatch({
        type: "thread.visit",
        commandId: CommandId.make("runtime-layer-visited-thread-visit-stale"),
        threadId,
        visitedAt: "2026-07-24T00:30:00.000Z",
      });
      const afterStaleVisit = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(DateTime.formatIso(afterStaleVisit.thread.lastVisitedAt!), visitedAt);

      const shell = yield* orchestrator.getShellSnapshot();
      const thread = shell.threads.find((candidate) => candidate.id === threadId);
      assert.isDefined(thread);
      assert.equal(DateTime.formatIso(thread!.lastVisitedAt!), visitedAt);
      assert.deepEqual(thread!.updatedAt, createdUpdatedAt);

      // No completed run yet → nothing to mark unread against.
      const markUnread = yield* orchestrator
        .dispatch({
          type: "thread.mark-unread",
          commandId: CommandId.make("runtime-layer-visited-thread-mark-unread"),
          threadId,
        })
        .pipe(Effect.flip);
      assert.instanceOf(markUnread, Orchestrator.OrchestratorDispatchError);

      // A read receipt must not decode any transcript, including inherited or
      // unreadable historical rows. It only advances the thread's watermark.
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO orchestration_v2_projection_turn_items (
        turn_item_id, thread_id, ordinal, type, status, updated_at, payload_json
      ) VALUES (
        'item:visited:unreadable-history', ${threadId}, 1, 'dynamic_tool', 'completed',
        ${visitedAt}, '{broken'
      )`;
      const nextVisitedAt = "2026-07-24T02:00:00.000Z";
      yield* orchestrator.dispatch({
        type: "thread.visit",
        commandId: CommandId.make("runtime-layer-visited-without-history"),
        threadId,
        visitedAt: nextVisitedAt,
      });
      const [watermark] = yield* sql<{ readonly visited_at: string; readonly updated_at: string }>`
        SELECT json_extract(payload_json, '$.lastVisitedAt') AS visited_at, updated_at
        FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}
      `;
      assert.equal(watermark!.visited_at, nextVisitedAt);
      assert.equal(watermark!.updated_at, DateTime.formatIso(createdUpdatedAt));
      const invalidVisit = yield* orchestrator
        .dispatch({
          type: "thread.visit",
          commandId: CommandId.make("runtime-layer-visited-invalid-timestamp"),
          threadId,
          visitedAt: "invalid-timestamp",
        })
        .pipe(Effect.flip);
      assert.instanceOf(invalidVisit, Orchestrator.OrchestratorDispatchError);
    }),
  );
});

it.layer(layerSharedApplicationDataPlaneTest)("shared application data plane", (it) => {
  it.effect("orders retained project transactions and V2 thread transactions in one source", () =>
    Effect.gen(function* () {
      const projects = yield* ProjectService.ProjectService;
      const applicationEvents = yield* OrchestrationEventStore.OrchestrationEventStore;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sql = yield* SqlClient.SqlClient;
      const projectId = ProjectId.make("runtime-layer-shared-project");
      const threadId = ThreadId.make("runtime-layer-shared-thread");
      const projectInput = {
        commandId: CommandId.make("runtime-layer-shared-project-create"),
        projectId,
        title: "Shared application source",
        workspaceRoot: "/tmp/runtime-layer-shared-project",
      };

      const created = yield* projects.create(projectInput);
      const retried = yield* projects.create(projectInput);
      assert.deepEqual(retried, created);

      const delivered = yield* Queue.unbounded<ApplicationStoredEvent>();
      yield* applicationEvents.streamApplicationEvents().pipe(
        Stream.take(2),
        Stream.runForEach((event) => Queue.offer(delivered, event)),
        Effect.forkScoped,
      );

      const projectEvent = yield* Queue.take(delivered);
      assert.isTrue("aggregateKind" in projectEvent && projectEvent.aggregateId === projectId);

      const threadResult = yield* orchestrator.dispatch({
        type: "thread.create",
        createdBy: "user",
        creationSource: "web",
        commandId: CommandId.make("runtime-layer-shared-thread-create"),
        threadId,
        projectId,
        title: "Shared thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      const threadEvent = yield* Queue.take(delivered);

      assert.equal(threadEvent.sequence, threadResult.sequence);
      assert.isAbove(threadEvent.sequence, projectEvent.sequence);
      assert.isTrue("aggregateKind" in projectEvent);
      assert.isTrue("event" in threadEvent);
      assert.equal((yield* projects.getById(projectId))._tag, "Some");

      const retainedReceipts = yield* sql<{
        readonly aggregate_kind: string;
        readonly aggregate_id: string;
      }>`
        SELECT aggregate_kind, aggregate_id
        FROM orchestration_command_receipts
        ORDER BY result_sequence ASC
      `;
      assert.deepEqual(retainedReceipts, [
        { aggregate_kind: "project", aggregate_id: projectId },
        { aggregate_kind: "thread", aggregate_id: threadId },
      ]);

      const retiredWrites = yield* sql<{ readonly count: number }>`
        SELECT
          (SELECT COUNT(*) FROM orchestration_v2_events) +
          (SELECT COUNT(*) FROM orchestration_v2_command_receipts) AS count
      `;
      assert.equal(retiredWrites[0]?.count, 0);
    }),
  );
});

it.layer(layerTest)("usage-limit recovery", (it) => {
  it.effect.each(["interrupted", "usage_limit"] as const)(
    "manually resumes an %s run ahead of its queued message only once",
    (reason) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const events = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make(`manual-resume:${reason}`);
        const projectId = ProjectId.make(`manual-resume:project:${reason}`);
        const now = yield* DateTime.now;
        const createdAt = DateTime.formatIso(now);
        yield* seedProject({
          projectId,
          title: "Resume project",
          workspaceRoot: process.cwd(),
          defaultModelSelection: modelSelection,
          createdAt,
        });
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`manual-resume:create:${reason}`),
          threadId,
          projectId,
          title: "Interrupted thread",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`manual-resume:start:${reason}`),
          threadId,
          messageId: MessageId.make(`manual-resume:start:${reason}`),
          text: "Start work.",
          attachments: [],
          dispatchMode: { type: "defer_start" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`manual-resume:queue:${reason}`),
          threadId,
          messageId: MessageId.make(`manual-resume:queue:${reason}`),
          text: "Follow up.",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const [source, queued] = (yield* orchestrator.getThreadProjection(threadId)).runs as [
          OrchestrationV2Run,
          OrchestrationV2Run,
        ];
        // A user stop holds the queue as the run ends (thread.turn.interrupt with
        // holdQueue). Without the hold, the terminal-run worker may start the
        // queued run before the resume below, depending on fiber scheduling.
        yield* events.write({
          events: [
            {
              id: EventId.make(`manual-resume:hold:${reason}`),
              type: "run.updated",
              threadId,
              occurredAt: now,
              payload: { ...queued, queueHeld: true },
            },
            {
              id: EventId.make(`manual-resume:stop:${reason}`),
              type: "run.updated",
              threadId,
              occurredAt: now,
              payload: {
                ...source,
                status: reason === "interrupted" ? "interrupted" : "failed",
                completedAt: now,
              },
            },
          ],
        });
        let scheduledResume: ReturnType<typeof limitRecoveryCommand> = null;
        if (reason === "usage_limit") {
          const resetAt = DateTime.formatIso(DateTime.add(now, { minutes: 1 }));
          yield* events.write({
            events: [
              {
                id: EventId.make(`manual-resume:error:${reason}`),
                type: "turn-item.updated",
                threadId,
                occurredAt: now,
                payload: {
                  id: TurnItemId.make(`manual-resume:error:${reason}`),
                  type: "error",
                  threadId,
                  runId: source.id,
                  nodeId: source.rootNodeId,
                  providerThreadId: null,
                  providerTurnId: null,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal: 2,
                  status: "failed",
                  title: "Usage limit reached",
                  startedAt: now,
                  completedAt: now,
                  updatedAt: now,
                  failure: {
                    class: "usage_limit",
                    message: "Plan limit reached.",
                    code: "usageLimitExceeded",
                    retryable: null,
                    resetAt,
                  },
                },
              },
            ],
          });
          const shell = (yield* orchestrator.getShellSnapshot()).threads.find(
            (thread) => thread.id === threadId,
          )!;
          yield* orchestrator.dispatch(
            limitRecoveryCommand(shell, true, DateTime.toEpochMillis(now))!,
          );
          const armed = (yield* orchestrator.getShellSnapshot()).threads.find(
            (thread) => thread.id === threadId,
          )!;
          scheduledResume = limitRecoveryCommand(armed, true, Date.parse(resetAt));
          assert.isNotNull(scheduledResume);
        }
        const resume = (suffix: string) => ({
          type: "message.dispatch" as const,
          commandId: CommandId.make(`manual-resume:${suffix}:${reason}`),
          threadId,
          messageId: MessageId.make(`manual-resume:${suffix}:${reason}`),
          manualContinuationOfRunId: source.id,
          text: "Continue where you left off.",
          attachments: [],
          dispatchMode: { type: "start_immediately" as const },
          createdBy: "user" as const,
          creationSource: "web" as const,
        });
        yield* orchestrator.dispatch(resume("first"));
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.lengthOf(after.runs, 3);
        assert.equal(after.runs[1]?.status, "queued");
        assert.equal(after.runs[2]?.status, "starting");
        assert.equal(
          (yield* orchestrator.dispatch(resume("second")).pipe(Effect.exit))._tag,
          "Failure",
        );
        if (scheduledResume !== null) {
          yield* TestClock.adjust("1 minute");
          yield* orchestrator.dispatch(scheduledResume);
        }
        assert.lengthOf((yield* orchestrator.getThreadProjection(threadId)).runs, 3);
      }),
  );

  it.effect.each([
    "resume",
    "queued-resume",
    "cancel",
    "rearm",
    "snooze-race",
    "new-message",
    "archive",
    "settle",
    "replacement",
    "manual-snooze",
    "manual-snooze-after-recovery",
    "invalid-snooze",
    "snooze-only",
    "snooze-resume",
    "cancel-resume-keep-snooze",
    "wake-preserve-resume",
    "independent-patches",
    "expired-snooze",
    "wake",
  ] as const)("guards a scheduled usage-limit continuation against %s", (scenario) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const events = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make(`recovery:${scenario}`);
      const projectId = ProjectId.make(`recovery:project:${scenario}`);
      yield* seedProject({
        projectId,
        title: "Recovery project",
        workspaceRoot: process.cwd(),
        defaultModelSelection: modelSelection,
        createdAt: DateTime.formatIso(yield* DateTime.now),
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make(`recovery:create:${scenario}`),
        threadId,
        projectId,
        title: "Limited thread",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`recovery:message:${scenario}`),
        threadId,
        messageId: MessageId.make(`recovery:message:${scenario}`),
        text: "Work on this.",
        attachments: [],
        dispatchMode: { type: "defer_start" },
        createdBy: "user",
        creationSource: "web",
      });
      if (scenario === "queued-resume") {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`recovery:queued:${scenario}`),
          threadId,
          messageId: MessageId.make(`recovery:queued:${scenario}`),
          text: "Run after recovery.",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
      }
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const run = projection.runs[0]!;
      const now = yield* DateTime.now;
      const resetAt =
        scenario === "invalid-snooze"
          ? "not-a-date"
          : DateTime.formatIso(DateTime.add(now, { minutes: 1 })).replace(
              "Z",
              scenario === "wake" ? "+00:00" : "Z",
            );
      yield* events.write({
        commandId: CommandId.make(`recovery:failure:${scenario}`),
        events: [
          {
            id: EventId.make(`recovery:run:${scenario}`),
            type: "run.updated",
            threadId,
            occurredAt: now,
            payload: { ...run, status: "failed", completedAt: now },
          },
          {
            id: EventId.make(`recovery:error:${scenario}`),
            type: "turn-item.updated",
            threadId,
            occurredAt: now,
            payload: {
              id: TurnItemId.make(`recovery:error:${scenario}`),
              type: "error",
              threadId,
              runId: run.id,
              nodeId: run.rootNodeId,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 2,
              status: "failed",
              title: "Usage limit reached",
              startedAt: now,
              completedAt: now,
              updatedAt: now,
              failure: {
                class: "usage_limit",
                message: "Plan limit reached.",
                code: "usageLimitExceeded",
                retryable: null,
                resetAt,
              },
            },
          },
        ],
      });
      if (scenario === "queued-resume") {
        const queuedRun = projection.runs[1]!;
        yield* events.write({
          events: [
            {
              id: EventId.make("recovery:held:queued-resume"),
              type: "run.updated",
              threadId,
              runId: queuedRun.id,
              occurredAt: now,
              payload: { ...queuedRun, queueHeld: true },
            },
          ],
        });
        const resumeHeldQueue = yield* orchestrator
          .dispatch({
            type: "queue.resume",
            commandId: CommandId.make("recovery:resume-held:queued-resume"),
            threadId,
          })
          .pipe(Effect.exit);
        assert.equal(resumeHeldQueue._tag, "Failure");
        assert.isTrue((yield* orchestrator.getThreadProjection(threadId)).runs[1]?.queueHeld);
      }
      const shell = (yield* orchestrator.getShellSnapshot()).threads.find(
        (thread) => thread.id === threadId,
      )!;
      assert.isNull(limitRecoveryCommand(shell, false, DateTime.toEpochMillis(now)));
      if (scenario === "invalid-snooze") {
        const result = yield* orchestrator
          .dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make("recovery:invalid-snooze"),
            threadId,
            limitRecovery: { runId: run.id, resetAt, snooze: true },
          })
          .pipe(Effect.exit);
        assert.equal(result._tag, "Failure");
        const current = yield* orchestrator.getThreadProjection(threadId);
        assert.isNull(current.thread.limitRecovery ?? null);
        assert.isNull(current.thread.snoozedUntil);
        return;
      }
      const snooze = [
        "snooze-only",
        "manual-snooze-after-recovery",
        "snooze-resume",
        "wake",
        "cancel-resume-keep-snooze",
        "wake-preserve-resume",
      ].includes(scenario);
      const autoResume = scenario !== "snooze-only" && scenario !== "wake";
      const arm = limitRecoveryCommand(shell, autoResume, DateTime.toEpochMillis(now), snooze);
      assert.isNotNull(arm);
      yield* orchestrator.dispatch(arm!);
      let armedShell = (yield* orchestrator.getShellSnapshot()).threads.find(
        (thread) => thread.id === threadId,
      )!;
      assert.deepEqual(armedShell.limitRecovery, {
        runId: run.id,
        resetAt,
        autoResume,
        snooze,
        requestId: arm!.commandId,
      });
      if (snooze)
        assert.equal(DateTime.toEpochMillis(armedShell.snoozedUntil!), Date.parse(resetAt));
      if (scenario === "cancel-resume-keep-snooze" || scenario === "wake-preserve-resume") {
        yield* TestClock.adjust("10 seconds");
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`recovery:independent-choice:${scenario}`),
          threadId,
          limitRecovery: {
            runId: run.id,
            resetAt,
            autoResume: scenario === "wake-preserve-resume",
            snooze: scenario === "cancel-resume-keep-snooze",
          },
        });
        armedShell = (yield* orchestrator.getShellSnapshot()).threads.find(
          (thread) => thread.id === threadId,
        )!;
        if (scenario === "cancel-resume-keep-snooze") {
          assert.equal(DateTime.toEpochMillis(armedShell.snoozedUntil!), Date.parse(resetAt));
          // Failed runtime timestamps advance with metadata. Acknowledging the
          // same failed run must not turn cancellation into an early wake.
          assert.equal(
            DateTime.toEpochMillis(armedShell.snoozedAt!),
            DateTime.toEpochMillis(armedShell.updatedAt),
          );
          assert.isFalse(armedShell.limitRecovery!.autoResume);
        } else {
          assert.isNull(armedShell.snoozedUntil);
          assert.isNull(armedShell.snoozedAt);
          assert.isTrue(armedShell.limitRecovery!.autoResume);
        }
      }
      if (scenario === "independent-patches") {
        yield* TestClock.adjust("10 seconds");
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("recovery:patch-snooze"),
          threadId,
          limitRecovery: { runId: run.id, resetAt, snooze: true },
        });
        let current = yield* orchestrator.getThreadProjection(threadId);
        assert.isTrue(current.thread.limitRecovery!.autoResume);
        assert.isTrue(current.thread.limitRecovery!.snooze);
        // This is also the payload an older auto-resume-only client sends.
        yield* TestClock.adjust("10 seconds");
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("recovery:patch-cancel-resume"),
          threadId,
          limitRecovery: { runId: run.id, resetAt, autoResume: false },
        });
        current = yield* orchestrator.getThreadProjection(threadId);
        assert.isFalse(current.thread.limitRecovery!.autoResume);
        assert.isTrue(current.thread.limitRecovery!.snooze);
        assert.equal(DateTime.toEpochMillis(current.thread.snoozedUntil!), Date.parse(resetAt));
        assert.equal(
          DateTime.toEpochMillis(current.thread.snoozedAt!),
          DateTime.toEpochMillis(current.thread.updatedAt),
        );
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("recovery:patch-resume"),
          threadId,
          limitRecovery: { runId: run.id, resetAt, autoResume: true },
        });
        armedShell = (yield* orchestrator.getShellSnapshot()).threads.find(
          (thread) => thread.id === threadId,
        )!;
        assert.isTrue(armedShell.limitRecovery!.autoResume);
        assert.isTrue(armedShell.limitRecovery!.snooze);
      }
      if (scenario === "manual-snooze" || scenario === "manual-snooze-after-recovery") {
        yield* orchestrator.dispatch({
          type: "thread.snooze",
          commandId: CommandId.make(`recovery:manual-snooze:${scenario}`),
          threadId,
          snoozedUntil: resetAt,
        });
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`recovery:manual-cancel:${scenario}`),
          threadId,
          limitRecovery: { runId: run.id, resetAt, autoResume: false, snooze: false },
        });
        assert.equal(
          DateTime.toEpochMillis(
            (yield* orchestrator.getThreadProjection(threadId)).thread.snoozedUntil!,
          ),
          Date.parse(resetAt),
        );
        yield* orchestrator.dispatch({
          type: "thread.unsnooze",
          commandId: CommandId.make(`recovery:manual-wake:${scenario}`),
          threadId,
          reason: "user",
        });
        assert.isNull((yield* orchestrator.getThreadProjection(threadId)).thread.snoozedUntil);
      }
      if (scenario === "wake") {
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`recovery:wake:${scenario}`),
          threadId,
          limitRecovery: { runId: run.id, resetAt, autoResume: false, snooze: false },
        });
        assert.isNull((yield* orchestrator.getThreadProjection(threadId)).thread.snoozedUntil);
      }
      assert.isNull(limitRecoveryCommand(armedShell, true, DateTime.toEpochMillis(now)));
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`recovery:early:${scenario}`),
        messageId: MessageId.make(`recovery:early:${scenario}`),
        threadId,
        usageLimitContinuationOfRunId: run.id,
        text: "Continue where you left off.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "server",
      });
      assert.lengthOf(
        (yield* orchestrator.getThreadProjection(threadId)).runs,
        scenario === "queued-resume" ? 2 : 1,
      );
      yield* TestClock.adjust("1 minute");
      const resume = limitRecoveryCommand(
        armedShell,
        true,
        DateTime.toEpochMillis(yield* DateTime.now),
      );
      if (autoResume && scenario !== "cancel-resume-keep-snooze") assert.isNotNull(resume);
      else assert.isNull(resume);
      if (scenario === "snooze-race") {
        const wakeAt = DateTime.formatIso(DateTime.add(yield* DateTime.now, { minutes: 1 }));
        yield* orchestrator.dispatch({
          type: "thread.snooze",
          commandId: CommandId.make("recovery:raced-snooze"),
          threadId,
          snoozedUntil: wakeAt,
        });
        yield* orchestrator.dispatch(resume!);
        assert.lengthOf((yield* orchestrator.getThreadProjection(threadId)).runs, 1);
        yield* TestClock.adjust("1 minute");
        const current = (yield* orchestrator.getShellSnapshot()).threads.find(
          (thread) => thread.id === threadId,
        )!;
        const freshResume = limitRecoveryCommand(
          current,
          true,
          DateTime.toEpochMillis(yield* DateTime.now),
        );
        assert.isNotNull(freshResume);
        assert.notEqual(freshResume!.commandId, resume!.commandId);
        yield* orchestrator.dispatch(freshResume!);
        yield* orchestrator.dispatch(freshResume!);
        assert.lengthOf((yield* orchestrator.getThreadProjection(threadId)).runs, 2);
      }
      if (scenario === "expired-snooze") {
        const staleSnooze = yield* orchestrator
          .dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make("recovery:expired-snooze"),
            threadId,
            limitRecovery: { runId: run.id, resetAt, snooze: true },
          })
          .pipe(Effect.exit);
        assert.equal(staleSnooze._tag, "Failure");
        const current = yield* orchestrator.getThreadProjection(threadId);
        assert.isNull(current.thread.snoozedUntil);
        assert.isFalse(current.thread.limitRecovery!.snooze);
        assert.isTrue(current.thread.limitRecovery!.autoResume);
      }

      if (scenario === "cancel" || scenario === "rearm")
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`recovery:cancel:${scenario}`),
          threadId,
          limitRecovery: { runId: run.id, resetAt, autoResume: false },
        });
      if (scenario === "archive")
        yield* orchestrator.dispatch({
          type: "thread.archive",
          commandId: CommandId.make(`recovery:archive:${scenario}`),
          threadId,
        });
      if (scenario === "new-message")
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`recovery:new-message:${scenario}`),
          threadId,
          messageId: MessageId.make(`recovery:new-message:${scenario}`),
          text: "I will continue manually.",
          attachments: [],
          dispatchMode: { type: "defer_start" },
          createdBy: "user",
          creationSource: "web",
        });
      if (scenario === "settle")
        yield* orchestrator.dispatch({
          type: "thread.settle",
          commandId: CommandId.make(`recovery:settle:${scenario}`),
          threadId,
        });
      if (scenario === "replacement") {
        const current = yield* orchestrator.getThreadProjection(threadId);
        const error = current.turnItems.find((item) => item.type === "error")!;
        if (error.type !== "error") throw new Error("Expected provider error");
        yield* events.write({
          commandId: CommandId.make(`recovery:replacement:${scenario}`),
          events: [
            {
              id: EventId.make(`recovery:replacement:${scenario}`),
              type: "turn-item.updated",
              threadId,
              occurredAt: yield* DateTime.now,
              payload: {
                ...error,
                failure: {
                  ...error.failure,
                  class: "provider_error",
                  message: "A replacement failure.",
                },
              },
            },
          ],
        });
      }
      if (scenario === "rearm") {
        yield* orchestrator.dispatch(resume!);
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`recovery:rearm:${scenario}`),
          threadId,
          limitRecovery: { runId: run.id, resetAt, autoResume: true },
        });
        yield* orchestrator.dispatch(resume!);
        assert.lengthOf((yield* orchestrator.getThreadProjection(threadId)).runs, 1);
        const rearmedShell = (yield* orchestrator.getShellSnapshot()).threads.find(
          (thread) => thread.id === threadId,
        )!;
        const freshResume = limitRecoveryCommand(
          rearmedShell,
          true,
          DateTime.toEpochMillis(yield* DateTime.now),
        );
        assert.isNotNull(freshResume);
        assert.notEqual(freshResume!.commandId, resume!.commandId);
        yield* orchestrator.dispatch(freshResume!);
        yield* orchestrator.dispatch(freshResume!);
        assert.lengthOf((yield* orchestrator.getThreadProjection(threadId)).runs, 2);
      }
      const before = yield* orchestrator.getThreadProjection(threadId);
      if (resume !== null) {
        yield* orchestrator.dispatch(resume);
        yield* orchestrator.dispatch(resume);
      }
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.lengthOf(
        after.runs,
        before.runs.length +
          (scenario === "resume" ||
          scenario === "queued-resume" ||
          scenario === "snooze-resume" ||
          scenario === "wake-preserve-resume" ||
          scenario === "independent-patches" ||
          scenario === "expired-snooze"
            ? 1
            : 0),
      );
      assert.lengthOf(
        after.messages,
        before.messages.length +
          (scenario === "resume" ||
          scenario === "queued-resume" ||
          scenario === "snooze-resume" ||
          scenario === "wake-preserve-resume" ||
          scenario === "independent-patches" ||
          scenario === "expired-snooze"
            ? 1
            : 0),
      );
      if (scenario === "queued-resume") {
        assert.equal(after.runs[1]?.status, "queued");
        assert.isTrue(after.runs[1]?.queueHeld);
        const continuation = after.runs[2]!;
        const completedAt = yield* DateTime.now;
        yield* events.write({
          events: [
            {
              id: EventId.make("recovery:continuation-completed:queued-resume"),
              type: "run.updated",
              threadId,
              runId: continuation.id,
              occurredAt: completedAt,
              payload: { ...continuation, status: "completed", completedAt },
            },
          ],
        });
        yield* orchestrator.dispatch({
          type: "queue.resume",
          commandId: CommandId.make("recovery:resume-held-after-limit:queued-resume"),
          threadId,
        });
        const resumed = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(resumed.runs[1]?.status, "starting");
        assert.isFalse(resumed.runs[1]?.queueHeld);
      }
    }),
  );
});

const queueResumeDisabledInstanceProjectId = ProjectId.make(
  "project:queue-resume-disabled-instance",
);

const layerQueueResumeDisabledInstanceTest = Layer.mergeAll(
  RuntimeLayer.layer,
  RuntimeLayer.layerEventSink,
  ProjectStore.layer,
  ProjectionStore.layer,
  EffectOutbox.layer,
  ThreadCommandExecutor.layer,
).pipe(
  Layer.provide(McpSessionRegistryTestkit.layer),
  Layer.provide(McpProviderSessions.layer),
  Layer.provide(SqlitePersistence.layerMemory),
  Layer.provide(layerCheckpointStoreTest),
  Layer.provide(layerServerConfig),
  Layer.provide(
    ServerSettings.layerTest({
      providerInstances: {
        [alternateInstanceId]: { driver: "codex", enabled: true },
      },
      projectSettingsOverrides: {
        [queueResumeDisabledInstanceProjectId]: {
          providerInstances: { [alternateInstanceId]: { enabled: false } },
        },
      },
    }),
  ),
  Layer.provide(layerTestProviderInstanceRegistry),
  Layer.provide(layerGitWorkflowTest),
  Layer.provide(layerProjectServiceTest),
  Layer.provide(layerPlatformTest),
);

it.layer(layerQueueResumeDisabledInstanceTest)(
  "queue.resume on a project-disabled instance",
  (it) => {
    it.effect(
      "holds a queued run instead of starting it on an instance the project turned off",
      () =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const events = yield* EventSink.EventSinkV2;
          const threadId = ThreadId.make("queue-resume-disabled-instance-thread");
          const queuedModelSelection = {
            instanceId: alternateInstanceId,
            model: "gpt-5.5",
          } satisfies ModelSelection;

          yield* orchestrator.dispatch({
            type: "thread.create",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make(`${threadId}:create`),
            threadId,
            projectId: queueResumeDisabledInstanceProjectId,
            title: "Queue resume disabled instance",
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
          });
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make(`${threadId}:message:active`),
            threadId,
            messageId: MessageId.make(`${threadId}:message:active`),
            text: "Active",
            attachments: [],
            dispatchMode: { type: "defer_start" },
          });
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            createdBy: "user",
            creationSource: "web",
            commandId: CommandId.make(`${threadId}:message:queued`),
            threadId,
            messageId: MessageId.make(`${threadId}:message:queued`),
            text: "Queued",
            attachments: [],
            modelSelection: queuedModelSelection,
            dispatchMode: { type: "queue_after_active" },
          });

          const before = yield* orchestrator.getThreadProjection(threadId);
          const activeRun = before.runs.find((run) => run.status !== "queued")!;
          const queuedRun = before.runs.find((run) => run.status === "queued")!;
          const now = yield* DateTime.now;
          // A raw event ends the active run without going through dispatch,
          // so nothing has yet tried to advance the queue onto the disabled
          // instance.
          yield* events.write({
            events: [
              {
                id: EventId.make(`${threadId}:active-completed`),
                type: "run.updated",
                threadId,
                runId: activeRun.id,
                occurredAt: now,
                payload: { ...activeRun, status: "completed", completedAt: now },
              },
            ],
          });

          yield* orchestrator.dispatch({
            type: "queue.resume",
            commandId: CommandId.make(`${threadId}:resume`),
            threadId,
          });

          const resumed = yield* orchestrator.getThreadProjection(threadId);
          const resumedQueuedRun = resumed.runs.find((run) => run.id === queuedRun.id);
          assert.equal(
            resumedQueuedRun?.status,
            "queued",
            "a disabled instance must not start the queued run",
          );
          assert.isTrue(
            resumedQueuedRun?.queueHeld,
            "a refused resume re-holds the queue instead of leaving it looking resumable",
          );
        }),
    );
  },
);

it.layer(
  layerTest.pipe(
    Layer.provideMerge(SqlitePersistence.layerMemory),
    Layer.provideMerge(IdAllocator.layer),
  ),
)("bounded work continuity", (it) => {
  const seedContinuity = (
    name: string,
    maxChecks?: number,
    automatic = false,
    selection = modelSelection,
  ) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = ThreadId.make(`continuity:${name}`);
      const authorityMessageId = MessageId.make(`${threadId}:authority`);
      const generation = CommandId.make(`${threadId}:enroll`);
      yield* seedProject({
        projectId: ProjectId.make(`${threadId}:project`),
        title: name,
        workspaceRoot: process.cwd(),
        defaultModelSelection: selection,
        createdAt: DateTime.formatIso(yield* DateTime.now),
      });
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make(`${threadId}:create`),
        threadId,
        projectId: ProjectId.make(`${threadId}:project`),
        title: name,
        createdBy: "user",
        creationSource: "web",
        modelSelection: selection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`${threadId}:request`),
        threadId,
        messageId: authorityMessageId,
        text: "Finish this assignment, including verification.",
        createdBy: "user",
        creationSource: "web",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
      });
      if (!automatic)
        yield* orchestrator.dispatch({
          type: "thread.continuity.set",
          creationSource: "web",
          commandId: generation,
          threadId,
          enabled: true,
          authorityMessageId,
          ...(maxChecks === undefined ? {} : { maxChecks }),
        });
      const run = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
      return { threadId, authorityMessageId, generation, run };
    });

  const seedR89Cut = (name: string, maxChecks = 2) =>
    Effect.gen(function* () {
      const input = yield* seedContinuity(`r89-${name}`, maxChecks);
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const events = yield* EventSink.EventSinkV2;
      const projection = yield* orchestrator.getThreadProjection(input.threadId);
      const providerThread = projection.providerThreads[0]!;
      const sessionId = ProviderSessionId.make(`${input.threadId}:session`);
      const now = yield* DateTime.now;
      const running = { ...input.run, status: "running" as const, startedAt: now };
      const turn = {
        id: ProviderTurnId.make(`${input.threadId}:turn`),
        providerThreadId: providerThread.id,
        nodeId: input.run.rootNodeId!,
        runAttemptId: input.run.activeAttemptId,
        nativeTurnRef: {
          driver,
          nativeId: `${input.threadId}:native-turn`,
          strength: "strong" as const,
        },
        ordinal: 1,
        status: "running" as const,
        startedAt: now,
        completedAt: null,
      };
      yield* events.write({
        events: [
          {
            id: EventId.make(`${input.threadId}:running`),
            type: "run.updated",
            threadId: input.threadId,
            runId: running.id,
            occurredAt: now,
            payload: running,
          },
          {
            id: EventId.make(`${input.threadId}:session`),
            type: "provider-session.attached",
            threadId: input.threadId,
            occurredAt: now,
            payload: {
              id: sessionId,
              driver,
              providerInstanceId: modelSelection.instanceId,
              status: "running",
              cwd: process.cwd(),
              model: modelSelection.model,
              capabilities: CodexProviderCapabilitiesV2,
              createdAt: now,
              updatedAt: now,
              lastError: null,
            },
          },
          {
            id: EventId.make(`${input.threadId}:native`),
            type: "provider-thread.updated",
            threadId: input.threadId,
            occurredAt: now,
            payload: {
              ...providerThread,
              providerSessionId: sessionId,
              status: "active",
              nativeThreadRef: {
                driver,
                nativeId: `${input.threadId}:native-thread`,
                strength: "strong",
              },
            },
          },
          {
            id: EventId.make(`${input.threadId}:turn`),
            type: "provider-turn.updated",
            threadId: input.threadId,
            runId: running.id,
            occurredAt: now,
            payload: turn,
          },
        ],
      });
      yield* (yield* ServerSettings.ServerSettingsService).updateSettings({
        continueThreadsAfterServerUpdate: true,
      });
      return { ...input, run: running, turn, sessionId };
    });

  const cutR89 = (input: { threadId: ThreadId }) =>
    Effect.gen(function* () {
      const recovery = yield* ProviderRuntimeRecoveryService.make;
      yield* recovery.prepareForShutdown;
      yield* recovery.reconcile("shutdown");
      const sql = yield* SqlClient.SqlClient;
      return yield* sql<{
        effect_type: string;
        payload_json: string;
      }>`SELECT effect_type, payload_json
        FROM orchestration_v2_effect_outbox WHERE thread_id = ${input.threadId}
          AND effect_type = ${"provider-runtime.continue"}`;
    });

  it.effect.each([
    "blocked",
    "complete",
    "paused",
    "null",
    "optout",
    "needs_input",
    "active-zero",
  ] as const)(
    "R89 restart restores the cut turn with %s continuity without changing authority or allowance",
    (status) =>
      Effect.gen(function* () {
        const input = yield* seedR89Cut(status);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const events = yield* EventSink.EventSinkV2;
        const before = yield* orchestrator.getThreadProjection(input.threadId);
        const continuity =
          status === "null" || status === "optout"
            ? null
            : {
                ...before.thread.continuity!,
                status: status === "active-zero" ? ("active" as const) : status,
                ...(status === "active-zero" ? { checksRemaining: 0 } : {}),
              };
        yield* events.write({
          events: [
            {
              id: EventId.make(`${input.threadId}:disposition`),
              type: "thread.metadata-updated",
              threadId: input.threadId,
              occurredAt: yield* DateTime.now,
              payload: {
                ...before.thread,
                continuity,
                continuityAutoEnrollDisabled: status === "optout",
              },
            },
          ],
        });
        assert.lengthOf(yield* cutR89(input), 1);
        yield* continueRestartedRun({ threadId: input.threadId, sourceRunId: input.run.id });
        yield* continueRestartedRun({ threadId: input.threadId, sourceRunId: input.run.id });
        const after = yield* orchestrator.getThreadProjection(input.threadId);
        assert.lengthOf(after.runs, 2);
        assert.equal(after.runs[1]?.restartContinuationOfRunId, input.run.id);
        assert.deepEqual(after.thread.continuity, continuity);
        assert.equal(after.thread.continuityAutoEnrollDisabled, status === "optout");
        assert.deepEqual(
          after.messages.find((message) => message.id === input.authorityMessageId),
          before.messages.find((message) => message.id === input.authorityMessageId),
        );
        assert.isNull(after.providerThreads[0]?.goal);
      }),
  );

  it.effect(
    "R89 pending runtime approval survives the cut unanswered and admits no continuation",
    () =>
      Effect.gen(function* () {
        const input = yield* seedR89Cut("approval");
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const events = yield* EventSink.EventSinkV2;
        const request = {
          id: RuntimeRequestId.make(`${input.threadId}:approval`),
          nodeId: input.run.rootNodeId!,
          providerTurnId: input.turn.id,
          nativeRequestRef: null,
          kind: "command" as const,
          status: "pending" as const,
          responseCapability: { type: "live" as const, providerSessionId: input.sessionId },
          createdAt: yield* DateTime.now,
          resolvedAt: null,
        };
        yield* events.write({
          events: [
            {
              id: EventId.make(`${input.threadId}:approval`),
              type: "runtime-request.updated",
              threadId: input.threadId,
              nodeId: request.nodeId,
              occurredAt: request.createdAt,
              payload: request,
            },
          ],
        });
        assert.lengthOf(yield* cutR89(input), 0);
        yield* continueRestartedRun({ threadId: input.threadId, sourceRunId: input.run.id });
        const after = yield* orchestrator.getThreadProjection(input.threadId);
        assert.lengthOf(after.runs, 1);
        assert.equal(after.runtimeRequests[0]?.id, request.id);
        assert.equal(after.runtimeRequests[0]?.status, "cancelled");
        assert.equal(after.runtimeRequests[0]?.responseCapability.type, "not_resumable");
        assert.isUndefined(after.runtimeRequests[0]?.decision);
        assert.isUndefined(after.runtimeRequests[0]?.answers);
      }),
  );

  it.effect("R89 TRUE to FALSE after durable shutdown intent prevents delivery admission", () =>
    Effect.gen(function* () {
      const input = yield* seedR89Cut("flag-flip");
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      assert.lengthOf(yield* cutR89(input), 1);
      yield* (yield* ServerSettings.ServerSettingsService).updateSettings({
        continueThreadsAfterServerUpdate: false,
      });
      yield* continueRestartedRun({ threadId: input.threadId, sourceRunId: input.run.id });
      assert.lengthOf((yield* orchestrator.getThreadProjection(input.threadId)).runs, 1);
    }),
  );

  it.effect.each([false, true])(
    "R89 human accepted after reconciliation invalidates stale restart with earlier held queue %s",
    (withQueue) =>
      Effect.gen(function* () {
        const input = yield* seedR89Cut(`human-${withQueue}`);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        if (withQueue)
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`${input.threadId}:queue`),
            threadId: input.threadId,
            messageId: MessageId.make(`${input.threadId}:queued`),
            text: "Earlier queued requirement",
            createdBy: "user",
            creationSource: "web",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
          });
        assert.lengthOf(yield* cutR89(input), 1);
        const reconciled = yield* orchestrator.getThreadProjection(input.threadId);
        const held = reconciled.runs.find((run) => run.status === "queued");
        if (withQueue) assert.isTrue(held?.queueHeld);
        yield* TestClock.adjust("1 millis");
        const humanId = MessageId.make(`${input.threadId}:new-human`);
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`${input.threadId}:new-human`),
          threadId: input.threadId,
          messageId: humanId,
          text: "New accepted human requirement",
          createdBy: "user",
          creationSource: "web",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
        });
        const human = yield* orchestrator.getThreadProjection(input.threadId);
        yield* continueRestartedRun({ threadId: input.threadId, sourceRunId: input.run.id });
        const after = yield* orchestrator.getThreadProjection(input.threadId);
        assert.deepEqual(after.runs, human.runs);
        assert.deepEqual(after.messages, human.messages);
        assert.isTrue(
          after.runs.some((run) => run.userMessageId === humanId && run.status !== "queued"),
        );
        assert.isFalse(after.runs.some((run) => run.restartContinuationOfRunId === input.run.id));
        if (withQueue)
          assert.deepEqual(
            after.runs.find((run) => run.id === held?.id),
            held,
          );
      }),
  );

  it.effect.each(["active", "complete", "blocked", "paused"] as const)(
    "R89 completed restart consumes one check only for active continuity; %s preserved",
    (status) =>
      Effect.gen(function* () {
        const input = yield* seedR89Cut(`consume-${status}`, 2);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const events = yield* EventSink.EventSinkV2;
        if (status !== "active") {
          const before = yield* orchestrator.getThreadProjection(input.threadId);
          yield* events.write({
            events: [
              {
                id: EventId.make(`${input.threadId}:closed`),
                type: "thread.metadata-updated",
                threadId: input.threadId,
                occurredAt: yield* DateTime.now,
                payload: { ...before.thread, continuity: { ...before.thread.continuity!, status } },
              },
            ],
          });
        }
        const before = (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity!;
        assert.lengthOf(yield* cutR89(input), 1);
        yield* continueRestartedRun({ threadId: input.threadId, sourceRunId: input.run.id });
        const resumed = (yield* orchestrator.getThreadProjection(input.threadId)).runs[1]!;
        assert.equal(
          (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity
            ?.checksRemaining,
          2,
        );
        const advanced =
          status === "active"
            ? yield* events
                .stream({
                  afterSequence: yield* events.latestSequence(),
                  eventType: "thread.metadata-updated",
                })
                .pipe(
                  Stream.filter(
                    (stored) =>
                      stored.event.type === "thread.metadata-updated" &&
                      stored.event.threadId === input.threadId &&
                      stored.event.payload.continuity?.lastAdvancedRunId === resumed.id,
                  ),
                  Stream.take(1),
                  Stream.runDrain,
                  Effect.forkScoped,
                )
            : undefined;
        yield* finishRun(input.threadId, resumed);
        yield* advance({ ...input, run: resumed }, "first");
        if (advanced !== undefined) yield* Fiber.join(advanced);
        yield* advance({ ...input, run: resumed }, "repeat");
        yield* orchestrator.recoverDelegatedTasks;
        const after = yield* orchestrator.getThreadProjection(input.threadId);
        assert.lengthOf(after.runs, status === "active" ? 3 : 2);
        if (status === "active") {
          assert.equal(after.thread.continuity?.checksRemaining, 1);
          assert.equal(after.thread.continuity?.generation, before.generation);
          assert.equal(after.thread.continuity?.authorityMessageId, before.authorityMessageId);
          assert.equal(after.thread.continuity?.lastAdvancedRunId, resumed.id);
          assert.equal(
            after.runs[2]?.userMessageId,
            MessageId.make(`message:work-continuity:${before.generation}:${resumed.id}`),
          );
        } else assert.deepEqual(after.thread.continuity, before);
      }),
  );

  it.effect.each(["paused", "blocked", "budget_limited", "usage_limited"] as const)(
    "R89 restart must respect a persisted %s native goal pause or limit",
    (status) =>
      Effect.gen(function* () {
        const input = yield* seedR89Cut(`goal-${status}`);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const events = yield* EventSink.EventSinkV2;
        const projection = yield* orchestrator.getThreadProjection(input.threadId);
        const goal = {
          objective: "Previously authorized native objective",
          status,
          tokensUsed: 100,
          tokenBudget: 100,
          timeUsedSeconds: 3,
        };
        yield* events.write({
          events: [
            {
              id: EventId.make(`${input.threadId}:goal-notification`),
              type: "provider-thread.updated",
              threadId: input.threadId,
              occurredAt: yield* DateTime.now,
              payload: { ...projection.providerThreads[0]!, goal },
            },
          ],
        });
        const persisted = yield* orchestrator.getThreadProjection(input.threadId);
        assert.equal(persisted.runs[0]?.status, "running");
        assert.equal(persisted.providerTurns[0]?.status, "running");
        assert.deepEqual(persisted.providerThreads[0]?.goal, goal);
        assert.isAtMost((yield* cutR89(input)).length, 1);
        yield* continueRestartedRun({ threadId: input.threadId, sourceRunId: input.run.id });
        const after = yield* orchestrator.getThreadProjection(input.threadId);
        assert.deepEqual(after.providerThreads[0]?.goal, goal);
        assert.equal(after.thread.continuity?.checksRemaining, 2);
        const continuations = after.runs.filter(
          (run) => run.restartContinuationOfRunId === input.run.id,
        );
        assert.lengthOf(
          continuations,
          0,
          JSON.stringify({
            incident: "R89NativeGoalRestartAdmission",
            action: "deliver durable restart continuation",
            observed: {
              goalStatus: status,
              sourceRunId: input.run.id,
              admittedRunIds: continuations.map((run) => run.id),
              expectedAdmissions: 0,
            },
            guard: "restart must preserve the native goal pause or limit",
            eliminated: [
              "flag false",
              "unbound native session",
              "goal only in an unreachable object",
            ],
            next: "Root must repair native-goal eligibility at restart intent and dispatch; product source is frozen.",
          }),
        );
      }),
  );

  const seedLiveChild = (input: { threadId: ThreadId; run: OrchestrationV2Run }) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const events = yield* EventSink.EventSinkV2;
      const rootNodeId = input.run.rootNodeId;
      if (rootNodeId === null)
        return yield* Effect.die("The delegate fixture requires an allocated root node.");
      yield* orchestrator.dispatch({
        type: "delegated_task.request",
        commandId: CommandId.make(`${input.threadId}:delegate`),
        parentThreadId: input.threadId,
        parentRunId: input.run.id,
        parentNodeId: rootNodeId,
        task: "Complete the isolated child assignment.",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        completionWake: "always",
        createdBy: "agent",
        creationSource: "server",
      });
      const task = (yield* orchestrator.getThreadProjection(input.threadId)).subagents[0]!;
      const childThreadId = task.childThreadId;
      if (childThreadId === null)
        return yield* Effect.die("The delegate fixture requires its app-owned child binding.");
      const now = yield* DateTime.now;
      const running = { ...task, status: "running" as const, startedAt: now, updatedAt: now };
      yield* events.write({
        events: [
          {
            id: EventId.make(`${input.threadId}:child-running`),
            type: "subagent.updated",
            threadId: input.threadId,
            runId: input.run.id,
            nodeId: task.id,
            occurredAt: now,
            payload: running,
          },
        ],
      });
      return running;
    });

  const finishRun = (
    threadId: ThreadId,
    run: OrchestrationV2Run,
    status: "completed" | "failed" | "cancelled" = "completed",
  ) =>
    Effect.gen(function* () {
      const events = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      yield* events.write({
        events: [
          {
            id: EventId.make(`${run.id}:terminal`),
            type: "run.updated",
            threadId,
            runId: run.id,
            occurredAt: now,
            payload: { ...run, status, startedAt: run.startedAt ?? now, completedAt: now },
          },
        ],
      });
    });

  const advance = (
    input: { threadId: ThreadId; generation: CommandId; run: OrchestrationV2Run },
    suffix: string,
  ) =>
    Effect.flatMap(Orchestrator.OrchestratorV2, (orchestrator) =>
      orchestrator.dispatch({
        type: "thread.continuity.advance",
        commandId: CommandId.make(`${input.run.id}:advance:${suffix}`),
        threadId: input.threadId,
        endedRunId: input.run.id,
        generation: input.generation,
      }),
    );

  const withCapacityRecovery = <A, E, R>(program: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const settings = yield* ServerSettings.ServerSettingsService;
      yield* settings.updateSettings({ autoResumeCapacityThreads: true });
      return yield* program.pipe(
        Effect.ensuring(
          settings.updateSettings({ autoResumeCapacityThreads: false }).pipe(Effect.orDie),
        ),
      );
    });
  const failCapacity = (
    threadId: ThreadId,
    run: OrchestrationV2Run,
    code = "serverOverloaded",
    commandId?: CommandId,
  ) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const events = yield* EventSink.EventSinkV2;
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const providerThread = projection.providerThreads.find(
        (entry) => entry.id === run.providerThreadId,
      )!;
      const now = yield* DateTime.now;
      yield* events.write({
        ...(commandId === undefined ? {} : { commandId }),
        events: [
          {
            id: EventId.make(`${run.id}:native-ready`),
            type: "provider-thread.updated",
            threadId,
            occurredAt: now,
            payload: {
              ...providerThread,
              status: "active",
              nativeThreadRef: {
                driver: providerThread.driver,
                nativeId: `capacity-native:${threadId}`,
                strength: "strong",
              },
            },
          },
          {
            id: EventId.make(`${run.id}:capacity-error`),
            type: "turn-item.updated",
            threadId,
            runId: run.id,
            nodeId: run.rootNodeId ?? undefined,
            occurredAt: now,
            payload: {
              id: TurnItemId.make(`${run.id}:capacity-error`),
              threadId,
              runId: run.id,
              nodeId: run.rootNodeId,
              providerThreadId: run.providerThreadId,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: run.ordinal * 100 + 1,
              status: "failed",
              title: "Model capacity unavailable",
              startedAt: now,
              completedAt: now,
              updatedAt: now,
              type: "error",
              failure: {
                class:
                  code === "usageLimitExceeded" || code === "rateLimitExceeded"
                    ? "usage_limit"
                    : "provider_error",
                code,
                message: "Selected model is at capacity. Please try a different model.",
                retryable: null,
              },
            },
          },
          {
            id: EventId.make(`${run.id}:capacity-terminal`),
            type: "run.updated",
            threadId,
            runId: run.id,
            occurredAt: now,
            payload: {
              ...run,
              status: "failed",
              startedAt: run.startedAt ?? now,
              completedAt: now,
            },
          },
        ],
      });
    });
  const awaitCapacityState = (
    threadId: ThreadId,
    status: "pending" | "exhausted" | "cancelled" | "complete",
  ) =>
    Effect.gen(function* () {
      const events = yield* EventSink.EventSinkV2;
      return yield* events
        .stream({
          afterSequence: yield* events.latestSequence(),
          eventType: "thread.metadata-updated",
        })
        .pipe(
          Stream.filter(
            (stored) =>
              stored.event.threadId === threadId &&
              stored.event.type === "thread.metadata-updated" &&
              stored.event.payload.capacityRecovery?.status === status,
          ),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );
    });

  it.effect(
    "capacity recovery retains native history, authority, model and a three-attempt budget across startup",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const selected = {
            ...modelSelection,
            options: [{ id: "reasoningEffort", value: "high" }],
          };
          const input = yield* seedContinuity("capacity-bounded", undefined, false, selected);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const events = yield* EventSink.EventSinkV2;
          const nativeGoal = {
            objective: "Existing explicit objective",
            status: "active" as const,
            tokensUsed: 17,
            tokenBudget: 100_000,
          };
          const providerThread = (yield* orchestrator.getThreadProjection(input.threadId))
            .providerThreads[0]!;
          yield* events.write({
            events: [
              {
                id: EventId.make(`${input.run.id}:native-goal`),
                type: "provider-thread.updated",
                threadId: input.threadId,
                occurredAt: yield* DateTime.now,
                payload: { ...providerThread, goal: nativeGoal },
              },
            ],
          });
          const toolCompletedAt = yield* DateTime.now;
          const completedTool = {
            id: TurnItemId.make(`${input.run.id}:completed-tool`),
            threadId: input.threadId,
            runId: input.run.id,
            nodeId: input.run.rootNodeId,
            providerThreadId: input.run.providerThreadId,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 101,
            status: "completed" as const,
            title: "Completed mutation",
            startedAt: toolCompletedAt,
            completedAt: toolCompletedAt,
            updatedAt: toolCompletedAt,
            type: "command_execution" as const,
            input: "create-once external-operation",
            output: "Operation receipt abc-123",
            exitCode: 0,
          };
          yield* events.write({
            events: [
              {
                id: EventId.make(`${input.run.id}:completed-tool`),
                type: "turn-item.updated",
                threadId: input.threadId,
                occurredAt: toolCompletedAt,
                payload: completedTool,
              },
            ],
          });
          const storedTool = (yield* orchestrator.getThreadProjection(
            input.threadId,
          )).turnItems.find((entry) => entry.id === completedTool.id)!;
          const pending = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, input.run);
          yield* Fiber.join(pending);
          const armed = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(armed.thread.capacityRecovery?.attemptsUsed, 0);
          assert.equal(armed.thread.capacityRecovery?.authorityMessageId, input.authorityMessageId);
          assert.equal(armed.thread.capacityRecovery?.status, "pending");
          assert.equal((yield* orchestrator.getThreadShell(input.threadId))?.status, "waiting");
          assert.equal(armed.thread.continuity?.checksRemaining, 3);
          const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
          assert.isTrue((yield* maintenance.rebuild).valid);
          yield* ModelCapacityRecoveryWorker.sweep;
          assert.lengthOf((yield* orchestrator.getThreadProjection(input.threadId)).runs, 1);
          for (let attempt = 1; attempt <= 3; attempt++) {
            yield* TestClock.adjust(
              attempt === 1 ? "20 seconds" : attempt === 2 ? "35 seconds" : "65 seconds",
            );
            if (attempt === 1) yield* orchestrator.recoverDelegatedTasks;
            else yield* ModelCapacityRecoveryWorker.sweep;
            const retrying = yield* orchestrator.getThreadProjection(input.threadId);
            const recovery = retrying.thread.capacityRecovery!;
            assert.equal(recovery.attemptsUsed, attempt);
            assert.equal(recovery.status, "retrying");
            yield* ModelCapacityRecoveryWorker.sweep;
            yield* orchestrator.recoverDelegatedTasks;
            assert.lengthOf(
              (yield* orchestrator.getThreadProjection(input.threadId)).runs,
              attempt + 1,
            );
            assert.equal(recovery.originalRunId, input.run.id);
            const retry = retrying.runs.find((run) => run.id === recovery.retryRunId)!;
            assert.deepEqual(retry.modelSelection, input.run.modelSelection);
            assert.deepEqual(retry.modelSelection, selected);
            assert.equal(retry.runtimeMode, input.run.runtimeMode);
            assert.equal(retry.interactionMode, input.run.interactionMode);
            assert.deepEqual(
              retrying.providerThreads.find((entry) => entry.id === retry.providerThreadId)?.goal,
              nativeGoal,
            );
            assert.equal(retry.providerThreadId, input.run.providerThreadId);
            assert.deepEqual(
              retrying.turnItems.find((entry) => entry.id === completedTool.id),
              storedTool,
            );
            const message = retrying.messages.find((entry) => entry.id === retry.userMessageId)!;
            assert.include(message.text, input.authorityMessageId);
            assert.include(message.text, "actual current state");
            assert.notEqual(
              message.text,
              retrying.messages.find((entry) => entry.id === input.authorityMessageId)?.text,
            );
            const next = yield* awaitCapacityState(
              input.threadId,
              attempt === 3 ? "exhausted" : "pending",
            );
            yield* failCapacity(input.threadId, retry);
            yield* Fiber.join(next);
          }
          yield* TestClock.adjust("5 minutes");
          yield* ModelCapacityRecoveryWorker.sweep;
          const exhausted = yield* orchestrator.getThreadProjection(input.threadId);
          assert.lengthOf(exhausted.runs, 4);
          assert.equal(exhausted.thread.capacityRecovery?.attemptsUsed, 3);
          assert.equal(exhausted.thread.capacityRecovery?.status, "exhausted");
          assert.equal(exhausted.thread.continuity?.checksRemaining, 3);
        }),
      ),
  );

  it.effect.each(["unwatched", "watched"] as const)(
    "capacity recovery rejects a stale cancel and Stop prevents the pending wake with %s PR",
    (watchMode) =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity(`capacity-stop-${watchMode}`);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          if (watchMode === "watched") {
            yield* orchestrator.dispatch({
              type: "thread.pull-request.watch",
              commandId: CommandId.make(`${input.threadId}:watch`),
              threadId: input.threadId,
              host: "github.com",
              repository: "pingdotgg/t3code",
              number: 7,
              watching: true,
              link: { url: "https://github.com/pingdotgg/t3code/pull/7", source: "agent" },
            });
            assert.isDefined(
              (yield* orchestrator.getThreadProjection(input.threadId)).thread.pullRequests?.[0]
                ?.watch,
            );
          }
          const pending = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, input.run);
          yield* Fiber.join(pending);
          const recovery = (yield* orchestrator.getThreadProjection(input.threadId)).thread
            .capacityRecovery!;
          yield* orchestrator.dispatch({
            type: "thread.capacity.cancel",
            commandId: CommandId.make(`${input.threadId}:stale-cancel`),
            threadId: input.threadId,
            generation: CommandId.make("old-capacity-generation"),
          });
          assert.equal(
            (yield* orchestrator.getThreadProjection(input.threadId)).thread.capacityRecovery
              ?.status,
            "pending",
          );

          yield* orchestrator.dispatch({
            type: "thread.stop",
            commandId: CommandId.make(`${input.threadId}:stop`),
            threadId: input.threadId,
          });
          const immediateStop = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(immediateStop.thread.capacityRecovery?.status, "cancelled");
          yield* TestClock.adjust("1 minute");
          yield* ModelCapacityRecoveryWorker.sweep;
          const stopped = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(stopped.thread.capacityRecovery?.generation, recovery.generation);
          assert.equal(stopped.thread.capacityRecovery?.status, "cancelled");
          assert.lengthOf(stopped.runs, 1);
          assert.equal(stopped.thread.capacityRecovery?.attemptsUsed, recovery.attemptsUsed);
          assert.lengthOf(stopped.thread.pullRequests ?? [], watchMode === "watched" ? 1 : 0);
          assert.isTrue(
            stopped.thread.pullRequests?.every((link) => link.watch === undefined) ?? true,
          );
        }),
      ),
  );

  it.effect(
    "continuity advancement arms exact capacity recovery under its lock before marking a failed assignment blocked",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity("capacity-continuity-preflight");
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* failCapacity(
            input.threadId,
            input.run,
            "serverOverloaded",
            CommandId.make("command:runtime-reconcile:capacity-continuity-preflight"),
          );
          yield* advance(input, "capacity-preflight");
          const after = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(after.thread.capacityRecovery?.status, "pending");
          assert.equal(after.thread.continuity?.status, "active");
          assert.equal(after.thread.continuity?.checksRemaining, 3);
          assert.lengthOf(after.runs, 1);
          yield* orchestrator.dispatch({
            type: "thread.stop",
            commandId: CommandId.make(`${input.threadId}:cleanup-stop`),
            threadId: input.threadId,
          });
        }),
      ),
  );

  const awaitCapacityRelease = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const events = yield* EventSink.EventSinkV2;
      return yield* events
        .stream({
          afterSequence: yield* events.latestSequence(),
          eventType: "thread.metadata-updated",
        })
        .pipe(
          Stream.filter(
            (stored) =>
              stored.event.threadId === threadId &&
              stored.event.type === "thread.metadata-updated" &&
              stored.event.payload.capacityRecovery?.reactionsReleased === true,
          ),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );
    });

  it.effect(
    "startup releases the capacity retry cancelled by real runtime reconciliation without spending another attempt",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity("capacity-restart-retrying");
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const pending = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, input.run);
          yield* Fiber.join(pending);
          yield* TestClock.adjust("20 seconds");
          yield* ModelCapacityRecoveryWorker.sweep;
          const before = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(before.thread.capacityRecovery?.status, "retrying");
          const runtime = yield* ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService;
          yield* runtime.reconcile("startup");
          const cancelled = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(
            cancelled.runs.find((run) => run.id === before.thread.capacityRecovery?.retryRunId)
              ?.status,
            "cancelled",
          );
          assert.equal(cancelled.thread.capacityRecovery?.status, "retrying");
          const release = yield* awaitCapacityRelease(input.threadId);
          yield* orchestrator.recoverDelegatedTasks;
          yield* Fiber.join(release);
          yield* orchestrator.recoverDelegatedTasks;
          yield* ModelCapacityRecoveryWorker.sweep;
          const after = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(after.thread.capacityRecovery?.status, "cancelled");
          assert.equal(after.thread.capacityRecovery?.attemptsUsed, 1);
          assert.isTrue(after.thread.capacityRecovery?.reactionsReleased);
          assert.lengthOf(after.runs, 2);
        }),
      ),
  );

  it.effect(
    "a refused capacity dispatch cancels its durable episode once and later due candidates still progress",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const failed = yield* seedContinuity("capacity-dispatch-refusal", undefined, true);
          const later = yield* seedContinuity("capacity-dispatch-later", undefined, true);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          for (const input of [failed, later]) {
            const pending = yield* awaitCapacityState(input.threadId, "pending");
            yield* failCapacity(input.threadId, input.run);
            yield* Fiber.join(pending);
          }
          const threads = yield* ThreadManagementService.ThreadManagementService;
          let refusedDispatches = 0;
          const sweep = ModelCapacityRecoveryWorker.sweep.pipe(
            Effect.provideService(ThreadManagementService.ThreadManagementService, {
              ...threads,
              dispatch: (command) => {
                if (
                  command.type === "thread.capacity.retry" &&
                  command.threadId === failed.threadId
                ) {
                  refusedDispatches++;
                  return Effect.fail(
                    new Orchestrator.OrchestratorDispatchError({
                      commandId: command.commandId,
                      commandType: command.type,
                      cause: "Injected dispatch infrastructure refusal",
                    }),
                  );
                }
                return threads.dispatch(command);
              },
            }),
          );
          yield* sweep;
          assert.equal(refusedDispatches, 0);
          yield* TestClock.adjust("20 seconds");
          const release = yield* awaitCapacityRelease(failed.threadId);
          yield* sweep;
          yield* Fiber.join(release);
          yield* sweep;
          assert.equal(refusedDispatches, 1);
          const stopped = yield* orchestrator.getThreadProjection(failed.threadId);
          assert.equal(stopped.thread.capacityRecovery?.status, "cancelled");
          assert.equal(stopped.thread.capacityRecovery?.attemptsUsed, 0);
          assert.lengthOf(stopped.runs, 1);
          const progressed = yield* orchestrator.getThreadProjection(later.threadId);
          assert.equal(progressed.thread.capacityRecovery?.status, "retrying");
          assert.equal(progressed.thread.capacityRecovery?.attemptsUsed, 1);
        }),
      ),
  );

  it.effect(
    "capacity refusal cannot cancel a continuation whose run was already allocated or is still active",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity("capacity-refusal-after-commit", undefined, true);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const pending = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, input.run);
          yield* Fiber.join(pending);
          yield* TestClock.adjust("20 seconds");
          yield* ModelCapacityRecoveryWorker.sweep;
          const recovery = (yield* orchestrator.getThreadProjection(input.threadId)).thread
            .capacityRecovery!;
          for (const runId of [recovery.failedRunId, recovery.retryRunId!]) {
            yield* orchestrator.dispatch({
              type: "thread.capacity.abort",
              commandId: CommandId.make(`${input.threadId}:abort:${runId}`),
              threadId: input.threadId,
              generation: recovery.generation,
              runId,
            });
          }
          const after = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(after.thread.capacityRecovery?.status, "retrying");
          assert.equal(after.thread.capacityRecovery?.attemptsUsed, 1);
          assert.lengthOf(after.runs, 2);
        }),
      ),
  );

  it.effect(
    "a newer human run ending before the capacity sweep releases its own terminal reactions",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity("capacity-new-human-terminal", undefined, true);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const pending = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, input.run);
          yield* Fiber.join(pending);
          const release = yield* awaitCapacityRelease(input.threadId);
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`${input.threadId}:new-human`),
            threadId: input.threadId,
            messageId: MessageId.make(`${input.threadId}:new-human`),
            text: "A new authorized assignment",
            createdBy: "user",
            creationSource: "web",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
          });
          const human = (yield* orchestrator.getThreadProjection(input.threadId)).runs.at(-1)!;
          assert.equal(
            (yield* orchestrator.getThreadProjection(input.threadId)).thread.capacityRecovery
              ?.status,
            "cancelled",
          );
          assert.notEqual((yield* orchestrator.getThreadShell(input.threadId))?.status, "waiting");
          yield* finishRun(input.threadId, human);
          yield* Fiber.join(release);
          const after = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(after.thread.capacityRecovery?.status, "cancelled");
          assert.isTrue(after.thread.capacityRecovery?.reactionsReleased);
          assert.equal(after.runs.find((run) => run.id === human.id)?.status, "completed");
          yield* TestClock.adjust("20 seconds");
          yield* ModelCapacityRecoveryWorker.sweep;
          assert.lengthOf((yield* orchestrator.getThreadProjection(input.threadId)).runs, 2);
        }),
      ),
  );

  it.effect(
    "a refused terminal capacity reconciliation durably aborts retrying instead of suppressing later reactions",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity("capacity-terminal-refusal", undefined, true);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const pending = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, input.run);
          yield* Fiber.join(pending);
          yield* TestClock.adjust("20 seconds");
          yield* ModelCapacityRecoveryWorker.sweep;
          const before = yield* orchestrator.getThreadProjection(input.threadId);
          const retry = before.runs.find(
            (run) => run.id === before.thread.capacityRecovery?.retryRunId,
          )!;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const original = projections.getThreadRecords;
          let refused = false;
          const spy = vi
            .spyOn(projections, "getThreadRecords")
            .mockImplementation((threadId, sections, options) => {
              if (
                !refused &&
                threadId === input.threadId &&
                sections.includes("checkpointScopes")
              ) {
                refused = true;
                return Effect.fail(
                  new ProjectionStore.ProjectionStoreReadError({
                    threadId,
                    cause: "Injected terminal projection refusal",
                  }),
                );
              }
              return original(threadId, sections, options);
            });
          yield* Effect.gen(function* () {
            const release = yield* awaitCapacityRelease(input.threadId);
            yield* finishRun(input.threadId, retry);
            yield* Fiber.join(release);
            assert.isTrue(refused);
            const after = yield* orchestrator.getThreadProjection(input.threadId);
            assert.equal(after.thread.capacityRecovery?.status, "cancelled");
            assert.isTrue(after.thread.capacityRecovery?.reactionsReleased);
            assert.equal(after.thread.capacityRecovery?.failedRunId, retry.id);
            assert.equal(after.thread.capacityRecovery?.attemptsUsed, 1);
            yield* ModelCapacityRecoveryWorker.sweep;
            assert.lengthOf((yield* orchestrator.getThreadProjection(input.threadId)).runs, 2);
          }).pipe(Effect.ensuring(Effect.sync(() => spy.mockRestore())));
        }),
      ),
  );

  it.effect(
    "ordinary child completion still finalizes its parent after a queue promotion failure with capacity disabled",
    () =>
      Effect.gen(function* () {
        const input = yield* seedContinuity("capacity-off-queue-isolation", undefined, true);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const task = yield* seedLiveChild(input);
        const childId = task.childThreadId!;
        const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const original = projections.canStartQueuedRun;
        let refused = false;
        const spy = vi.spyOn(projections, "canStartQueuedRun").mockImplementation((threadId) => {
          if (threadId === childId) {
            refused = true;
            return Effect.fail(
              new ProjectionStore.ProjectionStoreReadError({
                threadId,
                cause: "Injected ordinary queue promotion refusal",
              }),
            );
          }
          return original(threadId);
        });
        yield* Effect.gen(function* () {
          const events = yield* EventSink.EventSinkV2;
          const done = yield* events
            .stream({
              afterSequence: yield* events.latestSequence(),
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === task.id &&
                  stored.event.payload.status === "completed",
              ),
              Stream.take(1),
              Stream.runCollect,
              Effect.forkChild({ startImmediately: true }),
            );
          yield* finishRun(childId, childRun);
          yield* Fiber.join(done);
          assert.isTrue(refused);
          const parent = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(parent.subagents.find((entry) => entry.id === task.id)?.status, "completed");
          const sql = yield* SqlClient.SqlClient;
          const receipts = yield* sql<{
            count: number;
          }>`SELECT count(*) AS count FROM orchestration_command_receipts WHERE aggregate_id = ${childId} AND command_type = ${"thread.capacity.reconcile"}`;
          assert.equal(receipts[0]?.count, 0);
        }).pipe(Effect.ensuring(Effect.sync(() => spy.mockRestore())));
      }),
  );

  it.effect(
    "owned unreleased capacity reactions heal on the next sweep after queue promotion fails",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity("capacity-owned-queue-isolation", undefined, true);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const task = yield* seedLiveChild(input);
          const childId = task.childThreadId!;
          const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
          const pending = yield* awaitCapacityState(childId, "pending");
          yield* failCapacity(childId, childRun);
          yield* Fiber.join(pending);
          const recovery = (yield* orchestrator.getThreadProjection(childId)).thread
            .capacityRecovery!;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const original = projections.canStartQueuedRun;
          const refused = yield* Deferred.make<void>();
          const spy = vi.spyOn(projections, "canStartQueuedRun").mockImplementation((threadId) =>
            threadId === childId
              ? Effect.gen(function* () {
                  yield* Deferred.succeed(refused, undefined);
                  return yield* new ProjectionStore.ProjectionStoreReadError({
                    threadId,
                    cause: "Injected owned queue promotion refusal",
                  });
                })
              : original(threadId),
          );
          yield* Effect.gen(function* () {
            yield* orchestrator.dispatch({
              type: "thread.capacity.cancel",
              commandId: CommandId.make(`${childId}:cancel`),
              threadId: childId,
              generation: recovery.generation,
            });
            yield* Deferred.await(refused);
            assert.isFalse(
              (yield* orchestrator.getThreadProjection(childId)).thread.capacityRecovery
                ?.reactionsReleased,
            );
            assert.equal(
              (yield* orchestrator.getThreadProjection(input.threadId)).subagents.find(
                (entry) => entry.id === task.id,
              )?.status,
              "waiting",
            );
          }).pipe(Effect.ensuring(Effect.sync(() => spy.mockRestore())));
          yield* ModelCapacityRecoveryWorker.sweep;
          assert.isTrue(
            (yield* orchestrator.getThreadProjection(childId)).thread.capacityRecovery
              ?.reactionsReleased,
          );
          assert.equal(
            (yield* orchestrator.getThreadProjection(input.threadId)).subagents.find(
              (entry) => entry.id === task.id,
            )?.status,
            "failed",
          );
        }),
      ),
  );

  it.effect.each(["completed", "capacity-failed"] as const)(
    "an older unreleased episode preserves its owner while a newer human run is %s",
    (outcome) =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity(`capacity-old-release-${outcome}`, undefined, true);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const task = outcome === "completed" ? yield* seedLiveChild(input) : undefined;
          const threadId = task?.childThreadId ?? input.threadId;
          const source =
            task === undefined
              ? input.run
              : (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
          const pending = yield* awaitCapacityState(threadId, "pending");
          yield* failCapacity(threadId, source);
          yield* Fiber.join(pending);
          const recovery = (yield* orchestrator.getThreadProjection(threadId)).thread
            .capacityRecovery!;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const original = projections.canStartQueuedRun;
          const refused = yield* Deferred.make<void>();
          const spy = vi.spyOn(projections, "canStartQueuedRun").mockImplementation((candidate) =>
            candidate === threadId
              ? Effect.gen(function* () {
                  yield* Deferred.succeed(refused, undefined);
                  return yield* new ProjectionStore.ProjectionStoreReadError({
                    threadId,
                    cause: "Injected queue release refusal",
                  });
                })
              : original(candidate),
          );
          const originalMessage = projections.getRunMessage;
          const deliverySpy = vi
            .spyOn(projections, "getRunMessage")
            .mockImplementation((candidate, runId) =>
              candidate === threadId && runId === source.id
                ? Effect.gen(function* () {
                    yield* Deferred.succeed(refused, undefined);
                    return yield* new ProjectionStore.ProjectionStoreReadError({
                      threadId,
                      cause: "Injected older terminal release refusal",
                    });
                  })
                : originalMessage(candidate, runId),
            );
          let human!: OrchestrationV2Run;
          let releaseCommandId: CommandId | null | undefined;
          yield* Effect.gen(function* () {
            yield* orchestrator.dispatch({
              type: "thread.capacity.cancel",
              commandId: CommandId.make(`${threadId}:old-cancel`),
              threadId,
              generation: recovery.generation,
            });
            yield* Deferred.await(refused);
            releaseCommandId = (yield* orchestrator.getThreadProjection(threadId)).thread
              .capacityRecovery?.releaseCommandId;
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(`${threadId}:new-human`),
              threadId,
              messageId: MessageId.make(`${threadId}:new-human`),
              text: "New authorized assignment",
              createdBy: "user",
              creationSource: "web",
              attachments: [],
              dispatchMode: { type: "start_immediately" },
            });
            human = (yield* orchestrator.getThreadProjection(threadId)).runs.at(-1)!;
            if (outcome === "completed") {
              const events = yield* EventSink.EventSinkV2;
              const completed = yield* events.stream({ eventType: "subagent.updated" }).pipe(
                Stream.filter(
                  (stored) =>
                    stored.event.type === "subagent.updated" &&
                    stored.event.payload.id === task!.id &&
                    stored.event.payload.status === "completed",
                ),
                Stream.take(1),
                Stream.runCollect,
                Effect.forkChild({ startImmediately: true }),
              );
              yield* finishRun(threadId, human);
              yield* Fiber.join(completed);
              assert.equal(
                (yield* orchestrator.getThreadProjection(input.threadId)).subagents.find(
                  (entry) => entry.id === task!.id,
                )?.status,
                "completed",
              );
            } else {
              yield* failCapacity(threadId, human);
              yield* orchestrator.dispatch({
                type: "thread.capacity.reconcile",
                commandId: CommandId.make(`${threadId}:new-reconcile-before-release`),
                threadId,
                runId: human.id,
              });
            }
            const held = (yield* orchestrator.getThreadProjection(threadId)).thread
              .capacityRecovery!;
            assert.equal(held.generation, recovery.generation);
            assert.equal(held.releaseCommandId, releaseCommandId);
            assert.isFalse(held.reactionsReleased);
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                spy.mockRestore();
                deliverySpy.mockRestore();
              }),
            ),
          );
          const before = yield* orchestrator.getThreadProjection(threadId);
          yield* ModelCapacityRecoveryWorker.sweep;
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.lengthOf(after.runs, before.runs.length);
          assert.equal(
            after.thread.continuity?.checksRemaining,
            before.thread.continuity?.checksRemaining,
          );
          if (outcome === "capacity-failed") {
            assert.equal(after.thread.capacityRecovery?.status, "pending");
            assert.equal(after.thread.capacityRecovery?.failedRunId, human.id);
            assert.equal(after.thread.capacityRecovery?.authorityMessageId, human.userMessageId);
            assert.equal(after.thread.capacityRecovery?.attemptsUsed, 0);
            const events = yield* EventSink.EventSinkV2;
            const oldAck = yield* events
              .readByCommandId({
                commandId: CommandId.make(
                  `command:capacity:released:${recovery.generation}:${recovery.failedRunId}`,
                ),
              })
              .pipe(Stream.runCollect);
            assert.isTrue(
              oldAck.some(
                (stored) =>
                  stored.event.type === "thread.metadata-updated" &&
                  stored.event.payload.capacityRecovery?.generation === recovery.generation &&
                  stored.event.payload.capacityRecovery.reactionsReleased,
              ),
            );
            assert.notEqual(after.thread.capacityRecovery?.generation, recovery.generation);
          } else {
            assert.isTrue(after.thread.capacityRecovery?.reactionsReleased);
            assert.equal(
              (yield* orchestrator.getThreadProjection(input.threadId)).subagents.find(
                (entry) => entry.id === task!.id,
              )?.status,
              "completed",
            );
          }
        }),
      ),
  );

  it.effect("a rejected startup abort receipt does not poison later terminal cleanup", () =>
    withCapacityRecovery(
      Effect.gen(function* () {
        const input = yield* seedContinuity("capacity-startup-abort-receipt", undefined, true);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const pending = yield* awaitCapacityState(input.threadId, "pending");
        yield* failCapacity(input.threadId, input.run);
        yield* Fiber.join(pending);
        yield* TestClock.adjust("20 seconds");
        yield* ModelCapacityRecoveryWorker.sweep;
        const before = yield* orchestrator.getThreadProjection(input.threadId);
        const runtime = yield* ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService;
        yield* runtime.reconcile("startup");
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const original = projections.getThreadRecords;
        const spy = vi
          .spyOn(projections, "getThreadRecords")
          .mockImplementation((threadId, sections, options) =>
            threadId === input.threadId && sections.includes("checkpointScopes")
              ? Effect.fail(
                  new ProjectionStore.ProjectionStoreReadError({
                    threadId,
                    cause: "Injected startup reconciliation and abort refusal",
                  }),
                )
              : original(threadId, sections, options),
          );
        yield* orchestrator.recoverDelegatedTasks.pipe(
          Effect.ensuring(Effect.sync(() => spy.mockRestore())),
        );
        assert.equal(
          (yield* orchestrator.getThreadProjection(input.threadId)).thread.capacityRecovery?.status,
          "retrying",
        );
        const sql = yield* SqlClient.SqlClient;
        const rejected = yield* sql<{
          command_id: string;
          status: string;
        }>`SELECT command_id, status FROM orchestration_command_receipts WHERE aggregate_id = ${input.threadId} AND command_type = ${"thread.capacity.abort"}`;
        assert.lengthOf(rejected, 1);
        assert.equal(rejected[0]?.status, "rejected");
        const release = yield* awaitCapacityRelease(input.threadId);
        yield* orchestrator.recoverDelegatedTasks;
        yield* Fiber.join(release);
        const receipts = yield* sql<{
          command_id: string;
          status: string;
        }>`SELECT command_id, status FROM orchestration_command_receipts WHERE aggregate_id = ${input.threadId} AND command_type = ${"thread.capacity.abort"}`;
        assert.lengthOf(receipts, 2);
        assert.isTrue(
          receipts.some(
            (receipt) =>
              receipt.status === "accepted" && receipt.command_id !== rejected[0]?.command_id,
          ),
        );
        const after = yield* orchestrator.getThreadProjection(input.threadId);
        assert.isTrue(after.thread.capacityRecovery?.reactionsReleased);
        assert.equal(after.thread.capacityRecovery?.attemptsUsed, 1);
        assert.lengthOf(after.runs, before.runs.length);
      }),
    ),
  );

  it.effect(
    "independent supervision advances the authoritative completed retry after release replay heals a queue failure",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity("capacity-release-continuity");
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const pending = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, input.run);
          yield* Fiber.join(pending);
          yield* TestClock.adjust("20 seconds");
          yield* ModelCapacityRecoveryWorker.sweep;
          const retrying = yield* orchestrator.getThreadProjection(input.threadId);
          const retry = retrying.runs.find(
            (run) => run.id === retrying.thread.capacityRecovery?.retryRunId,
          )!;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const original = projections.canStartQueuedRun;
          const refused = yield* Deferred.make<void>();
          const spy = vi.spyOn(projections, "canStartQueuedRun").mockImplementation((threadId) =>
            threadId === input.threadId
              ? Effect.gen(function* () {
                  yield* Deferred.succeed(refused, undefined);
                  return yield* new ProjectionStore.ProjectionStoreReadError({
                    threadId,
                    cause: "Injected successful retry queue refusal",
                  });
                })
              : original(threadId),
          );
          yield* Effect.gen(function* () {
            yield* finishRun(input.threadId, retry);
            yield* Deferred.await(refused);
            assert.isFalse(
              (yield* orchestrator.getThreadProjection(input.threadId)).thread.capacityRecovery
                ?.reactionsReleased,
            );
            yield* ModelCapacityRecoveryWorker.releasedContinuitySweep;
            assert.equal(
              (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity
                ?.checksRemaining,
              retrying.thread.continuity?.checksRemaining,
            );
          }).pipe(Effect.ensuring(Effect.sync(() => spy.mockRestore())));
          yield* ModelCapacityRecoveryWorker.sweep;
          const healed = yield* orchestrator.getThreadProjection(input.threadId);
          assert.isTrue(healed.thread.capacityRecovery?.reactionsReleased);
          const debugSql = yield* SqlClient.SqlClient;
          const advanceReceipts = yield* debugSql<{
            command_id: string;
          }>`SELECT command_id FROM orchestration_command_receipts WHERE aggregate_id = ${input.threadId} AND command_type = ${"thread.continuity.advance"}`;
          assert.lengthOf(
            healed.runs,
            retrying.runs.length,
            JSON.stringify({ advanceReceipts, continuity: healed.thread.continuity }),
          );
          assert.equal(
            healed.thread.continuity?.checksRemaining,
            retrying.thread.continuity?.checksRemaining,
          );
          yield* ModelCapacityRecoveryWorker.releasedContinuitySweep;
          const advanced = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(advanced.thread.continuity?.lastAdvancedRunId, retry.id);
          assert.equal(advanced.thread.continuity?.authorityMessageId, input.authorityMessageId);
          assert.equal(
            advanced.thread.continuity?.checksRemaining,
            healed.thread.continuity!.checksRemaining - 1,
          );
          assert.equal(
            advanced.thread.continuity?.generation,
            healed.thread.continuity?.generation,
          );
          assert.lengthOf(advanced.runs, healed.runs.length + 1);
          const sql = yield* SqlClient.SqlClient;
          const receiptsBefore = yield* sql<{
            count: number;
          }>`SELECT count(*) AS count FROM orchestration_command_receipts WHERE aggregate_id = ${input.threadId} AND command_type = ${"thread.continuity.advance"}`;
          for (let tick = 0; tick < 5; tick++)
            yield* ModelCapacityRecoveryWorker.releasedContinuitySweep;
          const receiptsAfter = yield* sql<{
            count: number;
          }>`SELECT count(*) AS count FROM orchestration_command_receipts WHERE aggregate_id = ${input.threadId} AND command_type = ${"thread.continuity.advance"}`;
          assert.equal(receiptsAfter[0]?.count, receiptsBefore[0]?.count);
          assert.equal(
            (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity
              ?.checksRemaining,
            advanced.thread.continuity?.checksRemaining,
          );
          assert.lengthOf(
            (yield* orchestrator.getThreadProjection(input.threadId)).runs,
            advanced.runs.length,
          );
        }),
      ),
  );

  it.effect.each(["snooze", "queued", "pending-request"] as const)(
    "released capacity supervision emits no receipts while %s holds completion, then advances once",
    (hold) =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity(`capacity-held-continuity-${hold}`);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const events = yield* EventSink.EventSinkV2;
          const pending = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, input.run);
          yield* Fiber.join(pending);
          yield* TestClock.adjust("20 seconds");
          yield* ModelCapacityRecoveryWorker.sweep;
          const retrying = yield* orchestrator.getThreadProjection(input.threadId);
          const retry = retrying.runs.find(
            (run) => run.id === retrying.thread.capacityRecovery?.retryRunId,
          )!;
          const now = yield* DateTime.now;
          const request = {
            id: RuntimeRequestId.make(`${input.threadId}:held-input`),
            nodeId: retry.rootNodeId!,
            providerTurnId: null,
            nativeRequestRef: null,
            kind: "user_input" as const,
            status: "pending" as const,
            responseCapability: { type: "message" as const },
            createdAt: now,
            resolvedAt: null,
          };
          let queued: OrchestrationV2Run | undefined;
          if (hold === "queued") {
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              threadId: input.threadId,
              commandId: CommandId.make(`${input.threadId}:held-queue`),
              messageId: MessageId.make(`${input.threadId}:held-queue`),
              text: "Queued server follow-up",
              attachments: [],
              createdBy: "agent",
              creationSource: "server",
              dispatchMode: { type: "queue_after_active" },
            });
            queued = (yield* orchestrator.getThreadProjection(input.threadId)).runs.find(
              (run) => run.userMessageId === MessageId.make(`${input.threadId}:held-queue`),
            )!;
            yield* events.write({
              events: [
                {
                  id: EventId.make(`${input.threadId}:hold`),
                  type: "run.updated",
                  threadId: input.threadId,
                  runId: queued.id,
                  occurredAt: now,
                  payload: { ...queued, queueHeld: true },
                },
              ],
            });
          } else if (hold === "snooze") {
            yield* events.write({
              events: [
                {
                  id: EventId.make(`${input.threadId}:hold`),
                  type: "thread.metadata-updated",
                  threadId: input.threadId,
                  occurredAt: now,
                  payload: { ...retrying.thread, snoozedUntil: DateTime.add(now, { hours: 1 }) },
                },
              ],
            });
          } else {
            yield* events.write({
              events: [
                {
                  id: EventId.make(`${input.threadId}:hold`),
                  type: "runtime-request.updated",
                  threadId: input.threadId,
                  occurredAt: now,
                  payload: request,
                },
              ],
            });
          }
          const release = yield* awaitCapacityRelease(input.threadId);
          yield* finishRun(input.threadId, retry);
          yield* Fiber.join(release);
          const held = yield* orchestrator.getThreadProjection(input.threadId);
          assert.isTrue(held.thread.capacityRecovery?.reactionsReleased);
          const sql = yield* SqlClient.SqlClient;
          const before = yield* sql<{
            count: number;
          }>`SELECT count(*) AS count FROM orchestration_command_receipts WHERE aggregate_id = ${input.threadId} AND command_type = ${"thread.continuity.advance"}`;
          for (let tick = 0; tick < 5; tick++)
            yield* ModelCapacityRecoveryWorker.releasedContinuitySweep;
          const after = yield* sql<{
            count: number;
          }>`SELECT count(*) AS count FROM orchestration_command_receipts WHERE aggregate_id = ${input.threadId} AND command_type = ${"thread.continuity.advance"}`;
          assert.equal(after[0]?.count, before[0]?.count);
          const stillHeld = yield* orchestrator.getThreadProjection(input.threadId);
          assert.lengthOf(stillHeld.runs, held.runs.length);
          assert.equal(
            stillHeld.thread.continuity?.checksRemaining,
            held.thread.continuity?.checksRemaining,
          );
          assert.equal(
            stillHeld.thread.continuity?.lastAdvancedRunId,
            held.thread.continuity?.lastAdvancedRunId,
          );
          const advancedEvent = yield* events
            .stream({ afterSequence: yield* events.latestSequence(), eventType: "run.created" })
            .pipe(
              Stream.filter((stored) => stored.event.threadId === input.threadId),
              Stream.take(1),
              Stream.runCollect,
              Effect.forkChild({ startImmediately: true }),
            );
          if (hold === "queued") {
            yield* orchestrator.dispatch({
              type: "queued-run.cancel",
              commandId: CommandId.make(`${input.threadId}:clear-hold`),
              threadId: input.threadId,
              runId: queued!.id,
            });
          } else if (hold === "snooze") {
            yield* events.write({
              events: [
                {
                  id: EventId.make(`${input.threadId}:clear-hold`),
                  type: "thread.metadata-updated",
                  threadId: input.threadId,
                  occurredAt: yield* DateTime.now,
                  payload: { ...stillHeld.thread, snoozedUntil: null },
                },
              ],
            });
          } else {
            yield* events.write({
              events: [
                {
                  id: EventId.make(`${input.threadId}:clear-hold`),
                  type: "runtime-request.updated",
                  threadId: input.threadId,
                  occurredAt: yield* DateTime.now,
                  payload: { ...request, status: "resolved", resolvedAt: yield* DateTime.now },
                },
              ],
            });
          }
          yield* ModelCapacityRecoveryWorker.releasedContinuitySweep;
          yield* Fiber.join(advancedEvent);
          const advanced = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(advanced.thread.continuity?.generation, held.thread.continuity?.generation);
          assert.equal(advanced.thread.continuity?.authorityMessageId, input.authorityMessageId);
          assert.equal(advanced.thread.continuity?.lastAdvancedRunId, retry.id);
          assert.equal(
            advanced.thread.continuity?.checksRemaining,
            held.thread.continuity!.checksRemaining - 1,
          );
          assert.lengthOf(advanced.runs, held.runs.length + 1);
          const receiptsAdvanced = yield* sql<{
            count: number;
          }>`SELECT count(*) AS count FROM orchestration_command_receipts WHERE aggregate_id = ${input.threadId} AND command_type = ${"thread.continuity.advance"}`;
          for (let tick = 0; tick < 5; tick++)
            yield* ModelCapacityRecoveryWorker.releasedContinuitySweep;
          const receiptsRepeated = yield* sql<{
            count: number;
          }>`SELECT count(*) AS count FROM orchestration_command_receipts WHERE aggregate_id = ${input.threadId} AND command_type = ${"thread.continuity.advance"}`;
          assert.equal(receiptsRepeated[0]?.count, receiptsAdvanced[0]?.count);
          assert.lengthOf(
            (yield* orchestrator.getThreadProjection(input.threadId)).runs,
            advanced.runs.length,
          );
          assert.equal(
            (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity
              ?.checksRemaining,
            advanced.thread.continuity?.checksRemaining,
          );
        }),
      ),
  );

  it.effect(
    "capacity worker eligibility survives a backward clock step between its due read and serialized dispatch",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity("capacity-backward-clock", undefined, true);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const pending = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, input.run);
          yield* Fiber.join(pending);
          yield* TestClock.adjust("20 seconds");
          const dueNow = DateTime.toEpochMillis(yield* DateTime.now);
          const threads = yield* ThreadManagementService.ThreadManagementService;
          yield* ModelCapacityRecoveryWorker.sweep.pipe(
            Effect.provideService(ThreadManagementService.ThreadManagementService, {
              ...threads,
              dispatch: (command) =>
                command.type === "thread.capacity.retry" && command.threadId === input.threadId
                  ? Effect.gen(function* () {
                      yield* TestClock.setTime(dueNow - 60_000);
                      return yield* threads.dispatch(command);
                    })
                  : threads.dispatch(command),
            }),
          );
          const allocated = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(allocated.thread.capacityRecovery?.status, "retrying");
          assert.equal(allocated.thread.capacityRecovery?.attemptsUsed, 1);
          yield* TestClock.setTime(dueNow + 5_000);
          yield* ModelCapacityRecoveryWorker.sweep;
          assert.lengthOf((yield* orchestrator.getThreadProjection(input.threadId)).runs, 2);
        }),
      ),
  );

  it.effect(
    "a transient rejected capacity abort receipt cannot poison a later refusal cleanup",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity("capacity-abort-receipt", undefined, true);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const pending = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, input.run);
          yield* Fiber.join(pending);
          yield* TestClock.adjust("20 seconds");
          const threads = yield* ThreadManagementService.ThreadManagementService;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const original = projections.getThreadRecords;
          const abortIds: CommandId[] = [];
          const sweep = ModelCapacityRecoveryWorker.sweep.pipe(
            Effect.provideService(ThreadManagementService.ThreadManagementService, {
              ...threads,
              dispatch: (command) => {
                if (
                  (command.type !== "thread.capacity.retry" &&
                    command.type !== "thread.capacity.abort") ||
                  command.threadId !== input.threadId
                )
                  return threads.dispatch(command);
                if (command.type === "thread.capacity.retry")
                  return Effect.fail(
                    new Orchestrator.OrchestratorDispatchError({
                      commandId: command.commandId,
                      commandType: command.type,
                      cause: "Injected continuation refusal",
                    }),
                  );
                if (command.type !== "thread.capacity.abort") return threads.dispatch(command);
                abortIds.push(command.commandId);
                if (abortIds.length !== 1) return threads.dispatch(command);
                const spy = vi
                  .spyOn(projections, "getThreadRecords")
                  .mockImplementation((threadId, sections, options) =>
                    threadId === input.threadId && sections.includes("checkpointScopes")
                      ? Effect.fail(
                          new ProjectionStore.ProjectionStoreReadError({
                            threadId,
                            cause: "Injected transient abort projection refusal",
                          }),
                        )
                      : original(threadId, sections, options),
                  );
                return threads
                  .dispatch(command)
                  .pipe(Effect.ensuring(Effect.sync(() => spy.mockRestore())));
              },
            }),
          );
          yield* sweep;
          assert.equal(
            (yield* orchestrator.getThreadProjection(input.threadId)).thread.capacityRecovery
              ?.status,
            "pending",
          );
          const sql = yield* SqlClient.SqlClient;
          const receipts = yield* sql<{
            status: string;
          }>`SELECT status FROM orchestration_command_receipts WHERE command_id = ${abortIds[0]!}`;
          assert.equal(receipts[0]?.status, "rejected");
          const release = yield* awaitCapacityRelease(input.threadId);
          yield* sweep;
          yield* Fiber.join(release);
          assert.lengthOf(abortIds, 2);
          assert.notEqual(abortIds[0], abortIds[1]);
          assert.equal(
            (yield* orchestrator.getThreadProjection(input.threadId)).thread.capacityRecovery
              ?.status,
            "cancelled",
          );
          assert.lengthOf((yield* orchestrator.getThreadProjection(input.threadId)).runs, 1);
        }),
      ),
  );

  const restoreQueuedCapacityMessage = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const events = yield* EventSink.EventSinkV2;
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const source = projection.runs[0]!;
      const message = projection.messages.find((entry) => entry.id === source.userMessageId)!;
      const now = yield* DateTime.now;
      const messageId = MessageId.make(`${threadId}:queued`);
      const runId = RunId.make(`${threadId}:queued`);
      yield* events.write({
        events: [
          {
            id: EventId.make(`${threadId}:queued-message`),
            type: "message.updated",
            threadId,
            occurredAt: now,
            payload: { ...message, id: messageId, text: "A queued follow-up", createdAt: now },
          },
          {
            id: EventId.make(`${threadId}:queued-run`),
            type: "run.updated",
            threadId,
            runId,
            occurredAt: now,
            payload: {
              ...source,
              id: runId,
              ordinal: source.ordinal + 1,
              userMessageId: messageId,
              status: "queued",
              queuePosition: 1,
              queueHeld: false,
              rootNodeId: null,
              activeAttemptId: null,
              startedAt: null,
              completedAt: null,
            },
          },
        ],
      });
    });

  it.effect(
    "cancelling an unstarted held queue head still promotes its successor with capacity disabled",
    () =>
      Effect.gen(function* () {
        const input = yield* seedContinuity("capacity-off-unstarted-queue-cancel", undefined, true);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        for (const suffix of ["head", "successor"])
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`${input.threadId}:${suffix}`),
            threadId: input.threadId,
            messageId: MessageId.make(`${input.threadId}:${suffix}`),
            text: suffix,
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "queue_after_active" },
          });
        const projection = yield* orchestrator.getThreadProjection(input.threadId);
        const head = projection.runs.find(
          (run) => run.userMessageId === MessageId.make(`${input.threadId}:head`),
        )!;
        const successor = projection.runs.find(
          (run) => run.userMessageId === MessageId.make(`${input.threadId}:successor`),
        )!;
        const events = yield* EventSink.EventSinkV2;
        const now = yield* DateTime.now;
        yield* events.write({
          commandId: CommandId.make("command:runtime-reconcile:queue-head-fixture"),
          events: [
            {
              id: EventId.make(`${input.threadId}:predecessor-completed`),
              type: "run.updated",
              threadId: input.threadId,
              runId: input.run.id,
              occurredAt: now,
              payload: { ...input.run, status: "completed", startedAt: now, completedAt: now },
            },
            {
              id: EventId.make(`${input.threadId}:held-head`),
              type: "run.updated",
              threadId: input.threadId,
              runId: head.id,
              occurredAt: now,
              payload: { ...head, queueHeld: true },
            },
          ],
        });
        const promoted = yield* events
          .stream({ afterSequence: yield* events.latestSequence(), eventType: "run.updated" })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "run.updated" &&
                stored.event.payload.id === successor.id &&
                ["preparing", "starting", "running"].includes(stored.event.payload.status),
            ),
            Stream.take(1),
            Stream.runCollect,
            Effect.forkChild({ startImmediately: true }),
          );
        yield* orchestrator.dispatch({
          type: "queued-run.cancel",
          commandId: CommandId.make(`${input.threadId}:cancel-head`),
          threadId: input.threadId,
          runId: head.id,
        });
        yield* Fiber.join(promoted);
        const after = yield* orchestrator.getThreadProjection(input.threadId);
        assert.equal(after.runs.find((run) => run.id === head.id)?.status, "cancelled");
        assert.isNull(after.runs.find((run) => run.id === head.id)?.startedAt);
        assert.notEqual(after.runs.find((run) => run.id === successor.id)?.status, "queued");
      }),
  );

  it.effect(
    "a completed capacity retry remains a completed child result when reconciliation is refused",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity("capacity-completed-child-abort", undefined, true);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const task = yield* seedLiveChild(input);
          const childId = task.childThreadId!;
          const childRun = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
          const pending = yield* awaitCapacityState(childId, "pending");
          yield* failCapacity(childId, childRun);
          yield* Fiber.join(pending);
          yield* TestClock.adjust("20 seconds");
          yield* ModelCapacityRecoveryWorker.sweep;
          const retrying = yield* orchestrator.getThreadProjection(childId);
          const retry = retrying.runs.find(
            (run) => run.id === retrying.thread.capacityRecovery?.retryRunId,
          )!;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const original = projections.getThreadRecords;
          let refused = false;
          const spy = vi
            .spyOn(projections, "getThreadRecords")
            .mockImplementation((threadId, sections, options) => {
              if (!refused && threadId === childId && sections.includes("checkpointScopes")) {
                refused = true;
                return Effect.fail(
                  new ProjectionStore.ProjectionStoreReadError({
                    threadId,
                    cause: "Injected completed retry reconciliation refusal",
                  }),
                );
              }
              return original(threadId, sections, options);
            });
          yield* Effect.gen(function* () {
            const release = yield* awaitCapacityRelease(childId);
            yield* finishRun(childId, retry);
            yield* Fiber.join(release);
            assert.isTrue(refused);
            assert.equal(
              (yield* orchestrator.getThreadProjection(childId)).thread.capacityRecovery?.status,
              "cancelled",
            );
            assert.equal(
              (yield* orchestrator.getThreadProjection(input.threadId)).subagents.find(
                (entry) => entry.id === task.id,
              )?.status,
              "completed",
            );
          }).pipe(Effect.ensuring(Effect.sync(() => spy.mockRestore())));
        }),
      ),
  );

  it.effect(
    "restart continuations retain their original capacity generation and spent attempt budget",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity("capacity-restart-budget", undefined, true);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const pending = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, input.run);
          yield* Fiber.join(pending);
          yield* TestClock.adjust("20 seconds");
          yield* ModelCapacityRecoveryWorker.sweep;
          const first = yield* orchestrator.getThreadProjection(input.threadId);
          const recovery = first.thread.capacityRecovery!;
          const retry = first.runs.find((run) => run.id === recovery.retryRunId)!;
          const events = yield* EventSink.EventSinkV2;
          const now = yield* DateTime.now;
          yield* events.write({
            commandId: CommandId.make("command:runtime-reconcile:capacity-budget"),
            events: [
              {
                id: EventId.make(`${input.threadId}:runtime-cancelled`),
                type: "run.updated",
                threadId: input.threadId,
                runId: retry.id,
                occurredAt: now,
                payload: { ...retry, status: "cancelled", completedAt: now },
              },
            ],
          });
          yield* orchestrator.recoverDelegatedTasks;
          const sourceMessage = first.messages.find((entry) => entry.id === retry.userMessageId)!;
          const restarted = {
            ...retry,
            id: RunId.make(`${input.threadId}:restart`),
            ordinal: retry.ordinal + 1,
            userMessageId: MessageId.make(`${input.threadId}:restart`),
            status: "preparing" as const,
            startedAt: null,
            completedAt: null,
            restartContinuationOfRunId: retry.id,
          };
          yield* events.write({
            events: [
              {
                id: EventId.make(`${input.threadId}:restart-message`),
                type: "message.updated",
                threadId: input.threadId,
                occurredAt: now,
                payload: { ...sourceMessage, id: restarted.userMessageId },
              },
              {
                id: EventId.make(`${input.threadId}:restart-run`),
                type: "run.updated",
                threadId: input.threadId,
                runId: restarted.id,
                occurredAt: now,
                payload: restarted,
              },
            ],
          });
          const carried = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, restarted);
          yield* Fiber.join(carried);
          const after = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(after.thread.capacityRecovery?.generation, recovery.generation);
          assert.equal(after.thread.capacityRecovery?.originalRunId, input.run.id);
          assert.equal(after.thread.capacityRecovery?.attemptsUsed, 1);
          assert.equal(after.thread.capacityRecovery?.authorityMessageId, input.authorityMessageId);
          yield* TestClock.adjust("35 seconds");
          yield* ModelCapacityRecoveryWorker.sweep;
          assert.equal(
            (yield* orchestrator.getThreadProjection(input.threadId)).thread.capacityRecovery
              ?.attemptsUsed,
            2,
          );
        }),
      ),
  );

  it.effect(
    "cancelling capacity preserves the provider failure queue hold and acknowledges reactions only after release",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity("capacity-cancel-queue");
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const pending = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, input.run);
          yield* Fiber.join(pending);
          yield* restoreQueuedCapacityMessage(input.threadId);
          const recovery = (yield* orchestrator.getThreadProjection(input.threadId)).thread
            .capacityRecovery!;
          const release = yield* awaitCapacityRelease(input.threadId);
          const cancelled = yield* orchestrator.dispatch({
            type: "thread.capacity.cancel",
            commandId: CommandId.make(`${input.threadId}:cancel`),
            threadId: input.threadId,
            generation: recovery.generation,
          });
          assert.isTrue(
            cancelled.storedEvents.some(
              (stored) =>
                stored.event.type === "thread.metadata-updated" &&
                stored.event.payload.capacityRecovery?.reactionsReleased === false,
            ),
          );
          yield* Fiber.join(release);
          const after = yield* orchestrator.getThreadProjection(input.threadId);
          const queued = after.runs.find((run) => run.status === "queued");
          assert.isDefined(queued);
          assert.isTrue(queued?.queueHeld);
          assert.isTrue(after.thread.capacityRecovery?.reactionsReleased);
          yield* orchestrator.recoverDelegatedTasks;
          assert.lengthOf((yield* orchestrator.getThreadProjection(input.threadId)).runs, 2);
        }),
      ),
  );

  it.effect(
    "Stop records a durable capacity release and startup never promotes the stopped queue",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity("capacity-stop-crash");
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const pending = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, input.run);
          yield* Fiber.join(pending);
          yield* restoreQueuedCapacityMessage(input.threadId);
          const commandId = CommandId.make(`${input.threadId}:stop`);
          const release = yield* awaitCapacityRelease(input.threadId);
          const result = yield* orchestrator.dispatch({
            type: "thread.stop",
            commandId,
            threadId: input.threadId,
          });
          const stoppedEvent = result.storedEvents.find(
            (stored) =>
              stored.event.type === "thread.metadata-updated" &&
              stored.event.payload.capacityRecovery?.status === "cancelled",
          );
          assert.isDefined(stoppedEvent);
          if (stoppedEvent?.event.type === "thread.metadata-updated")
            assert.equal(stoppedEvent.event.payload.capacityRecovery?.releaseCommandId, commandId);
          yield* Fiber.join(release);
          // Rewind just the durable ACK to represent a crash after Stop committed and before release completed.
          const sql = yield* SqlClient.SqlClient;
          yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.capacityRecovery.reactionsReleased', json('false')) WHERE thread_id = ${input.threadId}`;
          const recovery = (yield* orchestrator.getThreadProjection(input.threadId)).thread
            .capacityRecovery!;
          yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${`command:capacity:released:${recovery.generation}:${recovery.failedRunId}`}`;
          yield* orchestrator.recoverDelegatedTasks;
          const after = yield* orchestrator.getThreadProjection(input.threadId);
          assert.isTrue(after.thread.capacityRecovery?.reactionsReleased);
          assert.isNull(after.thread.continuity);
          assert.equal(after.runs.filter((run) => run.status === "queued").length, 1);
          assert.isTrue(after.runs.find((run) => run.status === "queued")?.queueHeld);
          assert.lengthOf(after.runs, 2);
        }),
      ),
  );

  it.effect.each(["cancel", "cancel-delivery-refusal", "complete-retry"] as const)(
    "capacity %s releases claimed delegated delivery only after the recovery episode ends",
    (outcome) =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const input = yield* seedContinuity(`capacity-delivery-${outcome}`);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const task = yield* seedLiveChild(input);
          const events = yield* EventSink.EventSinkV2;
          const shifted = { ...input.run, ordinal: 2 };
          yield* events.write({
            events: [
              {
                id: EventId.make(`${input.threadId}:delivery-run-ordinal`),
                type: "run.updated",
                threadId: input.threadId,
                runId: shifted.id,
                occurredAt: yield* DateTime.now,
                payload: shifted,
              },
            ],
          });
          const projection = yield* orchestrator.getThreadProjection(input.threadId);
          const message = projection.messages.find(
            (entry) => entry.id === input.authorityMessageId,
          )!;
          const ownerRunId = RunId.make(`${input.threadId}:delivery-owner`);
          const ownerMessageId = MessageId.make(`${input.threadId}:delivery-owner`);
          const now = yield* DateTime.now;
          yield* events.write({
            events: [
              {
                id: EventId.make(`${input.threadId}:owner-message`),
                type: "message.updated",
                threadId: input.threadId,
                occurredAt: now,
                payload: {
                  ...message,
                  id: ownerMessageId,
                  text: "Original delegated assignment",
                },
              },
              {
                id: EventId.make(`${input.threadId}:delivery-message`),
                type: "message.updated",
                threadId: input.threadId,
                occurredAt: now,
                payload: {
                  ...message,
                  delegatedCompletion: {
                    parentRunId: ownerRunId,
                    generation: 1,
                    taskIds: [task.id],
                  },
                },
              },
              {
                id: EventId.make(`${input.threadId}:delivery-owner`),
                type: "run.updated",
                threadId: input.threadId,
                runId: ownerRunId,
                occurredAt: now,
                payload: {
                  ...input.run,
                  id: ownerRunId,
                  userMessageId: ownerMessageId,
                  ordinal: 1,
                  rootNodeId: null,
                  activeAttemptId: null,
                  status: "completed",
                  startedAt: now,
                  completedAt: now,
                  delegatedCompletion: {
                    disposition: "open",
                    nextGeneration: 2,
                    delivery: { generation: 1, messageId: message.id, taskIds: [task.id] },
                  },
                },
              },
              {
                id: EventId.make(`${input.threadId}:claimed-result`),
                type: "subagent.updated",
                threadId: input.threadId,
                runId: ownerRunId,
                nodeId: task.id,
                occurredAt: now,
                payload: {
                  ...task,
                  runId: ownerRunId,
                  status: "completed",
                  completedAt: now,
                  updatedAt: now,
                  result: "The delegated task completed.",
                  completionDelivery: { state: "claimed", observedByRunId: null },
                },
              },
            ],
          });
          const pending = yield* awaitCapacityState(input.threadId, "pending");
          yield* failCapacity(input.threadId, shifted);
          yield* Fiber.join(pending);
          yield* orchestrator.recoverDelegatedTasks;
          assert.equal(
            (yield* orchestrator.getThreadProjection(input.threadId)).subagents.find(
              (entry) => entry.id === task.id,
            )?.completionDelivery?.state,
            "claimed",
          );
          if (outcome !== "complete-retry") {
            const recovery = (yield* orchestrator.getThreadProjection(input.threadId)).thread
              .capacityRecovery!;
            const projections = yield* ProjectionStore.ProjectionStoreV2;
            const original = projections.getRunMessage;
            const refused = yield* Deferred.make<void>();
            const spy =
              outcome === "cancel-delivery-refusal"
                ? vi.spyOn(projections, "getRunMessage").mockImplementation((threadId, runId) =>
                    threadId === input.threadId && runId === shifted.id
                      ? Effect.gen(function* () {
                          yield* Deferred.succeed(refused, undefined);
                          return yield* new ProjectionStore.ProjectionStoreReadError({
                            threadId,
                            cause: "Injected delegated delivery refusal",
                          });
                        })
                      : original(threadId, runId),
                  )
                : undefined;
            const release = yield* awaitCapacityRelease(input.threadId);
            yield* Effect.gen(function* () {
              yield* orchestrator.dispatch({
                type: "thread.capacity.cancel",
                commandId: CommandId.make(`${input.threadId}:cancel`),
                threadId: input.threadId,
                generation: recovery.generation,
              });
              if (spy !== undefined) {
                yield* Deferred.await(refused);
                const held = yield* orchestrator.getThreadProjection(input.threadId);
                assert.isFalse(held.thread.capacityRecovery?.reactionsReleased);
                assert.equal(
                  held.subagents.find((entry) => entry.id === task.id)?.completionDelivery?.state,
                  "claimed",
                );
              }
            }).pipe(Effect.ensuring(Effect.sync(() => spy?.mockRestore())));
            if (spy !== undefined) {
              const held = yield* orchestrator.getThreadProjection(input.threadId);
              yield* ModelCapacityRecoveryWorker.sweep;
              const healed = yield* orchestrator.getThreadProjection(input.threadId);
              assert.lengthOf(healed.runs, held.runs.length);
              assert.equal(
                healed.thread.capacityRecovery?.attemptsUsed,
                held.thread.capacityRecovery?.attemptsUsed,
              );
              assert.equal(
                healed.thread.continuity?.checksRemaining,
                held.thread.continuity?.checksRemaining,
              );
            }
            yield* Fiber.join(release);
          } else {
            yield* TestClock.adjust("20 seconds");
            yield* ModelCapacityRecoveryWorker.sweep;
            yield* orchestrator.recoverDelegatedTasks;
            const retrying = yield* orchestrator.getThreadProjection(input.threadId);
            assert.equal(
              retrying.subagents.find((entry) => entry.id === task.id)?.completionDelivery?.state,
              "claimed",
            );
            const retry = retrying.runs.find(
              (run) => run.id === retrying.thread.capacityRecovery?.retryRunId,
            )!;
            const release = yield* awaitCapacityRelease(input.threadId);
            yield* finishRun(input.threadId, retry);
            yield* Fiber.join(release);
          }
          const after = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(
            after.subagents.find((entry) => entry.id === task.id)?.completionDelivery?.state,
            "delivered",
          );
          assert.isNull(
            after.runs.find((run) => run.id === ownerRunId)?.delegatedCompletion?.delivery,
          );
          assert.isTrue(after.thread.capacityRecovery?.reactionsReleased);
        }),
      ),
  );

  it.effect(
    "capacity child results stay unpublished during retries and publish once after completion",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const parent = yield* seedContinuity("capacity-child");
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const task = yield* seedLiveChild(parent);
          const childThreadId = task.childThreadId!;
          const child = yield* orchestrator.getThreadProjection(childThreadId);
          const childRun = child.runs[0]!;
          const pending = yield* awaitCapacityState(childThreadId, "pending");
          yield* failCapacity(childThreadId, childRun);
          yield* Fiber.join(pending);
          const waiting = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.find(
            (entry) => entry.id === task.id,
          )!;
          assert.equal(waiting.status, "waiting");
          assert.isNull(waiting.result);
          assert.isNull(waiting.completedAt);
          yield* TestClock.adjust("20 seconds");
          yield* ModelCapacityRecoveryWorker.sweep;
          const retrying = yield* orchestrator.getThreadProjection(childThreadId);
          const retry = retrying.runs.find(
            (run) => run.id === retrying.thread.capacityRecovery?.retryRunId,
          )!;
          assert.equal(retry.delegatedTaskId, task.id);
          const events = yield* EventSink.EventSinkV2;
          const published = yield* events
            .stream({
              afterSequence: yield* events.latestSequence(),
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === task.id &&
                  stored.event.payload.status === "completed",
              ),
              Stream.take(1),
              Stream.runCollect,
              Effect.forkChild({ startImmediately: true }),
            );
          yield* finishRun(childThreadId, retry);
          yield* Fiber.join(published);
          yield* orchestrator.recoverDelegatedTasks;
          const result = yield* orchestrator.getThreadProjection(parent.threadId);
          assert.equal(result.subagents.find((entry) => entry.id === task.id)?.status, "completed");
          assert.lengthOf(
            result.contextTransfers.filter(
              (entry) => entry.type === "subagent_result" && entry.sourceThreadId === childThreadId,
            ),
            1,
          );
        }),
      ),
  );

  it.effect.each([
    "usageLimitExceeded",
    "rateLimitExceeded",
    "unauthorized",
    "internalServerError",
    "contextWindowExceeded",
    "-32001",
  ] as const)("capacity recovery never retries terminal %s errors", (code) =>
    withCapacityRecovery(
      Effect.gen(function* () {
        const input = yield* seedContinuity(`capacity-negative-${code}`);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        yield* failCapacity(input.threadId, input.run, code);
        yield* orchestrator.dispatch({
          type: "thread.capacity.reconcile",
          commandId: CommandId.make(`${input.threadId}:negative-reconcile`),
          threadId: input.threadId,
          runId: input.run.id,
        });
        yield* TestClock.adjust("2 minutes");
        yield* ModelCapacityRecoveryWorker.sweep;
        const projection = yield* orchestrator.getThreadProjection(input.threadId);
        assert.isNull(projection.thread.capacityRecovery ?? null);
        assert.lengthOf(projection.runs, 1);
      }),
    ),
  );
  it.effect(
    "capacity recovery remains disabled by default and does not reopen completed continuity",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const disabled = yield* seedContinuity("capacity-disabled");
        yield* failCapacity(disabled.threadId, disabled.run);
        yield* orchestrator.dispatch({
          type: "thread.capacity.reconcile",
          commandId: CommandId.make(`${disabled.threadId}:negative`),
          threadId: disabled.threadId,
          runId: disabled.run.id,
        });
        assert.isNull(
          (yield* orchestrator.getThreadProjection(disabled.threadId)).thread.capacityRecovery ??
            null,
        );
        yield* withCapacityRecovery(
          Effect.gen(function* () {
            const completed = yield* seedContinuity("capacity-complete");
            yield* orchestrator.dispatch({
              type: "thread.continuity.report",
              commandId: CommandId.make(`${completed.threadId}:complete`),
              threadId: completed.threadId,
              runId: completed.run.id,
              status: "complete",
              reason: "The assignment is verified complete.",
            });
            const before = (yield* orchestrator.getThreadProjection(completed.threadId)).thread
              .continuity;
            yield* failCapacity(completed.threadId, completed.run);
            yield* orchestrator.dispatch({
              type: "thread.capacity.reconcile",
              commandId: CommandId.make(`${completed.threadId}:negative`),
              threadId: completed.threadId,
              runId: completed.run.id,
            });
            const after = yield* orchestrator.getThreadProjection(completed.threadId);
            assert.isNull(after.thread.capacityRecovery ?? null);
            assert.deepEqual(after.thread.continuity, before);
          }),
        );
      }),
  );

  it.effect.each([
    "new-human",
    "model-options",
    "runtime-mode",
    "plan",
    "snooze",
    "archive",
    "pending-input",
    "paused-goal",
    "pref-off",
  ] as const)("%s cancels a pending capacity retry before it can start", (change) =>
    withCapacityRecovery(
      Effect.gen(function* () {
        const input = yield* seedContinuity(`capacity-race-${change}`);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const pending = yield* awaitCapacityState(input.threadId, "pending");
        yield* failCapacity(input.threadId, input.run);
        yield* Fiber.join(pending);
        const events = yield* EventSink.EventSinkV2;
        const now = yield* DateTime.now;
        const before = yield* orchestrator.getThreadProjection(input.threadId);
        if (change === "new-human") yield* sendHumanFollowup(input.threadId, "new-human");
        else if (change === "pref-off")
          yield* (yield* ServerSettings.ServerSettingsService).updateSettings({
            autoResumeCapacityThreads: false,
          });
        else if (change === "paused-goal")
          yield* events.write({
            events: [
              {
                id: EventId.make(`${input.threadId}:guard`),
                type: "provider-thread.updated",
                threadId: input.threadId,
                occurredAt: now,
                payload: {
                  ...before.providerThreads[0]!,
                  goal: { objective: "Paused native objective", status: "paused" },
                },
              },
            ],
          });
        else if (change === "pending-input")
          yield* events.write({
            events: [
              {
                id: EventId.make(`${input.threadId}:guard`),
                type: "runtime-request.updated",
                threadId: input.threadId,
                occurredAt: now,
                payload: {
                  id: RuntimeRequestId.make(`${input.threadId}:question`),
                  nodeId: input.run.rootNodeId!,
                  providerTurnId: null,
                  nativeRequestRef: null,
                  kind: "user_input",
                  status: "pending",
                  responseCapability: { type: "message" },
                  createdAt: now,
                  resolvedAt: null,
                },
              },
            ],
          });
        else
          yield* events.write({
            events: [
              {
                id: EventId.make(`${input.threadId}:guard`),
                type: "thread.metadata-updated",
                threadId: input.threadId,
                occurredAt: now,
                payload: {
                  ...before.thread,
                  ...(change === "model-options"
                    ? {
                        modelSelection: {
                          ...before.thread.modelSelection,
                          options: [{ id: "reasoningEffort", value: "high" }],
                        },
                      }
                    : {}),
                  ...(change === "runtime-mode"
                    ? { runtimeMode: "approval-required" as const }
                    : {}),
                  ...(change === "plan" ? { interactionMode: "plan" as const } : {}),
                  ...(change === "archive" ? { archivedAt: now } : {}),
                  ...(change === "snooze" ? { snoozedUntil: DateTime.add(now, { hours: 1 }) } : {}),
                },
              },
            ],
          });
        yield* TestClock.adjust("20 seconds");
        yield* ModelCapacityRecoveryWorker.sweep;
        const after = yield* orchestrator.getThreadProjection(input.threadId);
        assert.equal(after.thread.capacityRecovery?.status, "cancelled");
        assert.equal(after.thread.capacityRecovery?.attemptsUsed, 0);
        assert.lengthOf(after.runs, change === "new-human" ? 2 : 1);
      }),
    ),
  );

  it.effect(
    "a cancelled delegated owner prevents capacity continuation and published results never reopen",
    () =>
      withCapacityRecovery(
        Effect.gen(function* () {
          const parent = yield* seedContinuity("capacity-owner-stop");
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const task = yield* seedLiveChild(parent);
          const childId = task.childThreadId!;
          const run = (yield* orchestrator.getThreadProjection(childId)).runs[0]!;
          const pending = yield* awaitCapacityState(childId, "pending");
          yield* failCapacity(childId, run);
          yield* Fiber.join(pending);
          yield* orchestrator.dispatch({
            type: "thread.stop",
            commandId: CommandId.make(`${parent.threadId}:stop-child`),
            threadId: parent.threadId,
          });
          const events = yield* EventSink.EventSinkV2;
          const cancelled = yield* events
            .stream({
              afterSequence: yield* events.latestSequence(),
              eventType: "subagent.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "subagent.updated" &&
                  stored.event.payload.id === task.id &&
                  stored.event.payload.status === "cancelled",
              ),
              Stream.take(1),
              Stream.runCollect,
              Effect.forkChild({ startImmediately: true }),
            );
          yield* TestClock.adjust("20 seconds");
          yield* ModelCapacityRecoveryWorker.sweep;
          yield* Fiber.join(cancelled);
          const after = yield* orchestrator.getThreadProjection(childId);
          assert.equal(after.thread.capacityRecovery?.status, "cancelled");
          assert.lengthOf(after.runs, 1);
          const stopped = (yield* orchestrator.getThreadProjection(parent.threadId)).subagents.find(
            (entry) => entry.id === task.id,
          )!;
          assert.equal(stopped.status, "cancelled");
          yield* orchestrator.dispatch({
            type: "thread.capacity.reconcile",
            commandId: CommandId.make(`${parent.threadId}:late-try`),
            threadId: childId,
            runId: run.id,
          });
          assert.lengthOf((yield* orchestrator.getThreadProjection(childId)).runs, 1);
        }),
      ),
  );

  const withAutoEnrollment = <A, E, R>(program: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const settings = yield* ServerSettings.ServerSettingsService;
      yield* settings.updateSettings({ workContinuityAutoEnroll: true });
      return yield* program.pipe(
        Effect.ensuring(
          settings.updateSettings({ workContinuityAutoEnroll: false }).pipe(Effect.orDie),
        ),
      );
    });
  const sendHumanFollowup = (threadId: ThreadId, suffix: string) =>
    Effect.flatMap(Orchestrator.OrchestratorV2, (orchestrator) =>
      orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`${threadId}:${suffix}`),
        threadId,
        messageId: MessageId.make(`${threadId}:${suffix}`),
        text: "What is the status?",
        createdBy: "user",
        creationSource: "web",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
      }),
    );

  it.effect("disabled auto enrollment leaves accepted human messages unenrolled", () =>
    Effect.gen(function* () {
      const input = yield* seedContinuity("auto-off", undefined, true);
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      assert.isNull(
        (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity ?? null,
      );
    }),
  );

  it.effect(
    "automatic human enrollment spends exactly one check after a missing terminal report",
    () =>
      withAutoEnrollment(
        Effect.gen(function* () {
          const input = yield* seedContinuity("auto-on", undefined, true);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const before = (yield* orchestrator.getThreadProjection(input.threadId)).thread
            .continuity;
          assert.equal(before?.authorityMessageId, input.authorityMessageId);
          assert.equal(before?.checksRemaining, 3);
          const events = yield* EventSink.EventSinkV2;
          const started = yield* events
            .stream({ afterSequence: yield* events.latestSequence(), eventType: "run.created" })
            .pipe(
              Stream.filter((stored) => stored.event.threadId === input.threadId),
              Stream.take(1),
              Stream.runCollect,
              Effect.forkChild({ startImmediately: true }),
            );
          yield* finishRun(input.threadId, input.run);
          yield* Fiber.join(started);
          const after = yield* orchestrator.getThreadProjection(input.threadId);
          assert.lengthOf(after.runs, 2);
          assert.equal(after.thread.continuity?.checksRemaining, 2);
          assert.equal(after.thread.continuity?.generation, before?.generation);
          const continuation = after.runs.find((run) => run.id !== input.run.id)!;
          yield* orchestrator.dispatch({
            type: "thread.continuity.report",
            commandId: CommandId.make(`${input.threadId}:continuation-complete`),
            threadId: input.threadId,
            runId: continuation.id,
            status: "complete",
            reason: "The continuation verified the enrolled assignment.",
          });
          const completed = (yield* orchestrator.getThreadProjection(input.threadId)).thread
            .continuity;
          assert.equal(completed?.status, "complete");
          assert.equal(completed?.lastReportedRunId, continuation.id);
          assert.equal(completed?.authorityMessageId, input.authorityMessageId);
          assert.equal(completed?.checksRemaining, 2);
        }),
      ),
  );

  it.effect(
    "enabling the setting alone does not enroll or wake existing work, but a queued human turn does",
    () =>
      Effect.gen(function* () {
        const input = yield* seedContinuity("auto-queued", undefined, true);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        yield* withAutoEnrollment(
          Effect.gen(function* () {
            const before = yield* orchestrator.getThreadProjection(input.threadId);
            assert.isNull(before.thread.continuity ?? null);
            assert.lengthOf(before.runs, 1);
            yield* sendHumanFollowup(input.threadId, "queued");
            const after = yield* orchestrator.getThreadProjection(input.threadId);
            assert.equal(
              after.thread.continuity?.authorityMessageId,
              MessageId.make(`${input.threadId}:queued`),
            );
            assert.equal(after.thread.continuity?.checksRemaining, 3);
            assert.lengthOf(after.runs, 2);
          }),
        );
      }),
  );

  it.effect("an exhausted record is never replenished by a new human turn", () =>
    withAutoEnrollment(
      Effect.gen(function* () {
        const input = yield* seedContinuity("auto-exhausted", 1);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const projection = yield* orchestrator.getThreadProjection(input.threadId);
        const events = yield* EventSink.EventSinkV2;
        const now = yield* DateTime.now;
        const exhausted = { ...projection.thread.continuity!, checksRemaining: 0 };
        yield* events.write({
          events: [
            {
              id: EventId.make(`${input.threadId}:exhausted`),
              type: "thread.metadata-updated",
              threadId: input.threadId,
              occurredAt: now,
              payload: { ...projection.thread, continuity: exhausted },
            },
          ],
        });
        yield* sendHumanFollowup(input.threadId, "status");
        assert.deepEqual(
          (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity,
          exhausted,
        );
      }),
    ),
  );

  it.effect("a status question preserves an enrolled assignment and its remaining allowance", () =>
    withAutoEnrollment(
      Effect.gen(function* () {
        const input = yield* seedContinuity("auto-status", 1);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const before = (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity;
        yield* sendHumanFollowup(input.threadId, "status");
        assert.deepEqual(
          (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity,
          before,
        );
      }),
    ),
  );

  it.effect("a new accepted human assignment after complete receives its own three checks", () =>
    withAutoEnrollment(
      Effect.gen(function* () {
        const input = yield* seedContinuity("auto-next-assignment", 1);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        yield* orchestrator.dispatch({
          type: "thread.continuity.report",
          commandId: CommandId.make(`${input.threadId}:complete`),
          threadId: input.threadId,
          runId: input.run.id,
          status: "complete",
          reason: "First assignment verified.",
        });
        yield* finishRun(input.threadId, input.run);
        const nextCommandId = CommandId.make(`${input.threadId}:next-job`);
        const nextMessageId = MessageId.make(`${input.threadId}:next-job`);
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: nextCommandId,
          threadId: input.threadId,
          messageId: nextMessageId,
          text: "Implement and verify the next independent job.",
          createdBy: "user",
          creationSource: "mobile",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
        });
        const projection = yield* orchestrator.getThreadProjection(input.threadId);
        assert.lengthOf(projection.runs, 2);
        assert.equal(projection.thread.continuity?.generation, nextCommandId);
        assert.equal(projection.thread.continuity?.authorityMessageId, nextMessageId);
        assert.equal(projection.thread.continuity?.status, "active");
        assert.equal(projection.thread.continuity?.maxChecks, 3);
        assert.equal(projection.thread.continuity?.checksRemaining, 3);
        assert.isNull(projection.thread.continuity?.lastAdvancedRunId);
        assert.isNull(projection.thread.continuity?.lastReportedRunId);
        yield* sendHumanFollowup(input.threadId, "status");
        assert.deepEqual(
          (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity,
          projection.thread.continuity,
        );
      }),
    ),
  );

  it.effect("Stop ends supervision without a sticky opt-out for the next human job", () =>
    withAutoEnrollment(
      Effect.gen(function* () {
        const input = yield* seedContinuity("auto-after-stop");
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        yield* orchestrator.dispatch({
          type: "thread.continuity.report",
          commandId: CommandId.make(`${input.threadId}:input`),
          threadId: input.threadId,
          runId: input.run.id,
          status: "needs_input",
          reason: "Waiting for human clarification.",
        });
        yield* finishRun(input.threadId, input.run);
        yield* orchestrator.dispatch({
          type: "thread.stop",
          commandId: CommandId.make(`${input.threadId}:stop`),
          threadId: input.threadId,
        });
        const stopped = yield* orchestrator.getThreadProjection(input.threadId);
        assert.isNull(stopped.thread.continuity);
        assert.isFalse(stopped.thread.continuityAutoEnrollDisabled ?? false);
        yield* advance(input, "late-after-stop");
        assert.lengthOf((yield* orchestrator.getThreadProjection(input.threadId)).runs, 1);
        const nextMessageId = MessageId.make(`${input.threadId}:new-job`);
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`${input.threadId}:new-job`),
          threadId: input.threadId,
          messageId: nextMessageId,
          text: "Start and verify a new job.",
          createdBy: "user",
          creationSource: "web",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
        });
        const next = yield* orchestrator.getThreadProjection(input.threadId);
        assert.equal(next.thread.continuity?.authorityMessageId, nextMessageId);
        assert.equal(next.thread.continuity?.checksRemaining, 3);
      }),
    ),
  );

  it.effect(
    "cancelling the queued authority blocks supervision without spending or replacing its allowance",
    () =>
      Effect.gen(function* () {
        const input = yield* seedContinuity("auto-queued-cancel", undefined, true);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        yield* withAutoEnrollment(
          Effect.gen(function* () {
            yield* TestClock.adjust("1 millis");
            yield* sendHumanFollowup(input.threadId, "queued-job");
            const before = yield* orchestrator.getThreadProjection(input.threadId);
            const queued = before.runs.find(
              (run) => run.userMessageId === MessageId.make(`${input.threadId}:queued-job`),
            )!;
            const refused = yield* orchestrator
              .dispatch({
                type: "thread.continuity.report",
                commandId: CommandId.make(`${input.threadId}:old-complete`),
                threadId: input.threadId,
                runId: input.run.id,
                status: "complete",
                reason: "The predecessor is done, not the queued job.",
              })
              .pipe(Effect.flip);
            assert.instanceOf(refused, Orchestrator.OrchestratorDispatchError);
            assert.deepEqual(
              (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity,
              before.thread.continuity,
            );
            const events = yield* EventSink.EventSinkV2;
            const blockedEvent = yield* events
              .stream({
                afterSequence: yield* events.latestSequence(),
                eventType: "thread.metadata-updated",
              })
              .pipe(
                Stream.filter(
                  (stored) =>
                    stored.event.threadId === input.threadId &&
                    stored.event.type === "thread.metadata-updated" &&
                    stored.event.payload.continuity?.status === "blocked",
                ),
                Stream.take(1),
                Stream.runCollect,
                Effect.forkChild({ startImmediately: true }),
              );
            yield* orchestrator.dispatch({
              type: "queued-run.cancel",
              commandId: CommandId.make(`${input.threadId}:cancel-queue`),
              threadId: input.threadId,
              runId: queued.id,
            });
            yield* finishRun(input.threadId, { ...input.run, startedAt: input.run.requestedAt });
            yield* Fiber.join(blockedEvent);
            const blocked = yield* orchestrator.getThreadProjection(input.threadId);
            assert.equal(blocked.runs.find((run) => run.id === queued.id)?.status, "cancelled");
            assert.equal(blocked.thread.continuity?.status, "blocked");
            assert.equal(
              blocked.thread.continuity?.generation,
              before.thread.continuity?.generation,
            );
            assert.equal(blocked.thread.continuity?.authorityMessageId, queued.userMessageId);
            assert.equal(blocked.thread.continuity?.checksRemaining, 3);
            assert.equal(blocked.thread.continuity?.lastAdvancedRunId, queued.id);
            yield* sendHumanFollowup(input.threadId, "another-job");
            assert.deepEqual(
              (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity,
              blocked.thread.continuity,
            );
            assert.lengthOf((yield* orchestrator.getThreadProjection(input.threadId)).runs, 3);
          }),
        );
      }),
  );

  it.effect(
    "an accepted queued human job receives fresh supervision when its completed predecessor yields the queue",
    () =>
      withAutoEnrollment(
        Effect.gen(function* () {
          const input = yield* seedContinuity("auto-promoted-job", 1);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const original = (yield* orchestrator.getThreadProjection(input.threadId)).thread
            .continuity;
          yield* sendHumanFollowup(input.threadId, "queued-job");
          const queuedProjection = yield* orchestrator.getThreadProjection(input.threadId);
          const queued = queuedProjection.runs.find(
            (run) => run.userMessageId === MessageId.make(`${input.threadId}:queued-job`),
          )!;
          assert.equal(queued.status, "queued");
          assert.deepEqual(queuedProjection.thread.continuity, original);
          yield* orchestrator.dispatch({
            type: "thread.continuity.report",
            commandId: CommandId.make(`${input.threadId}:first-complete`),
            threadId: input.threadId,
            runId: input.run.id,
            status: "complete",
            reason: "First job verified; queued job still remains.",
          });
          const events = yield* EventSink.EventSinkV2;
          const promoted = yield* events
            .stream({
              afterSequence: yield* events.latestSequence(),
              eventType: "thread.metadata-updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.threadId === input.threadId &&
                  stored.event.type === "thread.metadata-updated" &&
                  stored.event.payload.continuity?.authorityMessageId === queued.userMessageId,
              ),
              Stream.take(1),
              Stream.runCollect,
              Effect.forkChild({ startImmediately: true }),
            );
          yield* finishRun(input.threadId, input.run);
          yield* Fiber.join(promoted);
          const after = yield* orchestrator.getThreadProjection(input.threadId);
          assert.equal(after.runs.find((run) => run.id === queued.id)?.status, "starting");
          assert.equal(after.thread.continuity?.authorityMessageId, queued.userMessageId);
          assert.equal(
            after.thread.continuity?.generation,
            CommandId.make(`command:system:start-queued:${queued.id}`),
          );
          assert.equal(after.thread.continuity?.status, "active");
          assert.equal(after.thread.continuity?.maxChecks, 3);
          assert.equal(after.thread.continuity?.checksRemaining, 3);
          yield* orchestrator.dispatch({
            type: "queue.resume",
            commandId: CommandId.make(`${input.threadId}:resume-again`),
            threadId: input.threadId,
          });
          assert.deepEqual(
            (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity,
            after.thread.continuity,
          );
        }),
      ),
  );

  it.effect(
    "agent continuity mutations preserve a human opt-out while human enable clears it",
    () =>
      withAutoEnrollment(
        Effect.gen(function* () {
          const input = yield* seedContinuity("auto-agent-optout");
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* orchestrator.dispatch({
            type: "thread.continuity.set",
            creationSource: "web",
            commandId: CommandId.make(`${input.threadId}:human-disable`),
            threadId: input.threadId,
            enabled: false,
          });
          const refused = yield* orchestrator
            .dispatch({
              type: "thread.continuity.set",
              creationSource: "mcp",
              commandId: CommandId.make(`${input.threadId}:agent-enable`),
              threadId: input.threadId,
              enabled: true,
              authorityMessageId: input.authorityMessageId,
            })
            .pipe(Effect.flip);
          assert.instanceOf(refused, Orchestrator.OrchestratorDispatchError);
          yield* orchestrator.dispatch({
            type: "thread.continuity.set",
            creationSource: "mcp",
            commandId: CommandId.make(`${input.threadId}:agent-disable`),
            threadId: input.threadId,
            enabled: false,
          });
          assert.isTrue(
            (yield* orchestrator.getThreadProjection(input.threadId)).thread
              .continuityAutoEnrollDisabled,
          );
          yield* orchestrator.dispatch({
            type: "thread.continuity.set",
            creationSource: "mobile",
            commandId: CommandId.make(`${input.threadId}:human-enable`),
            threadId: input.threadId,
            enabled: true,
            authorityMessageId: input.authorityMessageId,
          });
          assert.isFalse(
            (yield* orchestrator.getThreadProjection(input.threadId)).thread
              .continuityAutoEnrollDisabled,
          );
          yield* orchestrator.dispatch({
            type: "thread.continuity.set",
            creationSource: "mcp",
            commandId: CommandId.make(`${input.threadId}:agent-disable-unopted`),
            threadId: input.threadId,
            enabled: false,
          });
          const disabled = yield* orchestrator.getThreadProjection(input.threadId);
          assert.isNull(disabled.thread.continuity);
          assert.isFalse(disabled.thread.continuityAutoEnrollDisabled);
        }),
      ),
  );

  it.effect.each(["paused", "blocked"] as const)(
    "automatic enrollment preserves an existing %s record",
    (disposition) =>
      withAutoEnrollment(
        Effect.gen(function* () {
          const input = yield* seedContinuity(`auto-closed-${disposition}`);
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* orchestrator.dispatch({
            type: "thread.continuity.report",
            commandId: CommandId.make(`${input.threadId}:report`),
            threadId: input.threadId,
            runId: input.run.id,
            status: disposition,
            reason: "Keep the existing disposition.",
          });
          const before = (yield* orchestrator.getThreadProjection(input.threadId)).thread
            .continuity;
          yield* sendHumanFollowup(input.threadId, "status");
          assert.deepEqual(
            (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity,
            before,
          );
        }),
      ),
  );

  it.effect.each(["server", "mcp", "provider"] as const)(
    "human-shaped %s dispatch does not automatically enroll",
    (source) =>
      Effect.gen(function* () {
        const input = yield* seedContinuity(`auto-source-${source}`, undefined, true);
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        yield* finishRun(input.threadId, input.run);
        yield* withAutoEnrollment(
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`${input.threadId}:synthetic`),
            threadId: input.threadId,
            messageId: MessageId.make(`${input.threadId}:synthetic`),
            text: "Synthetic user-shaped message",
            createdBy: "user",
            creationSource: source,
            attachments: [],
            dispatchMode: { type: "start_immediately" },
          }),
        );
        assert.isNull(
          (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity ?? null,
        );
      }),
  );
  it.effect.each([
    "plan",
    "child",
    "archive",
    "snooze",
    "paused-goal",
    "scheduled",
    "sender",
  ] as const)("%s prevents automatic continuity enrollment", (exclusion) =>
    Effect.gen(function* () {
      const input = yield* seedContinuity(`auto-excluded-${exclusion}`, undefined, true);
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* finishRun(input.threadId, input.run);
      const projection = yield* orchestrator.getThreadProjection(input.threadId);
      const events = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      if (exclusion === "paused-goal") {
        const providerThread = projection.providerThreads[0]!;
        yield* events.write({
          events: [
            {
              id: EventId.make(`${input.threadId}:guard`),
              type: "provider-thread.updated",
              threadId: input.threadId,
              occurredAt: now,
              payload: {
                ...providerThread,
                goal: { objective: "Existing native goal", status: "paused" },
              },
            },
          ],
        });
      } else {
        yield* events.write({
          events: [
            {
              id: EventId.make(`${input.threadId}:guard`),
              type: "thread.metadata-updated",
              threadId: input.threadId,
              occurredAt: now,
              payload: {
                ...projection.thread,
                ...(exclusion === "plan" ? { interactionMode: "plan" as const } : {}),
                ...(exclusion === "child"
                  ? {
                      lineage: {
                        ...projection.thread.lineage,
                        parentThreadId: input.threadId,
                        relationshipToParent: "subagent" as const,
                      },
                    }
                  : {}),
                ...(exclusion === "archive" ? { archivedAt: now } : {}),
                ...(exclusion === "snooze"
                  ? { snoozedUntil: DateTime.add(now, { hours: 1 }) }
                  : {}),
              },
            },
          ],
        });
      }
      yield* withAutoEnrollment(
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`${input.threadId}:followup`),
          threadId: input.threadId,
          messageId: MessageId.make(`${input.threadId}:followup`),
          text: "Human-shaped follow-up",
          createdBy: "user",
          creationSource: "web",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          ...(exclusion === "scheduled"
            ? { scheduledTaskId: ScheduledTaskId.make("auto-scheduled-fixture") }
            : {}),
          ...(exclusion === "sender"
            ? { senderThreadId: ThreadId.make("auto-sender-fixture") }
            : {}),
        }),
      );
      assert.isNull(
        (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity ?? null,
      );
    }),
  );

  it.effect(
    "explicit disable remains sticky across human messages and explicit enable clears it",
    () =>
      withAutoEnrollment(
        Effect.gen(function* () {
          const input = yield* seedContinuity("auto-sticky");
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          yield* orchestrator.dispatch({
            type: "thread.continuity.set",
            creationSource: "web",
            commandId: CommandId.make(`${input.threadId}:disable`),
            threadId: input.threadId,
            enabled: false,
          });
          yield* sendHumanFollowup(input.threadId, "status");
          const disabled = yield* orchestrator.getThreadProjection(input.threadId);
          assert.isNull(disabled.thread.continuity);
          assert.isTrue(disabled.thread.continuityAutoEnrollDisabled);
          assert.isTrue(
            (yield* orchestrator.getThreadShell(input.threadId))?.continuityAutoEnrollDisabled,
          );
          const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
          assert.isTrue((yield* maintenance.rebuild).valid);
          const replayed = yield* orchestrator.getThreadProjection(input.threadId);
          assert.isTrue(replayed.thread.continuityAutoEnrollDisabled);
          assert.isNull(replayed.thread.continuity);
          yield* sendHumanFollowup(input.threadId, "after-rebuild");
          assert.isNull(
            (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity,
          );
          yield* orchestrator.dispatch({
            type: "thread.continuity.set",
            creationSource: "web",
            commandId: CommandId.make(`${input.threadId}:enable`),
            threadId: input.threadId,
            enabled: true,
            authorityMessageId: input.authorityMessageId,
          });
          assert.isFalse(
            (yield* orchestrator.getThreadProjection(input.threadId)).thread
              .continuityAutoEnrollDisabled,
          );
        }),
      ),
  );

  it.effect(
    "persists one bounded continuation for an unreported completion despite duplicate delivery",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const input = yield* seedContinuity("dedupe");
        yield* finishRun(input.threadId, input.run);
        yield* advance(input, "first");
        yield* advance(input, "duplicate");
        const projection = yield* orchestrator.getThreadProjection(input.threadId);
        assert.lengthOf(projection.runs, 2);
        assert.lengthOf(projection.messages, 2);
        assert.equal(projection.thread.continuity?.maxChecks, 3);
        assert.equal(projection.thread.continuity?.checksRemaining, 2);
        assert.equal(projection.thread.continuity?.lastAdvancedRunId, input.run.id);
        const sql = yield* SqlClient.SqlClient;
        const effects = yield* sql<{
          count: number;
        }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox WHERE thread_id = ${input.threadId} AND effect_type = ${"provider-turn.start"}`;
        assert.equal(effects[0]?.count, 2);
      }),
  );

  it.effect("a committed terminal event starts recovery without an external nudge", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const events = yield* EventSink.EventSinkV2;
      const input = yield* seedContinuity("terminal-hook");
      const started = yield* events
        .stream({ afterSequence: yield* events.latestSequence(), eventType: "run.created" })
        .pipe(
          Stream.filter((stored) => stored.event.threadId === input.threadId),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );
      yield* finishRun(input.threadId, input.run);
      const committed = yield* Fiber.join(started);
      assert.lengthOf(committed, 1);
      const projection = yield* orchestrator.getThreadProjection(input.threadId);
      assert.lengthOf(projection.runs, 2);
      assert.equal(projection.thread.continuity?.checksRemaining, 2);
    }),
  );

  it.effect("requires authority from a user request in the enrolled thread", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const source = yield* seedContinuity("authority-source");
      const target = yield* seedContinuity("authority-target", 20);
      const before = yield* orchestrator.getThreadProjection(target.threadId);
      const refused = yield* orchestrator
        .dispatch({
          type: "thread.continuity.set",
          creationSource: "web",
          commandId: CommandId.make("continuity:foreign-authority"),
          threadId: target.threadId,
          enabled: true,
          authorityMessageId: source.authorityMessageId,
        })
        .pipe(Effect.flip);
      assert.instanceOf(refused, Orchestrator.OrchestratorDispatchError);
      const after = yield* orchestrator.getThreadProjection(target.threadId);
      assert.deepEqual(after.thread.continuity, before.thread.continuity);
      assert.equal(after.thread.continuity?.maxChecks, 20);
      assert.lengthOf(after.runs, 1);
      const agentMessageId = MessageId.make(`${target.threadId}:agent-notification`);
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`${target.threadId}:agent-notification`),
        threadId: target.threadId,
        messageId: agentMessageId,
        text: "A child event is evidence, not new authority.",
        createdBy: "agent",
        creationSource: "server",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
      });
      const agentAuthority = yield* orchestrator
        .dispatch({
          type: "thread.continuity.set",
          creationSource: "web",
          commandId: CommandId.make(`${target.threadId}:agent-authority`),
          threadId: target.threadId,
          enabled: true,
          authorityMessageId: agentMessageId,
        })
        .pipe(Effect.flip);
      assert.instanceOf(agentAuthority, Orchestrator.OrchestratorDispatchError);
      assert.deepEqual(
        (yield* orchestrator.getThreadProjection(target.threadId)).thread.continuity,
        before.thread.continuity,
      );
    }),
  );

  it.effect("a completion report suppresses recovery and cannot be overwritten by an old run", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const input = yield* seedContinuity("complete-report");
      yield* orchestrator.dispatch({
        type: "thread.continuity.report",
        commandId: CommandId.make(`${input.threadId}:report`),
        threadId: input.threadId,
        runId: input.run.id,
        status: "complete",
        reason: "The requested verification passed.",
      });
      yield* finishRun(input.threadId, input.run);
      yield* advance(input, "reported");
      assert.lengthOf((yield* orchestrator.getThreadProjection(input.threadId)).runs, 1);
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`${input.threadId}:new-request`),
        threadId: input.threadId,
        messageId: MessageId.make(`${input.threadId}:new-request`),
        text: "Perform the next authorized unit.",
        createdBy: "user",
        creationSource: "web",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
      });
      const refused = yield* orchestrator
        .dispatch({
          type: "thread.continuity.report",
          commandId: CommandId.make(`${input.threadId}:stale-report`),
          threadId: input.threadId,
          runId: input.run.id,
          status: "continue",
          reason: "A late result from the older run.",
        })
        .pipe(Effect.flip);
      assert.instanceOf(refused, Orchestrator.OrchestratorDispatchError);
      assert.equal(
        (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity?.status,
        "complete",
      );
    }),
  );

  it.effect("Stop clears enrollment before a late terminal recovery can start work", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const input = yield* seedContinuity("stop");
      yield* orchestrator.dispatch({
        type: "thread.continuity.report",
        commandId: CommandId.make(`${input.threadId}:input-report`),
        threadId: input.threadId,
        runId: input.run.id,
        status: "needs_input",
        reason: "Await the requested clarification.",
      });
      yield* finishRun(input.threadId, input.run);
      const stopped = yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make(`${input.threadId}:stop`),
        threadId: input.threadId,
      });
      assert.isTrue(
        stopped.storedEvents.some((stored) => stored.event.type === "thread.metadata-updated"),
      );
      assert.isNull((yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity);
      yield* advance(input, "late-after-stop");
      const projection = yield* orchestrator.getThreadProjection(input.threadId);
      assert.lengthOf(projection.runs, 1);
      assert.isNull(projection.thread.continuity);
    }),
  );

  it.effect("Stop cancels an accepted continuation before its provider effect can execute", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const input = yield* seedContinuity("stop-pending-continuation");
      yield* finishRun(input.threadId, input.run);
      yield* advance(input, "accepted");
      const continuation = (yield* orchestrator.getThreadProjection(input.threadId)).runs[1]!;
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make(`${input.threadId}:stop`),
        threadId: input.threadId,
      });
      yield* advance({ ...input, run: continuation }, "late-after-stop");
      const projection = yield* orchestrator.getThreadProjection(input.threadId);
      assert.lengthOf(projection.runs, 2);
      assert.isNull(projection.thread.continuity);
      assert.equal(
        projection.runs.find((run) => run.id === continuation.id)?.status,
        "interrupted",
      );
      const sql = yield* SqlClient.SqlClient;
      const effects = yield* sql<{
        status: string;
      }>`SELECT status FROM orchestration_v2_effect_outbox
          WHERE thread_id = ${input.threadId} AND effect_type = ${"provider-turn.start"}
            AND json_extract(payload_json, '$.runId') = ${continuation.id}`;
      assert.deepEqual(
        effects.map((effect) => effect.status),
        ["cancelled"],
      );
    }),
  );

  it.effect("supervision cannot resume a paused provider-native goal", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const events = yield* EventSink.EventSinkV2;
      const input = yield* seedContinuity("paused-native-goal");
      const providerThread = (yield* orchestrator.getThreadProjection(input.threadId))
        .providerThreads[0]!;
      yield* events.write({
        events: [
          {
            id: EventId.make(`${input.threadId}:paused-goal`),
            type: "provider-thread.updated",
            threadId: input.threadId,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: yield* DateTime.now,
            payload: {
              ...providerThread,
              goal: { objective: "Finish the explicit native objective.", status: "paused" },
            },
          },
        ],
      });
      yield* finishRun(input.threadId, input.run);
      yield* advance(input, "native-paused");
      const projection = yield* orchestrator.getThreadProjection(input.threadId);
      assert.lengthOf(projection.runs, 1);
      assert.equal(projection.thread.continuity?.checksRemaining, 3);
      assert.equal(projection.providerThreads[0]?.goal?.status, "paused");
    }),
  );

  it.effect("queued user work takes precedence over an automatic continuation", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const input = yield* seedContinuity("user-queue");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`${input.threadId}:queue`),
        threadId: input.threadId,
        messageId: MessageId.make(`${input.threadId}:queue`),
        text: "Use this updated requirement first.",
        createdBy: "user",
        creationSource: "web",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
      });
      yield* finishRun(input.threadId, input.run);
      yield* advance(input, "queued-user");
      const projection = yield* orchestrator.getThreadProjection(input.threadId);
      assert.lengthOf(projection.runs, 2);
      assert.lengthOf(projection.messages, 2);
      assert.equal(projection.thread.continuity?.checksRemaining, 3);
    }),
  );

  it.effect("a live pull request watch prevents synthetic continuation while waiting", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const input = yield* seedContinuity("live-wait");
      const key = { host: "github.com", repository: "pingdotgg/t3code", number: 77 };
      yield* orchestrator.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make(`${input.threadId}:watch`),
        threadId: input.threadId,
        ...key,
        watching: true,
        link: { url: "https://github.com/pingdotgg/t3code/pull/77", source: "agent" },
      });
      yield* orchestrator.dispatch({
        type: "thread.continuity.report",
        commandId: CommandId.make(`${input.threadId}:report`),
        threadId: input.threadId,
        runId: input.run.id,
        status: "waiting",
        reason: "The linked pull request checks must finish.",
        wait: { kind: "pull_request", ...key },
      });
      yield* finishRun(input.threadId, input.run);
      yield* advance(input, "waiting");
      const projection = yield* orchestrator.getThreadProjection(input.threadId);
      assert.lengthOf(projection.runs, 1);
      assert.equal(projection.thread.continuity?.status, "waiting");
      assert.equal(projection.thread.continuity?.checksRemaining, 3);
      assert.deepEqual(projection.thread.continuity?.wait, { kind: "pull_request", ...key });
    }),
  );

  it.effect("a removed watch cannot leave the thread claiming a live wait", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const input = yield* seedContinuity("lost-wait");
      const key = { host: "github.com", repository: "pingdotgg/t3code", number: 78 };
      yield* orchestrator.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make(`${input.threadId}:watch`),
        threadId: input.threadId,
        ...key,
        watching: true,
        link: { url: "https://github.com/pingdotgg/t3code/pull/78", source: "agent" },
      });
      yield* orchestrator.dispatch({
        type: "thread.continuity.report",
        commandId: CommandId.make(`${input.threadId}:report`),
        threadId: input.threadId,
        runId: input.run.id,
        status: "waiting",
        reason: "Waiting for linked checks.",
        wait: { kind: "pull_request", ...key },
      });
      yield* orchestrator.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make(`${input.threadId}:unwatch`),
        threadId: input.threadId,
        ...key,
        watching: false,
      });
      yield* finishRun(input.threadId, input.run);
      yield* advance(input, "lost-wait");
      const projection = yield* orchestrator.getThreadProjection(input.threadId);
      assert.lengthOf(projection.runs, 1);
      assert.equal(projection.thread.continuity?.status, "blocked");
      assert.isNull(projection.thread.continuity?.wait);
      assert.equal(projection.thread.continuity?.checksRemaining, 3);
    }),
  );

  it.effect("ending with a pending question defers recovery until its resolution event", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const events = yield* EventSink.EventSinkV2;
      const input = yield* seedContinuity("question-wake");
      const rootNodeId = input.run.rootNodeId;
      if (rootNodeId === null)
        return yield* Effect.die("The question fixture requires an allocated root node.");
      const request = {
        id: RuntimeRequestId.make(`${input.threadId}:question`),
        nodeId: rootNodeId,
        providerTurnId: null,
        nativeRequestRef: null,
        kind: "user_input" as const,
        status: "pending" as const,
        responseCapability: { type: "message" as const },
        createdAt: yield* DateTime.now,
        resolvedAt: null,
      };
      yield* events.write({
        events: [
          {
            id: EventId.make(`${input.threadId}:question-pending`),
            type: "runtime-request.updated",
            threadId: input.threadId,
            nodeId: rootNodeId,
            occurredAt: yield* DateTime.now,
            payload: request,
          },
        ],
      });
      yield* finishRun(input.threadId, input.run);
      yield* advance(input, "pending");
      assert.lengthOf((yield* orchestrator.getThreadProjection(input.threadId)).runs, 1);
      const resumed = yield* events
        .stream({ afterSequence: yield* events.latestSequence(), eventType: "run.created" })
        .pipe(
          Stream.filter((stored) => stored.event.threadId === input.threadId),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );
      yield* events.write({
        events: [
          {
            id: EventId.make(`${input.threadId}:question-resolved`),
            type: "runtime-request.updated",
            threadId: input.threadId,
            nodeId: rootNodeId,
            occurredAt: yield* DateTime.now,
            payload: { ...request, status: "resolved", resolvedAt: yield* DateTime.now },
          },
        ],
      });
      yield* Fiber.join(resumed);
      const projection = yield* orchestrator.getThreadProjection(input.threadId);
      assert.lengthOf(projection.runs, 2);
      assert.equal(projection.thread.continuity?.checksRemaining, 2);
    }),
  );

  it.effect(
    "unwatching after the waiting run completes surfaces a lost wake without another nudge",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const events = yield* EventSink.EventSinkV2;
        const input = yield* seedContinuity("lost-wait-after-end");
        const key = { host: "github.com", repository: "pingdotgg/t3code", number: 79 };
        yield* orchestrator.dispatch({
          type: "thread.pull-request.watch",
          commandId: CommandId.make(`${input.threadId}:watch`),
          threadId: input.threadId,
          ...key,
          watching: true,
          link: { url: "https://github.com/pingdotgg/t3code/pull/79", source: "agent" },
        });
        yield* orchestrator.dispatch({
          type: "thread.continuity.report",
          commandId: CommandId.make(`${input.threadId}:report`),
          threadId: input.threadId,
          runId: input.run.id,
          status: "waiting",
          reason: "Waiting for linked checks.",
          wait: { kind: "pull_request", ...key },
        });
        yield* finishRun(input.threadId, input.run);
        yield* advance(input, "initial-live-wait");
        const blocked = yield* events
          .stream({
            afterSequence: yield* events.latestSequence(),
            eventType: "thread.metadata-updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.threadId === input.threadId &&
                stored.event.type === "thread.metadata-updated" &&
                stored.event.payload.continuity?.status === "blocked",
            ),
            Stream.take(1),
            Stream.runCollect,
            Effect.forkChild({ startImmediately: true }),
          );
        yield* orchestrator.dispatch({
          type: "thread.pull-request.watch",
          commandId: CommandId.make(`${input.threadId}:unwatch`),
          threadId: input.threadId,
          ...key,
          watching: false,
        });
        yield* Fiber.join(blocked);
        const projection = yield* orchestrator.getThreadProjection(input.threadId);
        assert.lengthOf(projection.runs, 1);
        assert.equal(projection.thread.continuity?.status, "blocked");
        assert.isNull(projection.thread.continuity?.wait);
        assert.equal(projection.thread.continuity?.checksRemaining, 3);
      }),
  );

  it.effect(
    "a second restart cutting an unstarted continuation preserves supervision until its descendant ends",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const threads = yield* ThreadManagementService.ThreadManagementService;
        const events = yield* EventSink.EventSinkV2;
        const sql = yield* SqlClient.SqlClient;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const input = yield* seedContinuity("restart-recut-root");
        const workerId = `${input.threadId}:restart-worker`;
        const cancelForRestart = (run: OrchestrationV2Run, started: boolean) =>
          Effect.gen(function* () {
            const completedAt = DateTime.formatIso(yield* DateTime.now);
            // Startup restores these persisted records before delivering live events.
            yield* sql`UPDATE orchestration_v2_projection_runs
                SET status = ${"cancelled"}, completed_at = ${completedAt},
                  payload_json = json_set(payload_json, '$.status', ${"cancelled"},
                    '$.startedAt', ${started ? completedAt : null}, '$.completedAt', ${completedAt})
                WHERE run_id = ${run.id} AND thread_id = ${input.threadId}`;
            yield* outbox.enqueue([
              {
                id: `effect:restart-continuation:${run.id}`,
                commandId: CommandId.make(`${run.id}:restart`),
                threadId: input.threadId,
                request: { type: "provider-runtime.continue", sourceRunId: run.id },
              },
            ]);
          });
        const resumeAndSettle = (run: OrchestrationV2Run) =>
          Effect.gen(function* () {
            const effectId = `effect:restart-continuation:${run.id}`;
            yield* sql`UPDATE orchestration_v2_effect_outbox SET status = ${"running"}, lease_owner = ${workerId} WHERE effect_id = ${effectId}`;
            yield* continueRestartedRun({ threadId: input.threadId, sourceRunId: run.id });
            assert.isTrue(yield* outbox.succeed({ effectId, workerId }));
            yield* threads.recoverDelegatedTask(input.threadId, run.id);
          });

        yield* cancelForRestart(input.run, true);
        yield* orchestrator.recoverDelegatedTasks;
        yield* TestClock.adjust("1 millis");
        yield* resumeAndSettle(input.run);
        const first = (yield* orchestrator.getThreadProjection(input.threadId)).runs.find(
          (run) => run.restartContinuationOfRunId === input.run.id,
        );
        assert.isDefined(first);
        if (first === undefined)
          return yield* Effect.die("Restart did not create its continuation.");
        assert.equal(first.status, "starting");
        assert.isNull(first.startedAt);
        assert.equal(
          Option.getOrThrow(yield* outbox.get(`effect:restart-continuation:${input.run.id}`))
            .status,
          "succeeded",
        );

        yield* TestClock.adjust("1 millis");
        yield* cancelForRestart(first, false);
        yield* orchestrator.recoverDelegatedTasks;
        // A replayed first effect must also keep the second continuation's hold.
        yield* threads.recoverDelegatedTask(input.threadId, input.run.id);
        const held = yield* orchestrator.getThreadProjection(input.threadId);
        assert.equal(held.thread.continuity?.status, "active");
        assert.equal(held.thread.continuity?.checksRemaining, 3);
        assert.isNull(held.thread.continuity?.lastAdvancedRunId);
        assert.equal(held.thread.continuity?.generation, input.generation);
        assert.lengthOf(held.runs, 2);
        assert.equal(
          Option.getOrThrow(yield* outbox.get(`effect:restart-continuation:${first.id}`)).status,
          "pending",
        );

        yield* TestClock.adjust("1 millis");
        yield* resumeAndSettle(first);
        const resumed = yield* orchestrator.getThreadProjection(input.threadId);
        const descendant = resumed.runs.find((run) => run.restartContinuationOfRunId === first.id);
        assert.isDefined(descendant);
        if (descendant === undefined) return yield* Effect.die("Second restart did not resume.");
        assert.equal(resumed.thread.continuity?.status, "active");
        assert.equal(resumed.thread.continuity?.checksRemaining, 3);
        const checked = yield* events
          .stream({ afterSequence: yield* events.latestSequence(), eventType: "run.created" })
          .pipe(
            Stream.filter((stored) => stored.event.threadId === input.threadId),
            Stream.take(1),
            Stream.runCollect,
            Effect.forkChild({ startImmediately: true }),
          );
        yield* finishRun(input.threadId, descendant);
        yield* Fiber.join(checked);
        const after = yield* orchestrator.getThreadProjection(input.threadId);
        assert.equal(after.thread.continuity?.status, "active");
        assert.equal(after.thread.continuity?.checksRemaining, 2);
        assert.equal(after.thread.continuity?.lastAdvancedRunId, descendant.id);
        assert.equal(after.thread.continuity?.generation, input.generation);
        assert.lengthOf(after.runs, 4);
      }),
  );

  it.effect.each([false, true] as const)(
    "startup cancellation respects pending restart ownership (%s)",
    (pendingRestart) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sql = yield* SqlClient.SqlClient;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const input = yield* seedContinuity(`cancelled-startup-${pendingRestart}`);
        const completedAt = DateTime.formatIso(yield* DateTime.now);
        yield* sql`UPDATE orchestration_v2_projection_runs
          SET status = ${"cancelled"}, completed_at = ${completedAt},
            payload_json = json_set(payload_json, '$.status', ${"cancelled"}, '$.startedAt', ${completedAt}, '$.completedAt', ${completedAt})
          WHERE run_id = ${input.run.id} AND thread_id = ${input.threadId}`;
        if (pendingRestart)
          yield* outbox.enqueue([
            {
              id: `effect:restart-continuation:${input.run.id}`,
              commandId: CommandId.make(`${input.threadId}:restart`),
              threadId: input.threadId,
              request: { type: "provider-runtime.continue", sourceRunId: input.run.id },
            },
          ]);
        yield* orchestrator.recoverDelegatedTasks;
        const projection = yield* orchestrator.getThreadProjection(input.threadId);
        assert.lengthOf(projection.runs, 1);
        assert.equal(projection.thread.continuity?.status, pendingRestart ? "active" : "blocked");
        assert.equal(projection.thread.continuity?.checksRemaining, 3);
        assert.equal(
          projection.thread.continuity?.lastAdvancedRunId,
          pendingRestart ? null : input.run.id,
        );
      }),
  );

  it.effect("startup surfaces a supervised assignment cancelled before it ever started", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sql = yield* SqlClient.SqlClient;
      const input = yield* seedContinuity("cancelled-before-start");
      const completedAt = DateTime.formatIso(yield* DateTime.now);
      yield* sql`UPDATE orchestration_v2_projection_runs
          SET status = ${"cancelled"}, completed_at = ${completedAt},
            payload_json = json_set(payload_json, '$.status', ${"cancelled"}, '$.startedAt', NULL, '$.completedAt', ${completedAt})
          WHERE run_id = ${input.run.id} AND thread_id = ${input.threadId}`;
      yield* orchestrator.recoverDelegatedTasks;
      const projection = yield* orchestrator.getThreadProjection(input.threadId);
      assert.lengthOf(projection.runs, 1);
      assert.equal(projection.thread.continuity?.status, "blocked");
      assert.equal(projection.thread.continuity?.checksRemaining, 3);
      assert.equal(projection.thread.continuity?.lastAdvancedRunId, input.run.id);
    }),
  );

  it.effect(
    "an unrelated cancelled queue item cannot replace the supervised executed context",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sql = yield* SqlClient.SqlClient;
        const input = yield* seedContinuity("unrelated-cancelled-queue");
        const key = { host: "github.com", repository: "pingdotgg/t3code", number: 80 };
        yield* orchestrator.dispatch({
          type: "thread.pull-request.watch",
          commandId: CommandId.make(`${input.threadId}:watch`),
          threadId: input.threadId,
          ...key,
          watching: true,
          link: { url: "https://github.com/pingdotgg/t3code/pull/80", source: "agent" },
        });
        yield* orchestrator.dispatch({
          type: "thread.continuity.report",
          commandId: CommandId.make(`${input.threadId}:report`),
          threadId: input.threadId,
          runId: input.run.id,
          status: "waiting",
          reason: "The executed request has a verified live dependency.",
          wait: { kind: "pull_request", ...key },
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`${input.threadId}:unrelated`),
          threadId: input.threadId,
          messageId: MessageId.make(`${input.threadId}:unrelated`),
          text: "A separate queued request that is subsequently cancelled.",
          createdBy: "user",
          creationSource: "web",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
        });
        const queued = (yield* orchestrator.getThreadProjection(input.threadId)).runs[1]!;
        const completedAt = DateTime.formatIso(yield* DateTime.now);
        yield* sql`UPDATE orchestration_v2_projection_runs
          SET status = ${"cancelled"}, completed_at = ${completedAt},
            payload_json = json_set(payload_json, '$.status', ${"cancelled"}, '$.startedAt', NULL, '$.completedAt', ${completedAt})
          WHERE run_id = ${queued.id} AND thread_id = ${input.threadId}`;
        yield* finishRun(input.threadId, input.run);
        yield* advance(input, "original-context");
        yield* orchestrator.recoverDelegatedTasks;
        const projection = yield* orchestrator.getThreadProjection(input.threadId);
        assert.lengthOf(projection.runs, 2);
        assert.equal(projection.thread.continuity?.status, "waiting");
        assert.equal(projection.thread.continuity?.lastReportedRunId, input.run.id);
        assert.equal(projection.thread.continuity?.checksRemaining, 3);
        assert.deepEqual(projection.thread.continuity?.wait, { kind: "pull_request", ...key });
      }),
  );

  it.effect("a live run cannot reopen its completed disposition with a continue report", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const input = yield* seedContinuity("complete-is-final");
      yield* orchestrator.dispatch({
        type: "thread.continuity.report",
        commandId: CommandId.make(`${input.threadId}:complete`),
        threadId: input.threadId,
        runId: input.run.id,
        status: "complete",
        reason: "The authorized work and its verification finished.",
      });
      const before = (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity;
      const refused = yield* orchestrator
        .dispatch({
          type: "thread.continuity.report",
          commandId: CommandId.make(`${input.threadId}:reopen`),
          threadId: input.threadId,
          runId: input.run.id,
          status: "continue",
          reason: "A late attempt to reopen the same run.",
        })
        .pipe(Effect.flip);
      assert.instanceOf(refused, Orchestrator.OrchestratorDispatchError);
      const projection = yield* orchestrator.getThreadProjection(input.threadId);
      assert.deepEqual(projection.thread.continuity, before);
      assert.equal(projection.thread.continuity?.generation, input.generation);
      assert.equal(projection.thread.continuity?.checksRemaining, 3);
      assert.lengthOf(projection.runs, 1);
    }),
  );

  it.effect(
    "a refused provider start commits only a blocked disposition without spending a check",
    () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const input = yield* seedContinuity("refused-start");
        const projects = yield* ProjectStore.ProjectStoreV2;
        const getProject = projects.get;
        const projectLookup = vi
          .spyOn(projects, "get")
          .mockImplementation((projectId, options) =>
            projectId === ProjectId.make(`${input.threadId}:project`)
              ? Effect.succeed(Option.none())
              : getProject(projectId, options),
          );
        yield* Effect.gen(function* () {
          yield* finishRun(input.threadId, input.run);
          yield* advance(input, "refused");
          const projection = yield* orchestrator.getThreadProjection(input.threadId);
          assert.lengthOf(projection.runs, 1);
          assert.lengthOf(projection.messages, 1);
          assert.equal(projection.thread.continuity?.status, "blocked");
          assert.equal(projection.thread.continuity?.checksRemaining, 3);
          const sql = yield* SqlClient.SqlClient;
          const effects = yield* sql<{
            count: number;
          }>`SELECT COUNT(*) AS count FROM orchestration_v2_effect_outbox WHERE thread_id = ${input.threadId} AND effect_type = ${"provider-turn.start"}`;
          assert.equal(effects[0]?.count, 1);
        }).pipe(Effect.ensuring(Effect.sync(() => projectLookup.mockRestore())));
      }),
  );

  it.effect.each([false, true] as const)(
    "a first live delegated child retains the parent's wait before a cohort exists (%s)",
    (reported) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const input = yield* seedContinuity(`first-child-${reported}`);
        const task = yield* seedLiveChild(input);
        const parent = yield* orchestrator.getThreadProjection(input.threadId);
        assert.isUndefined(parent.runs[0]?.delegatedCompletion);
        if (reported)
          yield* orchestrator.dispatch({
            type: "thread.continuity.report",
            commandId: CommandId.make(`${input.threadId}:report`),
            threadId: input.threadId,
            runId: input.run.id,
            status: "waiting",
            reason: "The delegated owner is running.",
            wait: { kind: "delegated_task", taskId: task.id },
          });
        yield* finishRun(input.threadId, input.run);
        yield* advance(input, "first-child-live");
        const projection = yield* orchestrator.getThreadProjection(input.threadId);
        assert.lengthOf(projection.runs, 1);
        assert.equal(projection.thread.continuity?.status, "waiting");
        assert.equal(projection.thread.continuity?.checksRemaining, 3);
        assert.deepEqual(projection.thread.continuity?.wait, {
          kind: "delegated_task",
          taskId: task.id,
        });
      }),
  );

  it.effect.each(["pending", "claimed"] as const)(
    "a terminal child's %s delivery retains waiting during pre-wake revalidation",
    (deliveryState) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const events = yield* EventSink.EventSinkV2;
        const input = yield* seedContinuity(`child-delivery-${deliveryState}`);
        const task = yield* seedLiveChild(input);
        yield* orchestrator.dispatch({
          type: "thread.continuity.report",
          commandId: CommandId.make(`${input.threadId}:report`),
          threadId: input.threadId,
          runId: input.run.id,
          status: "waiting",
          reason: "Waiting for the owned task's completion delivery.",
          wait: { kind: "delegated_task", taskId: task.id },
        });
        yield* finishRun(input.threadId, input.run);
        yield* advance(input, "child-live");
        const ended = (yield* orchestrator.getThreadProjection(input.threadId)).runs[0]!;
        const now = yield* DateTime.now;
        const sql = yield* SqlClient.SqlClient;
        const completedAt = DateTime.formatIso(now);
        yield* sql`UPDATE orchestration_v2_projection_runs
          SET status = ${"completed"}, completed_at = ${completedAt},
            payload_json = json_set(payload_json, '$.status', ${"completed"}, '$.startedAt', ${completedAt}, '$.completedAt', ${completedAt})
          WHERE thread_id = ${task.childThreadId}`;
        yield* events.write({
          events: [
            {
              id: EventId.make(`${input.threadId}:cohort`),
              type: "run.updated",
              threadId: input.threadId,
              runId: input.run.id,
              occurredAt: now,
              payload: {
                ...ended,
                delegatedCompletion: {
                  disposition: "open",
                  nextGeneration: 2,
                  delivery: {
                    generation: 1,
                    messageId: MessageId.make(`${input.threadId}:delivery`),
                    taskIds: [task.id],
                  },
                },
              },
            },
            {
              id: EventId.make(`${input.threadId}:task-terminal`),
              type: "subagent.updated",
              threadId: input.threadId,
              runId: input.run.id,
              nodeId: task.id,
              occurredAt: now,
              payload: {
                ...task,
                status: "completed",
                result: "The child finished its bounded work.",
                completedAt: now,
                updatedAt: now,
                completionDelivery: { state: deliveryState, observedByRunId: null },
              },
            },
          ],
        });
        yield* advance(input, "before-wake");
        const projection = yield* orchestrator.getThreadProjection(input.threadId);
        assert.lengthOf(projection.runs, 1);
        assert.equal(projection.thread.continuity?.status, "waiting");
        assert.equal(projection.thread.continuity?.checksRemaining, 3);
        assert.equal(projection.subagents[0]?.completionDelivery?.state, deliveryState);
      }),
  );

  it.effect.each(["succeeded", "failed"] as const)(
    "retiring a %s restart effect revalidates the root's cancelled assignment",
    (outcome) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const threads = yield* ThreadManagementService.ThreadManagementService;
        const sql = yield* SqlClient.SqlClient;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const input = yield* seedContinuity(`retired-restart-${outcome}`);
        const completedAt = DateTime.formatIso(yield* DateTime.now);
        yield* sql`UPDATE orchestration_v2_projection_runs
          SET status = ${"cancelled"}, completed_at = ${completedAt},
            payload_json = json_set(payload_json, '$.status', ${"cancelled"}, '$.startedAt', ${completedAt}, '$.completedAt', ${completedAt})
          WHERE run_id = ${input.run.id} AND thread_id = ${input.threadId}`;
        const effectId = `effect:restart-continuation:${input.run.id}`;
        const workerId = `${input.threadId}:worker`;
        yield* outbox.enqueue([
          {
            id: effectId,
            commandId: CommandId.make(`${input.threadId}:restart`),
            threadId: input.threadId,
            request: { type: "provider-runtime.continue", sourceRunId: input.run.id },
          },
        ]);
        yield* orchestrator.recoverDelegatedTasks;
        assert.equal(
          (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity?.status,
          "active",
        );
        // Model this worker's claim; other fixtures' intentionally pending starts stay untouched.
        yield* sql`UPDATE orchestration_v2_effect_outbox SET status = ${"running"}, lease_owner = ${workerId} WHERE effect_id = ${effectId}`;
        const retired =
          outcome === "succeeded"
            ? yield* outbox.succeed({ effectId, workerId })
            : yield* outbox.fail({
                effectId,
                workerId,
                error: "The bounded restart attempts failed.",
              });
        assert.isTrue(retired);
        yield* threads.recoverDelegatedTask(input.threadId, input.run.id);
        const projection = yield* orchestrator.getThreadProjection(input.threadId);
        assert.lengthOf(projection.runs, 1);
        assert.equal(projection.thread.continuity?.status, "blocked");
        assert.equal(projection.thread.continuity?.checksRemaining, 3);
        assert.equal(Option.getOrThrow(yield* outbox.get(effectId)).status, outcome);
      }),
  );

  it.effect.each(["paused", "blocked"] as const)(
    "a %s disposition cannot be reopened through an otherwise valid waiting report",
    (closedStatus) =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const input = yield* seedContinuity(`closed-wait-${closedStatus}`);
        const task = yield* seedLiveChild(input);
        yield* orchestrator.dispatch({
          type: "thread.continuity.report",
          commandId: CommandId.make(`${input.threadId}:closed`),
          threadId: input.threadId,
          runId: input.run.id,
          status: closedStatus,
          reason: "The owner explicitly ended supervised execution.",
        });
        const before = (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity;
        const refused = yield* orchestrator
          .dispatch({
            type: "thread.continuity.report",
            commandId: CommandId.make(`${input.threadId}:reopen`),
            threadId: input.threadId,
            runId: input.run.id,
            status: "waiting",
            reason: "A live dependency does not reopen closed supervision.",
            wait: { kind: "delegated_task", taskId: task.id },
          })
          .pipe(Effect.flip);
        assert.instanceOf(refused, Orchestrator.OrchestratorDispatchError);
        assert.deepEqual(
          (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity,
          before,
        );
      }),
  );

  it.effect("snooze clears supervision and unsnooze cannot silently reenable it", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const input = yield* seedContinuity("snooze-hold");
      yield* orchestrator.dispatch({
        type: "thread.snooze",
        commandId: CommandId.make(`${input.threadId}:snooze`),
        threadId: input.threadId,
        snoozedUntil: "2099-07-25T09:00:00.000Z",
      });
      assert.isNull((yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity);
      yield* finishRun(input.threadId, input.run);
      yield* orchestrator.dispatch({
        type: "thread.unsnooze",
        commandId: CommandId.make(`${input.threadId}:unsnooze`),
        threadId: input.threadId,
        reason: "user",
      });
      yield* advance(input, "after-unsnooze");
      const projection = yield* orchestrator.getThreadProjection(input.threadId);
      assert.isNull(projection.thread.continuity);
      assert.lengthOf(projection.runs, 1);
    }),
  );

  it.effect("startup repairs a missed completed run exactly once", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const input = yield* seedContinuity("startup");
      const sql = yield* SqlClient.SqlClient;
      const completedAt = DateTime.formatIso(yield* DateTime.now);
      // Model startup's persisted terminal projection without a live event delivery.
      yield* sql`UPDATE orchestration_v2_projection_runs
        SET status = ${"completed"}, completed_at = ${completedAt},
          payload_json = json_set(payload_json, '$.status', ${"completed"}, '$.completedAt', ${completedAt})
        WHERE run_id = ${input.run.id} AND thread_id = ${input.threadId}`;
      yield* orchestrator.recoverDelegatedTasks;
      yield* orchestrator.recoverDelegatedTasks;
      const projection = yield* orchestrator.getThreadProjection(input.threadId);
      assert.lengthOf(projection.runs, 2);
      assert.equal(projection.thread.continuity?.checksRemaining, 2);
      assert.equal(projection.thread.continuity?.lastAdvancedRunId, input.run.id);
    }),
  );

  it.effect("a wait without a live wake binding is refused without suppressing recovery", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const input = yield* seedContinuity("dead-wait");
      const refused = yield* orchestrator
        .dispatch({
          type: "thread.continuity.report",
          commandId: CommandId.make(`${input.threadId}:dead-wait`),
          threadId: input.threadId,
          runId: input.run.id,
          status: "waiting",
          reason: "Waiting for a nonexistent owner.",
          wait: { kind: "delegated_task", taskId: NodeId.make("nonexistent-continuity-owner") },
        })
        .pipe(Effect.flip);
      assert.instanceOf(refused, Orchestrator.OrchestratorDispatchError);
      assert.equal(
        (yield* orchestrator.getThreadProjection(input.threadId)).thread.continuity?.status,
        "active",
      );
      yield* finishRun(input.threadId, input.run);
      yield* advance(input, "unbound");
      assert.lengthOf((yield* orchestrator.getThreadProjection(input.threadId)).runs, 2);
    }),
  );

  it.effect("exhaustion is visible and failure or cancellation cannot trigger blind retries", () =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const exhausted = yield* seedContinuity("exhaustion", 1);
      yield* finishRun(exhausted.threadId, exhausted.run);
      yield* advance(exhausted, "first");
      const continuation = (yield* orchestrator.getThreadProjection(exhausted.threadId)).runs[1]!;
      yield* finishRun(exhausted.threadId, continuation);
      yield* advance({ ...exhausted, run: continuation }, "exhausted");
      const blocked = yield* orchestrator.getThreadProjection(exhausted.threadId);
      assert.lengthOf(blocked.runs, 2);
      assert.equal(blocked.thread.continuity?.checksRemaining, 0);
      assert.equal(blocked.thread.continuity?.status, "blocked");
      assert.isNotEmpty(blocked.thread.continuity?.reason);
      for (const status of ["failed", "cancelled"] as const) {
        const input = yield* seedContinuity(status);
        yield* finishRun(input.threadId, input.run, status);
        yield* advance(input, status);
        const projection = yield* orchestrator.getThreadProjection(input.threadId);
        assert.lengthOf(projection.runs, 1);
        assert.equal(projection.thread.continuity?.checksRemaining, 3);
        assert.equal(projection.thread.continuity?.status, "blocked");
      }
    }),
  );
});
