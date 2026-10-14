import {
  EnvironmentId,
  CommandId,
  MessageId,
  type OrchestrationV2WorkContinuity,
  OrchestrationV2Run,
  OrchestrationV2CapacityRecovery,
  OrchestrationV2Subagent,
  ProviderThreadId,
  NodeId,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadShell,
  type OrchestrationV2ProviderGoal,
  OrchestratorMcpThreadListResult,
  OrchestratorMcpWorkContinuityInput,
  OrchestratorMcpThreadReadResult,
  OrchestratorMcpThreadWaitResult,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ScheduledTaskId,
  ThreadId,
} from "@t3tools/contracts";
import { McpServer } from "effect/ai";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "vite-plus/test";

import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import type * as McpInvocationContext from "./McpInvocationContext.ts";
import * as OrchestratorMcpService from "./OrchestratorMcpService.ts";
import { idleThreadProjection, liveThreadShell } from "./McpToolAccess.testkit.ts";

const layerOrchestratorService = OrchestratorMcpService.layer.pipe(
  Layer.provide(Layer.mock(ThreadLaunchService.ThreadLaunchService)({})),
  Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
);

const environmentId = EnvironmentId.make("environment-mcp-orchestrator-detail");
const projectId = ProjectId.make("project-mcp-orchestrator-detail");
const parentThreadId = ThreadId.make("thread-mcp-orchestrator-parent");
const childThreadId = ThreadId.make("thread-mcp-orchestrator-child");
const activeRunId = RunId.make("run-mcp-active");
const cancelledRunId = RunId.make("run-mcp-cancelled");
const childRunId = RunId.make("run-mcp-child");
const taskId = NodeId.make("node-mcp-task-1");
const now = DateTime.makeUnsafe("2026-08-04T12:00:00.000Z");
const codexDriver = ProviderDriverKind.make("codex");
// Distinct from driver kind so a regression that re-derives from driver fails.
const customCodexInstanceId = ProviderInstanceId.make("codex-custom-workspace");
const parentInstanceId = ProviderInstanceId.make("codex");
const encodeThreadRead = Schema.encodeEffect(OrchestratorMcpThreadReadResult);
const encodeThreadList = Schema.encodeEffect(OrchestratorMcpThreadListResult);
const encodeThreadWait = Schema.encodeEffect(OrchestratorMcpThreadWaitResult);
const decodeContinuityInput = Schema.decodeUnknownEffect(OrchestratorMcpWorkContinuityInput);
const decodeThreadRead = Schema.decodeEffect(OrchestratorMcpThreadReadResult);

const decodeCapacityFixtureRun = Schema.decodeUnknownSync(OrchestrationV2Run);
const decodeCapacityFixtureRecovery = Schema.decodeUnknownSync(OrchestrationV2CapacityRecovery);
const decodeCapacityFixtureTask = Schema.decodeUnknownSync(OrchestrationV2Subagent);

const makeScope = (): McpInvocationContext.McpInvocationScope => ({
  environmentId,
  requestNamespace: "provider-session-mcp-orchestrator-detail",
  thread: {
    threadId: parentThreadId,
    providerSessionId: "provider-session-mcp-orchestrator-detail",
    providerInstanceId: parentInstanceId,
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
});

function baseThread(input: {
  readonly threadId: ThreadId;
  readonly title: string;
  readonly instanceId: ProviderInstanceId;
  readonly model: string;
}) {
  return {
    id: input.threadId,
    projectId,
    title: input.title,
    createdBy: "user" as const,
    creationSource: "mcp" as const,
    modelSelection: {
      instanceId: input.instanceId,
      model: input.model,
    },
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
    branch: null,
    worktreePath: null,
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: input.threadId,
    },
    archivedAt: null,
    deletedAt: null,
    providerInstanceId: input.instanceId,
    createdAt: now,
    updatedAt: now,
  };
}

function makeRun(input: {
  readonly id: RunId;
  readonly ordinal: number;
  readonly status: "running" | "waiting" | "cancelled" | "queued" | "completed";
  readonly instanceId?: ProviderInstanceId;
}) {
  return {
    id: input.id,
    ordinal: input.ordinal,
    status: input.status,
    modelSelection: {
      instanceId: input.instanceId ?? parentInstanceId,
      model: "gpt-5.4",
    },
    providerInstanceId: input.instanceId ?? parentInstanceId,
    requestedAt: now,
    startedAt: input.status === "cancelled" || input.status === "queued" ? null : now,
    completedAt: input.status === "cancelled" || input.status === "completed" ? now : null,
  };
}

it("the actual orchestrator MCP toolkit registers continuity with an object wire schema", async () => {
  const layer = McpHttpServer.layerOrchestratorToolkit.pipe(
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({}),
        ServerSettings.layerTest(),
        Layer.mock(ProviderRegistry.ProviderRegistry)({}),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({}),
        NodeCrypto.layer,
      ),
    ),
  );
  await Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    const continuity = server.tools.find(({ tool }) => tool.name === "t3_work_continuity");
    expect(continuity?.tool.inputSchema.type).toBe("object");
  }).pipe(Effect.provide(layer), Effect.scoped, Effect.runPromise);
});

it("the continuity object codec still rejects missing action authority and malformed reports", async () => {
  await Effect.gen(function* () {
    for (const input of [
      { action: "enable" },
      { action: "report", status: "complete", reason: "Done" },
      { action: "report", runId: activeRunId, status: "complete", reason: "   " },
    ]) {
      const refused = yield* decodeContinuityInput(input).pipe(Effect.flip);
      expect(refused._tag).toBe("SchemaError");
    }
    const enable = yield* decodeContinuityInput({
      action: "enable",
      authorityMessageId: "authority-message",
    });
    expect(enable).toEqual({ action: "enable", authorityMessageId: "authority-message" });
    const report = yield* decodeContinuityInput({
      action: "report",
      runId: activeRunId,
      status: "complete",
      reason: " Done ",
    });
    expect(report).toEqual({
      action: "report",
      runId: activeRunId,
      status: "complete",
      reason: "Done",
    });
  }).pipe(Effect.runPromise);
});

it("continuity mutations cannot target a different thread and read/list preserve the durable disposition", async () => {
  const continuity: OrchestrationV2WorkContinuity = {
    generation: CommandId.make("continuity-generation"),
    authorityMessageId: MessageId.make("authority-message"),
    maxChecks: 3,
    checksRemaining: 2,
    status: "waiting",
    reason: "Waiting for the delegated task",
    lastReportedRunId: activeRunId,
    lastAdvancedRunId: null,
    wait: { kind: "delegated_task", taskId },
  };
  const shell = { ...liveThreadShell(parentThreadId, { activeRunId: null }), continuity };
  const layer = OrchestratorMcpService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getTimelinePage: () => Effect.succeed({ items: [], totalItems: 0, hasMore: false }),
          getThreadRecords: () => Effect.succeed(idleThreadProjection(shell)),
          getThreadShell: () => Effect.succeed(shell),
          getProjectThreadRecords: () => Effect.succeed(idleThreadProjection(shell)),
          listProjectThreads: () => Effect.succeed([shell]),
          dispatch: () => Effect.die("A foreign continuity mutation must not reach dispatch"),
        }),
        ServerSettings.layerTest(),
        Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        }),
        NodeCrypto.layer,
      ),
    ),
  );
  await Effect.gen(function* () {
    const service = yield* OrchestratorMcpService.OrchestratorMcpService;
    const refused = yield* service
      .workContinuity(makeScope(), {
        action: "report",
        threadId: childThreadId,
        runId: activeRunId,
        status: "complete",
        reason: "Done",
      })
      .pipe(Effect.flip);
    expect(refused.code).toBe("invalid_request");
    for (const input of [
      {
        action: "enable" as const,
        threadId: childThreadId,
        authorityMessageId: MessageId.make("foreign-authority"),
      },
      { action: "disable" as const, threadId: childThreadId },
    ]) {
      const foreign = yield* service.workContinuity(makeScope(), input).pipe(Effect.flip);
      expect(foreign.code).toBe("invalid_request");
    }
    const read = yield* service.readThread(makeScope(), { threadId: parentThreadId });
    const listed = yield* service.listThreads(makeScope(), {});
    expect((yield* encodeThreadRead(read)).thread.continuity).toEqual(continuity);
    expect((yield* encodeThreadList(listed)).threads[0]?.continuity).toEqual(continuity);
  }).pipe(Effect.provide(layer), Effect.runPromise);
});

it("MCP re-enrollment for the same authority cannot refill exhausted checks or reopen blocked work", async () => {
  const continuity: OrchestrationV2WorkContinuity = {
    generation: CommandId.make("exhausted-continuity-generation"),
    authorityMessageId: MessageId.make("authority-message"),
    maxChecks: 3,
    checksRemaining: 0,
    status: "blocked",
    reason: "Continuation checks are exhausted",
    lastReportedRunId: activeRunId,
    lastAdvancedRunId: activeRunId,
    wait: null,
  };
  const shell = { ...liveThreadShell(parentThreadId, { activeRunId: null }), continuity };
  const layer = OrchestratorMcpService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getThreadRecords: () => Effect.succeed(idleThreadProjection(shell)),
          dispatch: () => Effect.die("Same-authority MCP enrollment must not dispatch a reset"),
        }),
        ServerSettings.layerTest(),
        Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        }),
        NodeCrypto.layer,
      ),
    ),
  );
  await Effect.gen(function* () {
    const service = yield* OrchestratorMcpService.OrchestratorMcpService;
    const result = yield* service.workContinuity(makeScope(), {
      action: "enable",
      authorityMessageId: continuity.authorityMessageId,
      maxChecks: 20,
      clientRequestId: "new-idempotency-key-cannot-reset-budget",
    });
    expect(result.threadId).toBe(parentThreadId);
    expect(result.continuity).toEqual(continuity);
    expect(result.continuity?.checksRemaining).toBe(0);
    expect(result.continuity?.status).toBe("blocked");
    expect(result.continuity?.generation).toBe(continuity.generation);
  }).pipe(Effect.provide(layer), Effect.runPromise);
});

it.each(["enable", "disable"] as const)(
  "MCP %s cannot change the human opt-out and uses authoritative agent provenance",
  async (action) => {
    const shell = {
      ...liveThreadShell(parentThreadId, { activeRunId: null }),
      continuity: null,
      continuityAutoEnrollDisabled: true,
    };
    let dispatched = 0;
    const layer = OrchestratorMcpService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ThreadManagementService.ThreadManagementService)({
            getThreadRecords: () => Effect.succeed(idleThreadProjection(shell)),
            dispatch: (command) =>
              Effect.sync(() => {
                dispatched += 1;
                expect(command).toMatchObject({
                  type: "thread.continuity.set",
                  creationSource: "mcp",
                  enabled: false,
                });
                return { sequence: 0, storedEvents: [] };
              }),
          }),
          ServerSettings.layerTest(),
          Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
          Layer.mock(ProjectService.ProjectService)({}),
          Layer.mock(SecretRequests.SecretRequests)({}),
          Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
          Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
            list: () => Effect.succeed([]),
          }),
          NodeCrypto.layer,
        ),
      ),
    );
    await Effect.gen(function* () {
      const service = yield* OrchestratorMcpService.OrchestratorMcpService;
      if (action === "enable") {
        const refused = yield* service
          .workContinuity(makeScope(), {
            action: "enable",
            authorityMessageId: MessageId.make("real-authority"),
            clientRequestId: "optout-enable",
          })
          .pipe(Effect.flip);
        expect(refused.code).toBe("invalid_request");
        expect(dispatched).toBe(0);
      } else {
        yield* service.workContinuity(makeScope(), {
          action: "disable",
          clientRequestId: "optout-disable",
        });
        expect(dispatched).toBe(1);
      }
      expect(shell.continuityAutoEnrollDisabled).toBe(true);
    }).pipe(Effect.provide(layer), Effect.runPromise);
  },
);

it.each(["active", "paused", "blocked", "usage_limited", "budget_limited", "complete"] as const)(
  "thread tools preserve a %s native goal independently of execution and clear it on the next read",
  async (status) => {
    const oversized = status === "blocked";
    let goal: OrchestrationV2ProviderGoal | null = {
      objective: oversized
        ? "Complete the assigned work. ".repeat(1_000).trim()
        : "Convert all eligible workers and verify their cutover",
      status,
      tokensUsed: 123,
      tokenBudget: 1000,
      checks: 2,
      lastCheck: oversized
        ? "The dependency remains unavailable. ".repeat(1_000)
        : "Availability remains unfinished",
    };
    const shell = (): OrchestrationV2ThreadShell => ({
      ...liveThreadShell(parentThreadId, { activeRunId: null }),
      status: "idle",
      goal,
    });
    const layer = OrchestratorMcpService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ThreadManagementService.ThreadManagementService)({
            getTimelinePage: () => Effect.succeed({ items: [], totalItems: 0, hasMore: false }),
            getThreadRecords: () => Effect.succeed(idleThreadProjection(shell())),
            getThreadShell: () => Effect.succeed(shell()),
            getProjectThreadRecords: () => Effect.succeed(idleThreadProjection(shell())),
            listProjectThreads: () => Effect.succeed([shell()]),
          }),
          ServerSettings.layerTest(),
          Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
          Layer.mock(ProjectService.ProjectService)({}),
          Layer.mock(SecretRequests.SecretRequests)({}),
          Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
          Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
            list: () => Effect.succeed([]),
          }),
          NodeCrypto.layer,
        ),
      ),
    );
    await Effect.gen(function* () {
      const service = yield* OrchestratorMcpService.OrchestratorMcpService;
      const read = yield* service.readThread(makeScope(), { threadId: parentThreadId });
      const listed = yield* service.listThreads(makeScope(), {});
      // Exercise the tool's wire schema too: an unrecognized field would be lost on encoding.
      const readWire = yield* encodeThreadRead(read);
      const listWire = yield* encodeThreadList(listed);
      expect(readWire.thread.goal).toEqual(goal);
      const listedThread = listWire.threads[0]!;
      expect(listedThread.goal?.status).toBe(status);
      expect(listedThread.goalTextTruncated).toBe(oversized);
      if (oversized) {
        expect(listedThread.goal!.objective.length).toBeLessThanOrEqual(1_000);
        expect(listedThread.goal!.lastCheck!.length).toBeLessThanOrEqual(2_000);
      } else {
        expect(listedThread.goal).toEqual(goal);
      }
      expect(readWire.thread.status).toBe("idle");
      expect(readWire.thread.activeRunId).toBeNull();
      expect(listWire.threads[0]?.status).toBe("idle");

      goal = null;
      const cleared = yield* service.readThread(makeScope(), { threadId: parentThreadId });
      const clearedList = yield* service.listThreads(makeScope(), {});
      expect(cleared.thread.goal).toBeNull();
      expect(clearedList.threads[0]?.goal).toBeNull();
      expect(clearedList.threads[0]?.goalTextTruncated).toBe(false);

      // Old servers omitted goals; do not reinterpret that omission as a completed objective.
      const { goal: _goal, ...legacyThread } = readWire.thread;
      const legacy = yield* decodeThreadRead({
        ...readWire,
        thread: legacyThread,
      });
      expect(legacy.thread.goal).toBeUndefined();
    }).pipe(Effect.provide(layer), Effect.runPromise);
  },
);

it("readThread prefers activity-run status over a newer cancelled queued run", async () => {
  const projection = {
    thread: baseThread({
      threadId: parentThreadId,
      title: "Parent",
      instanceId: parentInstanceId,
      model: "gpt-5.4",
    }),
    runs: [
      makeRun({ id: activeRunId, ordinal: 1, status: "running" }),
      makeRun({ id: cancelledRunId, ordinal: 2, status: "cancelled" }),
    ],
    visibleTurnItems: [],
    runtimeRequests: [],
    messages: [],
    contextTransfers: [],
    subagents: [],
    updatedAt: now,
  } as unknown as OrchestrationV2ThreadProjection;

  const layer = layerOrchestratorService.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getTimelinePage: () => Effect.succeed({ items: [], totalItems: 0, hasMore: false }),
          getThreadRecords: (threadId) =>
            threadId === parentThreadId
              ? Effect.succeed(projection)
              : Effect.die(`unexpected thread ${threadId}`),
          getThreadShell: (threadId) =>
            Effect.succeed(
              threadId === parentThreadId
                ? (projection.thread as unknown as OrchestrationV2ThreadShell)
                : null,
            ),
          getProjectThreadRecords: (input) =>
            input.threadId === parentThreadId
              ? Effect.succeed(projection)
              : Effect.die(`unexpected thread ${input.threadId}`),
        } satisfies Partial<ThreadManagementService.ThreadManagementService["Service"]>),
        ServerSettings.layerTest(),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed([]),
        } satisfies Partial<ProviderRegistry.ProviderRegistry["Service"]>),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({
          list: () => Effect.succeed({ tasks: [] }),
        } satisfies Partial<ScheduledTaskService.ScheduledTaskService["Service"]>),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        } satisfies Partial<ProviderAdapterRegistry.ProviderAdapterRegistryV2["Service"]>),
        NodeCrypto.layer,
      ),
    ),
  );

  await Effect.gen(function* () {
    const service = yield* OrchestratorMcpService.OrchestratorMcpService;
    const result = yield* service.readThread(makeScope(), { threadId: parentThreadId });
    expect(result.thread.status).toBe("running");
    expect(result.thread.latestRunId).toBe(cancelledRunId);
    expect(result.thread.activeRunId).toBe(activeRunId);
  }).pipe(Effect.provide(layer), Effect.runPromise);
});

it("readThread prefers waiting activity status over a newer cancelled queued run", async () => {
  const projection = {
    thread: baseThread({
      threadId: parentThreadId,
      title: "Parent waiting",
      instanceId: parentInstanceId,
      model: "gpt-5.4",
    }),
    runs: [
      makeRun({ id: activeRunId, ordinal: 1, status: "waiting" }),
      makeRun({ id: cancelledRunId, ordinal: 2, status: "cancelled" }),
    ],
    visibleTurnItems: [],
    runtimeRequests: [],
    messages: [],
    contextTransfers: [],
    subagents: [],
    updatedAt: now,
  } as unknown as OrchestrationV2ThreadProjection;

  const layer = layerOrchestratorService.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getTimelinePage: () => Effect.succeed({ items: [], totalItems: 0, hasMore: false }),
          getThreadRecords: (threadId) =>
            threadId === parentThreadId
              ? Effect.succeed(projection)
              : Effect.die(`unexpected thread ${threadId}`),
          getThreadShell: (threadId) =>
            Effect.succeed(
              threadId === parentThreadId
                ? (projection.thread as unknown as OrchestrationV2ThreadShell)
                : null,
            ),
          getProjectThreadRecords: (input) =>
            input.threadId === parentThreadId
              ? Effect.succeed(projection)
              : Effect.die(`unexpected thread ${input.threadId}`),
        } satisfies Partial<ThreadManagementService.ThreadManagementService["Service"]>),
        ServerSettings.layerTest(),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed([]),
        } satisfies Partial<ProviderRegistry.ProviderRegistry["Service"]>),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({
          list: () => Effect.succeed({ tasks: [] }),
        } satisfies Partial<ScheduledTaskService.ScheduledTaskService["Service"]>),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        } satisfies Partial<ProviderAdapterRegistry.ProviderAdapterRegistryV2["Service"]>),
        NodeCrypto.layer,
      ),
    ),
  );

  await Effect.gen(function* () {
    const service = yield* OrchestratorMcpService.OrchestratorMcpService;
    const result = yield* service.readThread(makeScope(), { threadId: parentThreadId });
    expect(result.thread.status).toBe("waiting");
    expect(result.thread.activeRunId).toBe(activeRunId);
  }).pipe(Effect.provide(layer), Effect.runPromise);
});

it.each(["read", "cancel"] as const)(
  "MCP %s preserves a live capacity wait instead of publishing or acknowledging failure",
  async (action) => {
    const parent = idleThreadProjection(liveThreadShell(parentThreadId));
    const child = idleThreadProjection(liveThreadShell(childThreadId));
    const providerThreadId = ProviderThreadId.make("capacity-child-native");
    const run = decodeCapacityFixtureRun({
      id: childRunId,
      threadId: childThreadId,
      ordinal: 1,
      providerInstanceId: customCodexInstanceId,
      modelSelection: { instanceId: customCodexInstanceId, model: "gpt-5.4" },
      providerThreadId,
      userMessageId: "capacity-accepted-request",
      rootNodeId: taskId,
      activeAttemptId: null,
      status: "failed",
      requestedAt: now,
      startedAt: now,
      completedAt: now,
      checkpointId: null,
      contextHandoffId: null,
    });
    const recovery = decodeCapacityFixtureRecovery({
      generation: "capacity-episode",
      originalRunId: childRunId,
      authorityMessageId: run.userMessageId,
      failedRunId: childRunId,
      retryRunId: null,
      providerThreadId,
      modelSelection: run.modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      reactionsReleased: false,
      attemptsUsed: 0,
      nextAttemptAt: DateTime.add(now, { seconds: 15 }),
      status: "pending",
      reason: "Waiting for model capacity.",
    });
    const task = decodeCapacityFixtureTask({
      id: taskId,
      threadId: parentThreadId,
      runId: activeRunId,
      parentNodeId: "parent-root-node",
      origin: "app_owned",
      createdBy: "agent",
      driver: codexDriver,
      providerInstanceId: customCodexInstanceId,
      providerThreadId,
      childThreadId,
      nativeTaskRef: null,
      prompt: "Finish the original job.",
      title: null,
      model: "gpt-5.4",
      status: "waiting",
      result: null,
      startedAt: now,
      completedAt: null,
      updatedAt: now,
    });
    const parentProjection = { ...parent, subagents: [task] };
    const childProjection = {
      ...child,
      thread: {
        ...child.thread,
        providerInstanceId: customCodexInstanceId,
        modelSelection: run.modelSelection,
        capacityRecovery: recovery,
        lineage: {
          parentThreadId,
          relationshipToParent: "subagent" as const,
          rootThreadId: parentThreadId,
        },
        forkedFrom: { type: "node" as const, nodeId: taskId },
      },
      runs: [run],
    };
    const writes: string[] = [];
    const layer = OrchestratorMcpService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(ThreadManagementService.ThreadManagementService)({
            getThreadRecords: (id) =>
              Effect.succeed(id === parentThreadId ? parentProjection : childProjection),
            getThreadShell: () => Effect.succeed(liveThreadShell(childThreadId)),
            stopDelegatedTasks: () => Effect.void,
            dispatch: (command) =>
              Effect.sync(() => {
                writes.push(command.type);
                return { sequence: 0, storedEvents: [] };
              }),
          }),
          ServerSettings.layerTest(),
          Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
          Layer.mock(ProjectService.ProjectService)({}),
          Layer.mock(SecretRequests.SecretRequests)({}),
          Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
          Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
            list: () => Effect.succeed([]),
          }),
          NodeCrypto.layer,
        ),
      ),
    );
    await Effect.gen(function* () {
      const service = yield* OrchestratorMcpService.OrchestratorMcpService;
      if (action === "read") {
        const result = yield* service.taskStatus(makeScope(), taskId);
        expect(result.status).toBe("queued");
        expect(result.workState).toBe("waiting_for_capacity");
        expect(result.summary).toBeNull();
        expect(result.latestTerminalStatus).toBeNull();
        expect(result.capacityRecovery?.generation).toBe(recovery.generation);
        expect(writes).toEqual([]);
      } else {
        const result = yield* service.cancelTask(makeScope(), {
          taskId,
          clientRequestId: "capacity-task-cancel",
        });
        expect(result.status).toBe("cancel_requested");
        expect(writes).toContain("thread.stop");
        expect(writes).not.toContain("delegated_task.completion-delivery.acknowledge");
      }
    }).pipe(Effect.provide(layer), Effect.runPromise);
  },
);

it("taskStatus returns task.providerInstanceId rather than the driver kind", async () => {
  const parentProjection = {
    thread: baseThread({
      threadId: parentThreadId,
      title: "Parent",
      instanceId: parentInstanceId,
      model: "gpt-5.4",
    }),
    runs: [makeRun({ id: activeRunId, ordinal: 1, status: "running" })],
    visibleTurnItems: [],
    runtimeRequests: [],
    messages: [],
    contextTransfers: [],
    subagents: [
      {
        id: taskId,
        threadId: parentThreadId,
        runId: activeRunId,
        parentNodeId: NodeId.make("node-parent"),
        origin: "app_owned",
        createdBy: "agent",
        driver: codexDriver,
        providerInstanceId: customCodexInstanceId,
        providerThreadId: null,
        childThreadId,
        nativeTaskRef: null,
        prompt: "Inspect the custom instance.",
        title: null,
        model: "gpt-5.4",
        status: "running",
        result: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      },
    ],
    updatedAt: now,
  } as unknown as OrchestrationV2ThreadProjection;

  const childProjection = {
    thread: {
      ...baseThread({
        threadId: childThreadId,
        title: "Child",
        instanceId: customCodexInstanceId,
        model: "gpt-5.4",
      }),
      lineage: {
        parentThreadId,
        relationshipToParent: "subagent",
        rootThreadId: parentThreadId,
      },
      createdBy: "agent",
    },
    runs: [
      makeRun({
        id: childRunId,
        ordinal: 1,
        status: "running",
        instanceId: customCodexInstanceId,
      }),
    ],
    visibleTurnItems: [],
    runtimeRequests: [],
    messages: [],
    contextTransfers: [
      {
        type: "subagent_spawn",
        sourceThreadId: parentThreadId,
        targetThreadId: childThreadId,
        targetRunId: childRunId,
      },
    ],
    subagents: [],
    providerThreads: [],
    updatedAt: now,
  } as unknown as OrchestrationV2ThreadProjection;

  const layer = layerOrchestratorService.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getTimelinePage: () => Effect.succeed({ items: [], totalItems: 0, hasMore: false }),
          getThreadRecords: (threadId) => {
            if (threadId === parentThreadId) return Effect.succeed(parentProjection);
            if (threadId === childThreadId) return Effect.succeed(childProjection);
            return Effect.die(`unexpected thread ${threadId}`);
          },
        } satisfies Partial<ThreadManagementService.ThreadManagementService["Service"]>),
        ServerSettings.layerTest(),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed([]),
        } satisfies Partial<ProviderRegistry.ProviderRegistry["Service"]>),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({
          list: () => Effect.succeed({ tasks: [] }),
        } satisfies Partial<ScheduledTaskService.ScheduledTaskService["Service"]>),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        } satisfies Partial<ProviderAdapterRegistry.ProviderAdapterRegistryV2["Service"]>),
        NodeCrypto.layer,
      ),
    ),
  );

  await Effect.gen(function* () {
    const service = yield* OrchestratorMcpService.OrchestratorMcpService;
    const result = yield* service.taskStatus(makeScope(), taskId);
    expect(result.providerInstanceId).toBe(customCodexInstanceId);
    expect(result.providerInstanceId).not.toBe(ProviderInstanceId.make(String(codexDriver)));
    expect(result.status).toBe("running");
    expect(result.taskId).toBe(taskId);
    expect(result.childThreadId).toBe(childThreadId);
  }).pipe(Effect.provide(layer), Effect.runPromise);
});

it("readThread and sendToThread reach threads in other projects", async () => {
  let parentRuns: ReadonlyArray<unknown> = [
    makeRun({ id: RunId.make("run-parent-live"), ordinal: 1, status: "running" }),
  ];
  const foreignProjectId = ProjectId.make("project-mcp-orchestrator-foreign");
  const foreignThreadId = ThreadId.make("thread-mcp-orchestrator-foreign");
  const parentProjection = {
    thread: baseThread({
      threadId: parentThreadId,
      title: "Parent",
      instanceId: parentInstanceId,
      model: "gpt-5.4",
    }),
    get runs() {
      return parentRuns;
    },
    visibleTurnItems: [],
    runtimeRequests: [],
    messages: [],
    contextTransfers: [],
    subagents: [],
    updatedAt: now,
  } as unknown as OrchestrationV2ThreadProjection;
  const foreignProjection = (threadId: ThreadId) =>
    ({
      thread: {
        ...baseThread({
          threadId,
          title: "Foreign",
          instanceId: parentInstanceId,
          model: "gpt-5.4",
        }),
        projectId: foreignProjectId,
      },
      runs: [],
      visibleTurnItems: [
        {
          position: 0,
          visibility: "inherited",
          sourceThreadId: threadId,
          sourceItemId: "item-1",
          item: {
            id: "item-1",
            type: "assistant_message",
            messageId: "assistant-1",
            status: "completed",
            title: null,
            text: "Foreign thread said hello",
            createdAt: now,
            updatedAt: now,
          },
        },
      ],
      runtimeRequests: [],
      messages: [],
      contextTransfers: [],
      subagents: [],
      updatedAt: now,
    }) as unknown as OrchestrationV2ThreadProjection;

  const layer = layerOrchestratorService.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getThreadRecords: (threadId) => {
            if (threadId === parentThreadId) return Effect.succeed(parentProjection);
            if (threadId === foreignThreadId) return Effect.succeed(foreignProjection(threadId));
            return Effect.die(`unexpected thread ${threadId}`);
          },
          getThreadShell: (threadId) =>
            Effect.succeed(
              threadId === foreignThreadId
                ? (foreignProjection(threadId).thread as unknown as OrchestrationV2ThreadShell)
                : null,
            ),
          getTimelinePage: (threadId) =>
            Effect.succeed({
              items: foreignProjection(threadId).visibleTurnItems,
              totalItems: 1,
              hasMore: false,
            }),
          getProjectThreadRecords: (input) =>
            input.threadId === foreignThreadId && input.projectId === foreignProjectId
              ? Effect.succeed(foreignProjection(input.threadId))
              : Effect.fail(
                  new ThreadManagementService.ThreadManagementThreadNotFoundError({
                    projectId: input.projectId,
                    threadId: input.threadId,
                  }),
                ),
          sendToThread: () =>
            Effect.succeed({
              run: { id: "run-foreign", status: "queued" },
              delivery: "started",
            } as unknown as ThreadManagementService.ThreadManagementSendResult),
        } satisfies Partial<ThreadManagementService.ThreadManagementService["Service"]>),
        ServerSettings.layerTest(),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed([]),
        } satisfies Partial<ProviderRegistry.ProviderRegistry["Service"]>),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({
          list: () =>
            Effect.succeed({
              tasks: [
                {
                  id: "task-foreign",
                  projectId: foreignProjectId,
                  runtimeMode: "approval-required",
                  interactionMode: "default",
                } as never,
              ],
            }),
        } satisfies Partial<ScheduledTaskService.ScheduledTaskService["Service"]>),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        } satisfies Partial<ProviderAdapterRegistry.ProviderAdapterRegistryV2["Service"]>),
        NodeCrypto.layer,
      ),
    ),
  );

  await Effect.gen(function* () {
    const service = yield* OrchestratorMcpService.OrchestratorMcpService;
    const foreign = yield* service.readThread(makeScope(), { threadId: foreignThreadId });
    expect(foreign.thread.threadId).toBe(foreignThreadId);
    expect(foreign.thread.projectId).toBe(foreignProjectId);
    expect(foreign.items.map((item) => item.text)).toEqual(["Foreign thread said hello"]);

    const sent = yield* service.sendToThread(makeScope(), {
      threadId: foreignThreadId,
      message: "hi",
    });
    expect(sent.threadId).toBe(foreignThreadId);

    // Once the caller's run ends, it can still read other threads but no longer write to them.
    parentRuns = [];
    yield* service.readThread(makeScope(), { threadId: foreignThreadId });
    const stale = yield* service
      .sendToThread(makeScope(), { threadId: foreignThreadId, message: "hi again" })
      .pipe(Effect.flip);
    expect(stale.code).toBe("parent_not_active");
    const staleInterrupt = yield* service
      .interruptThread(makeScope(), { threadId: foreignThreadId })
      .pipe(Effect.flip);
    expect(staleInterrupt.code).toBe("parent_not_active");
    // Nor create, change or remove scheduled work in another project.
    const staleSchedule = yield* service
      .scheduleTask(makeScope(), {
        projectId: foreignProjectId,
        prompt: "check in later",
        schedule: { type: "interval", everyMs: 3_600_000 },
      })
      .pipe(Effect.flip);
    expect(staleSchedule.code).toBe("parent_not_active");
    const staleUpdate = yield* service
      .updateScheduledTask(makeScope(), {
        scheduledTaskId: ScheduledTaskId.make("task-foreign"),
        enabled: false,
      })
      .pipe(Effect.flip);
    expect(staleUpdate.code).toBe("parent_not_active");
    const staleDelete = yield* service
      .deleteScheduledTask(makeScope(), { scheduledTaskId: ScheduledTaskId.make("task-foreign") })
      .pipe(Effect.flip);
    expect(staleDelete.code).toBe("parent_not_active");
  }).pipe(Effect.provide(layer), Effect.runPromise);
});

it("MCP read list and wait expose capacity ownership without hiding raw failure evidence", async () => {
  const providerThreadId = ProviderThreadId.make("capacity-top-level-native");
  const run = decodeCapacityFixtureRun({
    id: activeRunId,
    threadId: parentThreadId,
    ordinal: 1,
    providerInstanceId: parentInstanceId,
    modelSelection: { instanceId: parentInstanceId, model: "gpt-5.4" },
    providerThreadId,
    userMessageId: "capacity-top-level-request",
    rootNodeId: null,
    activeAttemptId: null,
    status: "failed",
    requestedAt: now,
    startedAt: now,
    completedAt: now,
    checkpointId: null,
    contextHandoffId: null,
  });
  const recovery = decodeCapacityFixtureRecovery({
    generation: "capacity-top-level-episode",
    originalRunId: run.id,
    authorityMessageId: run.userMessageId,
    failedRunId: run.id,
    retryRunId: null,
    providerThreadId,
    modelSelection: run.modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    reactionsReleased: false,
    attemptsUsed: 0,
    nextAttemptAt: DateTime.add(now, { seconds: 15 }),
    status: "pending",
    reason: "Waiting for model capacity.",
  });
  let shell: OrchestrationV2ThreadShell = {
    ...liveThreadShell(parentThreadId, { activeRunId: null }),
    status: "failed",
    latestRunId: run.id,
    capacityRecovery: recovery,
  };
  const projection = () => ({ ...idleThreadProjection(shell), runs: [run] });
  const layer = OrchestratorMcpService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          getTimelinePage: () => Effect.succeed({ items: [], totalItems: 0, hasMore: false }),
          getThreadRecords: () => Effect.succeed(projection()),
          getThreadShell: () => Effect.succeed(shell),
          getProjectThreadRecords: () => Effect.succeed(projection()),
          listProjectThreads: () => Effect.succeed([shell]),
          waitForThread: () =>
            Effect.succeed({
              threadId: parentThreadId,
              run,
              status: "waiting",
              capacityRecovery: recovery,
              timedOut: true,
            }),
        }),
        ServerSettings.layerTest(),
        Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.mock(ScheduledTaskService.ScheduledTaskService)({}),
        Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({
          list: () => Effect.succeed([]),
        }),
        NodeCrypto.layer,
      ),
    ),
  );
  await Effect.gen(function* () {
    const service = yield* OrchestratorMcpService.OrchestratorMcpService;
    const read = yield* encodeThreadRead(
      yield* service.readThread(makeScope(), { threadId: parentThreadId }),
    );
    const listed = yield* encodeThreadList(yield* service.listThreads(makeScope(), {}));
    for (const result of [read.thread, listed.threads[0]]) {
      expect(result).toMatchObject({
        status: "waiting",
        runStatus: "failed",
        capacityRecovery: {
          generation: recovery.generation,
          nextAttemptAt: DateTime.formatIso(recovery.nextAttemptAt ?? now),
        },
      });
    }
    const waitingOnly = yield* service.listThreads(makeScope(), { statuses: ["waiting"] });
    expect(waitingOnly.threads.map((thread) => thread.threadId)).toEqual([parentThreadId]);
    expect((yield* service.listThreads(makeScope(), { statuses: ["failed"] })).threads).toEqual([]);
    const wait = yield* encodeThreadWait(
      yield* service.waitForThread(makeScope(), { threadId: parentThreadId, runId: run.id }),
    );
    expect(wait).toMatchObject({
      runId: run.id,
      status: "waiting",
      runStatus: "failed",
      timedOut: true,
      capacityRecovery: {
        generation: recovery.generation,
        nextAttemptAt: DateTime.formatIso(recovery.nextAttemptAt ?? now),
      },
    });
    shell = { ...shell, latestRunId: cancelledRunId };
    const superseded = yield* encodeThreadList(yield* service.listThreads(makeScope(), {}));
    expect(superseded.threads[0]).toMatchObject({ status: "failed", runStatus: "failed" });
  }).pipe(Effect.provide(layer), Effect.runPromise);
});
