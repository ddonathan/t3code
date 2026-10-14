import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  OrchestrationV2CapacityRecovery,
  OrchestrationV2Run,
  type OrchestrationV2ServerCommand,
  ThreadMetadataMcpUpdateResult,
  OrchestratorMcpFailure,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import { McpSchema, McpServer } from "effect/ai";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as OrchestratorMcpService from "./OrchestratorMcpService.ts";
import * as OrchestratorHandlers from "./toolkits/orchestrator/handlers.ts";
import { OrchestratorToolkit } from "./toolkits/orchestrator/tools.ts";
import { idleThreadProjection, liveThreadShell } from "./McpToolAccess.testkit.ts";

import { OrchestratorProjectionError } from "../orchestration-v2/Orchestrator.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as ThreadMetadataMcp from "./ThreadMetadataMcpService.ts";

const threadId = ThreadId.make("thread:metadata-caller");
const scope: McpInvocationContext.McpInvocationScope = {
  environmentId: EnvironmentId.make("environment:metadata-test"),
  requestNamespace: "provider-session:metadata-test",
  thread: {
    threadId,
    providerSessionId: "provider-session:metadata-test",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  issuedAt: 1,
};

function layerService(
  getThreadShell: ThreadManagement.ThreadManagementService["Service"]["getThreadShell"],
) {
  return ThreadMetadataMcp.layer.pipe(
    Layer.provide(
      Layer.merge(
        Layer.mock(ThreadManagement.ThreadManagementService)({
          getThreadShell,
          getThreadRecords: () => Effect.die("projection must not load after shell failure"),
        } satisfies Partial<ThreadManagement.ThreadManagementService["Service"]>),
        NodeCrypto.layer,
      ),
    ),
  );
}

const updateCallingThread = Effect.gen(function* () {
  const service = yield* ThreadMetadataMcp.ThreadMetadataMcpService;
  return yield* service.update(scope, {
    action: "rename",
    title: "Renamed thread",
    clientRequestId: "metadata-caller-classification",
  });
});

it.effect("reports an absent calling thread as thread_not_found", () =>
  Effect.gen(function* () {
    const error = yield* updateCallingThread.pipe(
      Effect.provide(layerService(() => Effect.succeed(null))),
      Effect.flip,
    );

    expect(error.code).toBe("thread_not_found");
  }),
);

it.effect("keeps calling-thread storage failures as orchestration errors", () =>
  Effect.gen(function* () {
    const error = yield* updateCallingThread.pipe(
      Effect.provide(
        layerService(() =>
          Effect.fail(
            new OrchestratorProjectionError({
              threadId,
              cause: new Error("storage unavailable"),
            }),
          ),
        ),
      ),
      Effect.flip,
    );

    expect(error.code).toBe("orchestration_error");
  }),
);

const decodeRun = Schema.decodeUnknownSync(OrchestrationV2Run);
const decodeRecovery = Schema.decodeUnknownSync(OrchestrationV2CapacityRecovery);
const decodeOutcome = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Union([ThreadMetadataMcpUpdateResult, OrchestratorMcpFailure])),
);
const epoch = DateTime.makeUnsafe("2026-10-12T12:00:00.000Z");
const pendingRun = decodeRun({
  id: "run:capacity",
  threadId,
  ordinal: 1,
  providerInstanceId: "codex",
  modelSelection: { instanceId: "codex", model: "gpt-5" },
  providerThreadId: "native:capacity",
  userMessageId: "message:capacity",
  rootNodeId: null,
  activeAttemptId: null,
  status: "failed",
  requestedAt: epoch,
  startedAt: epoch,
  completedAt: epoch,
  checkpointId: null,
  contextHandoffId: null,
});
const pendingRecovery = decodeRecovery({
  generation: "capacity:episode",
  originalRunId: pendingRun.id,
  failedRunId: pendingRun.id,
  authorityMessageId: pendingRun.userMessageId,
  providerThreadId: pendingRun.providerThreadId,
  modelSelection: pendingRun.modelSelection,
  runtimeMode: "full-access",
  interactionMode: "default",
  reactionsReleased: false,
  retryRunId: null,
  attemptsUsed: 0,
  nextAttemptAt: DateTime.add(epoch, { seconds: 15 }),
  status: "pending",
  reason: "Waiting for capacity.",
});
const mcpClient = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "cancel-test", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "cancel-test", version: "1" },
  },
  getClient: Effect.die("unused"),
});
const clientScope: McpInvocationContext.McpInvocationScope = {
  ...scope,
  thread: undefined,
  client: { sessionId: "cancel-test", label: "test", access: "full-access" },
};

it.effect(
  "registered MCP cancellation preserves generation ownership, races, permissions and the object codec",
  () =>
    Effect.gen(function* () {
      let recovery = pendingRecovery;
      let run = pendingRun;
      let dispatched: OrchestrationV2ServerCommand[] = [];
      let afterDispatch = pendingRecovery;
      const shell = () => ({
        ...liveThreadShell(threadId, { activeRunId: null }),
        status: run.status,
        latestRunId: run.id,
        capacityRecovery: recovery,
      });
      const projection = () => ({ ...idleThreadProjection(shell()), runs: [run] });
      const threads = Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: () => Effect.succeed(shell()),
        getProjectThreadRecords: () => Effect.succeed(projection()),
        getThreadEventSequence: () => Effect.succeed(7),
        dispatch: (command) =>
          Effect.sync(() => {
            dispatched.push(command);
            recovery = afterDispatch;
            return { sequence: 7, storedEvents: [] };
          }),
      });
      const layer = McpHttpServer.toolkitRegistration(
        OrchestratorToolkit,
        OrchestratorHandlers.layer,
      ).pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provide(
          Layer.mergeAll(
            ThreadMetadataMcp.layer.pipe(Layer.provide(Layer.merge(threads, NodeCrypto.layer))),
            threads,
            Layer.mock(OrchestratorMcpService.OrchestratorMcpService)({}),
          ),
        ),
      );
      const call = (generation = pendingRecovery.generation, caller = clientScope) =>
        McpServer.McpServer.pipe(
          Effect.flatMap((server) =>
            server.callTool({
              name: "t3_thread_update",
              arguments: { threadId, action: "cancel_capacity_retry", generation },
            }),
          ),
          Effect.flatMap(({ content }) =>
            decodeOutcome(content.map((part) => (part.type === "text" ? part.text : "")).join("")),
          ),
          Effect.provideService(McpInvocationContext.McpInvocationContext, caller),
          Effect.provideService(McpSchema.McpServerClient, mcpClient),
        );
      yield* Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        const tool = server.tools.find(({ tool }) => tool.name === "t3_thread_update");
        expect(tool?.tool.inputSchema.type).toBe("object");
        expect(tool?.tool.inputSchema).not.toHaveProperty("anyOf");
        afterDispatch = { ...pendingRecovery, status: "cancelled", nextAttemptAt: null };
        const success = yield* call();
        expect(success).toMatchObject({
          capacityCancellation: "cancelled",
          capacityRecovery: { status: "cancelled", nextAttemptAt: null },
        });
        expect(dispatched).toHaveLength(1);
        expect(dispatched[0]).toMatchObject({
          type: "thread.capacity.cancel",
          generation: pendingRecovery.generation,
          threadId,
        });
        expect(yield* call()).toMatchObject({ capacityCancellation: "already_cancelled" });
        expect(dispatched).toHaveLength(1);
        expect(yield* call(CommandId.make("capacity:stale"))).toMatchObject({
          code: "invalid_request",
        });
        for (const invalid of [{ ...pendingRecovery, nextAttemptAt: null }, pendingRecovery]) {
          recovery = invalid;
          run =
            invalid.nextAttemptAt === null
              ? pendingRun
              : {
                  ...pendingRun,
                  id: RunId.make("run:newer-human"),
                  ordinal: 2,
                  status: "completed",
                };
          expect(yield* call()).toMatchObject({ code: "invalid_request" });
          expect(dispatched).toHaveLength(1);
        }
        run = pendingRun;
        for (const status of ["retrying", "complete", "exhausted"] as const) {
          recovery = { ...pendingRecovery, status, nextAttemptAt: null };
          expect(yield* call()).toMatchObject({
            capacityCancellation: "not_pending",
            capacityRecovery: { status },
          });
          expect(dispatched).toHaveLength(1);
        }
        recovery = pendingRecovery;
        afterDispatch = { ...pendingRecovery, status: "retrying", nextAttemptAt: null };
        expect(yield* call()).toMatchObject({
          capacityCancellation: "not_pending",
          capacityRecovery: { status: "retrying" },
        });
        recovery = pendingRecovery;
        afterDispatch = { ...pendingRecovery, generation: CommandId.make("capacity:new-human") };
        expect(yield* call()).toMatchObject({ code: "invalid_request" });
        expect(recovery.generation).toBe("capacity:new-human");
        recovery = pendingRecovery;
        dispatched = [];
        expect(
          yield* call(pendingRecovery.generation, {
            ...clientScope,
            client: { sessionId: "read-only", label: "test", access: "read-only" },
          }),
        ).toMatchObject({ code: "capability_denied" });
        expect(dispatched).toHaveLength(0);
      }).pipe(Effect.provide(layer), Effect.scoped);
    }),
);
