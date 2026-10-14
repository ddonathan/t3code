import {
  CommandId,
  OrchestratorMcpFailure,
  type OrchestrationV2AppThread,
  type OrchestrationV2Command,
  type ThreadMetadataMcpAction,
  type ThreadMetadataMcpUpdateInput,
  type ThreadMetadataMcpUpdateResult,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import type { McpInvocationScope } from "./McpInvocationContext.ts";

export class ThreadMetadataMcpService extends Context.Service<
  ThreadMetadataMcpService,
  {
    readonly update: (
      scope: McpInvocationScope,
      input: ThreadMetadataMcpUpdateInput,
    ) => Effect.Effect<ThreadMetadataMcpUpdateResult, OrchestratorMcpFailure>;
  }
>()("t3/mcp/ThreadMetadataMcpService") {}

function failure(code: OrchestratorMcpFailure["code"], message: string): OrchestratorMcpFailure {
  return new OrchestratorMcpFailure({ code, message });
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function threadLookupFailure(
  error: ThreadManagementService.ThreadManagementError,
): OrchestratorMcpFailure {
  return error._tag === "ThreadManagementThreadNotFoundError"
    ? failure("thread_not_found", error.message)
    : failure("orchestration_error", error.message);
}

function stablePart(value: string): string {
  return encodeURIComponent(value);
}

function commandId(input: {
  readonly scope: McpInvocationScope;
  readonly threadId: ThreadId;
  readonly action: ThreadMetadataMcpAction;
  readonly requestKey: string;
}): CommandId {
  return CommandId.make(
    [
      "command",
      "mcp",
      stablePart(input.scope.requestNamespace),
      "thread-update",
      stablePart(input.threadId),
      stablePart(input.action),
      stablePart(input.requestKey),
    ].join(":"),
  );
}

function metadataCommand(input: {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly projectId: OrchestrationV2AppThread["projectId"];
  readonly update: ThreadMetadataMcpUpdateInput;
}): Effect.Effect<OrchestrationV2Command, OrchestratorMcpFailure> {
  const base = { commandId: input.commandId, threadId: input.threadId };
  switch (input.update.action) {
    case "rename":
      return input.update.title === undefined
        ? Effect.fail(failure("invalid_request", "rename requires title."))
        : Effect.succeed({ type: "thread.metadata.update", ...base, title: input.update.title });
    case "regenerate_title":
      return Effect.succeed({ type: "thread.metadata.update", ...base, regenerateTitle: true });
    case "link_pull_request":
      return input.update.pullRequest === undefined
        ? Effect.fail(failure("invalid_request", "link_pull_request requires pullRequest."))
        : Effect.succeed({
            type: "thread.metadata.update",
            ...base,
            linkedPullRequest: { projectId: input.projectId, ...input.update.pullRequest },
          });
    case "unlink_pull_request":
      return Effect.succeed({ type: "thread.metadata.update", ...base, linkedPullRequest: null });
    case "cancel_capacity_retry":
      return input.update.generation === undefined
        ? Effect.fail(failure("invalid_request", "cancel_capacity_retry requires generation."))
        : Effect.succeed({
            type: "thread.capacity.cancel",
            ...base,
            generation: input.update.generation,
          });
  }
}

function resultFromThread(input: {
  readonly action: ThreadMetadataMcpAction;
  readonly commandId: CommandId;
  readonly sequence: number;
  readonly thread: OrchestrationV2AppThread;
  readonly capacityCancellation?: ThreadMetadataMcpUpdateResult["capacityCancellation"];
}): ThreadMetadataMcpUpdateResult {
  return {
    threadId: input.thread.id,
    action: input.action,
    commandId: input.commandId,
    sequence: input.sequence,
    title: input.thread.title,
    titleRegeneration:
      input.thread.titleRegeneration === undefined || input.thread.titleRegeneration === null
        ? null
        : {
            requestId: input.thread.titleRegeneration.requestId,
            startedAt: DateTime.formatIso(input.thread.titleRegeneration.startedAt),
          },
    linkedPullRequest: input.thread.linkedPullRequest ?? null,
    updatedAt: DateTime.formatIso(input.thread.updatedAt),
    ...(input.capacityCancellation === undefined
      ? {}
      : {
          capacityRecovery: input.thread.capacityRecovery ?? null,
          capacityCancellation: input.capacityCancellation,
        }),
  };
}

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;

  const update = Effect.fn("ThreadMetadataMcpService.update")(function* (
    scope: McpInvocationScope,
    input: ThreadMetadataMcpUpdateInput,
  ) {
    if (!scope.capabilities.has("orchestration")) {
      return yield* failure(
        "capability_denied",
        "This MCP credential does not grant orchestration capabilities.",
      );
    }

    const threadId = input.threadId ?? scope.thread?.threadId;
    if (threadId === undefined) {
      return yield* failure(
        "target_required",
        "Pass threadId: this MCP client is not running inside a T3 thread.",
      );
    }
    const shell = yield* threadManagement
      .getThreadShell(threadId)
      .pipe(
        Effect.mapError((error) =>
          failure(
            "orchestration_error",
            `Unable to locate thread ${threadId}: ${errorMessage(error)}`,
          ),
        ),
      );
    if (shell === null || shell.deletedAt !== null) {
      return yield* failure("thread_not_found", `Thread ${threadId} was not found.`);
    }
    const target = yield* threadManagement
      .getProjectThreadRecords(
        { projectId: shell.projectId, threadId },
        input.action === "cancel_capacity_retry" ? ["runs"] : [],
      )
      .pipe(Effect.mapError(threadLookupFailure));
    const requestKey =
      input.clientRequestId === undefined
        ? yield* crypto.randomUUIDv4.pipe(Effect.orDie)
        : input.clientRequestId;
    const updateCommandId = commandId({
      scope,
      threadId,
      action: input.action,
      requestKey,
    });
    const command = yield* metadataCommand({
      commandId: updateCommandId,
      threadId,
      projectId: target.thread.projectId,
      update: input,
    });
    if (input.action === "cancel_capacity_retry") {
      const recovery = target.thread.capacityRecovery;
      if (recovery == null || recovery.generation !== input.generation)
        return yield* failure(
          "invalid_request",
          "The capacity recovery generation is no longer current. Read the thread before cancelling.",
        );
      if (recovery.status === "cancelled" || recovery.status !== "pending") {
        const sequence = yield* threadManagement
          .getThreadEventSequence(threadId)
          .pipe(Effect.mapError((error) => failure("orchestration_error", errorMessage(error))));
        return resultFromThread({
          action: input.action,
          commandId: updateCommandId,
          sequence,
          thread: target.thread,
          capacityCancellation:
            recovery.status === "cancelled" ? "already_cancelled" : "not_pending",
        });
      }
      const latest = ThreadManagementService.latestRun(target);
      const active = ThreadManagementService.latestActiveRun(target);
      if (
        ThreadManagementService.capacityRecoveryDisposition(
          recovery,
          latest?.id ?? null,
          active?.id ?? null,
          active?.status ?? latest?.status ?? "idle",
        ) !== "pending"
      )
        return yield* failure(
          "invalid_request",
          "This generation has no owned deadline-backed pending retry to cancel.",
        );
      const dispatched = yield* threadManagement
        .dispatch(command)
        .pipe(
          Effect.mapError((error) =>
            failure(
              "orchestration_error",
              `Unable to cancel capacity retry for thread ${threadId}: ${errorMessage(error)}`,
            ),
          ),
        );
      // The generation/status guard may lose to a retry or new human intake.
      // Re-read instead of claiming cancellation from the requested command.
      const current = yield* threadManagement
        .getProjectThreadRecords({ projectId: shell.projectId, threadId }, ["runs"])
        .pipe(Effect.mapError(threadLookupFailure));
      if (current.thread.capacityRecovery?.generation !== input.generation)
        return yield* failure(
          "invalid_request",
          "The capacity recovery generation changed before cancellation; the newer assignment was left unchanged.",
        );
      return resultFromThread({
        action: input.action,
        commandId: updateCommandId,
        sequence: dispatched.sequence,
        thread: current.thread,
        capacityCancellation:
          current.thread.capacityRecovery.status === "cancelled" ? "cancelled" : "not_pending",
      });
    }
    const dispatched = yield* threadManagement
      .dispatch(command)
      .pipe(
        Effect.mapError((error) =>
          failure(
            "orchestration_error",
            `Unable to ${input.action} for thread ${threadId}: ${errorMessage(error)}`,
          ),
        ),
      );
    const metadataEvent = dispatched.storedEvents.find(
      (stored) => stored.event.type === "thread.metadata-updated",
    );
    if (metadataEvent === undefined || metadataEvent.event.type !== "thread.metadata-updated") {
      return yield* failure(
        "orchestration_error",
        `Thread ${threadId} metadata update completed without a resultant state.`,
      );
    }
    return resultFromThread({
      action: input.action,
      commandId: updateCommandId,
      sequence: dispatched.sequence,
      thread: metadataEvent.event.payload,
    });
  });

  return ThreadMetadataMcpService.of({ update });
});

export const layer: Layer.Layer<
  ThreadMetadataMcpService,
  never,
  Crypto.Crypto | ThreadManagementService.ThreadManagementService
> = Layer.effect(ThreadMetadataMcpService, make);
