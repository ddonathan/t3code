import { CommandId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";

export const sweep = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const now = yield* DateTime.now;
  const candidates = yield* projections.getRecoveryThreadIds("capacity");
  for (const threadId of candidates) {
    yield* Effect.gen(function* () {
      const recovery = (yield* projections.getThread(threadId)).capacityRecovery;
      if (
        recovery != null &&
        !recovery.reactionsReleased &&
        ["cancelled", "exhausted", "complete"].includes(recovery.status)
      ) {
        yield* orchestrator.recoverCapacityReactions(threadId, recovery.generation);
        return;
      }
      if (
        recovery?.status !== "pending" ||
        recovery.nextAttemptAt === null ||
        DateTime.toEpochMillis(recovery.nextAttemptAt) > DateTime.toEpochMillis(now)
      )
        return;
      yield* threads
        .dispatch({
          type: "thread.capacity.retry",
          threadId,
          generation: recovery.generation,
          commandId: CommandId.make(
            `command:capacity:tick:${recovery.generation}:${recovery.failedRunId}:${recovery.attemptsUsed + 1}`,
          ),
        })
        .pipe(
          Effect.catchCause((cause) =>
            Effect.gen(function* () {
              yield* Effect.logWarning(
                "Capacity continuation dispatch refused; cancelling the pending episode",
                {
                  threadId,
                  generation: recovery.generation,
                  failedRunId: recovery.failedRunId,
                  cause,
                },
              );
              yield* threads.dispatch({
                type: "thread.capacity.abort",
                threadId,
                generation: recovery.generation,
                runId: recovery.failedRunId,
                commandId: yield* idAllocator.allocate.command({
                  fixtureName: "capacity-dispatch-refusal",
                  commandName: "abort",
                }),
              });
            }),
          ),
        );
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Capacity recovery candidate failed", { threadId, cause }),
      ),
    );
  }
});
// Release replay and existing supervision are separate scheduler sources: a
// release never allocates a provider turn or consumes a supervised-work check.
export const releasedContinuitySweep = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const candidates = yield* projections.getRecoveryThreadIds("continuity");
  for (const threadId of candidates) {
    yield* Effect.gen(function* () {
      const recovery = (yield* projections.getThread(threadId)).capacityRecovery;
      if (recovery?.reactionsReleased)
        yield* orchestrator.recoverCapacityContinuity(threadId, recovery.generation);
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Released capacity continuity candidate failed", { threadId, cause }),
      ),
    );
  }
});

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const scheduler = yield* Scheduler.Scheduler;
    yield* scheduler.register("model-capacity-recovery", sweep);
    yield* scheduler.register("released-capacity-continuity", releasedContinuitySweep);
  }),
).pipe(Layer.provide(IdAllocator.layer));
