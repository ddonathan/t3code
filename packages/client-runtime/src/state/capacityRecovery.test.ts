import { describe, expect, it } from "vite-plus/test";
import {
  CommandId,
  MessageId,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  type OrchestrationV2CapacityRecovery,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { capacityRecoveryPresentation } from "./capacityRecovery.ts";
import type { ThreadRunSummary, ThreadRuntimeSummary } from "./models.ts";

const failedId = RunId.make("capacity-failed");
const retryId = RunId.make("capacity-retry");
const recovery: OrchestrationV2CapacityRecovery = {
  generation: CommandId.make("capacity-generation"),
  originalRunId: failedId,
  authorityMessageId: MessageId.make("capacity-authority"),
  failedRunId: failedId,
  providerThreadId: ProviderThreadId.make("capacity-native-thread"),
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1-sol" },
  runtimeMode: "full-access",
  interactionMode: "default",
  reactionsReleased: false,
  retryRunId: null,
  attemptsUsed: 0,
  nextAttemptAt: DateTime.makeUnsafe("2026-10-12T12:00:00Z"),
  status: "pending",
  reason: "Temporary model capacity failure.",
};
const latestRun: ThreadRunSummary = {
  runId: failedId,
  status: "failed",
  requestedAt: null,
  startedAt: null,
  completedAt: null,
  assistantMessageId: null,
};
const runtime: ThreadRuntimeSummary = {
  status: "failed",
  activeRunId: null,
  providerInstanceId: ProviderInstanceId.make("codex"),
  providerName: null,
  lastError: null,
  updatedAt: "2026-10-12T11:00:00Z",
};

describe("capacity recovery presentation", () => {
  it("reports a bound deadline as waiting and allows pending cancellation", () => {
    expect(
      capacityRecoveryPresentation({ capacityRecovery: recovery, latestRun, runtime }),
    ).toMatchObject({
      title: "Waiting for model capacity",
      sidebarLabel: "Retry scheduled",
      canCancel: true,
    });
  });
  it("does not claim a wake when a pending record has no deadline", () => {
    expect(
      capacityRecoveryPresentation({
        capacityRecovery: { ...recovery, nextAttemptAt: null },
        latestRun,
        runtime,
      }),
    ).toBeNull();
  });
  it("does not show an older retry against a newer run", () => {
    expect(
      capacityRecoveryPresentation({
        capacityRecovery: recovery,
        latestRun: { ...latestRun, runId: RunId.make("new-human-run") },
        runtime,
      }),
    ).toBeNull();
  });
  it("calls a retry running only when its actual run is active", () => {
    const retryRecovery = {
      ...recovery,
      status: "retrying" as const,
      retryRunId: retryId,
      attemptsUsed: 1,
      nextAttemptAt: null,
    };
    const retryRun = { ...latestRun, runId: retryId };
    expect(
      capacityRecoveryPresentation({
        capacityRecovery: retryRecovery,
        latestRun: retryRun,
        runtime,
      }),
    ).toBeNull();
    expect(
      capacityRecoveryPresentation({
        capacityRecovery: retryRecovery,
        latestRun: retryRun,
        runtime: { ...runtime, status: "running", activeRunId: retryId },
      }),
    ).toMatchObject({ sidebarLabel: "Retrying", canCancel: false });
  });
  it("shows exhausted retries as stopped with no automatic cancellation action", () => {
    expect(
      capacityRecoveryPresentation({
        capacityRecovery: {
          ...recovery,
          status: "exhausted",
          attemptsUsed: 3,
          nextAttemptAt: null,
        },
        latestRun,
        runtime,
      }),
    ).toMatchObject({
      title: "Automatic capacity retries exhausted",
      sidebarLabel: null,
      canCancel: false,
    });
  });
});
