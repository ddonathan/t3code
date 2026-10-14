import * as DateTime from "effect/DateTime";
import type { EnvironmentThreadShell } from "./models.ts";
import { threadRuntimeIsActive } from "./models.ts";

export function capacityRecoveryPresentation(
  thread: Pick<EnvironmentThreadShell, "capacityRecovery" | "latestRun" | "runtime">,
) {
  const recovery = thread.capacityRecovery;
  if (!recovery || recovery.status === "complete") return null;
  const currentRunId = thread.latestRun?.runId;
  if (currentRunId !== recovery.failedRunId && currentRunId !== recovery.retryRunId) return null;
  if (recovery.status === "pending" && recovery.nextAttemptAt !== null) {
    return {
      title: "Waiting for model capacity",
      description: `Retry ${recovery.attemptsUsed + 1} of 3 is scheduled for ${DateTime.toDateUtc(recovery.nextAttemptAt).toLocaleString()}. The selected model stays the same.`,
      canCancel: true,
      sidebarLabel: "Retry scheduled" as const,
    };
  }
  if (
    recovery.status === "retrying" &&
    recovery.retryRunId !== null &&
    thread.runtime?.activeRunId === recovery.retryRunId &&
    threadRuntimeIsActive(thread.runtime)
  ) {
    return {
      title: `Retrying the same model (${recovery.attemptsUsed} of 3)`,
      description: "Continuing the existing request after a temporary capacity failure.",
      canCancel: false,
      sidebarLabel: "Retrying" as const,
    };
  }
  if (recovery.status === "exhausted" || recovery.status === "cancelled") {
    return {
      title:
        recovery.status === "exhausted"
          ? "Automatic capacity retries exhausted"
          : "Automatic capacity retry cancelled",
      description: recovery.reason,
      canCancel: false,
      sidebarLabel: null,
    };
  }
  return null;
}
