import type { OrchestrationV2ProviderFailure } from "@t3tools/contracts";

export const CAPACITY_RECOVERY_MAX_ATTEMPTS = 3;
export function isModelCapacityFailure(failure: OrchestrationV2ProviderFailure | null): boolean {
  return failure?.class === "provider_error" && failure.code === "serverOverloaded";
}
export function capacityRetryDelayMs(originRunId: string, attemptsUsed: number): number {
  const floorMs = attemptsUsed === 0 ? 15_000 : attemptsUsed === 1 ? 30_000 : 60_000;
  let jitter = 0;
  for (const character of originRunId) jitter = (jitter * 31 + character.charCodeAt(0)) % 3_000;
  return floorMs + jitter;
}
