import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { capacityRecoveryPresentation } from "@t3tools/client-runtime/state/capacity-recovery";
import { RefreshCwIcon } from "lucide-react";
import { useState } from "react";
import { Button } from "../ui/button";
import type { ComposerBannerStackItem } from "./ComposerBannerStack";

export function capacityRecoveryBannerItem(
  thread: EnvironmentThreadShell,
  onCancel: () => Promise<void>,
): ComposerBannerStackItem | null {
  const presentation = capacityRecoveryPresentation(thread);
  if (presentation === null || !thread.capacityRecovery) return null;
  return {
    id: `capacity-recovery:${thread.capacityRecovery.generation}`,
    variant: "warning",
    priority: "urgent",
    icon: <RefreshCwIcon />,
    title: presentation.title,
    description: presentation.description,
    actions: presentation.canCancel ? <CancelCapacityRetry onCancel={onCancel} /> : null,
  };
}

function CancelCapacityRetry({ onCancel }: { readonly onCancel: () => Promise<void> }) {
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  async function cancel() {
    setPending(true);
    setError(null);
    try {
      await onCancel();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not cancel the capacity retry.");
    } finally {
      setPending(false);
    }
  }
  return (
    <div className="flex flex-wrap items-center gap-2">
      <Button size="xs" variant="ghost" disabled={pending} onClick={() => void cancel()}>
        {pending ? "Cancelling..." : "Cancel retry"}
      </Button>
      {error ? (
        <p role="alert" className="basis-full text-xs text-destructive">
          {error}
        </p>
      ) : null}
    </div>
  );
}
