import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { capacityRecoveryPresentation } from "@t3tools/client-runtime/state/capacity-recovery";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { CommandId, type EnvironmentId } from "@t3tools/contracts";
import { useState } from "react";
import { Pressable, View } from "react-native";
import { AppText as Text } from "../../components/AppText";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { uuidv4 } from "../../lib/uuid";

export function CapacityRecoveryCard({
  thread,
  environmentId,
}: {
  readonly thread: EnvironmentThreadShell;
  readonly environmentId: EnvironmentId;
}) {
  const cancelRecovery = useAtomCommand(threadEnvironment.cancelCapacityRecovery, {
    reportFailure: false,
  });
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const presentation = capacityRecoveryPresentation(thread);
  if (presentation === null) return null;
  async function cancel() {
    const recovery = thread.capacityRecovery;
    if (!recovery) return;
    setPending(true);
    setError(null);
    try {
      const result = await cancelRecovery({
        environmentId,
        input: {
          type: "thread.capacity.cancel",
          commandId: CommandId.make(uuidv4()),
          threadId: thread.id,
          generation: recovery.generation,
        },
      });
      if (result._tag === "Failure" && !isAtomCommandInterrupted(result)) {
        setError(String(squashAtomCommandFailure(result)));
      }
    } finally {
      setPending(false);
    }
  }
  return (
    <View className="mx-3 mb-2 gap-2 rounded-xl border border-warning-foreground/25 bg-screen p-3">
      <Text className="text-sm text-warning-foreground">{presentation.title}</Text>
      <Text className="text-sm text-foreground">{presentation.description}</Text>
      {presentation.canCancel ? (
        <Pressable
          accessibilityRole="button"
          disabled={pending}
          onPress={() => void cancel()}
          className="min-h-11 self-start justify-center rounded-lg bg-subtle px-3 py-2 active:opacity-70"
        >
          <Text className="text-sm text-foreground">
            {pending ? "Cancelling..." : "Cancel retry"}
          </Text>
        </Pressable>
      ) : null}
      {error ? (
        <Text accessibilityRole="alert" className="text-sm text-destructive">
          {error}
        </Text>
      ) : null}
    </View>
  );
}
