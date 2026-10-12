import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { CommandId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { Alert } from "react-native";
import { ControlPillMenu } from "../../components/ControlPill";
import { ComposerInlineControl } from "../../components/ComposerToolbar";
import { uuidv4 } from "../../lib/uuid";
import { environmentThreadDetails, threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";

export function ThreadWorkContinuityMenu({ thread }: { readonly thread: EnvironmentThreadShell }) {
  const detail = useAtomValue(
    environmentThreadDetails.threadAtom({
      environmentId: thread.environmentId,
      threadId: thread.id,
    }),
  );
  const permitted = useAtomValue(
    threadEnvironment.setWorkContinuity.permissionAtom(thread.environmentId, {
      type: "thread.continuity.set",
    }),
  );
  const update = useAtomCommand(threadEnvironment.setWorkContinuity, { reportFailure: false });
  const authority = detail?.projection.messages.findLast(
    (message) =>
      message.role === "user" && message.createdBy === "user" && message.threadId === thread.id,
  );
  if (thread.continuity === undefined) return null;
  return (
    <ControlPillMenu
      accessibilityLabel="Work continuity"
      actions={[
        {
          id: "state",
          title: `Work continuity: ${thread.continuity?.status.replaceAll("_", " ") ?? "off"}`,
          attributes: { disabled: true },
        },
        {
          id: "enable",
          title: "Enable continuation checks",
          attributes: { disabled: !permitted || authority === undefined },
        },
        {
          id: "disable",
          title: "Disable continuation checks",
          attributes: { disabled: !permitted || thread.continuity == null },
        },
        ...(thread.continuity?.reason
          ? [
              {
                id: "reason",
                title: thread.continuity.reason.slice(0, 200),
                attributes: { disabled: true },
              },
            ]
          : []),
      ]}
      onPressAction={({ nativeEvent }) => {
        if (nativeEvent.event !== "enable" && nativeEvent.event !== "disable") return;
        if (nativeEvent.event === "enable" && authority === undefined) return;
        void update({
          environmentId: thread.environmentId,
          input: {
            type: "thread.continuity.set",
            commandId: CommandId.make(uuidv4()),
            threadId: thread.id,
            enabled: nativeEvent.event === "enable",
            ...(nativeEvent.event === "enable" && authority !== undefined
              ? { authorityMessageId: authority.id }
              : {}),
          },
        }).then((result) => {
          if (result._tag === "Failure") {
            const cause = Cause.squash(result.cause);
            Alert.alert(
              "Could not update work continuity",
              cause instanceof Error ? cause.message : "The setting could not be changed.",
            );
          }
        });
      }}
    >
      <ComposerInlineControl accessibilityLabel="Work continuity" label="Continuity" />
    </ControlPillMenu>
  );
}
