import { EnvironmentId } from "@t3tools/contracts";
import type { ServerUpdateState } from "@t3tools/client-runtime/state/server";
import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

// Pinned so the direction cases below read as fixed versions instead of
// arithmetic on whatever version this checkout happens to be at.
const branding = vi.hoisted(() => ({ APP_VERSION: "0.0.34" }));
vi.mock("./branding", () => branding);

import { APP_VERSION } from "./branding";
import {
  appendVersionMismatchHint,
  buildVersionMismatchDismissalKey,
  dismissServerUpdateFailure,
  dismissVersionMismatch,
  isServerUpdateFailureDismissed,
  isVersionMismatchDismissed,
  manualServerUpdateCommand,
  resolveServerConfigVersionMismatch,
  resolveServerSelfUpdateCapability,
  resolveVersionMismatch,
  serverUpdateGuidance,
  supportsDesktopAppUpdate,
} from "./versionSkew";

const MISMATCH_HINT =
  "Version mismatch. Try syncing the client and server to the same T3 Code version.";

describe("versionSkew", () => {
  it("compares local continuity revisions by their published nightly baseline", () => {
    const official = "0.0.46-nightly.20261013.3053";
    const local = `${official}.continuity.45`;
    expect(resolveVersionMismatch(official, local)).toBeNull();
    expect(resolveVersionMismatch(local, official)).toBeNull();
    expect(resolveVersionMismatch(`${official}.continuity.1`, local)).toBeNull();
    expect(resolveVersionMismatch("0.0.46-nightly.20261014.3054", local)).toBeNull();
  });

  it("keeps the local client identity but requests a published version for an older server", () => {
    const official = "0.0.46-nightly.20261013.3053";
    const server = "0.0.46-nightly.20261012.3021.continuity.2";
    const mismatch = resolveVersionMismatch(server, `${official}.continuity.45`);
    expect(mismatch).toEqual({
      clientVersion: `${official}.continuity.45`,
      serverVersion: server,
      updateTargetVersion: official,
      hint: MISMATCH_HINT,
    });
    expect(manualServerUpdateCommand(official, undefined, server)).toBeNull();
    expect(
      manualServerUpdateCommand(
        `${official}.continuity.45`,
        undefined,
        "0.0.46-nightly.20261012.3021",
      ),
    ).toBe(`npx t3@${official}`);
  });

  it("updates only the proven npm prefix and safely quotes its path", () => {
    expect(
      manualServerUpdateCommand("0.0.45", { kind: "npm-global", prefix: "/opt/node" }, "0.0.44"),
    ).toBe("npm install --global --prefix '/opt/node' t3@0.0.45");
    expect(
      manualServerUpdateCommand(
        "0.0.45",
        { kind: "npm-global", prefix: "/opt/maria's node" },
        "0.0.44",
      ),
    ).toBe("npm install --global --prefix '/opt/maria'\\''s node' t3@0.0.45");
  });

  it("keeps runner and unknown commands as relaunches", () => {
    expect(manualServerUpdateCommand("0.0.45", undefined, "0.0.44")).toBe("npx t3@0.0.45");
    expect(manualServerUpdateCommand("0.0.45", { kind: "npx" }, "0.0.44")).toBe("npx t3@0.0.45");
    expect(manualServerUpdateCommand("0.0.45", { kind: "pnpm-dlx" }, "0.0.44")).toBe(
      "pnpm dlx t3@0.0.45",
    );
    expect(manualServerUpdateCommand("0.0.45", { kind: "bunx" }, "0.0.44")).toBe("bunx t3@0.0.45");
  });
  it.each([undefined, "0.0.46-nightly.20261012.3021.continuity.2", "dev"])(
    "never emits an npm replacement command for an unverified or patched server (%s)",
    (serverVersion) => {
      for (const installation of [
        undefined,
        { kind: "npx" },
        { kind: "pnpm-dlx" },
        { kind: "bunx" },
        { kind: "npm-global", prefix: "/opt/node" },
      ] as const) {
        expect(
          manualServerUpdateCommand("0.0.46-nightly.20261013.3053", installation, serverVersion),
        ).toBeNull();
      }
    },
  );
  beforeEach(() => {
    branding.APP_VERSION = "0.0.34";
  });

  it("dismisses only the current failed attempt without clearing its retry state", () => {
    const failure = {
      status: "failed",
      stage: "downloading",
      fromVersion: "0.0.33",
      targetVersion: "0.0.34",
      message: "Download failed.",
    } as const satisfies ServerUpdateState;
    const retryFailure = { ...failure };
    const otherEnvironmentFailure = { ...failure };

    dismissServerUpdateFailure(failure);

    expect(isServerUpdateFailureDismissed(failure)).toBe(true);
    expect(failure.status).toBe("failed");
    expect(failure.message).toBe("Download failed.");
    expect(isServerUpdateFailureDismissed(retryFailure)).toBe(false);
    expect(isServerUpdateFailureDismissed(otherEnvironmentFailure)).toBe(false);
  });

  it("does not dismiss an update that is still running", () => {
    const running = {
      status: "running",
      stage: "resuming",
      fromVersion: "0.0.33",
      targetVersion: "0.0.34",
    } as const satisfies ServerUpdateState;

    dismissServerUpdateFailure(running);

    expect(isServerUpdateFailureDismissed(running)).toBe(false);
  });

  it("does not warn when versions match", () => {
    expect(resolveVersionMismatch(APP_VERSION)).toBeNull();
  });

  it("returns a mismatch when the server is behind the client", () => {
    expect(resolveVersionMismatch("0.0.33")).toEqual({
      clientVersion: "0.0.34",
      serverVersion: "0.0.33",
      hint: MISMATCH_HINT,
    });
  });

  it("does not warn when the server is ahead of the client", () => {
    expect(resolveVersionMismatch("0.0.35")).toBeNull();
    expect(resolveVersionMismatch("9.9.9")).toBeNull();
  });

  it("does not warn when a nightly and a stable build share a core version", () => {
    expect(resolveVersionMismatch("0.0.34-nightly.20260818.1124")).toBeNull();

    branding.APP_VERSION = "0.0.34-nightly.20260818.1124";
    expect(resolveVersionMismatch("0.0.34")).toBeNull();
  });

  it.each(["0.0.34-nightly.20260823.1124", "0.0.34-nightly.20260824.1124"])(
    "warns when nightly server %s is behind a nightly client on the same release",
    (serverVersion) => {
      branding.APP_VERSION = "0.0.34-nightly.20260824.1125";

      expect(resolveVersionMismatch(serverVersion)).toEqual({
        clientVersion: "0.0.34-nightly.20260824.1125",
        serverVersion,
        hint: MISMATCH_HINT,
      });
    },
  );

  it("does not warn when a nightly server is ahead on the same release", () => {
    branding.APP_VERSION = "0.0.34-nightly.20260824.1125";

    expect(resolveVersionMismatch("0.0.34-nightly.20260824.1126")).toBeNull();
  });

  it("treats a nightly server built past the client as ahead, not skew", () => {
    expect(resolveVersionMismatch("0.0.35-nightly.20260818.1124")).toBeNull();
  });

  it("still warns when a nightly client outruns the server by a release", () => {
    branding.APP_VERSION = "0.0.35-nightly.20260818.1124";

    expect(resolveVersionMismatch("0.0.34")).toEqual({
      clientVersion: "0.0.35-nightly.20260818.1124",
      serverVersion: "0.0.34",
      hint: MISMATCH_HINT,
    });
  });

  it("falls back to string inequality when a version is not semver", () => {
    expect(resolveVersionMismatch("dev")).toEqual({
      clientVersion: "0.0.34",
      serverVersion: "dev",
      hint: MISMATCH_HINT,
    });

    branding.APP_VERSION = "dev";
    expect(resolveVersionMismatch("dev")).toBeNull();
    expect(resolveVersionMismatch("0.0.34")).toMatchObject({ serverVersion: "0.0.34" });
  });

  it("reads the server version from config descriptors", () => {
    expect(
      resolveServerConfigVersionMismatch({
        environment: {
          environmentId: EnvironmentId.make("environment-1"),
          label: "Remote",
          platform: {
            os: "darwin",
            arch: "arm64",
          },
          serverVersion: "0.0.33",
          capabilities: {
            repositoryIdentity: true,
          },
        },
      }),
    ).toMatchObject({
      serverVersion: "0.0.33",
    });
  });

  it("keys dismissals by environment, client version, and server version", () => {
    const environmentId = EnvironmentId.make("environment-dismissal");
    const key = buildVersionMismatchDismissalKey(environmentId, {
      clientVersion: APP_VERSION,
      serverVersion: "9.9.9",
    });

    expect(key).toBe(`${environmentId}:${APP_VERSION}:9.9.9`);
    expect(isVersionMismatchDismissed(key)).toBe(false);

    dismissVersionMismatch(key);

    expect(isVersionMismatchDismissed(key)).toBe(true);
    expect(
      isVersionMismatchDismissed(
        buildVersionMismatchDismissalKey(environmentId, {
          clientVersion: APP_VERSION,
          serverVersion: "9.9.10",
        }),
      ),
    ).toBe(false);
  });

  it("appends a hint to connection errors when the server is behind", () => {
    const mismatch = resolveVersionMismatch("0.0.33");

    expect(appendVersionMismatchHint("Socket closed.", mismatch)).toBe(
      `Socket closed. Hint: ${MISMATCH_HINT}`,
    );
  });

  it("reads desktop-managed update capabilities from config descriptors", () => {
    expect(
      resolveServerSelfUpdateCapability({
        environment: {
          environmentId: EnvironmentId.make("environment-desktop"),
          label: "Desktop",
          platform: { os: "darwin", arch: "arm64" },
          serverVersion: "9.9.9",
          capabilities: {
            repositoryIdentity: true,
            serverSelfUpdate: "desktop-managed",
          },
        },
      }),
    ).toBe("desktop-managed");
    expect(resolveServerSelfUpdateCapability(null)).toBeNull();
  });

  it("detects remote desktop-app update support from config descriptors", () => {
    const descriptor = (desktopAppUpdate?: boolean) => ({
      environment: {
        environmentId: EnvironmentId.make("environment-desktop"),
        label: "Desktop",
        platform: { os: "darwin", arch: "arm64" } as const,
        serverVersion: "9.9.9",
        capabilities: {
          repositoryIdentity: true,
          serverSelfUpdate: "desktop-managed" as const,
          ...(desktopAppUpdate === undefined ? {} : { desktopAppUpdate }),
        },
      },
    });

    expect(supportsDesktopAppUpdate(descriptor(true))).toBe(true);
    expect(supportsDesktopAppUpdate(descriptor(false))).toBe(false);
    expect(supportsDesktopAppUpdate(descriptor())).toBe(false);
    expect(supportsDesktopAppUpdate(null)).toBe(false);
  });

  it("matches version-drift guidance to the advertised update path", () => {
    expect(serverUpdateGuidance("respawn")).toBe("Update to stay in sync");
    expect(serverUpdateGuidance("desktop-managed")).toBe("Update the desktop app");
  });
});
