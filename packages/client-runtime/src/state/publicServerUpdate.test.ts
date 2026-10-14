import { DEFAULT_SERVER_SETTINGS, EnvironmentId, type ServerConfig } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { Atom, AtomRegistry } from "effect/reactivity";

import type { ConnectionCatalogEntry } from "../connection/catalog.ts";
import type { NetworkStatus } from "../connection/model.ts";
import * as EnvironmentRegistry from "../connection/registry.ts";
import * as Persistence from "../platform/persistence.ts";
import { runAtomCommand } from "./runtime.ts";
import { createServerEnvironmentAtoms, serverUpdateStateAtom } from "./server.ts";

const environmentId = EnvironmentId.make("patched-server");
const official = "0.0.46-nightly.20261013.3053";
const patched = "0.0.46-nightly.20261012.3021.continuity.2";

function config(
  serverVersion: string,
  serverSelfUpdate: "boot-service" | "respawn" | "desktop-managed" | undefined,
): ServerConfig {
  return {
    environment: {
      environmentId,
      label: "Patched server",
      platform: { os: "linux", arch: "x64" },
      serverVersion,
      capabilities: {
        repositoryIdentity: true,
        ...(serverSelfUpdate ? { serverSelfUpdate } : {}),
        desktopAppUpdate: true,
      },
    },
    auth: {
      policy: "loopback-browser",
      bootstrapMethods: ["one-time-token"],
      sessionMethods: ["browser-session-cookie", "bearer-access-token"],
      sessionCookieName: "t3_session",
    },
    cwd: "/workspace",
    keybindingsConfigPath: "/workspace/keybindings.json",
    keybindings: [],
    issues: [],
    providers: [],
    availableEditors: [],
    observability: {
      logsDirectoryPath: "/logs",
      localTracingEnabled: false,
      otlpTracesEnabled: false,
      otlpMetricsEnabled: false,
      otlpLogsEnabled: false,
    },
    settings: DEFAULT_SERVER_SETTINGS,
  };
}

const modes = [undefined, "boot-service", "respawn", "desktop-managed"] as const;
const cases = modes.flatMap((mode) => [
  { mode, config: config(patched, mode), reason: "patched-server" },
  { mode, config: null, reason: "unverified-server" },
  { mode, config: config("dev", mode), reason: "unverified-server" },
]);

describe("ordinary server update command", () => {
  it.effect.each(cases)("refuses before update RPC for $reason in mode $mode", (scenario) =>
    Effect.gen(function* () {
      let rpcCalls = 0;
      let cacheWrites = 0;
      const unused = () => Effect.die("unexpected registry mutation");
      const environments = EnvironmentRegistry.EnvironmentRegistry.of({
        entries: yield* SubscriptionRef.make<ReadonlyMap<EnvironmentId, ConnectionCatalogEntry>>(
          new Map(),
        ),
        networkStatus: yield* SubscriptionRef.make<NetworkStatus>("online"),
        start: Effect.void,
        register: unused,
        registerPlatform: unused,
        reconcilePlatform: unused,
        remove: unused,
        removeRoute: unused,
        reorderRoutes: unused,
        removeRelayEnvironments: unused,
        retryNow: unused,
        setEnabled: unused,
        setCompatibility: unused,
        state: unused,
        stateChanges: () => Stream.never,
        run: () =>
          Effect.sync(() => {
            rpcCalls += 1;
          }).pipe(Effect.andThen(Effect.die("unexpected update RPC"))),
        runStream: () => Stream.die("unexpected update RPC stream"),
        followStream: () => Stream.never,
      });
      const write = () =>
        Effect.sync(() => {
          cacheWrites += 1;
        });
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeedNone,
        saveShell: write,
        loadThread: () => Effect.succeedNone,
        saveThread: write,
        removeThread: write,
        loadServerConfig: () => Effect.succeedNone,
        saveServerConfig: write,
        loadVcsRefs: () => Effect.succeedNone,
        saveVcsRefs: write,
        removeVcsRefs: write,
        clearVcsRefs: write,
        clear: write,
      });
      const runtime = Atom.runtime(
        Layer.mergeAll(
          Layer.succeed(EnvironmentRegistry.EnvironmentRegistry, environments),
          Layer.succeed(Persistence.EnvironmentCacheStore, cache),
        ),
      );
      const atoms = createServerEnvironmentAtoms(runtime, {
        initialConfigValueAtom: () => Atom.make(scenario.config),
      });
      const registry = yield* Effect.acquireRelease(Effect.sync(AtomRegistry.make), (registry) =>
        Effect.sync(() => registry.dispose()),
      );
      const result = yield* Effect.promise(() =>
        runAtomCommand(
          registry,
          atoms.updateServer,
          {
            environmentId,
            input: { targetVersion: `${official}.continuity.45`, continueRunningThreads: true },
          },
          { reportFailure: false, reportDefect: false },
        ),
      );
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") {
        expect(Cause.hasDies(result.cause)).toBe(false);
        expect(Cause.squash(result.cause)).toMatchObject({
          _tag: "PublicCliUpdateBlocked",
          reason: scenario.reason,
          targetVersion: official,
        });
      }
      expect(rpcCalls).toBe(0);
      expect(cacheWrites).toBe(0);
      expect(registry.get(serverUpdateStateAtom(environmentId))).toMatchObject({
        status: "failed",
        targetVersion: official,
      });
    }),
  );
});
