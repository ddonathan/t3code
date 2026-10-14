import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { approvedRuntimeClientTreeSha256 } from "./cloud/approvedRuntime.ts";
import { Launcher, readServiceState, writeServiceState } from "./serviceLauncher.ts";
import {
  compareExactServiceVersions,
  decodeServiceState,
  isExactServiceVersion,
  SERVICE_LAUNCHER_PROTOCOL,
  SERVICE_RESTART_PENDING_FILE,
  SERVICE_STOP_MARKER_FILE,
} from "./cloud/serviceProtocol.ts";

it("accepts only exact semantic versions", () => {
  for (const version of ["0.0.0", "1.2.3", "1.2.3-alpha.1", "1.2.3-0", "1.2.3+001"]) {
    assert.isTrue(isExactServiceVersion(version), version);
  }
  for (const version of ["latest", "01.2.3", "1.2.3-01", "1.2.3-alpha..1", "1.2.3+."]) {
    assert.isFalse(isExactServiceVersion(version), version);
  }
});

it("orders exact semantic versions without treating build metadata as precedence", () => {
  assert.equal(compareExactServiceVersions("1.2.3", "1.2.3"), 0);
  assert.equal(compareExactServiceVersions("1.2.4", "1.2.3"), 1);
  assert.equal(compareExactServiceVersions("2.0.0-alpha.1", "2.0.0-alpha.2"), -1);
  assert.equal(compareExactServiceVersions("2.0.0-alpha.2", "2.0.0-alpha.beta"), -1);
  assert.equal(compareExactServiceVersions("2.0.0-alpha-beta", "2.0.0-alpha-alpha"), 1);
  assert.equal(compareExactServiceVersions("2.0.0", "2.0.0-rc.1"), 1);
  assert.equal(compareExactServiceVersions("2.0.0+one", "2.0.0+two"), 0);
});

it("rejects contradictory service state", () => {
  assert.isUndefined(
    decodeServiceState({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      activeVersion: "0.0.31",
      update: {
        id: "update-1",
        fromVersion: "0.0.30",
        targetVersion: "0.0.32",
        dbPath: "/tmp/state.sqlite",
        status: "pending",
      },
    }),
  );

  assert.isUndefined(
    decodeServiceState({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      activeVersion: "1.0.0",
      update: {
        id: "update-3",
        fromVersion: "1.0.0",
        targetVersion: "1.1.0",
        status: "pending",
      },
    }),
  );

  assert.isUndefined(
    decodeServiceState({
      protocol: SERVICE_LAUNCHER_PROTOCOL,
      activeVersion: "1.0.0",
      update: {
        id: "update-2",
        fromVersion: "1.0.0",
        targetVersion: "0.9.0",
        dbPath: "/tmp/state.sqlite",
        status: "pending",
      },
    }),
  );
});

// A pinned runtime is an executable at <versionDir>/t3. The tests stand one up
// as a Node shebang script so the launcher spawns it the way it spawns the
// real single-executable, IPC channel included.
const writeFakeRuntime = (
  fs: FileSystem.FileSystem,
  path: Path.Path,
  versionDir: string,
  childSource: string,
) =>
  Effect.gen(function* () {
    const entryPath = path.join(versionDir, "t3");
    yield* fs.makeDirectory(versionDir, { recursive: true });
    yield* fs.writeFileString(entryPath, `#!${process.execPath}\n${childSource}`);
    yield* fs.chmod(entryPath, 0o755);
    yield* fs.writeFileString(
      path.join(versionDir, ".install-complete"),
      `${path.basename(versionDir)}\n`,
    );
    return entryPath;
  });

const approveFakeRuntimes = (
  fs: FileSystem.FileSystem,
  path: Path.Path,
  root: string,
  versions: string[],
) =>
  Effect.gen(function* () {
    const sha = (text: string) =>
      Effect.promise(() => crypto.subtle.digest("SHA-256", new TextEncoder().encode(text))).pipe(
        Effect.map((digest) =>
          Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join(""),
        ),
      );
    const arch = yield* HostProcess.Architecture;
    const builds = [];
    for (const version of versions) {
      const directory = path.join(root, "runtime", "versions", version);
      yield* fs.makeDirectory(path.join(directory, "client"), { recursive: true });
      yield* fs.writeFileString(path.join(directory, "client", "index.html"), "patched client");
      yield* fs.writeFileString(
        path.join(directory, ".archive-sha256"),
        yield* sha("approved archive"),
      );
      builds.push({
        version,
        platform: "linux",
        arch,
        archiveSha256: yield* sha("approved archive"),
        executableSha256: yield* sha(yield* fs.readFileString(path.join(directory, "t3"))),
        clientTreeSha256: yield* Effect.promise(() =>
          approvedRuntimeClientTreeSha256(path.join(directory, "client")),
        ),
      });
    }
    yield* fs.writeFileString(
      path.join(root, "runtime", "approved-builds.json"),
      JSON.stringify({ format: 1, builds }),
    );
  });

it.layer(NodeServices.layer)("service state persistence", (it) => {
  it.effect("durably replaces and strictly reads one state document", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-test-" });
      const statePath = path.join(root, "runtime", "service-state.json");
      const state = {
        protocol: SERVICE_LAUNCHER_PROTOCOL,
        activeVersion: "0.0.31",
      } as const;

      yield* Effect.promise(() => writeServiceState(statePath, state));
      assert.deepEqual(yield* Effect.promise(() => readServiceState(statePath)), state);
    }),
  );

  it.effect("a fresh launcher clears a restart deferred by t3 update", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-restart-" });
      const statePath = path.join(root, "runtime", "service-state.json");
      const restartPending = path.join(root, "runtime", SERVICE_RESTART_PENDING_FILE);
      yield* writeFakeRuntime(
        fs,
        path,
        path.join(root, "runtime", "versions", "1.0.0"),
        "setInterval(() => {}, 1_000);\n",
      );
      yield* Effect.promise(() =>
        writeServiceState(statePath, {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
        }),
      );
      const run = () =>
        Effect.gen(function* () {
          const launcher = new Launcher(
            root,
            yield* Effect.promise(() => readServiceState(statePath)),
          );
          const running = launcher.run();
          yield* Effect.promise(() => launcher.stop("SIGTERM"));
          yield* Effect.promise(() => running);
        });

      // A launcher that is still the old version leaves a marker that waits
      // for a newer one.
      yield* fs.writeFileString(restartPending, "1.0.1\n");
      yield* run();
      assert.isTrue(yield* fs.exists(restartPending));

      // Whoever restarted the service, the launcher now runs what the unit
      // names, so the deferred-restart marker is gone.
      yield* fs.writeFileString(restartPending, "1.0.0\n");
      yield* run();
      assert.isFalse(yield* fs.exists(restartPending));
    }),
  );

  it.effect("serializes shutdown with launcher recovery", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-stop-" });
      const statePath = path.join(root, "runtime", "service-state.json");
      yield* writeFakeRuntime(
        fs,
        path,
        path.join(root, "runtime", "versions", "1.0.0"),
        "setInterval(() => {}, 1_000);\n",
      );
      yield* Effect.promise(() =>
        writeServiceState(statePath, {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
        }),
      );

      const launcher = new Launcher(root, yield* Effect.promise(() => readServiceState(statePath)));
      const running = launcher.run();
      const stopping = launcher.stop("SIGTERM");
      // An explicit stop leaves the marker that tells a child shutting down
      // mid-update that no replacement server is coming. It is present as
      // soon as stop() returns its promise, before queued transitions run.
      assert.isTrue(yield* fs.exists(path.join(root, "runtime", SERVICE_STOP_MARKER_FILE)));
      yield* Effect.promise(() => stopping);
      yield* Effect.promise(() => running);
    }),
  );

  it.effect.each(["unknown-forward", "corrupt-policy"])(
    "guard refusal leaves launcher selection and activation files unchanged (%s)",
    (scenario) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-launcher-guard-" });
        const statePath = path.join(root, "runtime", "service-state.json");
        const reasonPath = path.join(root, "refusal.txt");
        const childSource = `
process.send({type: "request-update", targetVersion: "1.1.0", dbPath: ${JSON.stringify(path.join(root, "database"))}});
process.on("message", message => {
  if (message.type === "update-rejected") {
    require("node:fs").writeFileSync(${JSON.stringify(reasonPath)}, message.reason);
    process.exit(0);
  }
});
`;
        for (const version of ["1.0.0", "1.1.0"])
          yield* writeFakeRuntime(
            fs,
            path,
            path.join(root, "runtime", "versions", version),
            childSource,
          );
        yield* approveFakeRuntimes(fs, path, root, ["1.0.0"]);
        yield* Effect.promise(() =>
          writeServiceState(statePath, {
            protocol: SERVICE_LAUNCHER_PROTOCOL,
            activeVersion: "1.0.0",
          }),
        );
        const before = yield* fs.readFileString(statePath);
        if (scenario === "corrupt-policy") {
          yield* fs.writeFileString(path.join(root, "runtime", "approved-builds.json"), "{");
          yield* fs.writeFileString(
            path.join(root, "runtime", SERVICE_STOP_MARKER_FILE),
            "keep marker",
          );
          yield* fs.writeFileString(
            path.join(root, "runtime", SERVICE_RESTART_PENDING_FILE),
            "1.0.0",
          );
        }
        const launcher = new Launcher(
          root,
          yield* Effect.promise(() => readServiceState(statePath)),
        );
        const error = yield* Effect.promise(() =>
          launcher.run().then(
            () => undefined,
            (cause: unknown) => cause,
          ),
        );
        assert.instanceOf(error, Error);
        assert.equal(yield* fs.readFileString(statePath), before);
        assert.isFalse(yield* fs.exists(path.join(root, "runtime", "db-backup")));
        if (scenario === "unknown-forward") {
          assert.include(yield* fs.readFileString(reasonPath), "not uniquely approved");
          assert.include(yield* fs.readFileString(reasonPath), "approved-builds.json");
        } else {
          assert.include(String(error), "invalid JSON");
          assert.equal(
            yield* fs.readFileString(path.join(root, "runtime", SERVICE_STOP_MARKER_FILE)),
            "keep marker",
          );
          assert.equal(
            yield* fs.readFileString(path.join(root, "runtime", SERVICE_RESTART_PENDING_FILE)),
            "1.0.0",
          );
          assert.isFalse(yield* fs.exists(reasonPath));
        }
      }),
  );

  it.effect.each([
    "plain",
    "deferred",
    "unrelated-marker",
    "unknown-keys",
    "pending",
    "protocol4",
    "corrupt-policy",
    "own-bytes",
    "own-client",
    "missing-receipt",
    "outside-executable",
    "duplicate-selection",
    "missing-sentinel",
  ])(
    "stable launcher recovers only an exact legacy selection to its own verified runtime (%s)",
    (scenario) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-launcher-legacy-recovery-" });
        const statePath = path.join(root, "runtime", "service-state.json");
        const outcomePath = path.join(root, "started.json");
        const officialPath = path.join(root, "official-spawned");
        const ownDir = path.join(root, "runtime", "versions", "1.0.0");
        const outsideExecutable = path.join(root, "outside-t3");
        const ownExecutable = yield* writeFakeRuntime(
          fs,
          path,
          ownDir,
          `
const fs = require("node:fs");
fs.writeFileSync(${JSON.stringify(outcomePath)}, JSON.stringify(JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT)));
process.exit(0);
`,
        );
        yield* writeFakeRuntime(
          fs,
          path,
          path.join(root, "runtime", "versions", "1.1.0"),
          `require("node:fs").writeFileSync(${JSON.stringify(officialPath)}, "unapproved spawn"); process.exit(0);`,
        );
        yield* approveFakeRuntimes(
          fs,
          path,
          root,
          scenario === "duplicate-selection" ? ["1.0.0", "1.1.0", "1.1.0"] : ["1.0.0"],
        );
        const rawState = {
          protocol: scenario === "protocol4" ? 4 : SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.1.0",
          ...(scenario === "unknown-keys" ? { extra: "unknown" } : {}),
          ...(scenario === "pending"
            ? {
                update: {
                  id: "legacy-pending",
                  fromVersion: "1.1.0",
                  targetVersion: "2.0.0",
                  dbPath: path.join(root, "database"),
                  status: "pending",
                },
              }
            : {}),
        };
        const before = JSON.stringify(rawState) + "\n";
        yield* fs.writeFileString(statePath, before);
        const stopMarker = path.join(root, "runtime", SERVICE_STOP_MARKER_FILE);
        const pendingMarker = path.join(root, "runtime", SERVICE_RESTART_PENDING_FILE);
        yield* fs.writeFileString(stopMarker, "preserve on refusal");
        const markerVersion = scenario === "unrelated-marker" ? "1.0.0" : "1.1.0";
        yield* fs.writeFileString(pendingMarker, markerVersion);
        if (scenario === "corrupt-policy")
          yield* fs.writeFileString(path.join(root, "runtime", "approved-builds.json"), "{");
        if (scenario === "own-bytes")
          yield* fs.writeFileString(ownExecutable, "unapproved own executable");
        if (scenario === "own-client")
          yield* fs.writeFileString(
            path.join(ownDir, "client", "index.html"),
            "unapproved own client",
          );
        if (scenario === "missing-receipt") yield* fs.remove(path.join(ownDir, ".archive-sha256"));
        if (scenario === "outside-executable")
          yield* fs.writeFileString(outsideExecutable, yield* fs.readFileString(ownExecutable));
        if (scenario === "missing-sentinel")
          yield* fs.remove(path.join(ownDir, ".install-complete"));
        const attempt = yield* Effect.promise(async () => {
          try {
            const state = await readServiceState(statePath);
            await new Launcher(
              root,
              state,
              scenario === "outside-executable" ? outsideExecutable : ownExecutable,
            ).run();
            return undefined;
          } catch (cause) {
            return cause;
          }
        });
        const succeeds =
          scenario === "plain" || scenario === "deferred" || scenario === "unrelated-marker";
        assert.isFalse(yield* fs.exists(officialPath));
        if (!succeeds) {
          assert.instanceOf(attempt, Error);
          assert.equal(yield* fs.readFileString(statePath), before);
          assert.equal(yield* fs.readFileString(stopMarker), "preserve on refusal");
          assert.equal(yield* fs.readFileString(pendingMarker), markerVersion);
          assert.isFalse(yield* fs.exists(outcomePath));
          return;
        }
        assert.include(String(attempt), "Active child exited unexpectedly (0)");
        assert.deepEqual(yield* Effect.promise(() => readServiceState(statePath)), {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
        });
        const observed: unknown = JSON.parse(yield* fs.readFileString(outcomePath));
        assert.deepEqual(observed, { protocol: SERVICE_LAUNCHER_PROTOCOL, childVersion: "1.0.0" });
        assert.isFalse(yield* fs.exists(stopMarker));
        if (scenario === "unrelated-marker")
          assert.equal(yield* fs.readFileString(pendingMarker), markerVersion);
        else assert.isFalse(yield* fs.exists(pendingMarker));
      }),
  );

  it.effect.each(["recovery", "before-trial", "start-trial", "prepared", "rollback-refused"])(
    "refused pending target preserves approved selection without a pending restart loop (%s)",
    (phase) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-launcher-pending-refusal-" });
        const statePath = path.join(root, "runtime", "service-state.json");
        const databasePath = path.join(root, "userdata", "state.sqlite");
        const outcomePath = path.join(root, "observed-outcome.json");
        const previousClient = path.join(
          root,
          "runtime",
          "versions",
          "1.0.0",
          "client",
          "index.html",
        );
        const targetClient = path.join(
          root,
          "runtime",
          "versions",
          "1.1.0",
          "client",
          "index.html",
        );
        yield* fs.makeDirectory(path.dirname(databasePath), { recursive: true });
        yield* fs.writeFileString(databasePath, "original database");
        const source = `
const fs = require("node:fs");
const context = JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
const phase = ${JSON.stringify(phase)};
const record = (extra = {}) => {
  fs.writeFileSync(${JSON.stringify(outcomePath)}, JSON.stringify({ version: context.childVersion, status: context.update?.status, ...extra }));
  process.exit(0);
};
if (context.update?.status === "pending") {
  fs.writeFileSync(${JSON.stringify(databasePath)}, "trial database");
  fs.writeFileSync(${JSON.stringify(databasePath + "-wal")}, "trial wal");
  fs.writeFileSync(${JSON.stringify(databasePath + "-shm")}, "trial shm");
  fs.writeFileSync(${JSON.stringify(targetClient)}, "target changed after spawn");
  process.send({ type: "prepared", updateId: context.update.id });
} else if (context.update !== undefined) {
  record();
} else {
  let acceptedPid;
  process.on("message", message => {
    if (message.type === "update-accepted") {
      acceptedPid = process.pid;
      if (phase === "before-trial" || phase === "start-trial") {
        fs.writeFileSync(${JSON.stringify(targetClient)}, "target changed after acceptance");
        if (phase === "start-trial") process.exit(0);
      }
    } else if (message.type === "update-rejected") {
      record({ acceptedPid, refusedPid: process.pid, reason: message.reason });
    }
  });
  process.send({ type: "request-update", targetVersion: "1.1.0", dbPath: ${JSON.stringify(databasePath)} });
}
`;
        for (const version of ["1.0.0", "1.1.0"])
          yield* writeFakeRuntime(
            fs,
            path,
            path.join(root, "runtime", "versions", version),
            source,
          );
        yield* approveFakeRuntimes(fs, path, root, ["1.0.0", "1.1.0"]);
        const initial =
          phase === "recovery" || phase === "rollback-refused"
            ? {
                protocol: SERVICE_LAUNCHER_PROTOCOL,
                activeVersion: "1.0.0",
                update: {
                  id: "pending-test",
                  fromVersion: "1.0.0",
                  targetVersion: "1.1.0",
                  dbPath: databasePath,
                  status: "pending" as const,
                },
              }
            : { protocol: SERVICE_LAUNCHER_PROTOCOL, activeVersion: "1.0.0" };
        yield* Effect.promise(() => writeServiceState(statePath, initial));
        const before = yield* fs.readFileString(statePath);
        if (phase === "recovery") {
          const backupDir = path.join(root, "runtime", "db-backup", "pending-test");
          yield* fs.makeDirectory(backupDir, { recursive: true });
          yield* fs.writeFileString(path.join(backupDir, "database"), "original database");
          yield* fs.writeFileString(databasePath, "interrupted trial database");
          yield* fs.writeFileString(`${databasePath}-wal`, "interrupted wal");
          yield* fs.writeFileString(targetClient, "target changed while stopped");
        } else if (phase === "rollback-refused")
          yield* fs.writeFileString(previousClient, "unapproved rollback");
        const launcher = new Launcher(
          root,
          yield* Effect.promise(() => readServiceState(statePath)),
        );
        const error = yield* Effect.promise(() =>
          launcher.run().then(
            () => undefined,
            (cause: unknown) => cause,
          ),
        );
        if (phase === "rollback-refused") {
          assert.include(String(error), "client tree SHA256");
          assert.equal(yield* fs.readFileString(statePath), before);
          assert.isFalse(yield* fs.exists(outcomePath));
          assert.equal(yield* fs.readFileString(databasePath), "original database");
          return;
        }
        // The fixture exits deliberately after observing recovery/rejection. A
        // guard fatal error would not produce this outcome and would remain pending.
        assert.include(String(error), "Active child exited unexpectedly (0)");
        const state = yield* Effect.promise(() => readServiceState(statePath));
        assert.equal(state.activeVersion, "1.0.0");
        assert.equal(state.update?.status, "failed");
        assert.equal(
          state.update?.status === "failed" ? state.update.reason : undefined,
          "target-not-approved",
        );
        const outcome: unknown = JSON.parse(yield* fs.readFileString(outcomePath));
        assert.isObject(outcome);
        assert.propertyVal(outcome, "version", "1.0.0");
        if (phase === "before-trial") {
          assert.isObject(outcome);
          if (
            typeof outcome !== "object" ||
            outcome === null ||
            !("acceptedPid" in outcome) ||
            !("refusedPid" in outcome)
          )
            return yield* Effect.die("missing same-child refusal proof");
          assert.equal(outcome.acceptedPid, outcome.refusedPid);
          if (!("reason" in outcome)) return yield* Effect.die("missing refusal reason");
          assert.include(String(outcome.reason), "client tree SHA256");
        }
        assert.equal(yield* fs.readFileString(databasePath), "original database");
        assert.isFalse(yield* fs.exists(`${databasePath}-wal`));
        assert.isFalse(yield* fs.exists(`${databasePath}-shm`));
        const backupRoot = path.join(root, "runtime", "db-backup");
        if (yield* fs.exists(backupRoot)) assert.deepEqual(yield* fs.readDirectory(backupRoot), []);
        // A fresh launcher sees terminal state, never retries the refused target.
        yield* fs.remove(outcomePath);
        const restarted = new Launcher(root, state);
        const restartError = yield* Effect.promise(() =>
          restarted.run().then(
            () => undefined,
            (cause: unknown) => cause,
          ),
        );
        assert.include(String(restartError), "Active child exited unexpectedly (0)");
        assert.equal(yield* fs.readFileString(statePath), JSON.stringify(state, null, 2) + "\n");
        const restartedOutcome: unknown = JSON.parse(yield* fs.readFileString(outcomePath));
        assert.propertyVal(restartedOutcome, "version", "1.0.0");
        assert.propertyVal(restartedOutcome, "status", "failed");
      }),
  );

  it.effect.each([false, true])(
    "commits only after the trial reports prepared (approved policy: %s)",
    (guarded) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-flow-" });
        const statePath = path.join(root, "runtime", "service-state.json");
        const databasePath = path.join(root, "userdata", "state.sqlite");
        yield* fs.makeDirectory(path.dirname(databasePath), { recursive: true });
        yield* fs.writeFileString(databasePath, "before trial");
        const encodedDatabasePath = JSON.stringify(databasePath);
        const childSource = `
const context = JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
if (context.update?.status === "pending") {
  process.send({ type: "prepared", updateId: context.update.id });
  process.on("message", (message) => {
    if (message.type === "committed") process.exit(0);
  });
} else if (context.update === undefined) {
  process.send({ type: "request-update", targetVersion: "1.1.0", dbPath: ${encodedDatabasePath} });
  setInterval(() => {}, 1_000);
} else {
  process.exit(0);
}
`;
        for (const version of ["1.0.0", "1.1.0"]) {
          yield* writeFakeRuntime(
            fs,
            path,
            path.join(root, "runtime", "versions", version),
            childSource,
          );
        }
        if (guarded) yield* approveFakeRuntimes(fs, path, root, ["1.0.0", "1.1.0"]);
        yield* Effect.promise(() =>
          writeServiceState(statePath, {
            protocol: SERVICE_LAUNCHER_PROTOCOL,
            activeVersion: "1.0.0",
          }),
        );

        const launcher = new Launcher(
          root,
          yield* Effect.promise(() => readServiceState(statePath)),
        );
        yield* Effect.promise(() =>
          launcher.run().then(
            () => Promise.reject(new Error("launcher unexpectedly completed")),
            () => Promise.resolve(),
          ),
        );

        const state = yield* Effect.promise(() => readServiceState(statePath));
        assert.equal(state.activeVersion, "1.1.0");
        assert.equal(state.update?.status, "committed");
      }),
  );

  it.effect.each([false, true])(
    "rolls back a trial that reports the wrong update ID (approved policy: %s)",
    (guarded) =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-rollback-" });
        const statePath = path.join(root, "runtime", "service-state.json");
        const databasePath = path.join(root, "userdata", "state.sqlite");
        yield* fs.makeDirectory(path.dirname(databasePath), { recursive: true });
        yield* fs.writeFileString(databasePath, "before trial");
        const encodedDatabasePath = JSON.stringify(databasePath);
        const childSource = `
const context = JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
if (context.update?.status === "pending") {
  process.send({ type: "prepared", updateId: "wrong-update" });
} else if (context.update === undefined) {
  process.send({ type: "request-update", targetVersion: "1.1.0", dbPath: ${encodedDatabasePath} });
  setInterval(() => {}, 1_000);
} else {
  process.exit(0);
}
`;
        for (const version of ["1.0.0", "1.1.0"]) {
          yield* writeFakeRuntime(
            fs,
            path,
            path.join(root, "runtime", "versions", version),
            childSource,
          );
        }
        if (guarded) yield* approveFakeRuntimes(fs, path, root, ["1.0.0", "1.1.0"]);
        yield* Effect.promise(() =>
          writeServiceState(statePath, {
            protocol: SERVICE_LAUNCHER_PROTOCOL,
            activeVersion: "1.0.0",
          }),
        );

        const launcher = new Launcher(
          root,
          yield* Effect.promise(() => readServiceState(statePath)),
        );
        yield* Effect.promise(() =>
          launcher.run().then(
            () => Promise.reject(new Error("launcher unexpectedly completed")),
            () => Promise.resolve(),
          ),
        );

        const state = yield* Effect.promise(() => readServiceState(statePath));
        assert.equal(state.activeVersion, "1.0.0");
        assert.equal(state.update?.status, "rolled-back");
        assert.equal(
          state.update?.status === "rolled-back" ? state.update.reason : undefined,
          "invalid-prepared",
        );
      }),
  );

  it.effect("restores the database when a migrating trial exits", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-service-launcher-db-" });
      const statePath = path.join(root, "runtime", "service-state.json");
      const databasePath = path.join(root, "userdata", "state.sqlite");
      const original = "database before migration";
      yield* fs.makeDirectory(path.dirname(databasePath), { recursive: true });
      yield* fs.writeFileString(databasePath, original);
      const encodedDatabasePath = JSON.stringify(databasePath);
      const childSource = `
import { writeFileSync } from "node:fs";
const context = JSON.parse(process.env.T3_SERVICE_LAUNCHER_CONTEXT);
if (context.update?.status === "pending") {
  writeFileSync(context.update.dbPath, "database after migration");
  writeFileSync(context.update.dbPath + "-wal", "trial wal");
  writeFileSync(context.update.dbPath + "-shm", "trial shm");
  process.exit(1);
} else if (context.update === undefined) {
  process.send({ type: "request-update", targetVersion: "1.1.0", dbPath: ${encodedDatabasePath} });
  setInterval(() => {}, 1_000);
} else {
  process.exit(0);
}
`;
      for (const version of ["1.0.0", "1.1.0"]) {
        yield* writeFakeRuntime(
          fs,
          path,
          path.join(root, "runtime", "versions", version),
          childSource,
        );
      }
      yield* Effect.promise(() =>
        writeServiceState(statePath, {
          protocol: SERVICE_LAUNCHER_PROTOCOL,
          activeVersion: "1.0.0",
        }),
      );

      const launcher = new Launcher(root, yield* Effect.promise(() => readServiceState(statePath)));
      yield* Effect.promise(() =>
        launcher.run().then(
          () => Promise.reject(new Error("launcher unexpectedly completed")),
          () => Promise.resolve(),
        ),
      );

      const state = yield* Effect.promise(() => readServiceState(statePath));
      assert.equal(state.activeVersion, "1.0.0");
      assert.equal(state.update?.status, "rolled-back");
      assert.equal(yield* fs.readFileString(databasePath), original);
      assert.isFalse(yield* fs.exists(`${databasePath}-wal`));
      assert.isFalse(yield* fs.exists(`${databasePath}-shm`));
      const updateId = state.update?.id;
      assert.isDefined(updateId);
      assert.isFalse(yield* fs.exists(path.join(root, "runtime", "db-backup", updateId)));
    }),
  );
});
