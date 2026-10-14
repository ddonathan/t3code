import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  SourceControlProviderKind,
  ThreadId,
  type PullRequestDetail,
  type OrchestrationV2WorkContinuity,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as PullRequestWatchReactor from "./PullRequestWatchReactor.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" };
const database = SqlitePersistence.layerMemory;
const layerTest = Layer.mergeAll(
  database,
  ProjectionStore.layer.pipe(Layer.provide(database)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "pr-merge-completion" },
    ProviderAdapterRegistry.layerFromAdapters([
      {
        instanceId: modelSelection.instanceId,
        driver: ProviderDriverKind.make("codex"),
        getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
        openSession: () => Effect.die("Watch tests must not start a provider"),
      },
    ]),
    { databaseLayer: database, runEffectWorker: false },
  ),
);
const key = { host: "github.com", repository: "pingdotgg/t3code", number: 7 };
const url = "https://github.com/pingdotgg/t3code/pull/7";
const at = "2026-10-02T12:00:00.000Z";

const watchedPullRequestDetail = (input: {
  readonly projectId: ProjectId;
  readonly number: number;
  readonly at: string;
}): PullRequestDetail => ({
  provider: SourceControlProviderKind.make("github"),
  capabilities: {
    diff: true,
    comment: true,
    actions: [],
    mergeMethods: [],
    search: false,
    review: { inlineComment: false, reply: false, resolve: false, verdicts: [] },
    reviewers: { request: false, listCandidates: false },
  },
  viewerPermissions: {
    actions: [],
    comment: true,
    resolve: true,
    verdicts: [],
    requestReviewers: false,
  },
  projectId: input.projectId,
  projectTitle: "Watch wake",
  workspaceRoot: "/workspace/watch",
  repository: "pingdotgg/t3code",
  number: input.number,
  title: "Watched pull request",
  body: "",
  url: `https://github.com/pingdotgg/t3code/pull/${input.number}`,
  author: { login: "agent-user", name: null, avatarUrl: null },
  state: "open",
  isDraft: false,
  mergeability: "mergeable",
  additions: 1,
  deletions: 0,
  changedFiles: 1,
  headBranch: "feature",
  headSha: "abc1234def",
  baseBranch: "main",
  createdAt: input.at,
  updatedAt: input.at,
  mergedAt: null,
  closedAt: null,
  reviewers: [],
  labels: [],
  checks: [{ name: "lint", status: "failure", description: null, url: null }],
  mergeCapabilities: { merge: true, squash: true, rebase: true },
  viewer: "agent-user",
});

const setup = (name: string) =>
  Effect.gen(function* () {
    const engine = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make(`watch-merge-${name}`);
    const projectId = ProjectId.make(`watch-merge-${name}`);
    yield* engine.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`${name}:create`),
      threadId,
      projectId,
      title: "Watched merge",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
    });
    const watch = (watching: boolean, id: string) =>
      engine.dispatch({
        type: "thread.pull-request.watch",
        commandId: CommandId.make(`${name}:${id}`),
        threadId,
        ...key,
        watching,
        link: { url, source: "agent" },
      });
    yield* watch(true, "start");
    const shell = engine.getThreadShell(threadId);
    const messages = Effect.map(engine.getThreadRecords(threadId, ["messages"]), (r) => r.messages);
    const snapshotMerged = engine.dispatch({
      type: "thread.pull-request-link.sync",
      commandId: CommandId.make(`${name}:snapshot`),
      threadId,
      ...key,
      stack: null,
      snapshot: {
        state: "merged",
        title: "Watched merge",
        headBranch: "feature",
        baseBranch: "main",
        isDraft: false,
        updatedAt: at,
        syncedAt: at,
        mergedAt: at,
      },
    });
    const detail = { ...watchedPullRequestDetail({ projectId, number: 7, at }), checks: [] };
    return { engine, threadId, projectId, watch, shell, messages, snapshotMerged, detail };
  });

const reactorWith = (detail: () => Effect.Effect<PullRequestDetail>) =>
  PullRequestWatchReactor.make.pipe(
    Effect.provide(
      Layer.mergeAll(
        NodeServices.layer,
        Layer.mock(PullRequestService.PullRequestService)({
          watchFingerprint: () => Effect.succeed(null),
          detail,
          activity: () =>
            Effect.succeed({
              comments: [],
              commentCount: 0,
              commentsTruncated: false,
              reviewThreads: [],
              commits: [],
            }),
        }),
      ),
    ),
  );

it.layer(layerTest)("merge completion through the production reactor and watch sync", (it) => {
  it.effect.each(["host", "snapshot", "completed"])(
    "merged %s ends its watch and wakes exactly once across sweeps and reactor restart",
    (mode) =>
      Effect.gen(function* () {
        const f = yield* setup(mode);
        let reads = 0;
        let merged = mode !== "snapshot";
        const read = () =>
          Effect.sync(() => {
            reads++;
            return { ...f.detail, state: merged ? ("merged" as const) : ("open" as const) };
          });
        const ends: Array<unknown> = [];
        const capture = Logger.make(({ message }) => {
          const [text, fields] = Array.isArray(message) ? message : [message];
          if (text === "pull request watch ended") ends.push(fields);
        });
        const reactor = yield* reactorWith(read);
        const sweep = reactor.sweep.pipe(Effect.provide(Logger.layer([capture])));
        if (mode === "snapshot") {
          yield* sweep;
          assert.equal(reads, 1);
          assert.lengthOf(yield* f.messages, 0);
          yield* f.snapshotMerged;
        }
        const sql = yield* SqlClient.SqlClient;
        const exhausted: OrchestrationV2WorkContinuity = {
          generation: CommandId.make("manual-three"),
          authorityMessageId: MessageId.make("original-human"),
          checksRemaining: 0,
          maxChecks: 3,
          status: mode === "completed" ? "complete" : "active",
          reason: "Manual allowance exhausted",
          lastReportedRunId: null,
          lastAdvancedRunId: null,
        };
        yield* sql`UPDATE orchestration_v2_projection_threads
        SET payload_json = json_set(payload_json, '$.continuity', json(${JSON.stringify(exhausted)}),
          '$.continuityAutoEnrollDisabled', json('true')) WHERE thread_id = ${f.threadId}`;
        yield* sweep;
        assert.isUndefined((yield* f.shell)?.pullRequests?.[0]?.watch);
        const messages = yield* f.messages;
        assert.lengthOf(messages, 1);
        assert.deepEqual(messages[0]?.notification, {
          source: { kind: "monitor" },
          outcome: "completed",
          summary: "#7: merged, stopped watching",
        });
        assert.include(messages[0]?.text ?? "", "original authorized assignment");
        assert.equal(reads, 1);
        yield* sweep;
        yield* (yield* reactorWith(read)).sweep.pipe(Effect.provide(Logger.layer([capture])));
        assert.lengthOf(yield* f.messages, 1);
        assert.equal(reads, 1);
        assert.lengthOf(ends, 1);
        assert.deepInclude(ends[0], {
          reason: "merged",
          wakes: 1,
          reads: 1,
        });
        assert.deepEqual((yield* f.shell)?.continuity, exhausted);
        assert.isTrue((yield* f.shell)?.continuityAutoEnrollDisabled);

        // A deliberate new watch has its own generation; no continuity allowance is refilled.
        yield* TestClock.adjust("1 second");
        merged = true;
        yield* f.watch(true, "new-generation");
        yield* sweep;
        assert.lengthOf(yield* f.messages, 2);
        assert.isUndefined((yield* f.shell)?.pullRequests?.[0]?.watch);
        assert.deepEqual((yield* f.shell)?.continuity, exhausted);
      }),
  );

  it.effect.each(["stop", "unwatch", "settle", "archive", "restart-watch"])(
    "%s during a merged host read wins over its stale completion",
    (action) =>
      Effect.gen(function* () {
        const f = yield* setup(`race-${action}`);
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const reactor = yield* reactorWith(() =>
          Effect.gen(function* () {
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
            return { ...f.detail, state: "merged" };
          }),
        );
        const original = (yield* f.shell)?.pullRequests?.[0]?.watch;
        const sweep = yield* Effect.forkChild(reactor.sweep);
        yield* Deferred.await(entered);
        if (action === "unwatch" || action === "restart-watch") {
          yield* f.watch(false, "unwatch");
          if (action === "restart-watch") {
            yield* TestClock.adjust("1 second");
            yield* f.watch(true, "replacement");
          }
        } else {
          yield* f.engine.dispatch({
            type:
              action === "stop"
                ? "thread.stop"
                : action === "settle"
                  ? "thread.settle"
                  : "thread.archive",
            commandId: CommandId.make(`race-${action}:end`),
            threadId: f.threadId,
          });
        }
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(sweep);
        assert.lengthOf(yield* f.messages, 0);
        const current = (yield* f.shell)?.pullRequests?.[0]?.watch;
        if (action === "restart-watch") {
          assert.isDefined(current);
          assert.notEqual(current?.startedAt, original?.startedAt);
        } else assert.isUndefined(current);
      }),
  );

  it.effect.each(["settled", "settledAt", "subagent"])(
    "legacy merged + %s watch ends silently without retry churn",
    (inactive) =>
      Effect.gen(function* () {
        const f = yield* setup(`inactive-${inactive}`);
        yield* f.snapshotMerged;
        const sql = yield* SqlClient.SqlClient;
        // Simulate a watch retained by an older server before inactive watches were cleared.
        if (inactive === "settled") {
          yield* sql`UPDATE orchestration_v2_projection_threads
          SET payload_json = json_set(payload_json, '$.settledOverride', 'settled')
          WHERE thread_id = ${f.threadId}`;
        } else if (inactive === "settledAt") {
          yield* sql`UPDATE orchestration_v2_projection_threads
          SET payload_json = json_set(payload_json, '$.settledAt', ${at})
          WHERE thread_id = ${f.threadId}`;
        } else {
          yield* sql`UPDATE orchestration_v2_projection_threads
          SET payload_json = json_set(payload_json, '$.lineage.relationshipToParent', 'subagent',
            '$.lineage.parentThreadId', 'legacy-parent') WHERE thread_id = ${f.threadId}`;
        }
        const reactor = yield* reactorWith(() =>
          Effect.die("Inactive watch must not read the host"),
        );
        yield* reactor.sweep;
        assert.isUndefined((yield* f.shell)?.pullRequests?.[0]?.watch);
        assert.lengthOf(yield* f.messages, 0);
        yield* reactor.sweep;
        assert.lengthOf(yield* f.messages, 0);
      }),
  );
});
