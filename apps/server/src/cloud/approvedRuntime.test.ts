// @effect-diagnostics nodeBuiltinImport:off
// Exercises the standalone Node adapter without an Effect runtime.
import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { assert, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { ApprovedRuntimeManifest } from "./approvedRuntimeSchema.ts";
import { approvedRuntimeJsonSchema } from "./approvedRuntimeJsonSchema.ts";
import {
  approvedRuntimeBuild,
  approvedRuntimeClientTreeSha256,
  verifyApprovedRuntime,
} from "./approvedRuntime.ts";

const isApprovedRuntimeManifest = Schema.is(ApprovedRuntimeManifest);
const sha = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");

it("standalone policy schema is generated from the authoritative Effect contract", () => {
  assert.deepEqual(
    approvedRuntimeJsonSchema,
    Schema.toJsonSchemaDocument(ApprovedRuntimeManifest).schema,
  );
});

it("presence enables byte checks including nested client assets and unreadable policy", async () => {
  const root = await NodeFSP.mkdtemp(NodePath.join(process.env.TMPDIR ?? "/tmp", "t3-approved-"));
  try {
    const directory = NodePath.join(root, "runtime", "versions", "1.0.0");
    await NodeFSP.mkdir(NodePath.join(directory, "client", "assets"), { recursive: true });
    await NodeFSP.writeFile(NodePath.join(directory, "t3"), "patched executable");
    await NodeFSP.writeFile(NodePath.join(directory, "client", "index.html"), "index");
    await NodeFSP.writeFile(
      NodePath.join(directory, "client", "assets", "app.js"),
      "patched script",
    );
    await NodeFSP.writeFile(NodePath.join(directory, ".archive-sha256"), sha("archive"));
    const expectedTree = sha(
      JSON.stringify([
        ["assets/app.js", sha("patched script")],
        ["index.html", sha("index")],
      ]),
    );
    assert.equal(
      await approvedRuntimeClientTreeSha256(NodePath.join(directory, "client")),
      expectedTree,
    );
    const build = {
      version: "1.0.0",
      platform: "linux",
      arch: "x64",
      archiveSha256: sha("archive"),
      executableSha256: sha("patched executable"),
      clientTreeSha256: expectedTree,
    };
    assert.isTrue(isApprovedRuntimeManifest({ format: 1, builds: [build] }));
    assert.isFalse(
      isApprovedRuntimeManifest({
        format: 1,
        builds: [{ ...build, archiveSha256: "bad" }],
      }),
    );
    const manifestPath = NodePath.join(root, "runtime", "approved-builds.json");
    await NodeFSP.writeFile(manifestPath, JSON.stringify({ format: 1, builds: [build] }));
    await verifyApprovedRuntime(root, "1.0.0", "linux", "x64", directory);
    await NodeAssert.rejects(
      approvedRuntimeBuild(root, "1.0.1", "linux", "x64"),
      /not uniquely approved/,
    );
    await NodeAssert.rejects(
      approvedRuntimeBuild(root, "1.0.0", "linux", "arm64"),
      /not uniquely approved/,
    );
    await NodeFSP.writeFile(NodePath.join(directory, "client", "assets", "app.js"), "stock script");
    await NodeAssert.rejects(
      verifyApprovedRuntime(root, "1.0.0", "linux", "x64", directory),
      /client tree SHA256/,
    );
    await NodeFSP.rm(NodePath.join(directory, "client", "assets", "app.js"));
    await NodeFSP.symlink(
      NodePath.join(directory, "client", "index.html"),
      NodePath.join(directory, "client", "assets", "app.js"),
    );
    await NodeAssert.rejects(
      verifyApprovedRuntime(root, "1.0.0", "linux", "x64", directory),
      /cannot be verified/,
    );
    for (const contents of [
      "{",
      JSON.stringify({ format: 1, builds: [{ ...build, archiveSha256: "bad" }] }),
      JSON.stringify({ format: 1, builds: [build, build] }),
    ]) {
      await NodeFSP.writeFile(manifestPath, contents);
      await NodeAssert.rejects(approvedRuntimeBuild(root, "1.0.0", "linux", "x64"));
    }
    await NodeFSP.rm(manifestPath);
    await NodeFSP.symlink(NodePath.join(root, "missing-policy"), manifestPath);
    await NodeAssert.rejects(approvedRuntimeBuild(root, "1.0.0", "linux", "x64"), /unreadable/);
    assert.isUndefined(await approvedRuntimeBuild(root, "1.0.0", "win32", "x64"));
    await NodeFSP.rm(manifestPath);
    assert.isUndefined(await approvedRuntimeBuild(root, "1.0.0", "linux", "x64"));
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});
