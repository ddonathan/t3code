// @effect-diagnostics nodeBuiltinImport:off
// Node-only adapter shared with the standalone launcher, which cannot load Effect.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import type { ApprovedRuntimeBuild, ApprovedRuntimeManifest } from "./approvedRuntimeSchema.ts";
import { approvedRuntimeJsonSchema } from "./approvedRuntimeJsonSchema.ts";
import { isExactServiceVersion } from "./serviceProtocol.ts";

export class ApprovedRuntimeRefused extends Error {
  override readonly name = "ApprovedRuntimeRefused";
  readonly manifestPath: string;
  readonly version: string;
  readonly reason: string;
  readonly code: "unknown-build" | "verification-failed";
  constructor(
    manifestPath: string,
    version: string,
    reason: string,
    code: "unknown-build" | "verification-failed" = "verification-failed",
  ) {
    super(
      `Approved runtime refused t3@${version}: ${reason}. Policy: ${manifestPath}. Verify and approve the exact Linux archive, executable and client tree before retrying; keep the current selection.`,
    );
    this.manifestPath = manifestPath;
    this.version = version;
    this.reason = reason;
    this.code = code;
  }
}

const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

// Only the JSON Schema subset emitted by this contract is supported. Unknown
// keywords fail closed; the contract test checks both schema drift and decoding.
function conforms(value: unknown, schema: unknown): boolean {
  if (!record(schema)) return false;
  const supported = new Set([
    "type",
    "properties",
    "required",
    "additionalProperties",
    "items",
    "enum",
    "pattern",
  ]);
  if (Object.keys(schema).some((key) => !supported.has(key))) return false;
  if (Array.isArray(schema.enum) && !schema.enum.includes(value)) return false;
  switch (schema.type) {
    case "number":
      return typeof value === "number" && Number.isFinite(value);
    case "string":
      return (
        typeof value === "string" &&
        (schema.pattern === undefined ||
          (typeof schema.pattern === "string" && new RegExp(schema.pattern, "u").test(value)))
      );
    case "array":
      return Array.isArray(value) && value.every((item) => conforms(item, schema.items));
    case "object": {
      if (!record(value) || !record(schema.properties) || !Array.isArray(schema.required))
        return false;
      const properties = schema.properties;
      return (
        schema.required.every((key) => typeof key === "string" && Object.hasOwn(value, key)) &&
        Object.entries(properties).every(
          ([key, child]) => !Object.hasOwn(value, key) || conforms(value[key], child),
        ) &&
        (schema.additionalProperties === true ||
          Object.keys(value).every((key) => Object.hasOwn(properties, key)))
      );
    }
    default:
      return false;
  }
}

function isManifest(value: unknown): value is typeof ApprovedRuntimeManifest.Type {
  return conforms(value, approvedRuntimeJsonSchema);
}

export const approvedRuntimeManifestPath = (baseDir: string) =>
  NodePath.join(baseDir, "runtime", "approved-builds.json");

const missing = (cause: unknown) =>
  cause instanceof Error && "code" in cause && cause.code === "ENOENT";

/** File presence enables the guard. A dangling symlink is present and fails closed. */
export async function approvedRuntimeBuild(
  baseDir: string,
  version: string,
  platform: string,
  arch: string,
): Promise<ApprovedRuntimeBuild | undefined> {
  // Windows post-install mutates the executable; its owning registry is separate.
  if (platform !== "linux") return undefined;
  const manifestPath = approvedRuntimeManifestPath(baseDir);
  try {
    await NodeFSP.lstat(manifestPath);
  } catch (cause) {
    if (missing(cause)) return undefined;
    throw new ApprovedRuntimeRefused(manifestPath, version, "policy presence cannot be checked");
  }
  let manifest: unknown;
  try {
    manifest = JSON.parse(await NodeFSP.readFile(manifestPath, "utf8"));
  } catch {
    throw new ApprovedRuntimeRefused(manifestPath, version, "policy is unreadable or invalid JSON");
  }
  if (
    !isManifest(manifest) ||
    manifest.builds.some((build) => !isExactServiceVersion(build.version))
  ) {
    throw new ApprovedRuntimeRefused(
      manifestPath,
      version,
      "policy does not match the approved-build contract",
    );
  }
  const matches = manifest.builds.filter(
    (build) => build.version === version && build.platform === platform && build.arch === arch,
  );
  if (matches.length !== 1 || matches[0] === undefined) {
    throw new ApprovedRuntimeRefused(
      manifestPath,
      version,
      "exact version/platform/architecture is not uniquely approved",
      matches.length === 0 ? "unknown-build" : "verification-failed",
    );
  }
  return matches[0];
}

async function fileSha256(filePath: string): Promise<string> {
  if (!(await NodeFSP.lstat(filePath)).isFile()) throw new Error("Expected a regular file");
  const hash = NodeCrypto.createHash("sha256");
  for await (const chunk of NodeFS.createReadStream(filePath)) hash.update(chunk);
  return hash.digest("hex");
}

/** SHA256 of UTF-8 JSON [[relative POSIX path, file SHA256], ...], sorted by path.
 * Includes every client asset; symlinks and special files are refused. */
export async function approvedRuntimeClientTreeSha256(clientDir: string): Promise<string> {
  const entries: Array<readonly [string, string]> = [];
  async function walk(directory: string, relative: string): Promise<void> {
    if (!(await NodeFSP.lstat(directory)).isDirectory()) throw new Error("Expected a directory");
    for (const name of await NodeFSP.readdir(directory)) {
      const child = NodePath.join(directory, name);
      const childRelative = relative === "" ? name : `${relative}/${name}`;
      const stat = await NodeFSP.lstat(child);
      if (stat.isDirectory()) await walk(child, childRelative);
      else entries.push([childRelative, await fileSha256(child)]);
    }
  }
  await walk(clientDir, "");
  entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return NodeCrypto.createHash("sha256").update(JSON.stringify(entries)).digest("hex");
}

/** Cached archives retain a digest receipt; executable/client bytes are checked
 * again on every use. This is an accidental-replacement guard, not protection
 * from the same OS user editing both policy and files. */
export async function verifyApprovedRuntime(
  baseDir: string,
  version: string,
  platform: string,
  arch: string,
  versionDir: string,
  archiveSha256?: string,
): Promise<void> {
  const build = await approvedRuntimeBuild(baseDir, version, platform, arch);
  if (build === undefined) return;
  const refuse = (reason: string) =>
    new ApprovedRuntimeRefused(approvedRuntimeManifestPath(baseDir), version, reason);
  try {
    const archive =
      archiveSha256 ??
      (await NodeFSP.readFile(NodePath.join(versionDir, ".archive-sha256"), "utf8")).trim();
    if (archive !== build.archiveSha256) throw refuse("archive SHA256 does not match approval");
    if ((await fileSha256(NodePath.join(versionDir, "t3"))) !== build.executableSha256)
      throw refuse("executable SHA256 does not match approval");
    if (
      (await approvedRuntimeClientTreeSha256(NodePath.join(versionDir, "client"))) !==
      build.clientTreeSha256
    )
      throw refuse("client tree SHA256 does not match approval");
  } catch (cause) {
    if (cause instanceof ApprovedRuntimeRefused) throw cause;
    throw refuse("runtime files or archive receipt cannot be verified");
  }
}
