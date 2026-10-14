import * as Schema from "effect/Schema";

const Sha256 = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/u));
export const ApprovedRuntimeManifest = Schema.Struct({
  format: Schema.Literal(1),
  builds: Schema.Array(
    Schema.Struct({
      version: Schema.String,
      platform: Schema.Literal("linux"),
      arch: Schema.String,
      archiveSha256: Sha256,
      executableSha256: Sha256,
      clientTreeSha256: Sha256,
    }),
  ),
});
export type ApprovedRuntimeBuild = (typeof ApprovedRuntimeManifest.Type.builds)[number];
