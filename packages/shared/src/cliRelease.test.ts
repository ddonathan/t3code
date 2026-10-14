import { describe, expect, it } from "vite-plus/test";

import {
  cliArchiveFileName,
  cliArchivePlatformKey,
  cliArchiveTarCommand,
  cliReleaseDownloadBaseUrl,
  cliReleaseChannelOf,
  cliReleaseIndexPageUrl,
  newestCliReleaseVersion,
  parseChecksums,
  publishedCliVersion,
  resolvePublicCliUpdateTarget,
  PATCHED_SERVER_UPDATE_GUIDANCE,
} from "./cliRelease.ts";

describe("cliRelease", () => {
  it("names archives by version and platform, zip only on Windows", () => {
    expect(cliArchiveFileName("1.2.3-preview.20260911.4", "linux-x64")).toBe(
      "t3-1.2.3-preview.20260911.4-linux-x64.tar.gz",
    );
    expect(cliArchiveFileName("1.2.3", "win32-x64")).toBe("t3-1.2.3-win32-x64.zip");
  });

  it("only maps platforms and architectures that have a release archive", () => {
    expect(cliArchivePlatformKey("darwin", "arm64")).toBe("darwin-arm64");
    expect(cliArchivePlatformKey("linux", "x64")).toBe("linux-x64");
    expect(cliArchivePlatformKey("win32", "x64")).toBe("win32-x64");
    // Node single-executables are unsupported on x64 macOS.
    expect(cliArchivePlatformKey("darwin", "x64")).toBeUndefined();
    expect(cliArchivePlatformKey("linux", "arm64")).toBe("linux-arm64");
    expect(cliArchivePlatformKey("win32", "arm64")).toBe("win32-arm64");
    expect(cliArchivePlatformKey("freebsd", "x64")).toBeUndefined();
    expect(cliArchivePlatformKey("linux", "ia32")).toBeUndefined();
  });

  it("resolves download URLs under the tagged release, honoring a mirror", () => {
    expect(cliReleaseDownloadBaseUrl("1.2.3")).toBe(
      "https://github.com/pingdotgg/t3code/releases/download/v1.2.3",
    );
    expect(cliReleaseDownloadBaseUrl("1.2.3", "https://mirror.example/t3/")).toBe(
      "https://mirror.example/t3/v1.2.3",
    );
  });

  it("parses sha256sum output including binary-mode markers", () => {
    const checksums = parseChecksums(
      [
        `${"a".repeat(64)}  t3-1.2.3-linux-x64.tar.gz`,
        `${"B".repeat(64)} *t3-1.2.3-win32-x64.zip`,
        "not a checksum line",
        "",
      ].join("\n"),
    );
    expect(checksums.get("t3-1.2.3-linux-x64.tar.gz")).toBe("a".repeat(64));
    expect(checksums.get("t3-1.2.3-win32-x64.zip")).toBe("b".repeat(64));
    expect(checksums.size).toBe(2);
  });

  it("extracts with the System32 bsdtar on Windows and plain tar elsewhere", () => {
    expect(cliArchiveTarCommand("linux", {})).toBe("tar");
    expect(cliArchiveTarCommand("win32", { SystemRoot: "D:\\Win" })).toBe(
      "D:\\Win\\System32\\tar.exe",
    );
    expect(cliArchiveTarCommand("win32", {})).toBe("C:\\Windows\\System32\\tar.exe");
  });

  it("derives the release channel from the version alone", () => {
    expect(cliReleaseChannelOf("1.2.3")).toBe("stable");
    expect(cliReleaseChannelOf("1.2.3-nightly.20260911.4")).toBe("nightly");
    expect(cliReleaseChannelOf("1.2.3-preview.20260911.4")).toBe("preview");
    // A prerelease that is not one of our trains is not silently a nightly.
    expect(cliReleaseChannelOf("1.2.3-rc.1")).toBe("stable");
  });

  it("picks the newest non-draft release on the requested channel", () => {
    const releases = [
      { tag_name: "v1.2.4-preview.20260912.9.continuity.1" },
      { tag_name: "v1.2.4-nightly.20260912.9.continuity.1" },
      { tag_name: "v1.2.4-preview.20260912.9", draft: true },
      { tag_name: "v1.2.4-preview.20260912.8" },
      { tag_name: "v1.2.4-nightly.20260912.7" },
      { tag_name: "desktop-preview" },
      { tag_name: "v1.2.3" },
      { tag_name: "v1.2.3-nightly.20260911.2" },
    ];
    expect(newestCliReleaseVersion(releases, "preview")).toBe("1.2.4-preview.20260912.8");
    expect(newestCliReleaseVersion(releases, "nightly")).toBe("1.2.4-nightly.20260912.7");
    expect(newestCliReleaseVersion(releases, "stable")).toBe("1.2.3");
    expect(newestCliReleaseVersion([{ tag_name: "v1.2.3" }], "preview")).toBeUndefined();
  });

  it("normalizes only anchored numeric nightly continuity identities", () => {
    const official = "0.0.46-nightly.20261013.3053";
    expect(publishedCliVersion(`${official}.continuity.45`)).toBe(official);
    for (const version of [
      "0.0.46",
      "0.0.46-preview.20261013.3053.continuity.45",
      `${official}.other.45`,
      `${official}.continuity.x`,
      `${official}.continuity.45+build`,
      `prefix${official}.continuity.45`,
    ]) {
      expect(publishedCliVersion(version)).toBe(version);
    }
  });

  it("classifies local release trains without treating their identities as public targets", () => {
    for (const train of ["nightly", "preview"] as const) {
      const local = `0.0.46-${train}.20261013.3053.continuity.45`;
      expect(cliReleaseChannelOf(local)).toBe(train);
      expect(resolvePublicCliUpdateTarget(local, "0.0.46")).toMatchObject({
        _tag: "PublicCliUpdateBlocked",
        reason: "patched-server",
        serverVersion: local,
        message: PATCHED_SERVER_UPDATE_GUIDANCE,
      });
    }
  });

  it("allows official targets and repairs an old unpublished retry target only for an official server", () => {
    const official = "0.0.46-nightly.20261013.3053";
    expect(
      resolvePublicCliUpdateTarget("0.0.46-nightly.20261012.3021", `${official}.continuity.45`),
    ).toEqual({ _tag: "Allowed", targetVersion: official });
    for (const version of ["0.0.46", official, "0.0.46-preview.20261013.3053"]) {
      expect(resolvePublicCliUpdateTarget("0.0.45", version)).toEqual({
        _tag: "Allowed",
        targetVersion: version,
      });
    }
  });

  it.each([null, undefined, "", "dev", "0.0.46-nightly.20261012.3021.other.2"])(
    "refuses public replacement when the server identity cannot prove an official build (%s)",
    (serverVersion) =>
      expect(resolvePublicCliUpdateTarget(serverVersion, "0.0.46")).toMatchObject({
        _tag: "PublicCliUpdateBlocked",
        reason: "unverified-server",
      }),
  );

  it.each([
    "0.0.46-nightly.20261013.3053.other.45",
    "0.0.46-preview.20261013.3053.continuity.45",
    "latest",
    "0.0.46; touch bad",
  ])("refuses a target outside public release identities (%s)", (targetVersion) =>
    expect(resolvePublicCliUpdateTarget("0.0.45", targetVersion)).toMatchObject({
      _tag: "PublicCliUpdateBlocked",
      reason: "unpublished-target",
      targetVersion,
    }),
  );

  it("pages through the release index at the largest page GitHub allows", () => {
    expect(cliReleaseIndexPageUrl(1)).toBe(
      "https://api.github.com/repos/pingdotgg/t3code/releases?per_page=100&page=1",
    );
    expect(cliReleaseIndexPageUrl(3)).toContain("page=3");
  });
});
