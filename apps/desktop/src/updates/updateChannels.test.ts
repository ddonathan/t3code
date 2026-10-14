import { describe, expect, it } from "vite-plus/test";

import { isNightlyDesktopVersion, resolveDefaultDesktopUpdateChannel } from "./updateChannels.ts";

describe("updateChannels", () => {
  it("keeps preview builds branded as nightly but on the latest update channel", () => {
    expect(isNightlyDesktopVersion("0.0.41-preview.20260911.7")).toBe(true);
    expect(resolveDefaultDesktopUpdateChannel("0.0.41-preview.20260911.7")).toBe("latest");
    expect(resolveDefaultDesktopUpdateChannel("0.0.41-nightly.20260911.7")).toBe("nightly");
  });

  it("keeps explicitly identified continuity builds on the nightly brand and channel", () => {
    const version = "0.0.46-nightly.20261013.3053.continuity.45";
    expect(isNightlyDesktopVersion(version)).toBe(true);
    expect(resolveDefaultDesktopUpdateChannel(version)).toBe("nightly");
    expect(isNightlyDesktopVersion("0.0.46-nightly.20261013.3053.other.45")).toBe(false);
  });

  it("only matches the first prerelease identifier", () => {
    expect(isNightlyDesktopVersion("1.2.3-foo-preview.20260911.1")).toBe(false);
    expect(isNightlyDesktopVersion("1.2.3")).toBe(false);
  });
});
