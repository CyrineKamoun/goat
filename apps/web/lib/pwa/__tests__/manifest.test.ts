import { afterEach, describe, expect, it, vi } from "vitest";

import { buildManifest, isCustomDomainHost } from "@/lib/pwa/manifest";

const PROJECT = {
  id: "11111111-1111-1111-1111-111111111111",
  name: "VerkehrsInfo BW",
  builder_config: { settings: { primary_color: "#ffcc00" } },
};

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("isCustomDomainHost", () => {
  it("detects custom domains against NEXT_PUBLIC_APP_URL", () => {
    vi.stubEnv("NEXT_PUBLIC_APP_URL", "https://goat.plan4better.de");
    expect(isCustomDomainHost("dashboards.client.com")).toBe(true);
    expect(isCustomDomainHost("goat.plan4better.de")).toBe(false);
    expect(isCustomDomainHost("GOAT.plan4better.de:443")).toBe(false);
  });
  it("treats missing host or missing APP_URL as canonical", () => {
    vi.stubEnv("NEXT_PUBLIC_APP_URL", "");
    expect(isCustomDomainHost("anything.com")).toBe(false);
    vi.stubEnv("NEXT_PUBLIC_APP_URL", "https://goat.plan4better.de");
    expect(isCustomDomainHost(null)).toBe(false);
  });
});

describe("buildManifest", () => {
  it("builds a canonical-host manifest", () => {
    const m = buildManifest(PROJECT, { isCustomDomain: false });
    expect(m.name).toBe("VerkehrsInfo BW");
    expect(m.short_name).toBe("VerkehrsInfo BW");
    expect(m.id).toBe(PROJECT.id);
    expect(m.display).toBe("standalone");
    expect(m.start_url).toBe(`/map/public/${PROJECT.id}`);
    expect(m.scope).toBe(`/map/public/${PROJECT.id}`);
    expect(m.theme_color).toBe("#ffcc00");
    expect(m.background_color).toBe("#ffffff");
    expect(m.icons).toEqual([
      { src: `/api/pwa-icon/${PROJECT.id}?size=192`, sizes: "192x192", type: "image/png" },
      { src: `/api/pwa-icon/${PROJECT.id}?size=512`, sizes: "512x512", type: "image/png" },
      {
        src: `/api/pwa-icon/${PROJECT.id}?size=512`,
        sizes: "512x512",
        type: "image/png",
        purpose: "maskable",
      },
    ]);
  });

  it("uses root scope on custom domains", () => {
    const m = buildManifest(PROJECT, { isCustomDomain: true });
    expect(m.start_url).toBe("/");
    expect(m.scope).toBe("/");
  });

  it("falls back to the GOAT theme color and truncates long names for short_name", () => {
    const m = buildManifest(
      { id: PROJECT.id, name: "A very long dashboard name that exceeds thirty characters" },
      { isCustomDomain: false }
    );
    expect(m.theme_color).toBe("#2BB381");
    expect(m.short_name).toHaveLength(30);
    expect(m.name).toBe("A very long dashboard name that exceeds thirty characters");
  });
});
