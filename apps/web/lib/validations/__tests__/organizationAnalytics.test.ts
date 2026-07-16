import { describe, expect, it } from "vitest";

import {
  analyticsDashboardSchema,
  organizationAnalyticsCreateSchema,
  organizationAnalyticsSchema,
} from "@/lib/validations/organizationAnalytics";

const validCreate = {
  name: "P4B Matomo",
  provider: "matomo",
  config: {
    provider: "matomo",
    url: "https://matomo.example.org/",
    site_id: "5",
  },
};

describe("organizationAnalyticsCreateSchema", () => {
  it("accepts a valid payload with name", () => {
    const parsed = organizationAnalyticsCreateSchema.parse(validCreate);
    expect(parsed.name).toBe("P4B Matomo");
  });

  it("rejects a missing name", () => {
    const { name: _name, ...rest } = validCreate;
    expect(() => organizationAnalyticsCreateSchema.parse(rest)).toThrow();
  });

  it("rejects an empty name", () => {
    expect(() =>
      organizationAnalyticsCreateSchema.parse({ ...validCreate, name: " " })
    ).toThrow();
  });

  it("accepts a subdirectory url and normalizes the trailing slash", () => {
    const parsed = organizationAnalyticsCreateSchema.parse({
      ...validCreate,
      config: { ...validCreate.config, url: "https://analytics.example.org/matomo" },
    });
    expect(parsed.config.url).toBe("https://analytics.example.org/matomo/");
  });

  it("still rejects urls with query or fragment", () => {
    for (const bad of [
      "https://analytics.example.org/matomo/?x=1",
      "https://analytics.example.org/matomo/#frag",
    ]) {
      expect(() =>
        organizationAnalyticsCreateSchema.parse({
          ...validCreate,
          config: { ...validCreate.config, url: bad },
        })
      ).toThrow();
    }
  });

  it("rejects http urls", () => {
    expect(() =>
      organizationAnalyticsCreateSchema.parse({
        ...validCreate,
        config: { ...validCreate.config, url: "http://matomo.example.org/" },
      })
    ).toThrow();
  });
});

describe("organizationAnalyticsSchema", () => {
  it("parses a read payload with usage_count", () => {
    const parsed = organizationAnalyticsSchema.parse({
      id: "11111111-1111-1111-1111-111111111111",
      organization_id: "22222222-2222-2222-2222-222222222222",
      name: "P4B Matomo",
      provider: "matomo",
      config: { url: "https://matomo.example.org/", site_id: "5" },
      usage_count: 3,
      created_at: "2026-01-01T00:00:00Z",
      updated_at: "2026-01-01T00:00:00Z",
    });
    expect(parsed.usage_count).toBe(3);
  });
});

describe("analyticsDashboardSchema", () => {
  it("parses a dashboard row", () => {
    const parsed = analyticsDashboardSchema.parse({
      project_id: "11111111-1111-1111-1111-111111111111",
      name: "My dashboard",
      analytics_id: "22222222-2222-2222-2222-222222222222",
    });
    expect(parsed.name).toBe("My dashboard");
  });

  it("accepts null analytics_id", () => {
    const parsed = analyticsDashboardSchema.parse({
      project_id: "11111111-1111-1111-1111-111111111111",
      name: "Untracked dashboard",
      analytics_id: null,
    });
    expect(parsed.analytics_id).toBeNull();
  });

  it("rejects a non-uuid project_id", () => {
    expect(() =>
      analyticsDashboardSchema.parse({
        project_id: "not-a-uuid",
        name: "Bad dashboard",
        analytics_id: null,
      })
    ).toThrow();
  });
});
