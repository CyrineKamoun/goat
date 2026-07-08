import { describe, expect, it } from "vitest";

import {
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
