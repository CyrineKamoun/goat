import { describe, expect, it } from "vitest";

import { builderConfigSchema } from "@/lib/validations/project";

describe("builderConfigSchema app_icon_url", () => {
  it("is optional and absent by default", () => {
    const parsed = builderConfigSchema.parse({});
    expect(parsed.settings.app_icon_url).toBeUndefined();
  });

  it("round-trips a configured value", () => {
    const parsed = builderConfigSchema.parse({
      settings: { app_icon_url: "https://assets.example.com/icon.svg" },
    });
    expect(parsed.settings.app_icon_url).toBe("https://assets.example.com/icon.svg");
  });
});
