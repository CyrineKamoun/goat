import { describe, expect, it } from "vitest";

import { pageConfigSchema } from "@/lib/validations/reportLayout";

describe("pageConfigSchema", () => {
  it("accepts A2 and A1", () => {
    expect(pageConfigSchema.parse({ size: "A2" }).size).toBe("A2");
    expect(pageConfigSchema.parse({ size: "A1" }).size).toBe("A1");
  });

  it("bounds a custom side to 50–1500 mm", () => {
    expect(pageConfigSchema.safeParse({ size: "Custom", width: 1000, height: 700 }).success).toBe(true);
    expect(pageConfigSchema.safeParse({ size: "Custom", width: 20, height: 700 }).success).toBe(false);
    expect(pageConfigSchema.safeParse({ size: "Custom", width: 1000, height: 1501 }).success).toBe(false);
  });
});
