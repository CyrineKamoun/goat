import { describe, expect, it } from "vitest";

import { publicEnv } from "@/lib/utils/public-env";

describe("publicEnv", () => {
  it("returns real values", () => {
    expect(publicEnv("http://127.0.0.1:8000")).toBe("http://127.0.0.1:8000");
  });

  it("returns undefined for unset or empty values", () => {
    expect(publicEnv(undefined)).toBeUndefined();
    expect(publicEnv("")).toBeUndefined();
  });

  it("returns undefined for unsubstituted Docker placeholders", () => {
    expect(publicEnv("APP_NEXT_PUBLIC_API_URL")).toBeUndefined();
    expect(publicEnv("APP_NEXT_PUBLIC_APP_URL")).toBeUndefined();
  });
});
