import { describe, expect, it } from "vitest";

import { isAuthDisabled } from "@/lib/utils/auth-flag";

describe("isAuthDisabled", () => {
  it("disables auth for falsy AUTH values (matching pydantic bool parsing)", () => {
    expect(isAuthDisabled("False")).toBe(true);
    expect(isAuthDisabled("false")).toBe(true);
    expect(isAuthDisabled("FALSE")).toBe(true);
    expect(isAuthDisabled("0")).toBe(true);
    expect(isAuthDisabled("off")).toBe(true);
    expect(isAuthDisabled("no")).toBe(true);
    expect(isAuthDisabled("f")).toBe(true);
    expect(isAuthDisabled("n")).toBe(true);
  });

  it("keeps auth enabled for truthy values", () => {
    expect(isAuthDisabled("True")).toBe(false);
    expect(isAuthDisabled("true")).toBe(false);
    expect(isAuthDisabled("1")).toBe(false);
    expect(isAuthDisabled("on")).toBe(false);
  });

  it("keeps auth enabled when the flag is unset or empty", () => {
    expect(isAuthDisabled(undefined)).toBe(false);
    expect(isAuthDisabled("")).toBe(false);
  });

  it("keeps auth enabled for the unsubstituted Docker placeholder", () => {
    expect(isAuthDisabled("APP_NEXT_PUBLIC_AUTH")).toBe(false);
  });

  it("keeps auth enabled for unrecognized values", () => {
    expect(isAuthDisabled("disabled")).toBe(false);
    expect(isAuthDisabled("garbage")).toBe(false);
  });
});
