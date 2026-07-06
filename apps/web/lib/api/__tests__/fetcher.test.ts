import { beforeEach, describe, expect, it, vi } from "vitest";

import { clearTokenCache, fetcher } from "@/lib/api/fetcher";

const { getSessionMock, signInMock } = vi.hoisted(() => ({
  getSessionMock: vi.fn(),
  signInMock: vi.fn(),
}));

vi.mock("next-auth/react", () => ({
  getSession: getSessionMock,
  signIn: signInMock,
}));

describe("fetcher auth handling", () => {
  beforeEach(() => {
    clearTokenCache();
    getSessionMock.mockReset();
    signInMock.mockReset();
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({ ok: true, json: () => Promise.resolve({}) })
    );
  });

  it("sends the session access token as bearer", async () => {
    getSessionMock.mockResolvedValue({ access_token: "valid-token" });
    await fetcher("http://api.test/thing");
    const headers = vi.mocked(fetch).mock.calls[0][1]?.headers as Record<string, string>;
    expect(headers["Authorization"]).toBe("Bearer valid-token");
  });

  it("does not replay the stale token and re-authenticates when refresh failed", async () => {
    getSessionMock.mockResolvedValue({
      access_token: "stale-token",
      error: "RefreshAccessTokenError",
    });
    await fetcher("http://api.test/thing");
    const options = vi.mocked(fetch).mock.calls[0][1];
    expect(options?.headers?.["Authorization"]).toBeUndefined();
    expect(signInMock).toHaveBeenCalledWith("keycloak");
  });

  it("sends no auth header without a session", async () => {
    getSessionMock.mockResolvedValue(null);
    await fetcher("http://api.test/thing");
    const options = vi.mocked(fetch).mock.calls[0][1];
    expect(options?.headers?.["Authorization"]).toBeUndefined();
    expect(signInMock).not.toHaveBeenCalled();
  });
});
