import { afterEach, describe, expect, it, vi } from "vitest";

import { publicFetcher } from "@/lib/api/publicFetcher";

/**
 * `releases.ts` and `status.ts` fetch third-party hosts (docs.plan4better.de,
 * status.plan4better.de) and must never leak the caller's bearer token there —
 * unlike the shared `fetcher` in `@/lib/api/fetcher`, which attaches
 * `Authorization` to every request regardless of host.
 */
describe("publicFetcher", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("fetches the URL with no Authorization header and no extra options", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({ hello: "world" }) });
    vi.stubGlobal("fetch", fetchMock);

    const result = await publicFetcher("https://status.plan4better.de/goat.json");

    expect(result).toEqual({ hello: "world" });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith("https://status.plan4better.de/goat.json");
    const [, options] = fetchMock.mock.calls[0] as [string, RequestInit | undefined];
    expect(options).toBeUndefined();
  });

  it("throws when the response is not ok", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false, status: 404 }));

    await expect(publicFetcher("https://status.plan4better.de/goat.json")).rejects.toThrow();
  });
});
