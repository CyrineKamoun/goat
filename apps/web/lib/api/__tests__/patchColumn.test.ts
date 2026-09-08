/**
 * One PATCH per column, whatever changed about it: the endpoint's
 * `ColumnUpdate` takes every field, so a vocabulary edit and a formatting edit
 * on the same field are one round trip and one revision rather than two.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";

import { COLLECTIONS_API_BASE_URL, patchColumn, renameColumn } from "@/lib/api/layers";

const { fetchMock } = vi.hoisted(() => ({ fetchMock: vi.fn() }));
vi.mock("@/lib/api/fetcher", () => ({ apiRequestAuth: fetchMock, fetcher: vi.fn() }));
vi.mock("@/lib/api/processes", () => ({ executeProcessAsync: vi.fn() }));
vi.mock("swr", () => ({ default: vi.fn(() => ({})), mutate: vi.fn() }));

const okOnce = () => fetchMock.mockResolvedValue({ ok: true, status: 200, json: () => Promise.resolve({}) });

describe("patchColumn", () => {
  beforeEach(() => {
    fetchMock.mockReset();
  });

  it("sends every change in a single request", async () => {
    okOnce();

    await patchColumn("layer-1", "class", {
      allowed_values: [30, 50],
      allow_other: true,
      display_config: { decimals: 0 },
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe(`${COLLECTIONS_API_BASE_URL}/layer-1/columns/class`);
    expect(init.method).toBe("PATCH");
    expect(JSON.parse(init.body)).toEqual({
      allowed_values: [30, 50],
      allow_other: true,
      display_config: { decimals: 0 },
    });
  });

  it("keeps each wrapper's own failure message", async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 409, json: () => Promise.resolve({}) });

    await expect(renameColumn("layer-1", "class", "kind")).rejects.toThrow("Failed to rename column");
    await expect(patchColumn("layer-1", "class", { display_config: {} })).rejects.toThrow(
      "Failed to update column"
    );
  });

  it("passes the server's own message through when it gives one", async () => {
    fetchMock.mockResolvedValue({
      ok: false,
      status: 400,
      json: () => Promise.resolve({ detail: "Value 60 is not allowed" }),
    });

    await expect(patchColumn("layer-1", "class", { allowed_values: [] })).rejects.toThrow(
      "Value 60 is not allowed"
    );
  });
});
