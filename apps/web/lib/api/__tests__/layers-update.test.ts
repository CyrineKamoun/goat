import { beforeEach, describe, expect, it, vi } from "vitest";

import { LAYERS_API_BASE_URL, updateDataset } from "@/lib/api/layers";

const { fetchMock } = vi.hoisted(() => ({ fetchMock: vi.fn() }));
vi.mock("@/lib/api/fetcher", () => ({ apiRequestAuth: fetchMock, fetcher: vi.fn() }));
vi.mock("@/lib/api/processes", () => ({ executeProcessAsync: vi.fn() }));
vi.mock("swr", () => ({ default: vi.fn(() => ({})), mutate: vi.fn() }));

describe("updateDataset", () => {
  beforeEach(() => {
    fetchMock.mockReset();
  });

  it("PUTs the payload to the layer endpoint", async () => {
    fetchMock.mockResolvedValue({ ok: true, status: 200, json: () => Promise.resolve({}) });

    await updateDataset("abc", { folder_id: "folder-1" });

    expect(fetchMock.mock.calls[0][0]).toBe(`${LAYERS_API_BASE_URL}/abc`);
    expect(fetchMock.mock.calls[0][1].method).toBe("PUT");
  });

  it("rejects when the server refuses the write, rather than resolving as a success", async () => {
    // A refused move (a write-denied destination folder) used to resolve with
    // the failed Response, so callers toasted success and cleared the
    // selection while the dataset had not moved.
    fetchMock.mockResolvedValue({
      ok: false,
      status: 403,
      json: () => Promise.resolve({ detail: "Forbidden" }),
    });

    await expect(updateDataset("abc", { folder_id: "folder-1" })).rejects.toThrow("Failed to update dataset");
  });
});
