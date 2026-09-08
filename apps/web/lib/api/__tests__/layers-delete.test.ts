import { beforeEach, describe, expect, it, vi } from "vitest";

const { fetchMock, processMock } = vi.hoisted(() => ({ fetchMock: vi.fn(), processMock: vi.fn() }));
vi.mock("@/lib/api/fetcher", () => ({ apiRequestAuth: fetchMock, fetcher: vi.fn() }));
vi.mock("@/lib/api/processes", () => ({ executeProcessAsync: processMock }));
vi.mock("swr", () => ({ default: vi.fn(() => ({})), mutate: vi.fn() }));

import { LAYERS_API_BASE_URL, deleteLayer } from "@/lib/api/layers";

describe("deleteLayer", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    processMock.mockReset();
  });
  it("soft-deletes through the core endpoint, not the layer_delete process", async () => {
    fetchMock.mockResolvedValue({ ok: true, status: 204 });
    await deleteLayer("abc");
    expect(fetchMock.mock.calls[0][0]).toBe(`${LAYERS_API_BASE_URL}/abc`);
    expect(fetchMock.mock.calls[0][1].method).toBe("DELETE");
    expect(processMock).not.toHaveBeenCalled();
  });
  it("throws when the server refuses", async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 403, json: () => Promise.resolve({ detail: "Forbidden" }) });
    await expect(deleteLayer("abc")).rejects.toThrow("Forbidden");
  });
});
