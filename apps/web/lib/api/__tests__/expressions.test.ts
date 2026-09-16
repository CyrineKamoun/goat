import { beforeEach, describe, expect, it, vi } from "vitest";

import { previewSql } from "@/lib/api/expressions";

const { fetchMock } = vi.hoisted(() => ({ fetchMock: vi.fn() }));
vi.mock("@/lib/api/fetcher", () => ({
  apiRequestAuth: fetchMock,
  fetcher: vi.fn(),
}));

const ok = { success: true, columns: [], rows: [] };

const sentInputs = () => JSON.parse(fetchMock.mock.calls[0][1].body).inputs;

describe("previewSql", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    fetchMock.mockResolvedValue({ ok: true, json: async () => ok });
  });

  it("names the published project so an anonymous viewer's table can run its stored query", async () => {
    await previewSql({
      sql_query: "SELECT 1 FROM input_1",
      layers: { input_1: "layer-a" },
      project_id: "3e870eb8-9633-44e9-8a43-36a7290f1ced",
    });
    expect(sentInputs().project_id).toBe("3e870eb8-9633-44e9-8a43-36a7290f1ced");
  });

  it("sends no project_id for an editor's own preview", async () => {
    await previewSql({ sql_query: "SELECT 1 FROM input_1", layers: { input_1: "layer-a" } });
    expect(sentInputs()).not.toHaveProperty("project_id");
  });
});
