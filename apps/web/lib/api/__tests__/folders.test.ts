import { beforeEach, describe, expect, it, vi } from "vitest";

const { fetchMock } = vi.hoisted(() => ({ fetchMock: vi.fn() }));
vi.mock("@/lib/api/fetcher", () => ({ apiRequestAuth: fetchMock, fetcher: vi.fn() }));
vi.mock("swr", () => ({ default: vi.fn(() => ({})), mutate: vi.fn() }));

import { FOLDERS_API_BASE_URL, createFolder, memberSpaceIds, updateFolder } from "@/lib/api/folders";
import { folderSchema } from "@/lib/validations/folder";

const ok = (body: unknown) => ({ ok: true, status: 200, json: () => Promise.resolve(body) });

describe("folders api", () => {
  beforeEach(() => fetchMock.mockReset());
  it("parses nested fields with defaults", () => {
    const f = folderSchema.parse({ name: "home", id: "5b7d2b0e-6c2f-4f0a-9b1a-1f9a8c1d2e3f", user_id: "5b7d2b0e-6c2f-4f0a-9b1a-1f9a8c1d2e40" });
    expect(f.parent_id).toBeNull();
    expect(f.depth).toBe(0);
  });
  it("creates a nested folder", async () => {
    fetchMock.mockResolvedValue(ok({}));
    await createFolder("Week 35", "parent-id");
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({ name: "Week 35", parent_id: "parent-id" });
  });
  it("creates a root folder without parent_id", async () => {
    fetchMock.mockResolvedValue(ok({}));
    await createFolder("Drafts");
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({ name: "Drafts" });
  });
  it("creates a root folder in a space when a space id is given", async () => {
    fetchMock.mockResolvedValue(ok({}));
    await createFolder("Ops plans", null, "space-team");
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      name: "Ops plans",
      space_id: "space-team",
    });
  });
  it("prefers the parent over the space, so a nested folder never carries space_id", async () => {
    fetchMock.mockResolvedValue(ok({}));
    await createFolder("Week 36", "parent-id", "space-team");
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      name: "Week 36",
      parent_id: "parent-id",
    });
  });
  it("lists the member spaces a folder listing reveals through their home roots", () => {
    expect(
      memberSpaceIds([
        { name: "home", id: "f1", parent_id: null, space_id: "space-team", is_owned: false, depth: 0, restricted: false },
        { name: "home", id: "f2", parent_id: null, space_id: "space-personal", is_owned: true, depth: 0, restricted: false },
        { name: "Plans", id: "f3", parent_id: null, space_id: "space-team", is_owned: true, depth: 0, restricted: false },
      ])
    ).toEqual(new Set(["space-team"]));
  });
  it("moves a folder to the root with an explicit null", async () => {
    fetchMock.mockResolvedValue(ok({}));
    await updateFolder("f1", { parent_id: null });
    expect(fetchMock.mock.calls[0][0]).toBe(`${FOLDERS_API_BASE_URL}/f1`);
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({ parent_id: null });
  });
});
