import { beforeEach, describe, expect, it, vi } from "vitest";

const { fetchMock } = vi.hoisted(() => ({ fetchMock: vi.fn() }));
vi.mock("@/lib/api/fetcher", () => ({
  apiRequestAuth: fetchMock,
  fetcher: vi.fn(),
}));
vi.mock("@/lib/api/useAuthedSWR", () => ({ useAuthedSWR: vi.fn(() => ({ data: undefined })) }));

import { useAuthedSWR } from "@/lib/api/useAuthedSWR";
import { contentPageSchema, spaceSchema, spaceUsageSchema, transferPreviewSchema } from "@/lib/validations/content";
import {
  CONTENT_API_BASE_URL,
  SPACE_API_BASE_URL,
  matchesContentFeedKey,
  previewTransfer,
  restoreContent,
  setRestricted,
  transferContent,
  updateSpaceDefaultRole,
  useSpaceUsage,
} from "@/lib/api/content";

const ok = (body: unknown) => ({ ok: true, status: 200, json: () => Promise.resolve(body) });

describe("content validations", () => {
  it("parses a space", () => {
    expect(
      spaceSchema.parse({
        id: "5b7d2b0e-6c2f-4f0a-9b1a-1f9a8c1d2e3f",
        kind: "team",
        name: "Marketing",
        default_role: "editor",
        my_role: "owner",
        team_id: "5b7d2b0e-6c2f-4f0a-9b1a-1f9a8c1d2e40",
        organization_id: null,
      }).kind
    ).toBe("team");
  });
  it("parses a feed page with defaults", () => {
    const page = contentPageSchema.parse({
      items: [
        {
          type: "layer",
          id: "5b7d2b0e-6c2f-4f0a-9b1a-1f9a8c1d2e3f",
          name: "Bike network",
          space_id: "5b7d2b0e-6c2f-4f0a-9b1a-1f9a8c1d2e40",
          folder_id: null,
          updated_at: "2026-09-01T10:00:00Z",
          created_at: "2026-09-01T10:00:00Z",
          my_role: "owner",
          layer_type: "feature",
          feature_layer_geometry_type: "line",
        },
      ],
      total: 1,
      page: 1,
      size: 50,
    });
    expect(page.items[0].is_shortcut).toBe(false);
    expect(page.items[0].shared_with).toBeNull();
  });
  it("parses a space's usage", () => {
    const usage = spaceUsageSchema.parse({
      space_id: "5b7d2b0e-6c2f-4f0a-9b1a-1f9a8c1d2e3f",
      bytes: 3000,
      layers: 2,
      projects: 1,
    });
    expect(usage.bytes).toBe(3000);
    expect(usage.layers).toBe(2);
    expect(usage.projects).toBe(1);
  });

  it("parses a transfer preview", () => {
    const p = transferPreviewSchema.parse({
      items: [],
      datasets: [],
      grants_to_drop: 2,
      folders_in_subtrees: 0,
      trashed_in_subtrees: 0,
      warnings: [],
    });
    expect(p.name_collisions).toEqual([]);
    expect(p.skipped_foreign).toBe(0);
  });
});

describe("content api", () => {
  beforeEach(() => fetchMock.mockReset());

  it("matches feed keys only", () => {
    expect(matchesContentFeedKey([CONTENT_API_BASE_URL, { view: "space" }])).toBe(true);
    expect(matchesContentFeedKey(`${CONTENT_API_BASE_URL}/trash?space_id=x`)).toBe(true);
    expect(matchesContentFeedKey(["https://example.org/api/v2/project", {}])).toBe(false);
  });

  it("restores items with a 204", async () => {
    fetchMock.mockResolvedValue({ ok: true, status: 204 });
    await restoreContent([{ type: "layer", id: "a" }]);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe(`${CONTENT_API_BASE_URL}/restore`);
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body)).toEqual({ items: [{ type: "layer", id: "a" }] });
  });

  it("previews and executes a transfer", async () => {
    fetchMock.mockResolvedValue(
      ok({ items: [], datasets: [], grants_to_drop: 0, folders_in_subtrees: 0, trashed_in_subtrees: 0, warnings: [] })
    );
    await previewTransfer({ items: [{ type: "project", id: "p" }], target_space_id: "s" });
    expect(fetchMock.mock.calls[0][0]).toBe(`${CONTENT_API_BASE_URL}/transfer/preview`);

    fetchMock.mockResolvedValue(ok({ moved: { folder: 0, project: 1, layer: 2, bundle: 0 }, shortcuts: 1, trashed_moved: 0 }));
    const r = await transferContent({
      items: [{ type: "project", id: "p" }],
      target_space_id: "s",
      dataset_ids: ["d1", "d2"],
      leave_shortcut: true,
    });
    expect(r.moved.layer).toBe(2);
    expect(JSON.parse(fetchMock.mock.calls[1][1].body).leave_shortcut).toBe(true);
  });

  it("throws on a failed transfer with the server detail", async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 409, json: () => Promise.resolve({ detail: "Name collision: home" }) });
    await expect(
      transferContent({ items: [], target_space_id: "s", dataset_ids: [], leave_shortcut: false })
    ).rejects.toThrow("Name collision: home");
  });

  it("patches the space default role", async () => {
    fetchMock.mockResolvedValue(ok({ id: "s", kind: "team", name: "T", default_role: "viewer" }));
    await updateSpaceDefaultRole("s", "viewer");
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toMatch(/\/api\/v2\/space\/s$/);
    expect(init.method).toBe("PATCH");
  });

  it("patches an item's restricted flag with a 204", async () => {
    fetchMock.mockResolvedValue({ ok: true, status: 204 });
    await setRestricted("layer", "l1", true);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe(`${CONTENT_API_BASE_URL}/layer/l1/restricted`);
    expect(init.method).toBe("PATCH");
    expect(JSON.parse(init.body)).toEqual({ restricted: true });
  });

  it("throws on a failed restricted update with the server detail", async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 400, json: () => Promise.resolve({ detail: "A space root cannot be restricted" }) });
    await expect(setRestricted("folder", "f1", true)).rejects.toThrow("A space root cannot be restricted");
  });
});

describe("useSpaceUsage", () => {
  beforeEach(() => vi.mocked(useAuthedSWR).mockClear());

  it("builds the usage URL for the given space", () => {
    useSpaceUsage("s1");
    expect(vi.mocked(useAuthedSWR).mock.calls.at(-1)?.[0]).toBe(`${SPACE_API_BASE_URL}/s1/usage`);
  });

  it("skips the fetch with a null key when there is no active space", () => {
    useSpaceUsage(null);
    expect(vi.mocked(useAuthedSWR).mock.calls.at(-1)?.[0]).toBeNull();
  });
});
