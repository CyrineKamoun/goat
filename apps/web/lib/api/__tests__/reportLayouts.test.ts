import { beforeEach, describe, expect, it, vi } from "vitest";

import { PROJECTS_API_BASE_URL } from "@/lib/api/projects";
import { refreshReportLayout, updateReportLayout } from "@/lib/api/reportLayouts";

const { fetchMock, mutateMock } = vi.hoisted(() => ({ fetchMock: vi.fn(), mutateMock: vi.fn() }));
vi.mock("@/lib/api/fetcher", () => ({ apiRequestAuth: fetchMock, fetcher: vi.fn() }));
vi.mock("swr", () => ({ default: vi.fn(() => ({})), mutate: mutateMock }));

const PROJECT_ID = "11111111-1111-1111-1111-111111111111";
const LAYOUT_ID = "33333333-3333-3333-3333-333333333333";
const itemKey = [`${PROJECTS_API_BASE_URL}/${PROJECT_ID}/report-layout/${LAYOUT_ID}`];

describe("report layouts api cache invalidation", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    mutateMock.mockReset();
  });

  it("invalidates the layout key after a successful update, so a saved config is never read stale", async () => {
    fetchMock.mockResolvedValue({ ok: true, json: () => Promise.resolve({ id: LAYOUT_ID }) });

    const updated = await updateReportLayout(PROJECT_ID, LAYOUT_ID, { name: "Atlas" });

    expect(updated).toEqual({ id: LAYOUT_ID });
    expect(mutateMock).toHaveBeenCalledTimes(1);
    expect(mutateMock).toHaveBeenCalledWith(itemKey);
  });

  it("does not invalidate when the update fails", async () => {
    fetchMock.mockResolvedValue({ ok: false, json: () => Promise.resolve({}) });

    await expect(updateReportLayout(PROJECT_ID, LAYOUT_ID, { name: "Atlas" })).rejects.toThrow(
      "Failed to update report layout"
    );
    expect(mutateMock).not.toHaveBeenCalled();
  });

  it("refreshReportLayout invalidates the array key the read hook uses", () => {
    refreshReportLayout(PROJECT_ID, LAYOUT_ID);
    expect(mutateMock).toHaveBeenCalledWith(itemKey);
  });
});
