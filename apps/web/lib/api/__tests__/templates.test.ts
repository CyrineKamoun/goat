import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  TEMPLATE_API_BASE_URL,
  TemplateApiError,
  createTemplate,
  matchesTemplatesKey,
  publishTemplateWithDetail,
  updateTemplate,
  useTemplateCategories,
  useTemplates,
} from "@/lib/api/templates";
import type { TemplateCreate } from "@/lib/validations/template";

const { useAuthedSWRMock, apiRequestAuthMock } = vi.hoisted(() => ({
  useAuthedSWRMock: vi.fn(),
  apiRequestAuthMock: vi.fn(),
}));
vi.mock("@/lib/api/useAuthedSWR", () => ({ useAuthedSWR: useAuthedSWRMock }));
vi.mock("@/lib/api/fetcher", () => ({ apiRequestAuth: apiRequestAuthMock, fetcher: vi.fn() }));
vi.mock("swr", () => ({ mutate: vi.fn() }));

const createBody: TemplateCreate = {
  name: "Bus network",
  folder_id: "11111111-1111-1111-1111-111111111111",
  source: { kind: "workflow", project_id: "22222222-2222-2222-2222-222222222222" },
};

/** The SWR key `useTemplates` builds for the given params. Named `use…` so
 * the rules-of-hooks lint accepts a wrapper that calls a hook. */
const useTemplatesKey = (params: Parameters<typeof useTemplates>[0]) => {
  useTemplates(params);
  return useAuthedSWRMock.mock.calls.at(-1)?.[0];
};

/** The SWR key `useTemplateCategories` builds for the given params. */
const useCategoriesKey = (params?: Parameters<typeof useTemplateCategories>[0]) => {
  useTemplateCategories(params);
  return useAuthedSWRMock.mock.calls.at(-1)?.[0];
};

describe("templates api", () => {
  beforeEach(() => {
    useAuthedSWRMock.mockReset().mockReturnValue({ data: undefined, isLoading: true, mutate: vi.fn() });
    apiRequestAuthMock.mockReset();
  });

  it("leaves an unset filter out of the request params", () => {
    // `fetcher` serializes the params with `new URLSearchParams`, which turns
    // a `kind: undefined` entry into the literal `kind=undefined` — rejected
    // by the enum-typed query parameter.
    expect(useTemplatesKey({ source: "all", kind: undefined, size: 12 })).toEqual([
      TEMPLATE_API_BASE_URL,
      { source: "all", size: 12 },
    ]);
  });

  it("gives a kind-less query the same key whether the caller spelled the kind out or not", () => {
    expect(useTemplatesKey({ source: "mine", kind: undefined, size: 12 })).toEqual(
      useTemplatesKey({ source: "mine", size: 12 })
    );
  });

  it("keeps a set filter", () => {
    expect(useTemplatesKey({ source: "goat", kind: "workflow", search: "roads" })).toEqual([
      TEMPLATE_API_BASE_URL,
      { source: "goat", kind: "workflow", search: "roads" },
    ]);
  });

  it("sends the selected tags as one comma-separated value", () => {
    expect(useTemplatesKey({ source: "all", categories: "Mobility,Transit" })).toEqual([
      TEMPLATE_API_BASE_URL,
      { source: "all", categories: "Mobility,Transit" },
    ]);
  });

  it("asks the facet endpoint for the tag counts under the same filters", () => {
    expect(useCategoriesKey({ source: "goat", kind: "workflow" })).toEqual([
      `${TEMPLATE_API_BASE_URL}/categories`,
      { source: "goat", kind: "workflow" },
    ]);
  });

  it("gives a caller who named no source the same key as one who asked for all", () => {
    // The backend defaults `source` to `all`, so the save dialog's bare call
    // and the browser's `all` are one request rather than two cache entries.
    expect(useCategoriesKey()).toEqual([`${TEMPLATE_API_BASE_URL}/categories`, { source: "all" }]);
    expect(useCategoriesKey({ source: "all", kind: undefined })).toEqual(useCategoriesKey());
  });

  it("reads the facet response as the array the endpoint answers with", () => {
    useAuthedSWRMock.mockReturnValue({
      data: [{ name: "Mobility", count: 4 }],
      isLoading: false,
      mutate: vi.fn(),
    });

    expect(useTemplateCategories().categories).toEqual([{ name: "Mobility", count: 4 }]);
  });

  it("nulls the key when there is nothing to ask for", () => {
    expect(useTemplatesKey(null)).toBeNull();
  });

  it("matchesTemplatesKey recognises both a bare url and an array key", () => {
    expect(matchesTemplatesKey(TEMPLATE_API_BASE_URL)).toBe(true);
    expect(matchesTemplatesKey([`${TEMPLATE_API_BASE_URL}/abc`, { include_config: true }])).toBe(true);
    expect(matchesTemplatesKey([`${TEMPLATE_API_BASE_URL}/categories`, { source: "all" }])).toBe(true);
    expect(matchesTemplatesKey(["https://api.test/api/v2/project", {}])).toBe(false);
  });
});

describe("templates api — structured errors", () => {
  beforeEach(() => {
    apiRequestAuthMock.mockReset();
  });

  it("carries the create 422's unreadable-input code and layer", async () => {
    apiRequestAuthMock.mockResolvedValue({
      ok: false,
      status: 422,
      json: async () => ({ detail: { code: "template_input_not_readable", layer_id: "L1" } }),
    });

    await expect(createTemplate(createBody)).rejects.toMatchObject({
      detail: { code: "template_input_not_readable", layer_id: "L1" },
    });
  });

  it("keeps a plain string detail as the message", async () => {
    apiRequestAuthMock.mockResolvedValue({
      ok: false,
      status: 403,
      json: async () => ({ detail: "Not allowed" }),
    });

    await expect(createTemplate(createBody)).rejects.toThrow("Not allowed");
  });

  it("reports the publish 409 as a refusal naming the datasets", async () => {
    apiRequestAuthMock.mockResolvedValue({
      ok: false,
      status: 409,
      json: async () => ({
        detail: { code: "template_dataset_not_public", layers: [{ id: "L1", name: "Parks" }] },
      }),
    });

    await expect(publishTemplateWithDetail("t1")).resolves.toEqual({
      ok: false,
      layers: [{ id: "L1", name: "Parks" }],
    });
  });

  it("rethrows any other publish failure", async () => {
    apiRequestAuthMock.mockResolvedValue({ ok: false, status: 500, json: async () => ({}) });

    await expect(publishTemplateWithDetail("t1")).rejects.toBeInstanceOf(TemplateApiError);
  });

  it("PATCHes a folder move to the template endpoint", async () => {
    apiRequestAuthMock.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        id: "33333333-3333-3333-3333-333333333333",
        name: "Bus network",
        description: null,
        categories: [],
        thumbnail_url: null,
        space_id: "44444444-4444-4444-4444-444444444444",
        folder_id: "55555555-5555-5555-5555-555555555555",
        payload_kind: "workflow",
        kinds: ["workflow"],
        inputs: [],
        catalog_status: "none",
        my_role: "owner",
        created_at: "2026-01-01T00:00:00Z",
        updated_at: "2026-01-01T00:00:00Z",
      }),
    });

    const updated = await updateTemplate("33333333-3333-3333-3333-333333333333", {
      folder_id: "55555555-5555-5555-5555-555555555555",
    });

    expect(apiRequestAuthMock).toHaveBeenCalledWith(
      `${TEMPLATE_API_BASE_URL}/33333333-3333-3333-3333-333333333333`,
      expect.objectContaining({
        method: "PATCH",
        body: JSON.stringify({ folder_id: "55555555-5555-5555-5555-555555555555" }),
      })
    );
    expect(updated.folder_id).toBe("55555555-5555-5555-5555-555555555555");
  });
});
