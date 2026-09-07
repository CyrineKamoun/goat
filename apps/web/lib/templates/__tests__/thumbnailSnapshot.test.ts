import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  regenerateTemplateThumbnail,
  regenerateTemplateThumbnailResult,
  snapshotFileName,
} from "@/lib/templates/thumbnailSnapshot";
import type { TemplateRead } from "@/lib/validations/template";

const {
  uploadAssetMock,
  updateTemplateMock,
  readTemplateMock,
  renderWorkflowSnapshotMock,
  renderLayoutSnapshotMock,
} = vi.hoisted(() => ({
  uploadAssetMock: vi.fn(),
  updateTemplateMock: vi.fn(),
  readTemplateMock: vi.fn(),
  renderWorkflowSnapshotMock: vi.fn(),
  renderLayoutSnapshotMock: vi.fn(),
}));

vi.mock("@/lib/api/assets", () => ({ uploadAsset: uploadAssetMock }));
vi.mock("@/lib/api/templates", () => ({
  updateTemplate: updateTemplateMock,
  readTemplate: readTemplateMock,
}));
vi.mock("@/lib/templates/workflowSnapshot", () => ({
  renderWorkflowSnapshot: renderWorkflowSnapshotMock,
}));
vi.mock("@/lib/templates/layoutSnapshot", () => ({ renderLayoutSnapshot: renderLayoutSnapshotMock }));

const template = (overrides: Partial<TemplateRead>): TemplateRead =>
  ({
    id: "tmpl-1",
    name: "Bus network analysis",
    payload_kind: "workflow",
    thumbnail_url: null,
    config: null,
    ...overrides,
  }) as TemplateRead;

/** The frozen config the refresh left on the template — what the picture is
 * re-drawn from, read back with `readTemplate`. */
const workflowConfig = {
  nodes: [
    {
      id: "a",
      type: "dataset",
      position: { x: 0, y: 0 },
      data: { type: "dataset", label: "Stops", geometryType: "point" },
    },
  ],
  edges: [],
};

const layoutConfig = {
  page: { size: "A3", orientation: "landscape" },
  elements: [{ id: "m", type: "map", position: { x: 10, y: 10, width: 200, height: 150 } }],
};

beforeEach(() => {
  readTemplateMock.mockReset().mockImplementation(async (id: string) => template({ id }));
  uploadAssetMock.mockReset().mockResolvedValue({ url: "https://assets.example/thumb.png" });
  updateTemplateMock.mockReset().mockImplementation(async (id: string, body: Record<string, unknown>) => ({
    ...template({ id }),
    ...body,
  }));
  renderWorkflowSnapshotMock.mockReset().mockResolvedValue(new Blob(["png"], { type: "image/png" }));
  renderLayoutSnapshotMock.mockReset().mockResolvedValue(new Blob(["png"], { type: "image/png" }));
});

describe("snapshotFileName", () => {
  it("names the file after the template", () => {
    expect(snapshotFileName("Bus network analysis")).toBe("bus-network-analysis.png");
    expect(snapshotFileName("  Ürban — 2026 ")).toBe("rban-2026.png");
    expect(snapshotFileName("!!")).toBe("template.png");
  });
});

describe("regenerateTemplateThumbnail", () => {
  it("re-draws a workflow's picture from the config it reads back", async () => {
    const translate = (key: string) => key;
    readTemplateMock.mockResolvedValue(template({ config: workflowConfig }));
    const updated = await regenerateTemplateThumbnail(template({}), translate);

    // The refresh response carries no config, so it is read back — with the
    // switch only an owner/editor is answered on.
    expect(readTemplateMock).toHaveBeenCalledWith("tmpl-1", true);
    expect(renderWorkflowSnapshotMock).toHaveBeenCalledWith(
      expect.objectContaining({ kind: "workflow" }),
      undefined,
      translate
    );
    const [file, assetType, options] = uploadAssetMock.mock.calls[0];
    expect((file as File).name).toBe("bus-network-analysis.png");
    expect((file as File).type).toBe("image/png");
    expect(assetType).toBe("image");
    expect(options).toEqual(expect.objectContaining({ category: "template_thumbnail" }));
    expect(updateTemplateMock).toHaveBeenCalledWith("tmpl-1", {
      thumbnail_url: "https://assets.example/thumb.png",
    });
    expect(updated.thumbnail_url).toBe("https://assets.example/thumb.png");
  });

  it("re-draws a layout as its own wireframe and re-labels the page it prints on", async () => {
    readTemplateMock.mockResolvedValue(template({ payload_kind: "layout", config: layoutConfig }));
    await regenerateTemplateThumbnail(template({ payload_kind: "layout" }));

    expect(renderLayoutSnapshotMock).toHaveBeenCalledWith(expect.objectContaining({ kind: "layout" }));
    expect(renderWorkflowSnapshotMock).not.toHaveBeenCalled();
    expect(updateTemplateMock).toHaveBeenCalledWith("tmpl-1", {
      thumbnail_url: "https://assets.example/thumb.png",
      page_size: "A3",
      page_orientation: "landscape",
    });
  });

  it("leaves a template with nothing to draw exactly as it came in", async () => {
    for (const [read, given] of [
      [template({ config: null }), template({})],
      [template({ payload_kind: "project" }), template({ payload_kind: "project" })],
      [template({ config: { nodes: [] } }), template({})],
    ] as const) {
      readTemplateMock.mockResolvedValue(read);
      expect(await regenerateTemplateThumbnail(given)).toBe(given);
    }
    expect(uploadAssetMock).not.toHaveBeenCalled();
    expect(updateTemplateMock).not.toHaveBeenCalled();
  });

  it("keeps the template it was given when the config cannot even be read", async () => {
    const source = template({});
    readTemplateMock.mockRejectedValue(new Error("403"));

    expect(await regenerateTemplateThumbnail(source)).toBe(source);
    expect(renderWorkflowSnapshotMock).not.toHaveBeenCalled();
  });

  it("keeps the template it was given when the picture cannot be drawn or stored", async () => {
    const source = template({});
    readTemplateMock.mockResolvedValue(template({ config: workflowConfig }));

    renderWorkflowSnapshotMock.mockRejectedValue(new Error("no canvas"));
    expect(await regenerateTemplateThumbnail(source)).toBe(source);
    expect(uploadAssetMock).not.toHaveBeenCalled();

    renderWorkflowSnapshotMock.mockResolvedValue(new Blob(["png"], { type: "image/png" }));
    uploadAssetMock.mockRejectedValue(new Error("507"));
    expect(await regenerateTemplateThumbnail(source)).toBe(source);
    expect(updateTemplateMock).not.toHaveBeenCalled();

    uploadAssetMock.mockResolvedValue({ url: "https://assets.example/thumb.png" });
    updateTemplateMock.mockRejectedValue(new Error("409"));
    expect(await regenerateTemplateThumbnail(source)).toBe(source);
  });
});

describe("regenerateTemplateThumbnailResult", () => {
  it("draws the picture, stores it as an asset and writes its url on the template", async () => {
    readTemplateMock.mockResolvedValue(template({ config: workflowConfig }));

    const result = await regenerateTemplateThumbnailResult({ id: "tmpl-1", name: "Bus network analysis" });

    expect(readTemplateMock).toHaveBeenCalledWith("tmpl-1", true);
    const [file, assetType] = uploadAssetMock.mock.calls[0];
    expect((file as File).name).toBe("bus-network-analysis.png");
    expect(assetType).toBe("image");
    expect(updateTemplateMock).toHaveBeenCalledWith("tmpl-1", {
      thumbnail_url: "https://assets.example/thumb.png",
    });
    expect(result).toEqual({
      status: "updated",
      template: expect.objectContaining({ thumbnail_url: "https://assets.example/thumb.png" }),
    });
  });

  it("writes a layout's page along with its picture", async () => {
    readTemplateMock.mockResolvedValue(template({ payload_kind: "layout", config: layoutConfig }));

    const result = await regenerateTemplateThumbnailResult({ id: "tmpl-1", name: "Poster" });

    expect(renderLayoutSnapshotMock).toHaveBeenCalledWith(expect.objectContaining({ kind: "layout" }));
    expect(updateTemplateMock).toHaveBeenCalledWith("tmpl-1", {
      thumbnail_url: "https://assets.example/thumb.png",
      page_size: "A3",
      page_orientation: "landscape",
    });
    expect(result.status).toBe("updated");
  });

  it("writes nothing when the template comes back without a config", async () => {
    for (const read of [template({ config: null }), template({ payload_kind: "project", config: null })]) {
      readTemplateMock.mockResolvedValue(read);
      expect(await regenerateTemplateThumbnailResult({ id: "tmpl-1", name: "Bus" })).toEqual({
        status: "no_config",
      });
    }
    expect(uploadAssetMock).not.toHaveBeenCalled();
    expect(updateTemplateMock).not.toHaveBeenCalled();
  });

  it("writes nothing when the config it read holds no picture", async () => {
    readTemplateMock.mockResolvedValue(template({ config: { nodes: [] } }));

    expect(await regenerateTemplateThumbnailResult({ id: "tmpl-1", name: "Bus" })).toEqual({
      status: "nothing_to_draw",
    });
    expect(uploadAssetMock).not.toHaveBeenCalled();
    expect(updateTemplateMock).not.toHaveBeenCalled();
  });

  it("writes nothing when the read, the drawing, the upload or the write fails", async () => {
    readTemplateMock.mockRejectedValue(new Error("403"));
    expect((await regenerateTemplateThumbnailResult({ id: "tmpl-1", name: "Bus" })).status).toBe("failed");
    expect(uploadAssetMock).not.toHaveBeenCalled();

    readTemplateMock.mockResolvedValue(template({ config: workflowConfig }));
    renderWorkflowSnapshotMock.mockRejectedValue(new Error("no canvas"));
    expect((await regenerateTemplateThumbnailResult({ id: "tmpl-1", name: "Bus" })).status).toBe("failed");
    expect(uploadAssetMock).not.toHaveBeenCalled();

    renderWorkflowSnapshotMock.mockResolvedValue(new Blob(["png"], { type: "image/png" }));
    uploadAssetMock.mockRejectedValue(new Error("507"));
    expect((await regenerateTemplateThumbnailResult({ id: "tmpl-1", name: "Bus" })).status).toBe("failed");
    expect(updateTemplateMock).not.toHaveBeenCalled();

    uploadAssetMock.mockResolvedValue({ url: "https://assets.example/thumb.png" });
    updateTemplateMock.mockRejectedValue(new Error("409"));
    expect((await regenerateTemplateThumbnailResult({ id: "tmpl-1", name: "Bus" })).status).toBe("failed");
  });
});
