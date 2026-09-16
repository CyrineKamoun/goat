import { describe, expect, it } from "vitest";

import { materializeMapElements } from "@/lib/print/mapElements";
import type { ProjectLayer } from "@/lib/validations/project";
import type { ReportElement } from "@/lib/validations/reportLayout";

const initialView = {
  latitude: 52.5,
  longitude: 13.4,
  zoom: 11,
  bearing: 0,
  pitch: 0,
  min_zoom: 0,
  max_zoom: 20,
};

const layer = (id: number, visible = true, properties: Record<string, unknown> = {}): ProjectLayer =>
  ({
    id,
    properties: { visibility: visible, color: `#${id}${id}${id}`, ...properties },
  }) as unknown as ProjectLayer;

const mapElement = (config: Record<string, unknown>): ReportElement => ({
  id: "map-1",
  type: "map",
  position: { x: 0, y: 0, width: 100, height: 80, z_index: 1 },
  config,
  style: { opacity: 1, padding: 0 },
});

const projectLayers = [layer(7), layer(8, false), layer(9)];
const ctx = { initialView, projectLayers, basemapUrl: "https://tiles.example/streets.json" };

describe("materializeMapElements", () => {
  it("gives a map element with no view the project's initial view", () => {
    const [el] = materializeMapElements([mapElement({})], ctx);
    expect(el.config.viewState).toEqual({ latitude: 52.5, longitude: 13.4, zoom: 11, bearing: 0, pitch: 0 });
  });

  it("leaves a saved view alone", () => {
    const saved = { latitude: 1, longitude: 2, zoom: 3, bearing: 0, pitch: 0 };
    const [el] = materializeMapElements([mapElement({ viewState: saved })], ctx);
    expect(el.config.viewState).toBe(saved);
  });

  it("captures the lock snapshot from the project's visible layers when a locked map has none", () => {
    const [el] = materializeMapElements([mapElement({ lock_layers: true, lock_styles: true })], ctx);
    expect(el.config.locked_layer_ids).toEqual([7, 9]);
    expect(el.config.locked_basemap_url).toBe(ctx.basemapUrl);
    expect(el.config.locked_layer_styles).toEqual({
      7: { visibility: true, color: "#777" },
      9: { visibility: true, color: "#999" },
    });
  });

  it("does not freeze styles when only layers are locked", () => {
    const [el] = materializeMapElements([mapElement({ lock_layers: true })], ctx);
    expect(el.config.locked_layer_ids).toEqual([7, 9]);
    expect(el.config.locked_layer_styles).toBeUndefined();
  });

  it("recaptures a snapshot whose ids belong to another project", () => {
    const [el] = materializeMapElements(
      [mapElement({ lock_layers: true, locked_layer_ids: [301, 302], locked_basemap_url: "old" })],
      ctx
    );
    expect(el.config.locked_layer_ids).toEqual([7, 9]);
    expect(el.config.locked_basemap_url).toBe(ctx.basemapUrl);
  });

  it("keeps a snapshot that still points at this project's layers", () => {
    const input = mapElement({
      lock_layers: true,
      locked_layer_ids: [9, 301],
      viewState: { latitude: 1, longitude: 2, zoom: 3 },
    });
    const result = materializeMapElements([input], ctx);
    expect(result[0]).toBe(input);
  });

  it("waits for the project's layers before capturing", () => {
    const elements = [mapElement({ lock_layers: true, viewState: { latitude: 1, longitude: 2, zoom: 3 } })];
    expect(materializeMapElements(elements, { ...ctx, projectLayers: [] })).toBe(elements);
  });

  it("returns the same array when nothing needs to change", () => {
    const elements = [
      mapElement({ viewState: { latitude: 1, longitude: 2, zoom: 3 } }),
      { ...mapElement({}), id: "text-1", type: "text" as const },
    ];
    expect(materializeMapElements(elements, ctx)).toBe(elements);
  });

  it("waits for the initial view before filling one in", () => {
    const elements = [mapElement({})];
    expect(materializeMapElements(elements, { ...ctx, initialView: undefined })).toBe(elements);
  });
});
