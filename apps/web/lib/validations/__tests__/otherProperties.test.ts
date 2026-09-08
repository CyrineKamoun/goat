import { describe, expect, it } from "vitest";

import { isCatalogLayer } from "@/lib/utils/catalog-layer";
import { canSetDefaultStyle } from "@/lib/utils/layerPermissions";
import { projectLayerSchema } from "@/lib/validations/project";

/**
 * `useActiveLayer` runs the project layer through `projectLayerSchema.parse`,
 * and a plain `z.object` drops every key it does not declare. `catalog_item`
 * lives in `other_properties`, so parsing turned a catalog layer into an
 * ordinary one — and the style panel offered "Set as default" on a dataset
 * whose `PUT` the server refuses.
 */
const catalogProjectLayer = {
  id: 87197,
  layer_id: "f99556c1-3442-4712-a947-c0dc804e1e9f",
  name: "Quartiere Biel",
  type: "feature",
  in_catalog: false,
  created_at: "2026-08-30T18:49:26.000Z",
  updated_at: "2026-08-30T18:49:26.000Z",
  properties: { visibility: true },
  other_properties: {
    catalog_item: { id: "2b83c9a3-1026-4edb-ac7f-77bc2ddcb4cc", title: "Quartiere Biel" },
    catalog_materialize: { status: "ready" },
  },
};

describe("projectLayerSchema", () => {
  it("keeps the catalog markers through a parse", () => {
    const parsed = projectLayerSchema.parse(catalogProjectLayer);

    expect(parsed.other_properties?.catalog_item).toBeDefined();
    expect(isCatalogLayer(parsed)).toBe(true);
  });

  it("still hides the dataset-level style action after parsing", () => {
    expect(canSetDefaultStyle(projectLayerSchema.parse(catalogProjectLayer))).toBe(false);
  });

  it("keeps the declared WMS fields as they were", () => {
    const parsed = projectLayerSchema.parse({
      ...catalogProjectLayer,
      other_properties: { url: "https://wms.example/service", srs: "EPSG:3857" },
    });

    expect(parsed.other_properties?.url).toBe("https://wms.example/service");
    expect(parsed.other_properties?.srs).toBe("EPSG:3857");
  });
});
