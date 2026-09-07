import type { PopupProperties } from "@/lib/validations/layer";

import { EDGE_GAP } from "@/components/map/popover/PopupFixedHost";

/** The field list is the panel's only block, so its id is fixed rather than
 * generated: the schema requires a UUID and nothing distinguishes two of them. */
const FIELD_LIST_BLOCK_ID = "6f0c2b1e-4d3a-4a8e-9c1b-2e7d5f8a9b10";

/** The feature panel a detail map shows: hover previews, a click pins.
 *
 * Datasets read on a detail page carry no popup configuration of their own, so
 * both detail maps synthesise the same one — a pinned field table in the corner
 * opposite the legend, sized to the map it sits on.
 *
 * `panelHeight` is the map's rendered height and `attributionRoom` how much of
 * its bottom the credit strip takes, so the panel stops above it.
 */
export const detailPopupConfig = (
  fields: { name: string; type: string }[],
  panelHeight: number,
  attributionRoom: number
): PopupProperties => ({
  enabled: true,
  // The same behaviour the project map gives this value: hover previews (transient), click pins (sticky, and hovers stop changing it).
  trigger: "click_and_hover",
  mode: "simple",
  blocks: [
    {
      id: FIELD_LIST_BLOCK_ID,
      type: "fieldList",
      layout: "table",
      attributes: fields.map((field) => ({
        name: field.name,
        type: field.type === "number" ? ("number" as const) : ("string" as const),
      })),
      collapse_after: null,
    },
  ],
  html: "",
  // Pinned to a corner: an in-place popover sized for a full-screen map
  // covers most of a card-sized one.
  layout: "pinned",
  anchor: "top_right",
  // The title above the map already names the dataset, and a pan or a click
  // off the features closes the panel, so it needs no close control.
  header: "none",
  // The app's own recolour and pulse on whichever feature the panel describes.
  highlight_active_feature: true,
  width: 300,
  // The map's height, less the host's gap above, the credit strip below, and
  // the same gap again between the two. Rounded: the measured height is
  // fractional and the schema wants an integer.
  max_height: Math.round(Math.max(160, panelHeight - EDGE_GAP * 2 - attributionRoom)),
});
