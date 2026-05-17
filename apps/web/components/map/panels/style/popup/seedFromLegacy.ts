import { v4 } from "uuid";

import type {
  LayerInteractionContent,
  PopupBlock,
  PopupProperties,
} from "@/lib/validations/layer";

/**
 * Build a sensible popup config for a layer that has either:
 *  - no popup config at all (freshly added layer), or
 *  - only the legacy `interaction` field on its properties.
 *
 * The result is what the popup renderer falls back to when
 * `layer.properties.popup` is missing — so the popup "just works"
 * for layers that have never been touched in the popup editor.
 *
 * Defaults:
 *  - When legacy content has no `field_list`, we add an empty
 *    `fieldList` block. The renderer treats `attributes: []` as
 *    "show every column on the layer" — so by default a clicked
 *    feature renders every column without anyone having to configure
 *    anything. Users opting in to specific attributes via the editor
 *    fills in `attributes`, which overrides the all-fields fallback.
 */
export function seedPopupFromInteraction(
  interaction: { type?: string; content?: LayerInteractionContent[] } | undefined,
): PopupProperties {
  const blocks: PopupBlock[] = (interaction?.content ?? []).map((c) => {
    if (c.type === "field_list") {
      return {
        id: c.id ?? v4(),
        type: "fieldList",
        layout: "table",
        attributes: c.attributes ?? [],
        collapse_after: null,
      };
    }
    return {
      id: c.id ?? v4(),
      type: "image",
      source: "static",
      url: c.url ?? "",
      sizing: "fixed",
      height: 140,
      aspect: "16/9",
    };
  });

  // Guarantee a field-list block exists — empty `attributes` means
  // "all columns" at render time (see PopupBlockRenderer).
  const hasFieldList = blocks.some((b) => b.type === "fieldList");
  if (!hasFieldList) {
    blocks.unshift({
      id: v4(),
      type: "fieldList",
      layout: "table",
      attributes: [],
      collapse_after: null,
    });
  }

  return {
    enabled: interaction?.type !== "none",
    trigger: "click",
    mode: "simple",
    blocks,
    html: "",
    show_layer_header: true,
    position: "in_place",
    anchor: "top_right",
    highlight_active_feature: true,
  };
}
