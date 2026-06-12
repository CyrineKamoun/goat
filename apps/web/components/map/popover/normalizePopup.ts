import type { PopupProperties } from "@/lib/validations/layer";

/**
 * Maps legacy popup fields (position / show_layer_header) forward onto the
 * unified layout/header model and strips the deprecated keys. Pure and
 * idempotent — safe to call at every read site. New popups (no legacy keys)
 * pass through unchanged.
 *
 * Disambiguation rule: the PRESENCE of a legacy key signals legacy data and
 * wins over the zod-defaulted `layout`/`header`, because new code never writes
 * `position` / `show_layer_header`.
 */
export function normalizePopup(popup: PopupProperties): PopupProperties {
  const next: PopupProperties = { ...popup };

  if (popup.position !== undefined) {
    next.layout = popup.position === "fixed" ? "pinned" : "popup";
  }
  if (popup.show_layer_header !== undefined) {
    next.header = popup.show_layer_header ? "standard" : "none";
  }
  if (next.layout === "pinned" && !next.anchor) {
    next.anchor = "top_right";
  }

  delete next.position;
  delete next.show_layer_header;
  return next;
}
