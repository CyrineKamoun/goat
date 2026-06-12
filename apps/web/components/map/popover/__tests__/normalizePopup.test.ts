import { describe, expect, it } from "vitest";

import { popupProperties } from "@/lib/validations/layer";

import { normalizePopup } from "../normalizePopup";

const base = () => popupProperties.parse({});

describe("normalizePopup", () => {
  it("maps legacy position=fixed to layout=pinned and keeps anchor", () => {
    const out = normalizePopup({ ...base(), position: "fixed", anchor: "bottom_left" });
    expect(out.layout).toBe("pinned");
    expect(out.anchor).toBe("bottom_left");
    expect(out.position).toBeUndefined();
  });

  it("maps legacy position=in_place to layout=popup", () => {
    const out = normalizePopup({ ...base(), position: "in_place" });
    expect(out.layout).toBe("popup");
    expect(out.position).toBeUndefined();
  });

  it("maps show_layer_header boolean to header enum", () => {
    expect(normalizePopup({ ...base(), show_layer_header: false }).header).toBe("none");
    expect(normalizePopup({ ...base(), show_layer_header: true }).header).toBe("standard");
    expect(normalizePopup({ ...base(), show_layer_header: undefined }).show_layer_header).toBeUndefined();
  });

  it("defaults anchor to top_right when layout is pinned but anchor missing", () => {
    const out = normalizePopup({ ...base(), position: "fixed" });
    expect(out.anchor).toBe("top_right");
  });

  it("is idempotent on already-normalized popups", () => {
    const once = normalizePopup({ ...base(), position: "fixed", show_layer_header: false });
    const twice = normalizePopup(once);
    expect(twice).toEqual(once);
  });

  it("leaves a modern popup untouched (no legacy keys present)", () => {
    const modern = { ...base(), layout: "pinned" as const, anchor: "top_left" as const, header: "compact" as const };
    const out = normalizePopup(modern);
    expect(out.layout).toBe("pinned");
    expect(out.anchor).toBe("top_left");
    expect(out.header).toBe("compact");
  });
});
