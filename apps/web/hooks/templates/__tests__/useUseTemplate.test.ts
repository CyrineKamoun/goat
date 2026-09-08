/**
 * `templateResultHref` is the whole contract between "Use template" and the
 * map page: it must emit the `?mode=`/`?workflow=`/`?layout=` query params
 * that `useMapUrlIntent` (hooks/map/useMapUrlIntent.ts) reads on mount.
 */
import { describe, expect, it } from "vitest";

import type { TemplateRead, TemplateUseResult } from "@/lib/validations/template";

import { templateResultHref } from "@/hooks/templates/useUseTemplate";

const template = {} as TemplateRead;

const baseResult: TemplateUseResult = {
  project_id: "11111111-1111-1111-1111-111111111111",
  workflow_id: null,
  layout_id: null,
  added_layer_project_ids: [],
  unresolved_inputs: [],
};

describe("templateResultHref", () => {
  it("opens the workflows panel on the created workflow for a workflow payload", () => {
    const result: TemplateUseResult = { ...baseResult, workflow_id: "22222222-2222-2222-2222-222222222222" };
    expect(templateResultHref(template, result)).toBe(
      "/map/11111111-1111-1111-1111-111111111111?mode=workflows&workflow=22222222-2222-2222-2222-222222222222"
    );
  });

  it("opens the reports panel on the created layout for a layout payload", () => {
    const result: TemplateUseResult = { ...baseResult, layout_id: "33333333-3333-3333-3333-333333333333" };
    expect(templateResultHref(template, result)).toBe(
      "/map/11111111-1111-1111-1111-111111111111?mode=reports&layout=33333333-3333-3333-3333-333333333333"
    );
  });

  it("lands on the plain project route for a project payload (no workflow/layout id)", () => {
    expect(templateResultHref(template, baseResult)).toBe("/map/11111111-1111-1111-1111-111111111111");
  });
});
