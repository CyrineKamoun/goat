import { uploadAsset } from "@/lib/api/assets";
import { readTemplate, updateTemplate } from "@/lib/api/templates";
import { renderLayoutSnapshot } from "@/lib/templates/layoutSnapshot";
import type { PreviewTranslate } from "@/lib/templates/previewGeometry";
import {
  descriptorFromLayoutConfig,
  descriptorFromWorkflowConfig,
  layoutPageFromConfig,
} from "@/lib/templates/previewGeometry";
import { renderWorkflowSnapshot } from "@/lib/templates/workflowSnapshot";
import type { TemplatePreviewDescriptor, TemplateRead } from "@/lib/validations/template";

/** What a generated snapshot is uploaded as — the template's own name, since
 * the asset store lists files by name. */
export const snapshotFileName = (templateName: string): string => {
  const slug = templateName
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return `${slug || "template"}.png`;
};

/**
 * The picture a descriptor is stored as, or null where there is nothing to
 * draw: a layout's page as a wireframe, a workflow's node chain on the
 * canvas's own ground. A project descriptor carries only a layer count,
 * which is not a picture — it keeps the template mark.
 */
export const renderSnapshotFor = (
  descriptor: TemplatePreviewDescriptor | null,
  translate?: PreviewTranslate
): Promise<Blob> | null => {
  if (descriptor?.kind === "layout") return renderLayoutSnapshot(descriptor);
  if (descriptor?.kind === "workflow" && descriptor.nodes.length > 0) {
    return renderWorkflowSnapshot(descriptor, undefined, translate);
  }
  return null;
};

/** The structure a template's own frozen config stands for. Null for a
 * project payload, whose config lives on the frozen source project, and for
 * a caller the API answered without one (a viewer). */
export const descriptorForTemplate = (template: TemplateRead): TemplatePreviewDescriptor | null => {
  const config = (template.config ?? null) as Record<string, unknown> | null;
  if (template.payload_kind === "workflow") return descriptorFromWorkflowConfig(config);
  if (template.payload_kind === "layout") return descriptorFromLayoutConfig(config);
  return null;
};

/**
 * What a re-draw of a template's stored picture came to.
 *
 * `updated` carries the template as the write left it. The other three wrote
 * nothing: `no_config` is a template the API answered without its frozen
 * config — a caller with no write on it, and a project payload, whose config
 * lives on the frozen source project rather than on the template itself;
 * `nothing_to_draw` is a config holding no picture (an empty canvas);
 * `failed` is a read, drawing, upload or write that threw.
 */
export type TemplateThumbnailResult =
  | { status: "updated"; template: TemplateRead }
  | { status: "no_config" }
  | { status: "nothing_to_draw" }
  | { status: "failed"; error: unknown };

/**
 * Draws a template's picture from the config it carries now, stores the
 * drawing as an asset and writes that url on the template — and, for a
 * layout, the page it prints on with it, since a card labels a layout from
 * those two values rather than from the frozen config.
 *
 * The config is read back here (neither a feed row nor a refresh response
 * carries one) with the `include_config` switch the API answers to an
 * owner/editor only. Every outcome comes back as a value so a caller can say
 * what happened, and nothing is written unless there is a picture to write.
 */
export const regenerateTemplateThumbnailResult = async (
  template: Pick<TemplateRead, "id" | "name">,
  translate?: PreviewTranslate
): Promise<TemplateThumbnailResult> => {
  let withConfig: TemplateRead;
  try {
    withConfig = await readTemplate(template.id, true);
  } catch (error) {
    return { status: "failed", error };
  }
  if (!withConfig.config || withConfig.payload_kind === "project") return { status: "no_config" };
  const drawn = renderSnapshotFor(descriptorForTemplate(withConfig), translate);
  if (!drawn) return { status: "nothing_to_draw" };
  try {
    const file = new File([await drawn], snapshotFileName(template.name), { type: "image/png" });
    const asset = await uploadAsset(file, "image", {
      displayName: file.name,
      category: "template_thumbnail",
    });
    return {
      status: "updated",
      template: await updateTemplate(template.id, {
        thumbnail_url: asset.url,
        // A layout is labelled by the page it prints on, which the config
        // it carries now can name differently than the template does.
        ...(withConfig.payload_kind === "layout"
          ? layoutPageFromConfig(withConfig.config as Record<string, unknown>)
          : {}),
      }),
    };
  } catch (error) {
    return { status: "failed", error };
  }
};

/**
 * Re-draws a template's stored thumbnail from the config it carries now and
 * writes the new picture on it — what an update from source is for: the
 * frozen config has just been replaced, so the picture of it is stale. A
 * layout is also re-labelled with the page it now prints on.
 *
 * Returns the template as the write left it; a template with nothing to
 * draw, a drawing that could not be made and an upload that failed all leave
 * it exactly as it came in — the config update itself already stands, and a
 * picture that could not be stored is worth less than reporting that update
 * as a failure.
 */
export const regenerateTemplateThumbnail = async (
  template: TemplateRead,
  translate?: PreviewTranslate
): Promise<TemplateRead> => {
  const result = await regenerateTemplateThumbnailResult(template, translate);
  return result.status === "updated" ? result.template : template;
};
