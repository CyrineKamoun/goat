import { useEffect } from "react";

import { selectProject } from "@/lib/store/layer/selectors";
import type { Project } from "@/lib/validations/project";

import { useAppSelector } from "@/hooks/store/ContextHooks";

const DEFAULT_DASHBOARD_FONT = "Mulish, sans-serif";

/**
 * Extracts the primary font name from a CSS font-family string.
 * e.g. "'Open Sans', sans-serif" → "Open Sans"
 */
function extractFontName(fontFamily: string): string {
  const first = fontFamily.split(",")[0].trim();
  return first.replace(/^['"]|['"]$/g, "");
}

function injectFontFace(id: string, fontName: string, url: string) {
  const css = `@font-face{font-family:"${fontName}";src:url("${url}") format("woff2");font-display:swap;}`;
  const existing = document.getElementById(id) as HTMLStyleElement | null;
  if (existing) {
    if (existing.textContent !== css) existing.textContent = css;
    return;
  }
  const style = document.createElement("style");
  style.id = id;
  style.textContent = css;
  document.head.appendChild(style);
}

/**
 * Hook that reads the dashboard font setting from the project config and
 * returns the CSS font-family value.
 *
 * Preset Google Fonts are bundled with the app (see packages/js/ui/assets/fonts)
 * and imported globally, so they don't need to be loaded on demand. When the
 * project specifies a custom font_url, this hook injects an @font-face rule
 * pointing at it.
 *
 * Accepts an optional project prop (used in the builder where the project
 * lives in component props rather than the Redux store).
 */
export function useDashboardFont(projectProp?: Project): string {
  const storeProject = useAppSelector(selectProject);
  const project = projectProp ?? storeProject;
  const fontFamily = project?.builder_config?.settings?.font_family || DEFAULT_DASHBOARD_FONT;
  const fontUrl = project?.builder_config?.settings?.font_url;
  const fontName = extractFontName(fontFamily);
  const customUrl = fontUrl && fontUrl.trim() ? fontUrl.trim() : null;

  useEffect(() => {
    if (customUrl) injectFontFace("dashboard-font-custom", fontName, customUrl);
  }, [fontName, customUrl]);

  return fontFamily;
}
