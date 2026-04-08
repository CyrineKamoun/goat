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

/**
 * Loads a Google Font by injecting a <link> into <head>.
 * Mulish is already loaded by next/font so it's skipped.
 */
function loadGoogleFont(fontName: string) {
  if (fontName === "Mulish") return; // Already loaded by next/font

  const id = `google-font-${fontName.replace(/\s+/g, "-").toLowerCase()}`;
  if (document.getElementById(id)) return; // Already loaded

  const link = document.createElement("link");
  link.id = id;
  link.rel = "stylesheet";
  link.href = `https://fonts.googleapis.com/css2?family=${encodeURIComponent(fontName)}:wght@300;400;500;600;700&display=swap`;
  document.head.appendChild(link);
}

/**
 * Hook that reads the dashboard font setting from the project config,
 * loads the Google Font if needed, and returns the CSS font-family value.
 *
 * Accepts an optional project prop (used in the builder where the project
 * lives in component props rather than the Redux store).
 */
export function useDashboardFont(projectProp?: Project): string {
  const storeProject = useAppSelector(selectProject);
  const project = projectProp ?? storeProject;
  const fontFamily = project?.builder_config?.settings?.font_family || DEFAULT_DASHBOARD_FONT;
  const fontName = extractFontName(fontFamily);

  useEffect(() => {
    loadGoogleFont(fontName);
  }, [fontName]);

  return fontFamily;
}
