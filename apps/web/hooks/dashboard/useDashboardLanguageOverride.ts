import { useEffect } from "react";
import { useTranslation } from "react-i18next";

/**
 * Applies a public dashboard's configured language while it is shown in
 * view-only mode, restoring the previous language on unmount. Used by both the
 * desktop and mobile public layouts so the two views stay consistent.
 */
export function useDashboardLanguageOverride(
  dashboardLanguage: string | null | undefined,
  viewOnly: boolean | undefined,
): void {
  const { i18n } = useTranslation();
  useEffect(() => {
    if (viewOnly && dashboardLanguage && dashboardLanguage !== "auto" && dashboardLanguage !== i18n.language) {
      const prevLang = i18n.language;
      i18n.changeLanguage(dashboardLanguage);
      return () => {
        i18n.changeLanguage(prevLang);
      };
    }
  }, [viewOnly, dashboardLanguage, i18n]);
}
