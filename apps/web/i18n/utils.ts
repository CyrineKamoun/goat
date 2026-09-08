"use client";

import { de, enGB } from "date-fns/locale";
import { useTranslation } from "react-i18next";

// enGB rather than enUS: dates read day-first everywhere (19/08/2026), matching
// the de locale's ordering instead of switching to month-first for English.
export function useDateFnsLocale() {
  const { i18n } = useTranslation();
  return i18n?.language === "de" ? de : enGB;
}
