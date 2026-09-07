"use client";

import { useTranslation } from "react-i18next";

import { usePreferences } from "@/lib/api/preferences";
import { useReleases } from "@/lib/api/releases";

import { useHomeStage } from "@/hooks/dashboard/home/useHomeStage";
import { useSpotlight } from "@/hooks/dashboard/home/useSpotlight";

import SpotlightModal from "@/components/dashboard/home/SpotlightModal";

/**
 * H7's spotlight surface: mounted once by `HomePage` regardless of stage, so
 * it can still fire in Established where there is no "what's new" band to
 * attach it to. Renders nothing until `useSpotlight` clears every rule for
 * an entry.
 */
const HomeSpotlight = () => {
  const { i18n } = useTranslation("common");
  const locale = i18n.language === "de" ? "de" : "en";

  const { entries } = useReleases(locale);
  const { preferences } = usePreferences();
  const { stage } = useHomeStage();
  const { entry, dismiss } = useSpotlight(entries, preferences, stage);

  if (!entry) return null;

  return <SpotlightModal entry={entry} onClose={() => void dismiss()} />;
};

export default HomeSpotlight;
