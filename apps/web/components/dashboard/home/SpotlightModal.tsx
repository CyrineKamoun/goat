"use client";

import { useTranslation } from "react-i18next";

import type { ReleaseEntry } from "@/lib/validations/home";

import AnnouncementDialog, { type AnnouncementMedia } from "@/components/common/AnnouncementDialog";

interface SpotlightModalProps {
  entry: ReleaseEntry;
  onClose: () => void;
}

/**
 * H7's spotlight: one changelog entry announced once, through the shared
 * two-pane dialog. Headline, summary and highlights come from the entry's
 * `goat:spotlight` element with the item as fallback; a video leads when
 * there is one, else the screenshot. "Learn more" opens the entry's link
 * (the changelog anchor, or a blog post when the author set one). Any
 * action, including closing the video, dismisses for good.
 */
const SpotlightModal = ({ entry, onClose }: SpotlightModalProps) => {
  const { t } = useTranslation("common");
  const spotlight = entry.spotlight;
  const url = spotlight?.cta?.url ?? entry.url;
  const learnMore = { label: spotlight?.cta?.label ?? t("learn_more"), href: url };

  const media: AnnouncementMedia | undefined = spotlight?.video
    ? { kind: "video", src: spotlight.video, poster: spotlight.image ?? entry.thumbnail }
    : spotlight?.image
      ? { kind: "image", src: spotlight.image, alt: spotlight.headline ?? entry.title }
      : entry.thumbnail
        ? { kind: "image", src: entry.thumbnail, alt: entry.title }
        : undefined;

  return (
    <AnnouncementDialog
      open
      headline={spotlight?.headline ?? entry.title}
      body={[entry.summary]}
      highlights={spotlight?.highlights ?? []}
      media={media}
      primary={media?.kind === "video" ? { label: t("watch_video"), playsVideo: true } : learnMore}
      secondary={media?.kind === "video" ? learnMore : undefined}
      dismissLabel={t("skip")}
      onDismiss={onClose}
    />
  );
};

export default SpotlightModal;
