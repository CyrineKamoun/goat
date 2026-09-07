"use client";

import { Box, Button, Dialog, DialogActions, DialogContent, Typography } from "@mui/material";
import { useTranslation } from "react-i18next";

import type { ReleaseEntry } from "@/lib/validations/home";

interface SpotlightModalProps {
  entry: ReleaseEntry;
  onClose: () => void;
}

const isVideo = (url: string): boolean => /\.(mp4|webm)$/i.test(url);

/**
 * H7's spotlight surface: the entry's own media, headline, summary and one
 * CTA. No auto-play with sound — a video renders with native controls and
 * muted, and never starts itself. Both the CTA and "Got it" dismiss the
 * modal; the CTA also opens in a new tab so the caller never loses Home.
 */
const SpotlightModal = ({ entry, onClose }: SpotlightModalProps) => {
  const { t } = useTranslation("common");
  const spotlight = entry.spotlight;
  const headline = spotlight?.headline ?? entry.title;
  const media = spotlight?.media;
  const cta = spotlight?.cta;

  return (
    <Dialog open onClose={onClose} maxWidth="xs" fullWidth>
      {media &&
        (isVideo(media) ? (
          <Box component="video" src={media} controls muted sx={{ width: "100%", display: "block" }} />
        ) : (
          <Box component="img" src={media} alt={headline} sx={{ width: "100%", display: "block" }} />
        ))}
      <DialogContent>
        <Typography variant="h6" gutterBottom>
          {headline}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          {entry.summary}
        </Typography>
      </DialogContent>
      <DialogActions>
        {cta && (
          <Button component="a" href={cta.url} target="_blank" rel="noopener noreferrer" onClick={onClose}>
            {cta.label}
          </Button>
        )}
        <Button variant="contained" onClick={onClose}>
          {t("got_it")}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default SpotlightModal;
