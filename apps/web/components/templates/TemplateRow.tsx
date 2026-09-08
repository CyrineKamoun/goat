"use client";

import { Box, Typography, alpha, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import { Icon } from "@p4b/ui/components/Icon";

import { layoutPageDescription } from "@/lib/templates/previewGeometry";
import { stripMarkdown, templateMarkIcon } from "@/lib/utils/templates";
import type { TemplateRead } from "@/lib/validations/template";

import ContentThumbnail from "@/components/dashboard/common/ContentThumbnail";
import TemplateDefaultThumbnail, {
  hasTemplateDefaultThumbnail,
} from "@/components/templates/TemplateDefaultThumbnail";
import TemplateTag from "@/components/templates/TemplateTag";

/** The side of the tile leading a row. */
const THUMBNAIL_SIZE = 34;

interface TemplateRowProps {
  template: TemplateRead;
  selected: boolean;
  onSelect: () => void;
  /** The shelf this template came from. Unset while the list is grouped by
   * shelf — the section label above the row already says it. */
  sourceLabel?: string;
}

/** One template in the browser's list: its picture as a small tile — its
 * thumbnail where it has one, else the default its payload kind draws or the
 * template mark — the name, and two lines of description. An option in a
 * listbox rather than a link: picking one fills the preview beside it, it
 * does not navigate. */
const TemplateRow = ({ template, selected, onSelect, sourceLabel }: TemplateRowProps) => {
  const theme = useTheme();
  const { t } = useTranslation("common");
  /** A layout template says which page it prints on, the same tag its
   * preview carries. */
  const page = layoutPageDescription(template, t);

  return (
    <Box
      component="button"
      type="button"
      role="option"
      aria-selected={selected}
      onClick={onSelect}
      sx={{
        width: "100%",
        display: "flex",
        alignItems: "flex-start",
        gap: "12px",
        padding: "10px 12px",
        borderRadius: "10px",
        textAlign: "left",
        font: "inherit",
        cursor: "pointer",
        border: "1px solid transparent",
        backgroundColor: selected ? theme.palette.action.selected : "transparent",
        borderColor: selected ? theme.palette.primary.main : "transparent",
        "&:hover": { backgroundColor: selected ? theme.palette.action.selected : theme.palette.action.hover },
      }}>
      <Box
        sx={{
          width: THUMBNAIL_SIZE,
          height: THUMBNAIL_SIZE,
          flexShrink: 0,
          borderRadius: "8px",
          overflow: "hidden",
          // The tile's own corners are the row's; `ContentThumbnail` rounds
          // its frame to the theme's radius, which is wider than this.
          "& > div": { borderRadius: 0 },
        }}>
        <ContentThumbnail
          kind="template"
          variant="mark"
          height={THUMBNAIL_SIZE}
          href={template.thumbnail_url ?? undefined}
          fallback={
            // No picture: the default its payload kind draws, the same one
            // the card and the preview show at their own sizes. A project
            // has none and keeps the template mark.
            hasTemplateDefaultThumbnail(template.payload_kind) ? (
              <TemplateDefaultThumbnail payloadKind={template.payload_kind} page={page} variant="mark" />
            ) : (
              <Box
                sx={{
                  width: "100%",
                  height: "100%",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  backgroundColor: alpha(theme.palette.secondary.main, 0.12),
                }}>
                <Icon
                  iconName={templateMarkIcon(template)}
                  style={{ fontSize: 16 }}
                  htmlColor={theme.palette.secondary.main}
                />
              </Box>
            )
          }
        />
      </Box>
      <Box sx={{ minWidth: 0, flex: 1 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: "8px" }}>
          <Typography
            component="div"
            noWrap
            sx={{ fontSize: 13.5, fontWeight: 700, color: theme.palette.text.primary, minWidth: 0 }}>
            {template.name}
          </Typography>
          {page && <TemplateTag label={page.label} />}
          {sourceLabel && <TemplateTag label={sourceLabel} />}
        </Box>
        {template.description && (
          <Typography
            component="div"
            sx={{
              fontSize: 12,
              lineHeight: 1.4,
              marginTop: "2px",
              color: theme.palette.text.secondary,
              display: "-webkit-box",
              WebkitLineClamp: 2,
              WebkitBoxOrient: "vertical",
              overflow: "hidden",
            }}>
            {stripMarkdown(template.description)}
          </Typography>
        )}
      </Box>
    </Box>
  );
};

export default TemplateRow;
