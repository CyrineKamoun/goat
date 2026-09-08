"use client";

import { Box, alpha, useTheme } from "@mui/material";

import { tagColor } from "@/lib/utils/tagColor";

interface TemplateTagProps {
  label: string;
  /** `kind` takes the template accent the kind badges use; `category` takes
   * the colour the tag's own name hashes to, so one category reads the same
   * wherever it is rendered; `neutral` (the default) is the muted chip a
   * shelf name sits in. */
  tone?: "neutral" | "kind" | "category";
}

/** The small tag the browser marks a template with — the shelf it came from
 * on a row, the kind and the shelf above a preview's title, and the
 * categories it carries. Not `TypeTag`: that one is a dark scrim for sitting
 * on imagery, while these sit on the dialog's own paper. */
const TemplateTag = ({ label, tone = "neutral" }: TemplateTagProps) => {
  const theme = useTheme();
  const category = tone === "category" ? tagColor(label, theme.palette.mode) : null;
  const color = category
    ? category.fg
    : tone === "kind"
      ? theme.palette.secondary.main
      : theme.palette.text.secondary;

  return (
    <Box
      component="span"
      // A long category name is cut rather than pushing the row wide, so the
      // full spelling stays reachable on hover.
      title={tone === "category" ? label : undefined}
      sx={{
        flexShrink: 0,
        maxWidth: "100%",
        overflow: "hidden",
        textOverflow: "ellipsis",
        padding: "2px 6px",
        borderRadius: "4px",
        fontSize: category ? 11 : 10.5,
        fontWeight: category ? 600 : 700,
        // A category is a name the author typed, so it is shown as typed;
        // the shelf and kind labels are our own words and stay uppercase.
        letterSpacing: category ? "0.1px" : "0.5px",
        textTransform: category ? "none" : "uppercase",
        whiteSpace: "nowrap",
        color,
        backgroundColor: category ? category.bg : alpha(color, tone === "kind" ? 0.12 : 0.08),
      }}>
      {label}
    </Box>
  );
};

export default TemplateTag;
