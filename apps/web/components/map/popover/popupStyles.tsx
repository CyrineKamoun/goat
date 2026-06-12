import { GlobalStyles, useTheme } from "@mui/material";
import { alpha } from "@mui/material/styles";

/**
 * Default styles for custom-HTML (and ejected) popup content. Scoped under
 * `.goat-feature-popup` so they never leak into the rest of the app.
 *
 * These mirror what `PopupBlockRenderer` renders for the equivalent blocks
 * (field list, badge, buttons) so a popup ejected from Simple → Advanced HTML
 * looks identical to the simple-mode popup. Keep the two in sync.
 */
export function PopupHtmlStyles() {
  const theme = useTheme();
  // Same hairline row divider PopupBlockRenderer/MapFeaturePopover use.
  const hairline = alpha(theme.palette.text.primary, 0.08);

  return (
    <GlobalStyles
      styles={{
        ".goat-feature-popup": {
          fontFamily: theme.typography.fontFamily,
          color: theme.palette.text.primary,

          // ---- Field list: table layout (matches FieldListBlock isTable) ----
          "& table.attr-table": {
            width: "100%",
            borderCollapse: "collapse",
            tableLayout: "fixed",
          },
          "& .attr-table td": {
            padding: "7px 0",
            fontSize: 12,
            verticalAlign: "top",
            borderBottom: `1px solid ${hairline}`,
            overflowWrap: "anywhere",
            wordBreak: "break-word",
          },
          "& .attr-table tbody tr:last-child td": { borderBottom: 0 },
          "& .attr-table .attr-field": {
            width: "40%",
            paddingRight: 12,
            color: theme.palette.text.secondary,
          },
          "& .attr-table .attr-value": {
            width: "60%",
            fontWeight: 600,
            textAlign: "right",
          },

          // ---- Field list: definition-list layout (matches non-table) ----
          "& dl.attr-list": { margin: 0 },
          "& dl.attr-list .attr-field": {
            fontSize: 12,
            color: theme.palette.text.secondary,
            overflowWrap: "anywhere",
            wordBreak: "break-word",
          },
          "& dl.attr-list .attr-value": {
            margin: "2px 0 12px 0",
            fontSize: 14,
            fontWeight: 600,
            overflowWrap: "anywhere",
            wordBreak: "break-word",
          },
          "& dl.attr-list .attr-value:last-child": { marginBottom: 0 },

          // ---- Media / links ----
          "& img": { maxWidth: "100%", borderRadius: 4, display: "block" },
          "& a": { color: theme.palette.primary.main, textDecoration: "none" },

          // ---- Badge (matches PopupBlockRenderer badge defaults) ----
          "& .badge": {
            display: "inline-block",
            padding: "2px 8px",
            borderRadius: 999,
            fontSize: 10,
            fontWeight: 700,
            letterSpacing: 0.3,
            color: "#fff",
            backgroundColor: "#8A8D93",
          },

          // ---- Buttons (match PopupBlockRenderer button styles) ----
          "& .btn-link": {
            color: theme.palette.primary.main,
            textDecoration: "none",
            fontWeight: 600,
            fontSize: 12,
          },
          "& .btn-outlined": {
            display: "inline-block",
            padding: "6px 12px",
            borderRadius: 4,
            border: `1px solid ${theme.palette.primary.main}`,
            color: theme.palette.primary.main,
            textDecoration: "none",
            fontWeight: 600,
            fontSize: 12,
          },
          "& .btn-filled": {
            display: "inline-block",
            padding: "6px 12px",
            borderRadius: 4,
            backgroundColor: theme.palette.primary.main,
            color: "#fff",
            textDecoration: "none",
            fontWeight: 600,
            fontSize: 12,
          },
        },
      }}
    />
  );
}
