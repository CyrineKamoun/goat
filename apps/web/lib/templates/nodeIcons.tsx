import CallSplitIcon from "@mui/icons-material/CallSplit";
import SettingsIcon from "@mui/icons-material/Settings";
import type { SvgIconProps } from "@mui/material";
import type { CSSProperties, FC, ReactElement } from "react";

import { type TOOL_ICON_NAME, toolIconMap } from "@p4b/ui/assets/svg/ToolIcons";
import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

/**
 * The five tones a two-tone tool icon is drawn in. `ToolIcons.tsx` writes
 * them as `var(--icon-color-N, …)` and `NodeIconWrapper`
 * (`components/workflows/nodes/shared.tsx`) sets them on an idle node; a
 * drawing outside the canvas carries no custom properties, so the tone is
 * substituted into the markup instead.
 */
export const NODE_ICON_TONES = {
  light: ["#666666", "#999999", "#BDBDBD", "#E3E3E3", "#FAFAFA"],
  dark: ["#FAFAFA", "#999999", "#BDBDBD", "#E3E3E3", "#666666"],
} as const;

/** The geometry a dataset node's `icon` names, mapped to the glyph
 * `DatasetNode.getGeometryIcon` picks for it. Anything else — a table, a
 * node whose layer is not resolved — reads as the table glyph, the same
 * default that switch falls through to. */
const GEOMETRY_ICON: Record<string, ICON_NAME> = {
  point: ICON_NAME.POINT_FEATURE,
  line: ICON_NAME.LINE_FEATURE,
  polygon: ICON_NAME.POLYGON_FEATURE,
  table: ICON_NAME.TABLE,
};

/** The `font-size` the canvas gives a node's glyph inside the 40px icon
 * wrapper: 32 everywhere, 28 for the if node's `CallSplitIcon`. */
export const nodeIconGlyphSize = (type: string): number => (type === "if" ? 28 : 32);

/** A table lookup that stops at the table's own keys, so an `icon` naming
 * `constructor` or `toString` is not an entry. */
const tableEntry = <T,>(table: Record<string, T>, key: string): T | undefined =>
  Object.prototype.hasOwnProperty.call(table, key) ? table[key] : undefined;

/**
 * The icon component the canvas puts in a node's icon wrapper, for the node
 * type and the `icon` its descriptor carries — a dataset's geometry glyph
 * (`DatasetNode.getGeometryIcon`), a tool's own icon with the gear
 * `ToolNode` falls back to, the export glyph `ExportNode` takes out of
 * `toolIconMap`, and `IfNode`'s branch. An annotation shows none.
 */
export const nodeIconElement = (type: string, icon?: string | null): ReactElement<SvgIconProps> | null => {
  switch (type) {
    case "dataset":
      return <Icon iconName={(icon ? tableEntry(GEOMETRY_ICON, icon) : undefined) ?? ICON_NAME.TABLE} />;
    case "tool": {
      const ToolIconComponent = icon ? tableEntry(toolIconMap as Record<string, FC>, icon) : undefined;
      return ToolIconComponent ? <ToolIconComponent /> : <SettingsIcon />;
    }
    case "export": {
      const ExportIcon = toolIconMap["export_dataset" as TOOL_ICON_NAME];
      return <ExportIcon />;
    }
    case "if":
      return <CallSplitIcon />;
    default:
      return null;
  }
};

/**
 * The custom properties a two-tone tool icon reads its own colours off:
 * `ToolIcons.tsx` writes every fill as `var(--icon-color-N, …)`, and
 * `NodeIconWrapper` sets them on the node. A drawing rendered into the page
 * sets them the same way, on the element the glyph sits in — they inherit,
 * so the glyph's own markup needs no rewriting.
 */
export const nodeIconToneStyle = (tones: readonly string[], color: string): CSSProperties =>
  Object.assign(
    { color } as CSSProperties,
    ...tones.map((tone, index) => ({ [`--icon-color-${index + 1}`]: tone }))
  );
