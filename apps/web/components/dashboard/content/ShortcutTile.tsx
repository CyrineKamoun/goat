"use client";

import { Box, Typography, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { ContentItem } from "@/lib/validations/content";
import { markKindOf } from "@/lib/utils/content";

import type { PopperMenuItem } from "@/components/common/PopperMenu";
import MarkBlock from "@/components/dashboard/common/MarkBlock";
import SurfaceCard from "@/components/dashboard/common/SurfaceCard";
import ContentKebab from "@/components/dashboard/content/ContentKebab";
import ContentSelectCircle from "@/components/dashboard/content/ContentSelectCircle";

interface ShortcutTileProps {
  item: ContentItem;
  /** The shortcut's real home space — resolved by the caller from
   * `spaces.find(s => s.id === item.space_id)`, since a shortcut row's
   * `space_id` is the *target* item's space, not the folder it's listed in. */
  targetSpaceName: string;
  selected: boolean;
  anySelected: boolean;
  onToggleSelect: (id: string) => void;
  onOpen: (item: ContentItem) => void;
  menuItems: PopperMenuItem[];
  onMenuSelect: (item: PopperMenuItem) => void;
  /** Below `md`: a larger 28px select circle and a 40px kebab tap target,
   * matching `ContentCard`'s mobile sizing so the grid reads consistently. */
  mobile?: boolean;
}

/** A shortcut tile: dashed border to mark it as a pointer rather than the
 * real content, the target's own type icon with a small "external" badge,
 * and a "Shortcut · in {space}" caption. Only ever offers Open — the
 * shortcut isn't the thing to rename/move/delete, its target is. */
const ShortcutTile = ({
  item,
  targetSpaceName,
  selected,
  anySelected,
  onToggleSelect,
  onOpen,
  menuItems,
  onMenuSelect,
  mobile,
}: ShortcutTileProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  return (
    <SurfaceCard
      selected={selected}
      dashed
      onClick={(event) => {
        event.stopPropagation();
        if (anySelected) onToggleSelect(item.id);
        else onOpen(item);
      }}
      className="content-card"
      sx={{
        cursor: "pointer",
        display: "flex",
        alignItems: "center",
        gap: "10px",
        padding: "11px 14px",
        opacity: 0.92,
        "&:hover": { opacity: 1 },
        "&:hover .content-card-select": { opacity: 1 },
      }}>
      <MarkBlock
        kind={markKindOf(item)}
        geometryType={item.feature_layer_geometry_type}
        size={30}
        badge={
          <Box
            component="span"
            sx={{
              position: "absolute",
              right: -4,
              bottom: -4,
              width: 15,
              height: 15,
              borderRadius: "50%",
              backgroundColor: theme.palette.background.paper,
              border: `1px solid ${theme.palette.divider}`,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
            }}>
            <Icon
              iconName={ICON_NAME.EXTERNAL_LINK}
              style={{ fontSize: 9, color: theme.palette.text.secondary }}
            />
          </Box>
        }
      />

      <Box sx={{ flexGrow: 1, minWidth: 0 }}>
        <Typography component="div" noWrap sx={{ fontSize: 13, fontWeight: 700 }}>
          {item.name}
        </Typography>
        <Typography component="div" noWrap sx={{ fontSize: 11.5, color: theme.palette.text.secondary }}>
          {t("shortcut_in", { name: targetSpaceName })}
        </Typography>
      </Box>

      <ContentSelectCircle
        className="content-card-select"
        checked={selected}
        onToggle={() => onToggleSelect(item.id)}
        mobile={mobile}
      />
      <ContentKebab items={menuItems} onSelect={onMenuSelect} mobile={mobile} />
    </SurfaceCard>
  );
};

export default ShortcutTile;
