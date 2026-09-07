"use client";

import { IconButton } from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { PopperMenuItem } from "@/components/common/PopperMenu";
import PopperMenu from "@/components/common/PopperMenu";

interface ContentKebabProps {
  items: PopperMenuItem[];
  onSelect: (item: PopperMenuItem) => void;
  /** Below `md`, the button grows to a 40px tap target instead of MUI's
   * small `IconButton` default. */
  mobile?: boolean;
}

/** The per-card/-row "…" action menu. Wraps the shared `PopperMenu` with
 * the same icon `TileCard.tsx` uses for its own kebab button; `PopperMenu`
 * already stops the opening click from bubbling to the card/row underneath,
 * so this never doubles as a select-or-open click. Renders nothing when the
 * item has no actions (e.g. a shortcut whose target space no longer exists). */
const ContentKebab = ({ items, onSelect, mobile }: ContentKebabProps) => {
  const { t } = useTranslation("common");

  if (items.length === 0) return null;

  return (
    <PopperMenu
      disablePortal={false}
      menuItems={items}
      menuButton={
        <IconButton
          size="small"
          aria-label={t("more")}
          sx={{
            width: mobile ? 40 : 28,
            height: mobile ? 40 : 28,
            borderRadius: "6px",
            flexShrink: 0,
          }}>
          <Icon iconName={ICON_NAME.MORE_VERT} style={{ fontSize: mobile ? 20 : 18 }} />
        </IconButton>
      }
      onSelect={onSelect}
    />
  );
};

export default ContentKebab;
