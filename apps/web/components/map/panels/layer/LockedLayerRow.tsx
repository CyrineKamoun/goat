import Stack from "@mui/material/Stack";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import { useTheme } from "@mui/material/styles";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

interface LockedLayerRowProps {
  /** The project layer's display name — a locked row still carries this. */
  name: string;
}

/**
 * Row content for a project layer the current user has no access to (D7):
 * the lock icon, the layer's name, and a hint explaining why, in place of
 * the usual geometry-preview icon and style legend. A locked row's
 * `properties` are blanked by the backend, so there is nothing to draw a
 * preview from.
 *
 * Rendered as `ProjectTreeItem.contentOverride` (see DraggableTreeView),
 * which replaces the tree's normal icon + label + caption slots with this
 * single node.
 */
export const LockedLayerRow = ({ name }: LockedLayerRowProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  return (
    <Stack direction="row" spacing={1} alignItems="center" sx={{ minWidth: 0 }}>
      <Tooltip title={t("locked")} placement="top">
        <Icon
          iconName={ICON_NAME.LOCK}
          data-testid="locked-layer-row-icon"
          style={{ fontSize: "1rem", color: theme.palette.action.active, flexShrink: 0 }}
        />
      </Tooltip>
      <Stack sx={{ minWidth: 0 }}>
        <Typography variant="body2" noWrap>
          {name}
        </Typography>
        <Typography
          variant="caption"
          sx={{ color: "text.secondary", wordBreak: "break-word", lineHeight: 1.3 }}>
          {t("layer_locked_hint")}
        </Typography>
      </Stack>
    </Stack>
  );
};
