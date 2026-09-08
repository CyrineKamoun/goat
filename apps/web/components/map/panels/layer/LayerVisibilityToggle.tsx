import { IconButton, Tooltip } from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { ProjectLayer } from "@/lib/validations/project";

/** The eye button that shows or hides one project layer. */
export const LayerVisibilityToggle = ({
  layer,
  toggleLayerVisibility,
}: {
  layer: ProjectLayer;
  toggleLayerVisibility?: (layer: ProjectLayer) => void;
}) => {
  const { t } = useTranslation("common");
  if (layer.type === "table") {
    return null;
  }

  return (
    <Tooltip
      key={layer.id}
      title={layer.properties?.visibility ? t("hide_layer") : t("show_layer")}
      arrow
      placement="top">
      <IconButton
        size="small"
        onClick={(event) => {
          event.stopPropagation();
          toggleLayerVisibility?.(layer);
        }}>
        <Icon
          iconName={!layer.properties?.visibility ? ICON_NAME.EYE_SLASH : ICON_NAME.EYE}
          style={{
            fontSize: 15,
          }}
        />
      </IconButton>
    </Tooltip>
  );
};
