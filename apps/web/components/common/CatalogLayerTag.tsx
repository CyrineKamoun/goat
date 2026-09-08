import { Box, Tooltip, useTheme } from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

/**
 * Marks a layer as coming from the catalog, and says what follows from that.
 *
 * A catalog layer is a shared read-only snapshot: geoapi refuses every write to
 * it, so the data table hides Edit fields, Delete column and the row and
 * feature edits. Without this the panel just looks short of buttons.
 *
 * Deliberately neutral rather than accented: it sits beside outlined buttons in
 * a dense toolbar, read-only is not a warning, and the catalog's purple already
 * means "bundle" on the catalog cards.
 */
const CatalogLayerTag = () => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  return (
    <Tooltip title={t("catalog_layer_tag_tooltip")} placement="bottom" arrow>
      <Box
        component="span"
        sx={{
          display: "inline-flex",
          alignItems: "center",
          gap: 0.75,
          height: 22,
          px: 1,
          flexShrink: 0,
          border: "1px solid",
          borderColor: "divider",
          borderRadius: 1,
          fontSize: 11,
          fontWeight: 600,
          letterSpacing: 0.3,
          color: "text.secondary",
          whiteSpace: "nowrap",
        }}>
        {/* The globe is what the sidebar already means by "Catalog". */}
        <Icon
          iconName={ICON_NAME.GLOBE}
          style={{ fontSize: 11 }}
          htmlColor={theme.palette.text.secondary}
        />
        {t("catalog_layer_tag")}
      </Box>
    </Tooltip>
  );
};

export default CatalogLayerTag;
