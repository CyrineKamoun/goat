import { Box, Typography } from "@mui/material";

import type { BuilderWidgetSchema, ProjectLayer, ProjectLayerGroup } from "@/lib/validations/project";
import type { LinksElementSchema, TabsContainerSchema, WidgetElementConfig } from "@/lib/validations/widget";

import DividerElementWidget from "@/components/builder/widgets/elements/Divider";
import ImageElementWidget from "@/components/builder/widgets/elements/Image";
import LinksElementWidget from "@/components/builder/widgets/elements/Links";
import TabsWidget from "@/components/builder/widgets/elements/Tabs";
import type { TextEditorContext } from "@/components/builder/widgets/elements/text/Text";
import TextElementWidget from "@/components/builder/widgets/elements/text/Text";

interface WidgetElementProps {
  widget?: BuilderWidgetSchema;
  config: WidgetElementConfig;
  viewOnly?: boolean;
  onWidgetUpdate?: (newData: WidgetElementConfig) => void;
  onNestedWidgetUpdate?: (updatedWidget: BuilderWidgetSchema) => void;
  fitMode?: "auto" | "contain";
  context?: TextEditorContext;
  /** Available feature attribute names for dynamic text insertion */
  featureAttributes?: string[];
  // For tabs widget
  projectLayers?: ProjectLayer[];
  projectLayerGroups?: ProjectLayerGroup[];
  panelWidgets?: BuilderWidgetSchema[];
}

const hasOptions = (
  config: WidgetElementConfig
): config is WidgetElementConfig & { options: { description?: string } } =>
  "options" in config && typeof config.options === "object" && config.options !== null;

const WidgetElement: React.FC<WidgetElementProps> = ({
  widget,
  config,
  onWidgetUpdate,
  onNestedWidgetUpdate,
  viewOnly,
  fitMode,
  context,
  featureAttributes,
  projectLayers,
  projectLayerGroups,
  panelWidgets,
}) => {
  return (
    <Box sx={{ width: "100%", height: fitMode === "contain" || config.type === "text" ? "100%" : undefined }}>
      {config.type === "text" && (
        <TextElementWidget
          config={config}
          viewOnly={viewOnly}
          context={context}
          onWidgetUpdate={onWidgetUpdate}
          featureAttributes={featureAttributes}
        />
      )}
      {config.type === "divider" && <DividerElementWidget config={config} />}
      {config.type === "image" && (
        <ImageElementWidget
          config={config}
          viewOnly={viewOnly}
          onWidgetUpdate={onWidgetUpdate}
          fitMode={fitMode}
        />
      )}
      {config.type === "links" && <LinksElementWidget config={config as LinksElementSchema} />}
      {config.type === "tabs" && widget && projectLayers && projectLayerGroups && (
        <TabsWidget
          widget={widget}
          config={config as TabsContainerSchema}
          projectLayers={projectLayers}
          projectLayerGroups={projectLayerGroups}
          viewOnly={viewOnly}
          panelWidgets={panelWidgets}
          onNestedWidgetUpdate={onNestedWidgetUpdate}
        />
      )}
      {hasOptions(config) && config.options.description && (
        <Typography variant="body2" align="left">
          {config.options.description}
        </Typography>
      )}
    </Box>
  );
};

export default WidgetElement;
