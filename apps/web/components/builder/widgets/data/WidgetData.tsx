import { Box, Typography } from "@mui/material";

import type { ProjectLayer } from "@/lib/validations/project";
import { type RichTextDataSchema, type TableDataSchema, type WidgetDataConfig, dataTypes } from "@/lib/validations/widget";

import { NumbersDataWidget } from "@/components/builder/widgets/data/Numbers";
import { RichTextDataWidget } from "@/components/builder/widgets/data/RichText";
import { TableDataWidget } from "@/components/builder/widgets/data/Table";
import { FilterDataWidget } from "@/components/builder/widgets/data/WidgetFilter";

interface WidgetDataProps {
  id: string;
  config: WidgetDataConfig;
  projectLayers: ProjectLayer[];
  viewOnly?: boolean;
  onConfigChange?: (nextConfig: WidgetDataConfig) => void;
}

const WidgetData: React.FC<WidgetDataProps> = ({ id, config, projectLayers, viewOnly, onConfigChange }) => {
  // Rich text widget handles its own layout (inline TipTap editor, no title wrapper)
  if (config.type === dataTypes.Values.rich_text) {
    return (
      <Box sx={{ width: "100%", height: "100%" }}>
        <RichTextDataWidget
          config={config as unknown as RichTextDataSchema}
          projectLayers={projectLayers}
          viewOnly={viewOnly}
          onConfigChange={onConfigChange as ((nextConfig: RichTextDataSchema) => void) | undefined}
        />
      </Box>
    );
  }

  return (
    <Box sx={{ width: "100%" }}>
      <Typography variant="body1" fontWeight="bold" align="left" gutterBottom>
        {config.setup?.title}
      </Typography>
      {config.type === dataTypes.Values.numbers && (
        <NumbersDataWidget config={config} projectLayers={projectLayers} viewOnly={viewOnly} />
      )}
      {config.type === dataTypes.Values.filter && (
        <FilterDataWidget id={id} config={config} projectLayers={projectLayers} viewOnly={viewOnly} />
      )}
      {config.type === dataTypes.Values.table && (
        <TableDataWidget
          widgetId={id}
          config={config}
          projectLayers={projectLayers}
          viewOnly={viewOnly}
          onConfigChange={onConfigChange as ((nextConfig: TableDataSchema) => void) | undefined}
        />
      )}
      {config.options?.description && (
        <Typography variant="body2" align="left">
          {config.options.description}
        </Typography>
      )}
    </Box>
  );
};

export default WidgetData;
