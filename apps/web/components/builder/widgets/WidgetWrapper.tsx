/* eslint-disable @typescript-eslint/no-explicit-any */
import { useSortable } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { Box } from "@mui/material";
import { useMemo } from "react";

import { setSelectedBuilderItem } from "@/lib/store/map/slice";
import type { BuilderWidgetSchema, ProjectLayer, ProjectLayerGroup } from "@/lib/validations/project";
import type {
  WidgetChartConfig,
  WidgetDataConfig,
  WidgetElementConfig,
  WidgetInformationConfig,
} from "@/lib/validations/widget";
import { chartTypes, dataTypes, elementTypes, informationTypes } from "@/lib/validations/widget";

import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import WidgetChart from "@/components/builder/widgets/chart/WidgetChart";
import WidgetData from "@/components/builder/widgets/data/WidgetData";
import WidgetElement from "@/components/builder/widgets/elements/WidgetElement";
import WidgetInformation from "@/components/builder/widgets/information/WidgetInformation";
import { ElementWrapper } from "@/components/common/ElementWrapper";

interface WidgetWrapperProps {
  widget: BuilderWidgetSchema;
  projectLayers: ProjectLayer[];
  projectLayerGroups: ProjectLayerGroup[];
  viewOnly?: boolean;
  onWidgetDelete?: (widgetId: string) => void;
  onWidgetUpdate?: (updatedWidget: BuilderWidgetSchema) => void;
  panelWidgets?: BuilderWidgetSchema[];
}

interface DraggableWidgetContainerProps {
  children: React.ReactNode;
  widget: BuilderWidgetSchema;
  onWidgetDelete?: (widgetId: string) => void;
}

const DraggableWidgetContainer: React.FC<DraggableWidgetContainerProps> = ({
  children,
  widget,
  onWidgetDelete,
}) => {
  const dispatch = useAppDispatch();
  const selectedWidget = useAppSelector((state) => state.map.selectedBuilderItem);

  const { attributes, listeners, setNodeRef, transform, transition } = useSortable({
    id: widget.id,
    data: widget,
  });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition: `${transition}, border-color 0.2s ease-in-out`,
  };

  const isSelected = useMemo(() => {
    if (!selectedWidget || selectedWidget.type !== "widget") return false;
    if (selectedWidget.id === widget.id) return true;
    return false;
  }, [selectedWidget, widget.id]);

  return (
    <Box style={style} ref={setNodeRef}>
      <ElementWrapper
        isSelected={isSelected}
        onSelect={() => dispatch(setSelectedBuilderItem(widget))}
        onDelete={() => onWidgetDelete?.(widget.id)}
        dragAttributes={attributes}
        dragListeners={listeners}
        showDragHandle={true}
        disableContentPointerEvents={true}>
        {children}
      </ElementWrapper>
    </Box>
  );
};

const WidgetWrapper: React.FC<WidgetWrapperProps> = ({
  widget,
  projectLayers,
  projectLayerGroups,
  viewOnly,
  onWidgetDelete,
  onWidgetUpdate,
  panelWidgets,
}) => {
  const isFullHeight = widget.config?.type === "rich_text" || widget.config?.type === "text";
  const widgetContent = (
    <Box sx={{ p: 1, height: isFullHeight ? "100%" : undefined }}>
      {widget.config?.type && informationTypes.options.includes(widget.config?.type as any) && (
        <WidgetInformation
          widgetId={widget.id}
          config={widget.config as WidgetInformationConfig}
          projectLayers={projectLayers}
          projectLayerGroups={projectLayerGroups}
          viewOnly={viewOnly}
        />
      )}
      {widget.config?.type && dataTypes.options.includes(widget.config?.type as any) && (
        <WidgetData
          id={widget.id}
          config={widget.config as WidgetDataConfig}
          projectLayers={projectLayers}
          viewOnly={viewOnly}
          onConfigChange={(nextConfig) => {
            onWidgetUpdate?.({ ...widget, config: nextConfig });
          }}
        />
      )}
      {widget.config?.type && chartTypes.options.includes(widget.config?.type as any) && (
        <WidgetChart config={widget.config as WidgetChartConfig} viewOnly={viewOnly} />
      )}
      {widget.config?.type && elementTypes.options.includes(widget.config?.type as any) && (
        <WidgetElement
          widget={widget}
          config={widget.config as WidgetElementConfig}
          viewOnly={viewOnly}
          projectLayers={projectLayers}
          projectLayerGroups={projectLayerGroups}
          panelWidgets={panelWidgets}
          onWidgetUpdate={(newConfig) => {
            onWidgetUpdate?.({ ...widget, config: newConfig });
          }}
          onNestedWidgetUpdate={(updatedWidget) => {
            onWidgetUpdate?.(updatedWidget);
          }}
        />
      )}
    </Box>
  );

  return viewOnly ? (
    <Box sx={{ width: "100%", p: 1, pointerEvents: "all" }}>{widgetContent}</Box>
  ) : (
    <DraggableWidgetContainer widget={widget} onWidgetDelete={onWidgetDelete}>
      {widgetContent}
    </DraggableWidgetContainer>
  );
};

export default WidgetWrapper;
