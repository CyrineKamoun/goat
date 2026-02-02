"use client";

import { Delete as DeleteIcon } from "@mui/icons-material";
import { IconButton, Stack, Tooltip, useTheme } from "@mui/material";
import { styled } from "@mui/material/styles";
import { BaseEdge, EdgeLabelRenderer, type EdgeProps, getBezierPath, useViewport } from "@xyflow/react";
import React, { memo, useCallback, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useDispatch } from "react-redux";

import type { AppDispatch } from "@/lib/store";
import { removeEdges } from "@/lib/store/workflow/slice";

// Styled to match node toolbar style (same as DatasetNode)
const ToolbarContainer = styled(Stack)(({ theme }) => ({
  backgroundColor: theme.palette.background.paper,
  borderRadius: theme.shape.borderRadius * 2,
  padding: theme.spacing(1),
  gap: theme.spacing(0.5),
  flexDirection: "row",
  alignItems: "center",
  boxShadow: theme.shadows[4],
  border: `1px solid ${theme.palette.divider}`,
}));

const ToolbarButton = styled(IconButton)(({ theme }) => ({
  width: 36,
  height: 36,
  "&:hover": {
    backgroundColor: theme.palette.action.hover,
  },
}));

const DeletableEdge: React.FC<EdgeProps> = ({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style = {},
  markerEnd,
  selected,
}) => {
  const { t } = useTranslation("common");
  const dispatch = useDispatch<AppDispatch>();
  const theme = useTheme();
  const { zoom } = useViewport();

  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  // Merge style with selected color
  const edgeStyle = useMemo(
    () => ({
      ...style,
      stroke: selected ? theme.palette.primary.main : style.stroke,
      strokeWidth: selected ? 3 : (style.strokeWidth as number) || 2,
    }),
    [style, selected, theme.palette.primary.main]
  );

  const handleDelete = useCallback(
    (event: React.MouseEvent) => {
      event.stopPropagation();
      event.preventDefault();
      dispatch(removeEdges([id]));
    },
    [dispatch, id]
  );

  return (
    <>
      <BaseEdge path={edgePath} markerEnd={markerEnd} style={edgeStyle} />
      {selected && (
        <EdgeLabelRenderer>
          <div
            className="nodrag nopan"
            style={{
              position: "absolute",
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px) scale(${1 / zoom})`,
              transformOrigin: "center center",
              pointerEvents: "all",
              zIndex: 1000,
            }}
            onMouseDown={(e) => e.stopPropagation()}>
            <ToolbarContainer onClick={handleDelete}>
              <Tooltip title={t("delete")} placement="top" arrow>
                <ToolbarButton size="small">
                  <DeleteIcon fontSize="small" color="error" />
                </ToolbarButton>
              </Tooltip>
            </ToolbarContainer>
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
};

export default memo(DeletableEdge);
