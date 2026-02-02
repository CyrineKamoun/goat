"use client";

/**
 * Dataset Node Settings Panel
 *
 * Shows configuration for a selected dataset node with two tabs:
 * - SOURCE: Select/change the dataset from project layers, dataset explorer, or catalog
 * - FILTER: Apply workflow-specific filters to the dataset (not persisted to project layer)
 */
import {
  Box,
  Button,
  ClickAwayListener,
  Divider,
  ListItemIcon,
  Menu,
  MenuItem,
  MenuList,
  Stack,
  Tab,
  Tabs,
  Typography,
} from "@mui/material";
import { useParams } from "next/navigation";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { useDispatch } from "react-redux";
import { v4 } from "uuid";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { AppDispatch } from "@/lib/store";
import { requestMapView, updateNode } from "@/lib/store/workflow/slice";
import { parseCQLQueryToObject } from "@/lib/transformers/filter";
import { layerType } from "@/lib/validations/common";
import type { Expression as ExpressionType } from "@/lib/validations/filter";
import { FilterType } from "@/lib/validations/filter";
import type { ProjectLayer } from "@/lib/validations/project";
import type { WorkflowNode } from "@/lib/validations/workflow";

import type { SelectorItem } from "@/types/map/common";

import useLayerFields from "@/hooks/map/CommonHooks";
import { useFilteredProjectLayers } from "@/hooks/map/LayerPanelHooks";

import Container from "@/components/map/panels/Container";
import Selector from "@/components/map/panels/common/Selector";
import ToolsHeader from "@/components/map/panels/common/ToolsHeader";
import Expression from "@/components/map/panels/filter/Expression";
import CatalogExplorerModal from "@/components/modals/CatalogExplorer";
import DatasetExplorerModal from "@/components/modals/DatasetExplorer";

// Tab panel component
interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`dataset-tabpanel-${index}`}
      aria-labelledby={`dataset-tab-${index}`}
      {...other}>
      {value === index && <Box sx={{ pt: 1.5 }}>{children}</Box>}
    </div>
  );
}

function a11yProps(index: number) {
  return {
    id: `dataset-tab-${index}`,
    "aria-controls": `dataset-tabpanel-${index}`,
  };
}

// Dataset source type enum
enum DatasetSourceType {
  FromProject = "from_project",
  DatasetExplorer = "dataset_explorer",
  CatalogExplorer = "catalog_explorer",
}

interface DatasetNodeSettingsProps {
  node: WorkflowNode;
  projectLayers?: ProjectLayer[];
  onBack: () => void;
}

export default function DatasetNodeSettings({ node, projectLayers = [], onBack }: DatasetNodeSettingsProps) {
  const { t } = useTranslation("common");
  const dispatch = useDispatch<AppDispatch>();
  const { projectId } = useParams();

  // Fetch project layers
  const { layers: fetchedLayers } = useFilteredProjectLayers(projectId as string);
  const layers = fetchedLayers || projectLayers;

  // Tab state
  const [tabValue, setTabValue] = useState(0);

  // Dataset source menu state
  const [menuAnchorEl, setMenuAnchorEl] = useState<null | HTMLElement>(null);
  const menuOpen = Boolean(menuAnchorEl);

  // Modal states
  const [datasetExplorerOpen, setDatasetExplorerOpen] = useState(false);
  const [catalogExplorerOpen, setCatalogExplorerOpen] = useState(false);

  // Show layer selector (for "From project" option)
  const [showLayerSelector, setShowLayerSelector] = useState(false);

  // Get the selected layer from node data
  const selectedLayer = useMemo(() => {
    if (node.type !== "dataset" || node.data.type !== "dataset") return null;
    const datasetData = node.data;
    if (!datasetData.layerProjectId) return null;
    return layers.find((l) => l.id === datasetData.layerProjectId) || null;
  }, [node, layers]);

  // Get layer fields for filter (prefixed with _ as not yet used but will be needed)
  useLayerFields(selectedLayer?.layer_id || "");

  // Filter expressions state - workflow-specific, initialized from layer's existing filter
  const [expressions, setExpressions] = useState<ExpressionType[]>([]);
  const [logicalOperator, setLogicalOperator] = useState<SelectorItem | undefined>({
    value: "and",
    label: t("and"),
  });

  // Filter menu state
  const [filterMenuAnchorEl, setFilterMenuAnchorEl] = useState<null | HTMLElement>(null);
  const filterMenuOpen = Boolean(filterMenuAnchorEl);

  const logicalOperators = useMemo(
    () => [
      { value: "and", label: t("match_all_filters") },
      { value: "or", label: t("match_at_least_one_filter") },
    ],
    [t]
  );

  const filterExpressionTypes = [
    { sourceType: FilterType.Logical, iconName: ICON_NAME.TABLE, label: t("logical_expression") },
    { sourceType: FilterType.Spatial, iconName: ICON_NAME.MAP, label: t("spatial_expression") },
  ];

  // Track if we've loaded expressions for current node to avoid re-running on every render
  const loadedNodeIdRef = React.useRef<string | null>(null);

  // Initialize filter expressions from layer and node data (runs once per node selection)
  useEffect(() => {
    // Skip if we already loaded for this node
    if (loadedNodeIdRef.current === node.id) {
      return;
    }

    // Only handle dataset nodes with filter capability
    if (node.type !== "dataset" || node.data.type !== "dataset") {
      loadedNodeIdRef.current = node.id;
      setExpressions([]);
      return;
    }

    if (!selectedLayer) {
      loadedNodeIdRef.current = node.id;
      setExpressions([]);
      return;
    }

    // If filter was already initialized for this node (even if cleared), don't re-copy from layer
    // This flag persists in node.data so it survives component unmount/remount
    if (node.data.filterInitialized) {
      // Load existing filter if present
      if (node.data.filter) {
        const nodeFilter = node.data.filter as {
          op?: string;
          expressions?: ExpressionType[];
          args?: unknown[];
        };
        if (nodeFilter.op && nodeFilter.expressions && nodeFilter.expressions.length > 0) {
          setLogicalOperator(logicalOperators.find((item) => item.value === nodeFilter.op));
          setExpressions(nodeFilter.expressions);
        } else if (nodeFilter.op && nodeFilter.args) {
          const savedExpressions = parseCQLQueryToObject(nodeFilter as { op: string; args: unknown[] });
          setLogicalOperator(logicalOperators.find((item) => item.value === nodeFilter.op));
          setExpressions(savedExpressions);
        } else {
          setExpressions([]);
        }
      } else {
        setExpressions([]);
      }
      loadedNodeIdRef.current = node.id;
      return;
    }

    // Check if node has saved workflow filter (legacy nodes without filterInitialized)
    if (node.data.filter) {
      const nodeFilter = node.data.filter as {
        op?: string;
        expressions?: ExpressionType[];
        args?: unknown[];
      };

      // Check for workflow filter format (with expressions array)
      if (nodeFilter.op && nodeFilter.expressions && nodeFilter.expressions.length > 0) {
        setLogicalOperator(logicalOperators.find((item) => item.value === nodeFilter.op));
        setExpressions(nodeFilter.expressions);
        loadedNodeIdRef.current = node.id;
        return;
      }

      // Fall back to CQL format (with args array) for backward compatibility
      if (nodeFilter.op && nodeFilter.args) {
        const savedExpressions = parseCQLQueryToObject(nodeFilter as { op: string; args: unknown[] });
        setLogicalOperator(logicalOperators.find((item) => item.value === nodeFilter.op));
        setExpressions(savedExpressions);
        loadedNodeIdRef.current = node.id;
        return;
      }
    }

    // If node has no filter but layer has CQL filter, copy it to node (one-time inheritance)
    // This makes the filter independent and editable
    if (selectedLayer.query?.cql && node.type === "dataset") {
      const layerCql = selectedLayer.query.cql as { op?: string; args?: unknown[] };
      if (layerCql.op && layerCql.args) {
        const layerExpressions = parseCQLQueryToObject(layerCql as { op: string; args: unknown[] });
        if (layerExpressions.length > 0) {
          // Copy the filter to the node so it becomes independent
          const inheritedFilter = {
            op: layerCql.op,
            expressions: layerExpressions,
          };
          dispatch(
            updateNode({
              id: node.id,
              changes: {
                data: {
                  ...node.data,
                  filter: inheritedFilter,
                  filterInitialized: true,
                },
              },
            })
          );
          setLogicalOperator(logicalOperators.find((item) => item.value === layerCql.op));
          setExpressions(layerExpressions);
          loadedNodeIdRef.current = node.id;
          return;
        }
      }
    }

    // Mark as initialized even if no filter to inherit
    dispatch(
      updateNode({
        id: node.id,
        changes: {
          data: {
            ...node.data,
            filterInitialized: true,
          },
        },
      })
    );
    loadedNodeIdRef.current = node.id;
    setExpressions([]);
    // Only depend on node.id and selectedLayer.id to avoid re-running on filter changes
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedLayer?.id, node.id]);

  // Convert layers to selector items
  const layerSelectorItems: SelectorItem[] = useMemo(() => {
    return layers.map((layer) => ({
      value: String(layer.id),
      label: layer.name,
      icon:
        layer.feature_layer_geometry_type === "point"
          ? ICON_NAME.POINT_FEATURE
          : layer.feature_layer_geometry_type === "line"
            ? ICON_NAME.LINE_FEATURE
            : layer.feature_layer_geometry_type === "polygon"
              ? ICON_NAME.POLYGON_FEATURE
              : ICON_NAME.TABLE,
    }));
  }, [layers]);

  // Menu items for "Add Dataset" dropdown
  const menuItems = [
    { type: DatasetSourceType.FromProject, icon: ICON_NAME.LAYERS, label: t("from_project") },
    { type: DatasetSourceType.DatasetExplorer, icon: ICON_NAME.DATABASE, label: t("dataset_explorer") },
    { type: DatasetSourceType.CatalogExplorer, icon: ICON_NAME.GLOBE, label: t("catalog_explorer") },
  ];

  // Handle tab change
  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  // Handle menu open
  const handleMenuOpen = (event: React.MouseEvent<HTMLButtonElement>) => {
    setMenuAnchorEl(event.currentTarget);
  };

  // Handle menu close
  const handleMenuClose = () => {
    setMenuAnchorEl(null);
  };

  // Handle menu item click
  const handleMenuItemClick = (type: DatasetSourceType) => {
    handleMenuClose();
    switch (type) {
      case DatasetSourceType.FromProject:
        setShowLayerSelector(true);
        break;
      case DatasetSourceType.DatasetExplorer:
        setDatasetExplorerOpen(true);
        break;
      case DatasetSourceType.CatalogExplorer:
        setCatalogExplorerOpen(true);
        break;
    }
  };

  // Handle layer selection from "From project"
  const handleLayerSelect = useCallback(
    (item: SelectorItem | SelectorItem[] | undefined) => {
      if (!item || Array.isArray(item)) return;

      const layer = layers.find((l) => String(l.id) === item.value);
      if (!layer || node.type !== "dataset") return;

      // Copy layer's existing filter to the workflow node (one-way sync)
      let inheritedFilter: Record<string, unknown> | undefined;
      if (layer.query?.cql) {
        const layerCql = layer.query.cql as { op?: string; args?: unknown[] };
        if (layerCql.op && layerCql.args) {
          const layerExpressions = parseCQLQueryToObject(layerCql as { op: string; args: unknown[] });
          if (layerExpressions.length > 0) {
            inheritedFilter = {
              op: layerCql.op,
              expressions: layerExpressions,
            };
          }
        }
      }

      // Update node data with selected layer and inherited filter
      dispatch(
        updateNode({
          id: node.id,
          changes: {
            data: {
              ...node.data,
              label: layer.name,
              layerProjectId: layer.id,
              layerId: layer.layer_id,
              layerName: layer.name,
              geometryType: layer.feature_layer_geometry_type || undefined,
              // Inherit filter from layer instead of resetting
              filter: inheritedFilter,
            },
          },
        })
      );

      // Update local expressions state with inherited filter
      if (inheritedFilter) {
        const expressions = inheritedFilter.expressions as ExpressionType[];
        setExpressions(expressions);
        setLogicalOperator(logicalOperators.find((op) => op.value === inheritedFilter.op));
      } else {
        setExpressions([]);
      }
      setShowLayerSelector(false);
    },
    [layers, node, dispatch, logicalOperators]
  );

  // Create a new filter expression
  const createExpression = (type: FilterType) => {
    setExpressions((prev) => [
      ...prev,
      {
        id: v4(),
        attribute: "",
        expression: "",
        value: "",
        type,
      },
    ]);

    // If creating a spatial filter, dispatch action to open map view
    if (type === FilterType.Spatial) {
      dispatch(requestMapView());
    }
  };

  // Handle filter expression add menu
  const handleFilterMenuClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    if (selectedLayer?.type === layerType.Values.feature) {
      setFilterMenuAnchorEl(event.currentTarget);
    } else {
      createExpression(FilterType.Logical);
    }
  };

  // Save filter to node data
  const saveFilterToNode = useCallback(
    (newExpressions: ExpressionType[], operator: SelectorItem | undefined) => {
      if (node.type !== "dataset") return;

      // For now, store as simplified structure
      // The actual CQL generation will happen at workflow execution time
      const filter =
        newExpressions.length > 0
          ? {
              op: operator?.value || "and",
              expressions: newExpressions,
            }
          : undefined;

      dispatch(
        updateNode({
          id: node.id,
          changes: {
            data: {
              ...node.data,
              filter,
              filterInitialized: true, // Preserve this flag so filter doesn't get re-inherited
            },
          },
        })
      );
    },
    [node, dispatch]
  );

  // Handle expression delete
  const handleExpressionDelete = async (expression: ExpressionType) => {
    const updatedExpressions = expressions.filter((e) => e.id !== expression.id);
    setExpressions(updatedExpressions);
    saveFilterToNode(updatedExpressions, logicalOperator);
  };

  // Handle expression duplicate
  const handleExpressionDuplicate = async (expression: ExpressionType) => {
    const updatedExpressions = [...expressions, { ...expression, id: v4() }];
    setExpressions(updatedExpressions);
    saveFilterToNode(updatedExpressions, logicalOperator);
  };

  // Handle expression update
  const handleExpressionUpdate = async (expression: ExpressionType) => {
    const updatedExpressions = expressions.map((e) => (e.id === expression.id ? expression : e));
    setExpressions(updatedExpressions);
    saveFilterToNode(updatedExpressions, logicalOperator);
  };

  // Handle logical operator change
  const handleLogicalOperatorChange = (item: SelectorItem | SelectorItem[] | undefined) => {
    const newOperator = item as SelectorItem;
    setLogicalOperator(newOperator);
    saveFilterToNode(expressions, newOperator);
  };

  // Clear all filters
  const clearFilter = () => {
    setExpressions([]);
    saveFilterToNode([], logicalOperator);
  };

  // Validate expressions
  const areAllExpressionsValid = useMemo(() => {
    return expressions.every((expression) => {
      let hasValue = !!expression.value?.toString();
      if (
        expression.expression === "is_empty_string" ||
        expression.expression === "is_not_empty_string" ||
        expression.expression === "is_blank" ||
        expression.expression === "is_not_blank"
      ) {
        hasValue = true;
      }
      return expression.attribute && expression.expression && hasValue;
    });
  }, [expressions]);

  // Get node title
  const nodeTitle =
    node.type === "dataset" && node.data.type === "dataset"
      ? node.data.label || t("imported_dataset")
      : t("dataset");

  return (
    <Container
      header={<ToolsHeader onBack={onBack} title={nodeTitle} />}
      disablePadding={false}
      body={
        <Box sx={{ mt: -1 }}>
          {/* Tabs */}
          <Box sx={{ borderBottom: 1, borderColor: "divider", mx: -3 }}>
            <Tabs value={tabValue} onChange={handleTabChange} variant="fullWidth" sx={{ minHeight: 40 }}>
              <Tab
                label={t("source")}
                {...a11yProps(0)}
                sx={{ textTransform: "uppercase", fontWeight: 600, minHeight: 40, py: 1 }}
              />
              <Tab
                label={t("filter")}
                {...a11yProps(1)}
                disabled={!selectedLayer}
                sx={{ textTransform: "uppercase", fontWeight: 600, minHeight: 40, py: 1 }}
              />
            </Tabs>
          </Box>

          {/* SOURCE Tab */}
          <TabPanel value={tabValue} index={0}>
            <Typography variant="body2" sx={{ fontStyle: "italic", mb: 3 }}>
              {t("dataset_source_description")}
            </Typography>

            {/* Selected layer info */}
            {selectedLayer && !showLayerSelector && (
              <Box sx={{ mb: 3 }}>
                <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
                  <Icon
                    iconName={
                      selectedLayer.feature_layer_geometry_type === "point"
                        ? ICON_NAME.POINT_FEATURE
                        : selectedLayer.feature_layer_geometry_type === "line"
                          ? ICON_NAME.LINE_FEATURE
                          : selectedLayer.feature_layer_geometry_type === "polygon"
                            ? ICON_NAME.POLYGON_FEATURE
                            : ICON_NAME.TABLE
                    }
                    style={{ fontSize: 20 }}
                  />
                  <Typography variant="body1" fontWeight="medium">
                    {selectedLayer.name}
                  </Typography>
                </Stack>
                <Button
                  variant="text"
                  size="small"
                  onClick={() => setShowLayerSelector(true)}
                  sx={{ textTransform: "none" }}>
                  {t("change")}
                </Button>
              </Box>
            )}

            {/* Layer selector (shown when "From project" is selected) */}
            {showLayerSelector && (
              <Box sx={{ mb: 3 }}>
                <Selector
                  selectedItems={
                    selectedLayer ? { value: String(selectedLayer.id), label: selectedLayer.name } : undefined
                  }
                  setSelectedItems={handleLayerSelect}
                  items={layerSelectorItems}
                  label={t("select_layer")}
                  placeholder={t("select_layer")}
                />
                <Button
                  variant="text"
                  size="small"
                  onClick={() => setShowLayerSelector(false)}
                  sx={{ mt: 1, textTransform: "none" }}>
                  {t("cancel")}
                </Button>
              </Box>
            )}

            {/* Add Dataset button */}
            {!showLayerSelector && (
              <Box>
                <Button
                  variant="outlined"
                  size="small"
                  fullWidth
                  startIcon={<Icon iconName={ICON_NAME.PLUS} style={{ fontSize: 15 }} />}
                  onClick={handleMenuOpen}
                  sx={{ borderRadius: 4, textTransform: "none", fontWeight: "bold" }}>
                  {selectedLayer ? t("change_dataset") : t("add_dataset")}
                </Button>
                <Menu
                  anchorEl={menuAnchorEl}
                  open={menuOpen}
                  onClose={handleMenuClose}
                  anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
                  transformOrigin={{ vertical: "top", horizontal: "left" }}
                  slotProps={{
                    paper: {
                      sx: {
                        width: menuAnchorEl?.offsetWidth,
                        minWidth: menuAnchorEl?.offsetWidth,
                      },
                    },
                  }}>
                  {menuItems.map((item) => (
                    <MenuItem key={item.type} onClick={() => handleMenuItemClick(item.type)}>
                      <ListItemIcon>
                        <Icon iconName={item.icon} style={{ fontSize: 15 }} />
                      </ListItemIcon>
                      <Typography variant="body2">{item.label}</Typography>
                    </MenuItem>
                  ))}
                </Menu>
              </Box>
            )}
          </TabPanel>

          {/* FILTER Tab */}
          <TabPanel value={tabValue} index={1}>
            {/* Description */}
            {expressions.length === 0 && (
              <Typography variant="body2" sx={{ fontStyle: "italic", mb: 3 }}>
                {t("filter_layer_message")}
              </Typography>
            )}

            {/* Logical operator selector */}
            {expressions.length > 1 && (
              <>
                <Divider />
                <Selector
                  selectedItems={logicalOperator}
                  setSelectedItems={handleLogicalOperatorChange}
                  items={logicalOperators}
                  label={t("filter_results")}
                />
              </>
            )}

            {/* Expression list */}
            {expressions.length > 0 && (
              <Stack spacing={4} sx={{ pt: 2 }}>
                <Divider />
                {expressions.map((expression) => (
                  <Expression
                    key={expression.id}
                    expression={expression}
                    onDelete={handleExpressionDelete}
                    onDuplicate={handleExpressionDuplicate}
                    onUpdate={handleExpressionUpdate}
                    layerId={selectedLayer?.layer_id}
                  />
                ))}
              </Stack>
            )}

            {/* Add expression button */}
            {selectedLayer && (
              <Stack spacing={2} sx={{ pt: 4 }}>
                <Button
                  onClick={handleFilterMenuClick}
                  fullWidth
                  size="small"
                  disabled={!areAllExpressionsValid}
                  startIcon={<Icon iconName={ICON_NAME.PLUS} style={{ fontSize: 15 }} />}>
                  <Typography variant="body2" fontWeight="bold" color="inherit">
                    {t("add_expression")}
                  </Typography>
                </Button>

                {/* Expression type menu (for feature layers) */}
                {selectedLayer.type === layerType.Values.feature && (
                  <Menu
                    anchorEl={filterMenuAnchorEl}
                    sx={{
                      "& .MuiPaper-root": {
                        boxShadow: "0px 0px 10px 0px rgba(58, 53, 65, 0.1)",
                      },
                    }}
                    anchorOrigin={{ vertical: "top", horizontal: "center" }}
                    transformOrigin={{ vertical: "bottom", horizontal: "center" }}
                    open={filterMenuOpen}
                    onClose={() => setFilterMenuAnchorEl(null)}>
                    <ClickAwayListener onClickAway={() => setFilterMenuAnchorEl(null)}>
                      <MenuList>
                        {filterExpressionTypes.map((item, index) => (
                          <MenuItem
                            key={index}
                            onClick={() => {
                              createExpression(item.sourceType);
                              setFilterMenuAnchorEl(null);
                            }}>
                            <ListItemIcon>
                              <Icon iconName={item.iconName} style={{ fontSize: 15 }} />
                            </ListItemIcon>
                            <Typography variant="body2">{item.label}</Typography>
                          </MenuItem>
                        ))}
                      </MenuList>
                    </ClickAwayListener>
                  </Menu>
                )}

                {/* Clear filter button */}
                <Button
                  variant="outlined"
                  fullWidth
                  size="small"
                  color="error"
                  disabled={expressions.length === 0}
                  onClick={clearFilter}>
                  <Typography variant="body2" color="inherit">
                    {t("clear_filter")}
                  </Typography>
                </Button>
              </Stack>
            )}
          </TabPanel>

          {/* Modals */}
          {datasetExplorerOpen && (
            <DatasetExplorerModal
              open={datasetExplorerOpen}
              onClose={() => setDatasetExplorerOpen(false)}
              projectId={projectId as string}
            />
          )}
          {catalogExplorerOpen && (
            <CatalogExplorerModal
              open={catalogExplorerOpen}
              onClose={() => setCatalogExplorerOpen(false)}
              projectId={projectId as string}
            />
          )}
        </Box>
      }
    />
  );
}
