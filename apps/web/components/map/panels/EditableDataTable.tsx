import AddIcon from "@mui/icons-material/Add";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import BarChartIcon from "@mui/icons-material/BarChart";
import CalculateIcon from "@mui/icons-material/Calculate";
import CloseIcon from "@mui/icons-material/Close";
import DeleteIcon from "@mui/icons-material/Delete";
import DownloadIcon from "@mui/icons-material/Download";
import EditIcon from "@mui/icons-material/Edit";
import FilterAltIcon from "@mui/icons-material/FilterAlt";
import bbox from "@turf/bbox";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";
import FullscreenIcon from "@mui/icons-material/Fullscreen";
import FullscreenExitIcon from "@mui/icons-material/FullscreenExit";
import SearchIcon from "@mui/icons-material/Search";
import { emphasize } from "@mui/material/styles";
import {
  Badge,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  IconButton,
  InputAdornment,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Menu,
  MenuItem,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import { debounce } from "@mui/material/utils";
import { useParams } from "next/navigation";
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import {
  deleteColumn,
  deleteFeaturesBulk,
  useDatasetCollectionItems,
  useLayerQueryables,
} from "@/lib/api/layers";
import type { GetCollectionItemsQueryParams } from "@/lib/validations/layer";
import type { ProjectLayer } from "@/lib/validations/project";

import {
  addPendingFeature,
  commitFeature,
  startEditing,
  stopEditing,
  updatePendingProperties,
} from "@/lib/store/featureEditor/slice";
import { setSelectedLayers } from "@/lib/store/layer/slice";
import { setActiveRightPanel, setHighlightedFeature, setPopupInfo } from "@/lib/store/map/slice";
import { MapSidebarItemID } from "@/types/map/common";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";
import { useMap } from "react-map-gl/maplibre";
import useLayerFields from "@/hooks/map/CommonHooks";

import { useProjectLayers } from "@/lib/api/projects";
import ColumnStatsPanel from "@/components/map/panels/ColumnStatsPanel";
import QuickFilterPopover from "@/components/map/panels/QuickFilterPopover";
import ConfirmModal from "@/components/modals/Confirm";
import EditFieldsModal from "@/components/modals/EditFields";

type SortDirection = "asc" | "desc";
type EditingCell = { rowId: string; column: string } | null;
type DirtyCell = { rowId: string; column: string; originalValue: unknown; newValue: unknown };

interface EditableDataTableProps {
  layerId: string;
  projectLayer: ProjectLayer;
  layerName?: string;
  isExpanded?: boolean;
  onToggleExpand?: () => void;
  onClose?: () => void;
  onDownload?: () => void;
}

const ROWS_PER_PAGE_OPTIONS = [10, 25, 50, 100];

const EditableDataTable: React.FC<EditableDataTableProps> = ({
  layerId,
  projectLayer,
  layerName,
  isExpanded,
  onToggleExpand,
  onClose,
  onDownload,
}) => {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const { map } = useMap();
  const { projectId } = useParams();
  const { layers: projectLayers, mutate: mutateProjectLayers } = useProjectLayers(projectId as string);
  const activeRightPanel = useAppSelector((state) => state.map.activeRightPanel);
  const editLayerId = useAppSelector((state) => state.featureEditor.activeLayerId);
  const pendingFeatures = useAppSelector((state) => state.featureEditor.pendingFeatures);
  const isEditing = editLayerId === layerId;
  const { layerFields, isLoading: areFieldsLoading } = useLayerFields(layerId);
  const { mutate: mutateQueryables } = useLayerQueryables(layerId);

  // CQL filter from layer settings — applied to table queries and stats
  const cqlArgs = projectLayer?.query?.cql?.args;
  const activeFilterCount = cqlArgs?.length ?? 0;
  const cqlFilter = useMemo(() => {
    const cql = projectLayer?.query?.cql;
    if (!cql || !cql.args?.length) return undefined;
    return JSON.stringify(cql);
  }, [projectLayer?.query?.cql]);

  // Pagination state
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);

  // Sort state
  const [sortBy, setSortBy] = useState<string | undefined>(undefined);
  const [sortDirection, setSortDirection] = useState<SortDirection>("asc");

  // Selection state (single row)
  const [selectedRowId, setSelectedRowId] = useState<string | null>(null);

  // Editing state
  const [editingCell, setEditingCell] = useState<EditingCell>(null);
  const [editValue, setEditValue] = useState<string>("");

  // Dirty tracking
  const [dirtyCells, setDirtyCells] = useState<Map<string, DirtyCell>>(new Map());

  // Search state
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchText, setSearchText] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");

  // eslint-disable-next-line react-hooks/exhaustive-deps
  const debouncedSetSearch = useCallback(debounce((val: string) => setDebouncedSearch(val), 400), []);

  // Column header menu state
  const [columnMenuAnchor, setColumnMenuAnchor] = useState<HTMLElement | null>(null);
  const [columnMenuField, setColumnMenuField] = useState<string | null>(null);

  // Edit fields modal state
  const [editFieldsOpen, setEditFieldsOpen] = useState(false);
  const [editFieldsInitialField, setEditFieldsInitialField] = useState<string | null>(null);

  // Stop editing confirmation state
  const [stopEditConfirmOpen, setStopEditConfirmOpen] = useState(false);

  // Delete column confirmation state
  const [deleteColumnConfirmOpen, setDeleteColumnConfirmOpen] = useState(false);
  const [pendingDeleteColumn, setPendingDeleteColumn] = useState<string | null>(null);

  // Column stats state
  const [statsColumn, setStatsColumn] = useState<string | null>(null);
  const statsNavRef = useRef(false); // true when navigating via prev/next buttons

  // Quick filter popover state
  const [quickFilterAnchor, setQuickFilterAnchor] = useState<HTMLElement | null>(null);
  const [quickFilterColumn, setQuickFilterColumn] = useState<string | null>(null);

  // Row context menu state
  const [rowMenuAnchor, setRowMenuAnchor] = useState<{ top: number; left: number } | null>(null);
  const [rowMenuRowId, setRowMenuRowId] = useState<string | null>(null);
  const tableContainerRef = useRef<HTMLDivElement>(null);

  // Column resize state
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});
  const activeResizeRef = useRef<{
    columnKey: string;
    startX: number;
    startWidth: number;
  } | null>(null);

  // Filter to primitive fields only (no objects/geometry)
  const displayFields = useMemo(
    () => layerFields.filter((f) => f.type !== "object" && f.type !== "geometry"),
    [layerFields]
  );

  // Build search CQL — OR across all display fields (temporary, not persisted)
  const searchCql = useMemo(() => {
    const term = debouncedSearch.trim();
    if (!term || displayFields.length === 0) return undefined;
    const isNum = !isNaN(Number(term));
    const args: object[] = [];
    for (const field of displayFields) {
      if (field.type === "string") {
        args.push({ op: "like", args: [{ property: field.name }, `%${term}%`] });
      } else if ((field.type === "number" || field.type === "integer") && isNum) {
        args.push({ op: "=", args: [{ property: field.name }, Number(term)] });
      }
    }
    if (args.length === 0) return undefined;
    return args.length === 1 ? args[0] : { op: "or", args };
  }, [debouncedSearch, displayFields]);

  // Combine layer CQL filter + search CQL into one filter param
  const combinedFilter = useMemo(() => {
    const layerCql = projectLayer?.query?.cql;
    const hasLayerCql = layerCql && layerCql.args?.length;
    if (!hasLayerCql && !searchCql) return undefined;
    if (!hasLayerCql) return JSON.stringify(searchCql);
    if (!searchCql) return JSON.stringify(layerCql);
    // Merge: AND(layerFilter, searchFilter)
    return JSON.stringify({ op: "and", args: [layerCql, searchCql] });
  }, [projectLayer?.query?.cql, searchCql]);

  // Build query params
  const queryParams = useMemo<GetCollectionItemsQueryParams>(() => {
    const params: GetCollectionItemsQueryParams = {
      limit: rowsPerPage,
      offset: page * rowsPerPage,
    };
    if (sortBy) {
      params.sortby = sortDirection === "desc" ? `-${sortBy}` : sortBy;
    }
    if (combinedFilter) {
      params.filter = combinedFilter;
    }
    return params;
  }, [page, rowsPerPage, sortBy, sortDirection, combinedFilter]);

  // Fetch data
  const { data: collectionData, isLoading, mutate } = useDatasetCollectionItems(layerId, queryParams);

  const filteredFeatures = collectionData?.features || [];

  // Reset page when layer changes
  useEffect(() => {
    setPage(0);
    setSelectedRowId(null);
    setDirtyCells(new Map());
    setEditingCell(null);
    setSearchText("");
    setDebouncedSearch("");
    setSearchOpen(false);
    setStatsColumn(null);
    dispatch(setHighlightedFeature(undefined));
  }, [layerId, dispatch]);

  // Clear highlight on unmount (table closed)
  useEffect(() => {
    return () => {
      dispatch(setHighlightedFeature(undefined));
    };
  }, [dispatch]);

  // Scroll to active stats column only when navigating via prev/next
  useEffect(() => {
    if (!statsNavRef.current || !statsColumn || !tableContainerRef.current) return;
    statsNavRef.current = false;
    const idx = displayFields.findIndex((f) => f.name === statsColumn);
    if (idx < 0) return;
    // Find the header cell by index (+1 for the row-number column)
    const headerRow = tableContainerRef.current.querySelector("thead tr");
    const cell = headerRow?.children[idx + 1] as HTMLElement | undefined;
    if (cell) {
      cell.scrollIntoView({ behavior: "smooth", block: "nearest", inline: "nearest" });
    }
  }, [statsColumn, displayFields]);

  // --- Column Resize ---

  const startColumnResize = useCallback(
    (event: React.MouseEvent, columnKey: string) => {
      event.preventDefault();
      event.stopPropagation();
      const currentWidth = (event.currentTarget.parentElement as HTMLElement | null)?.getBoundingClientRect().width;
      activeResizeRef.current = {
        columnKey,
        startX: event.clientX,
        startWidth: columnWidths[columnKey] ?? currentWidth ?? 140,
      };
    },
    [columnWidths]
  );

  useEffect(() => {
    const handleMouseMove = (event: MouseEvent) => {
      const activeResize = activeResizeRef.current;
      if (!activeResize) return;
      const nextWidth = Math.max(60, Math.min(600, activeResize.startWidth + (event.clientX - activeResize.startX)));
      setColumnWidths((prev) => ({ ...prev, [activeResize.columnKey]: nextWidth }));
    };

    const handleMouseUp = () => {
      activeResizeRef.current = null;
    };

    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp);
    return () => {
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };
  }, []);

  // --- Sort ---

  const handleSort = (field: string, direction?: SortDirection) => {
    if (direction) {
      setSortBy(field);
      setSortDirection(direction);
    } else if (sortBy === field) {
      setSortDirection((prev) => (prev === "asc" ? "desc" : "asc"));
    } else {
      setSortBy(field);
      setSortDirection("asc");
    }
    setPage(0);
  };

  // --- Selection (single row) ---

  const selectRow = (rowId: string) => {
    setSelectedRowId(rowId);
    dispatch(setPopupInfo(undefined));

    // Highlight feature on the map (only outside edit mode)
    if (!isEditing) {
      const feature = collectionData?.features.find((f, i) => `${f.id}-${page}-${i}` === rowId);
      if (feature) {
        // Determine the MapLibre layer type from the geometry type
        const geomType = projectLayer.feature_layer_geometry_type;
        const isCustomMarker = !!projectLayer.properties?.["custom_marker"];
        const layerType = geomType === "polygon" ? "fill" : geomType === "line" ? "line" : isCustomMarker ? "symbol" : "circle";
        dispatch(setHighlightedFeature({
          id: feature.id != null ? Number(feature.id) : undefined,
          properties: feature.properties || {},
          layer: { id: projectLayer.id.toString(), type: layerType },
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        } as any));
      }
    }
  };

  const handleRowDoubleClick = (rowId: string) => {
    if (!map) return;
    const feature = collectionData?.features.find((f, i) => `${f.id}-${page}-${i}` === rowId);
    if (feature?.geometry) {
      const bounds = bbox(feature) as [number, number, number, number];
      map.fitBounds(bounds, { padding: 100, maxZoom: 18, duration: 1000 });
    }
  };

  // --- Cell Editing ---

  const getCellValue = (rowId: string, column: string, originalValue: unknown): unknown => {
    const key = `${rowId}:${column}`;
    const dirty = dirtyCells.get(key);
    return dirty ? dirty.newValue : originalValue;
  };

  // Track selected (highlighted) cell — separate from editing
  const [selectedCell, setSelectedCell] = useState<{ rowId: string; column: string } | null>(null);

  // Clear dirty state when editing stops or pending features are cleared (save/discard)
  const pendingCount = Object.keys(pendingFeatures).length;
  useEffect(() => {
    if (!isEditing || pendingCount === 0) {
      setDirtyCells(new Map());
      setEditingCell(null);
      setSelectedCell(null);
      setSelectedRowId(null);
      dispatch(setHighlightedFeature(undefined));
    }
  }, [isEditing, pendingCount, dispatch]);

  const handleCellClick = (rowId: string, column: string, value: unknown) => {
    if (!isEditing) return; // Cells are only editable in edit mode
    const isAlreadySelected = selectedCell?.rowId === rowId && selectedCell?.column === column;
    if (isAlreadySelected) {
      // Second click — enter edit mode
      setEditingCell({ rowId, column });
      const displayValue = getCellValue(rowId, column, value);
      setEditValue(displayValue === null || displayValue === undefined ? "" : String(displayValue));
    } else {
      // First click — select only
      setSelectedCell({ rowId, column });
      setEditingCell(null);
    }
  };

  const handleCellBlur = () => {
    if (!editingCell) return;

    const { rowId, column } = editingCell;
    const feature = collectionData?.features.find((f, i) => `${f.id}-${page}-${i}` === rowId);
    const originalValue = feature?.properties?.[column];
    const key = `${rowId}:${column}`;

    // Parse the value based on field type
    const field = displayFields.find((f) => f.name === column);
    let parsedValue: unknown = editValue;
    if (field?.type === "number" || field?.type === "integer") {
      parsedValue = editValue === "" ? null : Number(editValue);
    } else if (editValue === "") {
      parsedValue = null;
    }

    // Check if value actually changed
    const unchanged = parsedValue === originalValue || (parsedValue === null && (originalValue === null || originalValue === undefined));
    if (unchanged) {
      setDirtyCells((prev) => {
        const next = new Map(prev);
        next.delete(key);
        return next;
      });
    } else {
      setDirtyCells((prev) => {
        const next = new Map(prev);
        next.set(key, { rowId, column, originalValue, newValue: parsedValue });
        return next;
      });

      // Dispatch to Redux pending features when in edit mode
      if (isEditing && feature) {
        const featureId = getFeatureId(rowId);
        const existingPending = pendingFeatures[featureId];
        if (existingPending) {
          // Update existing pending feature's properties
          dispatch(updatePendingProperties({
            id: featureId,
            properties: { ...existingPending.properties, [column]: parsedValue },
          }));
        } else {
          // Create a new pending feature for this row
          dispatch(addPendingFeature({
            id: featureId,
            drawFeatureId: null,
            geometry: (feature.geometry as GeoJSON.Geometry) || null,
            properties: { ...feature.properties, [column]: parsedValue },
            committed: false,
            action: "update",
            originalGeometry: (feature.geometry as GeoJSON.Geometry) || null,
            originalProperties: { ...feature.properties },
          }));
        }
        // Auto-commit table edits (no "Done" button needed for inline editing)
        if (!pendingFeatures[featureId]?.committed) {
          dispatch(commitFeature(featureId));
        }
      }
    }

    setEditingCell(null);
    setSelectedCell(null);
  };

  const handleCellKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === "Enter") {
      handleCellBlur();
    } else if (event.key === "Escape") {
      setEditingCell(null);
    }
  };

  // --- Delete Column ---

  const handleDeleteColumnRequest = (columnName: string) => {
    setPendingDeleteColumn(columnName);
    setDeleteColumnConfirmOpen(true);
  };

  const handleDeleteColumnConfirm = async () => {
    if (!pendingDeleteColumn) return;
    const columnName = pendingDeleteColumn;
    setDeleteColumnConfirmOpen(false);
    setPendingDeleteColumn(null);
    try {
      await deleteColumn(layerId, columnName);
      // Clear any state referencing the deleted column
      if (statsColumn === columnName) setStatsColumn(null);
      if (quickFilterColumn === columnName) {
        setQuickFilterColumn(null);
        setQuickFilterAnchor(null);
      }
      if (sortBy === columnName) {
        setSortBy(undefined);
        setSortDirection("asc");
      }
      // Refresh data and schema
      mutate();
      mutateQueryables();
      // Optimistically update project layers for tile cache busting
      if (projectLayers) {
        const now = new Date().toISOString();
        mutateProjectLayers(
          projectLayers.map((l) =>
            l.layer_id === layerId ? { ...l, updated_at: now } : l
          ),
          { revalidate: false },
        );
      }
      toast.success(t("column_deleted", { defaultValue: "Column deleted" }));
    } catch (error) {
      toast.error(t("error_deleting_column", { defaultValue: "Failed to delete column" }));
      console.error("Delete column error:", error);
    }
  };


  // --- Column Header Menu ---

  const handleColumnMenuOpen = (event: React.MouseEvent<HTMLElement>, fieldName: string) => {
    event.preventDefault();
    event.stopPropagation();
    setColumnMenuAnchor(event.currentTarget);
    setColumnMenuField(fieldName);
  };

  // --- Row Context Menu ---

  const handleRowContextMenu = (event: React.MouseEvent, rowId: string) => {
    event.preventDefault();
    setRowMenuAnchor({ top: event.clientY, left: event.clientX });
    setRowMenuRowId(rowId);
    setSelectedRowId(rowId);
  };

  const handleRowMenuClose = () => {
    setRowMenuAnchor(null);
    setRowMenuRowId(null);
  };

  const handleZoomToFeature = () => {
    if (!rowMenuRowId || !map) return;
    const feature = collectionData?.features.find(
      (f, i) => `${f.id}-${page}-${i}` === rowMenuRowId
    );
    if (feature?.geometry) {
      const bounds = bbox(feature) as [number, number, number, number];
      map.fitBounds(bounds, { padding: 100, maxZoom: 18, duration: 1000 });
    }
    handleRowMenuClose();
  };

  const getFeatureId = (rowId: string): string => {
    const parts = rowId.split("-");
    return parts.slice(0, -2).join("-");
  };

  const handleDeleteRow = async () => {
    if (!rowMenuRowId) return;
    try {
      await deleteFeaturesBulk(layerId, [getFeatureId(rowMenuRowId)]);
      setSelectedRowId(null);
      mutate();
      toast.success(t("rows_deleted", { defaultValue: "{{count}} row(s) deleted", count: 1 }));
    } catch (error) {
      toast.error(t("error_deleting_rows", { defaultValue: "Failed to delete rows" }));
      console.error("Delete error:", error);
    }
    handleRowMenuClose();
  };

  const handleColumnMenuClose = () => {
    setColumnMenuAnchor(null);
    setColumnMenuField(null);
  };

  // --- Pagination ---

  const handleChangePage = (_: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const totalCount = collectionData?.numberMatched ?? 0;

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        flex: 1,
        minHeight: 0,
      }}>
      {/* Header / Toolbar — single row */}
      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          minHeight: 42,
          gap: 1,
          px: 1.5,
          py: 0.5,
          borderBottom: "1px solid",
          borderColor: "divider",
          flexShrink: 0,
        }}>
        {/* Left: layer name */}
        <Typography variant="body2" fontWeight="bold" noWrap sx={{ mr: 1 }}>
          {layerName}
        </Typography>

        <Box sx={{ flex: 1 }} />

        {/* Right: action buttons + utility icons */}
        <Button
          size="small"
          variant="outlined"
          startIcon={<AddIcon />}
          onClick={() => {
            setEditFieldsInitialField(null);
            setEditFieldsOpen(true);
          }}
          sx={{ textTransform: "none", whiteSpace: "nowrap" }}>
          {t("add_field", { defaultValue: "Add a field" })}
        </Button>
        <Button
          size="small"
          variant="outlined"
          color={isEditing ? "error" : "primary"}
          startIcon={isEditing ? <CloseIcon /> : <EditIcon />}
          onClick={() => {
            if (isEditing) {
              if (pendingCount > 0) {
                setStopEditConfirmOpen(true);
              } else {
                dispatch(stopEditing());
              }
            } else {
              dispatch(startEditing({
                layerId,
                geometryType: projectLayer.feature_layer_geometry_type as "point" | "line" | "polygon" | null ?? null,
              }));
            }
          }}
          sx={{ textTransform: "none", whiteSpace: "nowrap" }}>
          {isEditing ? t("stop_editing") : (
            <>
              {t("edit_features")}
              <Chip label="Beta" size="small" sx={{ ml: 1, height: 18, fontSize: "0.65rem", fontWeight: 600 }} />
            </>
          )}
        </Button>

        <Divider orientation="vertical" flexItem sx={{ mx: 0.5 }} />

        {searchOpen ? (
          <TextField
            autoFocus
            size="small"
            placeholder={t("search", { defaultValue: "Search..." })}
            value={searchText}
            onChange={(e) => {
              setSearchText(e.target.value);
              debouncedSetSearch(e.target.value);
              setPage(0);
            }}
            onKeyDown={(e) => {
              if (e.key === "Escape") {
                setSearchOpen(false);
                setSearchText("");
                setDebouncedSearch("");
              }
            }}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon fontSize="small" />
                </InputAdornment>
              ),
              endAdornment: (
                <InputAdornment position="end">
                  <IconButton
                    size="small"
                    onClick={() => {
                      setSearchOpen(false);
                      setSearchText("");
                      setDebouncedSearch("");
                    }}>
                    <CloseIcon sx={{ fontSize: 14 }} />
                  </IconButton>
                </InputAdornment>
              ),
            }}
            sx={{ width: 200, "& .MuiInputBase-root": { height: 28, fontSize: "0.8rem" } }}
          />
        ) : (
          <Tooltip title={t("search", { defaultValue: "Search" })}>
            <IconButton size="small" onClick={() => setSearchOpen(true)}>
              <SearchIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        )}
        <Tooltip title={t("filter", { defaultValue: "Filter" })}>
          <IconButton
            size="small"
            color={activeFilterCount > 0 ? "primary" : "default"}
            onClick={() => {
              if (activeRightPanel === MapSidebarItemID.FILTER) {
                dispatch(setActiveRightPanel(undefined));
              } else {
                dispatch(setSelectedLayers([projectLayer.id]));
                dispatch(setActiveRightPanel(MapSidebarItemID.FILTER));
              }
            }}>
            <Badge
              badgeContent={activeFilterCount}
              color="primary"
              sx={{
                "& .MuiBadge-badge": {
                  fontSize: 9,
                  height: 15,
                  minWidth: 15,
                },
              }}>
              <FilterAltIcon fontSize="small" />
            </Badge>
          </IconButton>
        </Tooltip>
        <Tooltip title={isExpanded ? t("collapse", { defaultValue: "Collapse" }) : t("expand", { defaultValue: "Expand" })}>
          <IconButton size="small" onClick={onToggleExpand}>
            {isExpanded ? <FullscreenExitIcon fontSize="small" /> : <FullscreenIcon fontSize="small" />}
          </IconButton>
        </Tooltip>
        <Tooltip title={t("download", { defaultValue: "Download" })}>
          <span>
            <IconButton size="small" disabled={!onDownload} onClick={onDownload}>
              <DownloadIcon fontSize="small" />
            </IconButton>
          </span>
        </Tooltip>
        {onClose && (
          <Tooltip title={t("close", { defaultValue: "Close" })}>
            <IconButton size="small" onClick={onClose}>
              <CloseIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        )}
      </Box>

      {/* Stop Editing Confirmation */}
      <ConfirmModal
        open={stopEditConfirmOpen}
        title={t("stop_editing")}
        body={t("discard_edits_confirmation")}
        closeText={t("cancel")}
        confirmText={t("discard_edits")}
        onClose={() => setStopEditConfirmOpen(false)}
        onConfirm={() => {
          setStopEditConfirmOpen(false);
          dispatch(stopEditing());
        }}
      />

      {/* Delete Column Confirmation */}
      <ConfirmModal
        open={deleteColumnConfirmOpen}
        title={t("delete_field")}
        body={t("delete_field_confirmation", { name: pendingDeleteColumn })}
        closeText={t("cancel")}
        confirmText={t("delete")}
        onClose={() => {
          setDeleteColumnConfirmOpen(false);
          setPendingDeleteColumn(null);
        }}
        onConfirm={handleDeleteColumnConfirm}
      />

      {/* Edit Fields Modal */}
      <EditFieldsModal
        open={editFieldsOpen}
        onClose={() => {
          setEditFieldsOpen(false);
          setEditFieldsInitialField(null);
          mutate();
          // Optimistically update updated_at so tile URLs get a new cache-buster
          if (projectLayers) {
            const now = new Date().toISOString();
            mutateProjectLayers(
              projectLayers.map((l) =>
                l.layer_id === layerId ? { ...l, updated_at: now } : l
              ),
              { revalidate: false },
            );
          }
        }}
        layerId={layerId}
        initialFieldName={editFieldsInitialField}
      />

      {/* Table + Stats panel side by side */}
      <Box sx={{ display: "flex", flex: 1, minHeight: 0 }}>
      <TableContainer
        ref={tableContainerRef}
        sx={{
          flex: 1,
          minHeight: 0,
          overflow: "auto",
          // Thin, unobtrusive scrollbar
          "&::-webkit-scrollbar": { width: 6, height: 6 },
          "&::-webkit-scrollbar-thumb": {
            backgroundColor: "action.disabled",
            borderRadius: 3,
          },
          "&::-webkit-scrollbar-track": { backgroundColor: "transparent" },
          scrollbarWidth: "thin",
        }}>
        {(isLoading || areFieldsLoading) && !collectionData ? (
          <Box sx={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100%" }}>
            <CircularProgress size={32} />
          </Box>
        ) : displayFields.length === 0 ? (
          <Box sx={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100%" }}>
            <Typography variant="body2" color="text.secondary">
              {t("no_attributes")}
            </Typography>
          </Box>
        ) : (
          <Table
            size="small"
            stickyHeader
            sx={{
              width: "max-content",
              minWidth: "100%",
              "& .MuiTableCell-root": {
                verticalAlign: "top",
                borderRight: "1px solid",
                borderColor: "divider",
              },
              "& .MuiTableRow-root > .MuiTableCell-root:last-of-type": {
                borderRight: 0,
              },
              "& .MuiTableCell-stickyHeader": {
                backgroundColor: (theme) => emphasize(theme.palette.background.paper, 0.03),
                zIndex: 3,
              },
            }}>
            <TableHead>
              <TableRow>
                {/* Row number column */}
                <TableCell
                  sx={{
                    width: 48,
                    minWidth: 48,
                    maxWidth: 48,
                    position: "sticky",
                    left: 0,
                    zIndex: 4,
                    backgroundColor: (theme) => emphasize(theme.palette.background.paper, 0.03),
                    textAlign: "center",
                    px: 0,
                  }}>
                  <Typography variant="caption" color="text.secondary">
                    #
                  </Typography>
                </TableCell>
                {displayFields.map((field) => {
                  const w = columnWidths[field.name];
                  return (
                    <TableCell
                      key={field.name}
                      sx={{
                        ...(w ? { width: w, minWidth: w, maxWidth: w } : { minWidth: 100 }),
                        cursor: "pointer",
                        userSelect: "none",
                        whiteSpace: "nowrap",
                        ...(statsColumn === field.name && {
                          boxShadow: (theme) =>
                            `inset 2px 0 0 0 ${theme.palette.primary.main}, inset -2px 0 0 0 ${theme.palette.primary.main}, inset 0 2px 0 0 ${theme.palette.primary.main}`,
                        }),
                      }}
                      onClick={(e) => handleColumnMenuOpen(e, field.name)}>
                      <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
                        <Typography variant="body2" fontWeight="bold" noWrap sx={{ flex: 1 }}>
                          {field.name}
                        </Typography>
                        <Typography variant="caption" color="text.disabled" sx={{ flexShrink: 0 }}>
                          {field.type}
                        </Typography>
                      </Box>
                      {/* Resize handle */}
                      <Box
                        sx={{
                          position: "absolute",
                          top: 0,
                          right: 0,
                          width: 8,
                          height: "100%",
                          cursor: "col-resize",
                          userSelect: "none",
                          zIndex: 2,
                        }}
                        onClick={(e) => e.stopPropagation()}
                        onMouseDown={(e) => startColumnResize(e, field.name)}
                      />
                    </TableCell>
                  );
                })}
              </TableRow>
            </TableHead>
            <TableBody>
              {filteredFeatures.length === 0 && (
                <TableRow>
                  <TableCell colSpan={displayFields.length + 1} align="center" sx={{ py: 4 }}>
                    <Typography variant="body2" color="text.secondary">
                      {t("no_data", { defaultValue: "No data" })}
                    </Typography>
                  </TableCell>
                </TableRow>
              )}
              {filteredFeatures.map((feature, index) => {
                const rowId = `${feature.id}-${page}-${index}`;
                const isSelected = selectedRowId === rowId;
                const isRowDirty = Array.from(dirtyCells.values()).some((c) => c.rowId === rowId);
                const rowNumber = page * rowsPerPage + index + 1;

                return (
                  <TableRow
                    key={rowId}
                    hover
                    selected={isSelected}
                    onClick={() => selectRow(rowId)}
                    onDoubleClick={() => handleRowDoubleClick(rowId)}
                    onContextMenu={(e) => handleRowContextMenu(e, rowId)}
                    sx={{
                      cursor: "pointer",
                      backgroundColor: isRowDirty ? "rgba(255, 193, 7, 0.08)" : undefined,
                    }}>
                    <TableCell
                      sx={{
                        position: "sticky",
                        left: 0,
                        zIndex: 1,
                        backgroundColor: isSelected ? "action.selected" : "action.hover",
                        textAlign: "center",
                        px: 0,
                      }}>
                      <Typography variant="caption" color="text.secondary">
                        {rowNumber}
                      </Typography>
                    </TableCell>
                    {displayFields.map((field) => {
                      const originalValue = feature.properties?.[field.name];
                      const displayValue = getCellValue(rowId, field.name, originalValue);
                      const isEditing = editingCell?.rowId === rowId && editingCell?.column === field.name;
                      const isSelected = selectedCell?.rowId === rowId && selectedCell?.column === field.name;
                      const isDirty = dirtyCells.has(`${rowId}:${field.name}`);

                      return (
                        <TableCell
                          key={field.name}
                          sx={{
                            ...(columnWidths[field.name] ? { width: columnWidths[field.name], minWidth: columnWidths[field.name], maxWidth: columnWidths[field.name] } : {}),
                            cursor: "text",
                            position: "relative",
                            backgroundColor: isDirty
                              ? "rgba(255, 193, 7, 0.12)"
                              : isSelected && !isEditing
                                ? "action.hover"
                                : undefined,
                            p: isEditing ? 0 : undefined,
                            ...(isEditing && {
                              outline: (theme) => `2px solid ${theme.palette.primary.main}`,
                              outlineOffset: -2,
                            }),
                            ...(statsColumn === field.name && {
                              boxShadow: (theme) =>
                                `inset 2px 0 0 0 ${theme.palette.primary.main}, inset -2px 0 0 0 ${theme.palette.primary.main}`,
                            }),
                          }}
                          onClick={(e) => {
                            e.stopPropagation();
                            selectRow(rowId);
                            handleCellClick(rowId, field.name, originalValue);
                          }}>
                          {isEditing ? (
                            <TextField
                              autoFocus
                              fullWidth
                              size="small"
                              value={editValue}
                              onChange={(e) => setEditValue(e.target.value)}
                              onBlur={handleCellBlur}
                              onKeyDown={handleCellKeyDown}
                              type={field.type === "number" || field.type === "integer" ? "number" : "text"}
                              variant="outlined"
                              sx={{
                                "& .MuiInputBase-root": {
                                  fontSize: "0.875rem",
                                  borderRadius: 0,
                                },
                                "& .MuiOutlinedInput-notchedOutline": {
                                  border: "none",
                                },
                              }}
                            />
                          ) : (
                            <Typography
                              variant="body2"
                              noWrap
                              sx={{
                                display: "block",
                                lineHeight: 1.43,
                                minHeight: "1.43em",
                              }}>
                              {displayValue === null || displayValue === undefined
                                ? ""
                                : String(displayValue)}
                            </Typography>
                          )}
                        </TableCell>
                      );
                    })}
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        )}
      </TableContainer>

      {/* Column Stats Panel */}
      {statsColumn && (
        <ColumnStatsPanel
          layerId={layerId}
          columnName={statsColumn}
          columnType={displayFields.find((f) => f.name === statsColumn)?.type ?? "string"}
          cqlFilter={cqlFilter}
          onClose={() => setStatsColumn(null)}
          onPrev={() => {
            const idx = displayFields.findIndex((f) => f.name === statsColumn);
            const prevIdx = idx <= 0 ? displayFields.length - 1 : idx - 1;
            statsNavRef.current = true;
            setStatsColumn(displayFields[prevIdx].name);
          }}
          onNext={() => {
            const idx = displayFields.findIndex((f) => f.name === statsColumn);
            const nextIdx = idx >= displayFields.length - 1 ? 0 : idx + 1;
            statsNavRef.current = true;
            setStatsColumn(displayFields[nextIdx].name);
          }}
        />
      )}
      </Box>

      {/* Pagination */}
      <TablePagination
        component="div"
        count={totalCount}
        page={page}
        onPageChange={handleChangePage}
        rowsPerPage={rowsPerPage}
        onRowsPerPageChange={handleChangeRowsPerPage}
        rowsPerPageOptions={ROWS_PER_PAGE_OPTIONS}
        sx={{ borderTop: "1px solid", borderColor: "divider", flexShrink: 0 }}
      />

      {/* Column Header Context Menu */}
      <Menu
        anchorEl={columnMenuAnchor}
        open={!!columnMenuAnchor}
        onClose={handleColumnMenuClose}
        slotProps={{
          paper: {
            sx: {
              minWidth: 180,
              "& .MuiMenuItem-root": {
                py: 0.5,
                minHeight: 32,
                fontSize: "0.8rem",
              },
              "& .MuiListItemIcon-root": {
                minWidth: 28,
              },
              "& .MuiListItemText-root .MuiTypography-root": {
                fontSize: "0.8rem",
              },
              "& .MuiSvgIcon-root": {
                fontSize: "1rem",
              },
            },
          },
        }}>
        <MenuItem
          onClick={() => {
            if (columnMenuField) handleSort(columnMenuField, "asc");
            handleColumnMenuClose();
          }}>
          <ListItemIcon>
            <ArrowUpwardIcon />
          </ListItemIcon>
          <ListItemText>{t("sort_asc", { defaultValue: "Sort A-Z" })}</ListItemText>
        </MenuItem>
        <MenuItem
          onClick={() => {
            if (columnMenuField) handleSort(columnMenuField, "desc");
            handleColumnMenuClose();
          }}>
          <ListItemIcon>
            <ArrowDownwardIcon />
          </ListItemIcon>
          <ListItemText>{t("sort_desc", { defaultValue: "Sort Z-A" })}</ListItemText>
        </MenuItem>
        <Divider sx={{ my: 0.5 }} />
        <MenuItem
          onClick={() => {
            if (columnMenuField) {
              setEditFieldsInitialField(columnMenuField);
              setEditFieldsOpen(true);
            }
            handleColumnMenuClose();
          }}>
          <ListItemIcon>
            <EditIcon />
          </ListItemIcon>
          <ListItemText>{t("edit_field", { defaultValue: "Edit field" })}</ListItemText>
        </MenuItem>
        <MenuItem
          onClick={() => {
            if (columnMenuField) setStatsColumn(columnMenuField);
            handleColumnMenuClose();
          }}>
          <ListItemIcon>
            <BarChartIcon />
          </ListItemIcon>
          <ListItemText>{t("view_stats", { defaultValue: "View stats" })}</ListItemText>
        </MenuItem>
        <MenuItem
          onClick={() => {
            if (columnMenuField && columnMenuAnchor) {
              setQuickFilterColumn(columnMenuField);
              setQuickFilterAnchor(columnMenuAnchor);
            }
            handleColumnMenuClose();
          }}>
          <ListItemIcon>
            <FilterAltIcon />
          </ListItemIcon>
          <ListItemText>{t("add_filter", { defaultValue: "Add filter" })}</ListItemText>
        </MenuItem>
        <MenuItem disabled>
          <ListItemIcon>
            <CalculateIcon />
          </ListItemIcon>
          <ListItemText>{t("calculate_field", { defaultValue: "Calculate field" })}</ListItemText>
        </MenuItem>
        <Divider sx={{ my: 0.5 }} />
        <MenuItem
          onClick={() => {
            if (columnMenuField) handleDeleteColumnRequest(columnMenuField);
            handleColumnMenuClose();
          }}
          sx={{ color: "error.main" }}>
          <ListItemIcon>
            <DeleteIcon sx={{ color: "error.main" }} />
          </ListItemIcon>
          <ListItemText>{t("delete_column", { defaultValue: "Delete column" })}</ListItemText>
        </MenuItem>
      </Menu>

      {/* Row Context Menu */}
      <Menu
        open={!!rowMenuAnchor}
        onClose={handleRowMenuClose}
        anchorReference="anchorPosition"
        anchorPosition={rowMenuAnchor ?? undefined}
        slotProps={{
          paper: {
            sx: { minWidth: 220, maxWidth: 340, py: 2 },
          },
        }}
        MenuListProps={{ dense: true, disablePadding: true }}>
        <ListItemButton onClick={handleZoomToFeature}>
          <ListItemIcon sx={{ minWidth: 0, pr: 4 }}>
            <Icon iconName={ICON_NAME.ZOOM_IN} style={{ fontSize: 15 }} htmlColor="inherit" />
          </ListItemIcon>
          <ListItemText primary={t("zoom_to_feature", { defaultValue: "Zoom to feature" })} />
        </ListItemButton>
        <ListItemButton
          onClick={handleDeleteRow}
          sx={{ color: (theme) => theme.palette.error.main }}>
          <ListItemIcon sx={{ minWidth: 0, pr: 4, color: "inherit" }}>
            <Icon iconName={ICON_NAME.TRASH} style={{ fontSize: 15 }} htmlColor="inherit" />
          </ListItemIcon>
          <ListItemText
            primary={t("delete")}
            sx={{ "& .MuiTypography-root": { color: "inherit" } }}
          />
        </ListItemButton>
      </Menu>

      {/* Quick Filter Popover */}
      {quickFilterColumn && (
        <QuickFilterPopover
          anchorEl={quickFilterAnchor}
          columnName={quickFilterColumn}
          columnType={displayFields.find((f) => f.name === quickFilterColumn)?.type ?? "string"}
          layerId={layerId}
          projectLayer={projectLayer}
          onClose={() => {
            setQuickFilterAnchor(null);
            setQuickFilterColumn(null);
          }}
        />
      )}
    </Box>
  );
};

export default EditableDataTable;
