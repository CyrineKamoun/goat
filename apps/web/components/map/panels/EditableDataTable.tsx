import AddIcon from "@mui/icons-material/Add";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import BarChartIcon from "@mui/icons-material/BarChart";
import CalculateIcon from "@mui/icons-material/Calculate";
import CloseIcon from "@mui/icons-material/Close";
import DeleteIcon from "@mui/icons-material/Delete";
import DownloadIcon from "@mui/icons-material/Download";
import EditIcon from "@mui/icons-material/Edit";
import FilterListIcon from "@mui/icons-material/FilterList";
import FullscreenIcon from "@mui/icons-material/Fullscreen";
import FullscreenExitIcon from "@mui/icons-material/FullscreenExit";
import SaveIcon from "@mui/icons-material/Save";
import SearchIcon from "@mui/icons-material/Search";
import UndoIcon from "@mui/icons-material/Undo";
import {
  Box,
  Button,
  CircularProgress,
  Divider,
  IconButton,
  InputAdornment,
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
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import {
  deleteColumn,
  deleteFeaturesBulk,
  renameColumn,
  updateFeatureProperties,
  useDatasetCollectionItems,
} from "@/lib/api/layers";
import type { GetCollectionItemsQueryParams } from "@/lib/validations/layer";
import type { ProjectLayer } from "@/lib/validations/project";

import useLayerFields from "@/hooks/map/CommonHooks";

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
  layerName,
  isExpanded,
  onToggleExpand,
  onClose,
  onDownload,
}) => {
  const { t } = useTranslation("common");
  const { layerFields, isLoading: areFieldsLoading } = useLayerFields(layerId);

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
  const [isSaving, setIsSaving] = useState(false);

  // Search state
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchText, setSearchText] = useState("");

  // Column header menu state
  const [columnMenuAnchor, setColumnMenuAnchor] = useState<HTMLElement | null>(null);
  const [columnMenuField, setColumnMenuField] = useState<string | null>(null);

  // Rename field state
  const [renameFieldOpen, setRenameFieldOpen] = useState(false);
  const [renameFieldName, setRenameFieldName] = useState("");
  const [renameFieldOriginal, setRenameFieldOriginal] = useState("");

  // Column resize state
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});
  const activeResizeRef = useRef<{
    columnKey: string;
    startX: number;
    startWidth: number;
  } | null>(null);

  // Build query params
  const queryParams = useMemo<GetCollectionItemsQueryParams>(() => {
    const params: GetCollectionItemsQueryParams = {
      limit: rowsPerPage,
      offset: page * rowsPerPage,
    };
    if (sortBy) {
      params.sortby = sortDirection === "desc" ? `-${sortBy}` : sortBy;
    }
    return params;
  }, [page, rowsPerPage, sortBy, sortDirection]);

  // Fetch data
  const { data: collectionData, isLoading, mutate } = useDatasetCollectionItems(layerId, queryParams);

  // Filter to primitive fields only (no objects/geometry)
  const displayFields = useMemo(
    () => layerFields.filter((f) => f.type !== "object" && f.type !== "geometry"),
    [layerFields]
  );

  // Client-side search filter
  const filteredFeatures = useMemo(() => {
    const features = collectionData?.features || [];
    if (!searchText.trim()) return features;
    const lower = searchText.toLowerCase();
    return features.filter((f) =>
      displayFields.some((field) => {
        const val = f.properties?.[field.name];
        return val != null && String(val).toLowerCase().includes(lower);
      })
    );
  }, [collectionData?.features, searchText, displayFields]);

  // Reset page when layer changes
  useEffect(() => {
    setPage(0);
    setSelectedRowId(null);
    setDirtyCells(new Map());
    setEditingCell(null);
    setSearchText("");
    setSearchOpen(false);
  }, [layerId]);

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
    setSelectedRowId((prev) => (prev === rowId ? null : rowId));
  };

  // --- Cell Editing ---

  const getCellValue = (rowId: string, column: string, originalValue: unknown): unknown => {
    const key = `${rowId}:${column}`;
    const dirty = dirtyCells.get(key);
    return dirty ? dirty.newValue : originalValue;
  };

  const handleCellClick = (rowId: string, column: string, value: unknown) => {
    setEditingCell({ rowId, column });
    const displayValue = getCellValue(rowId, column, value);
    setEditValue(displayValue === null || displayValue === undefined ? "" : String(displayValue));
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
    if (parsedValue === originalValue || (parsedValue === null && (originalValue === null || originalValue === undefined))) {
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
    }

    setEditingCell(null);
  };

  const handleCellKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === "Enter") {
      handleCellBlur();
    } else if (event.key === "Escape") {
      setEditingCell(null);
    }
  };

  // --- Save Changes ---

  const handleSave = async () => {
    if (dirtyCells.size === 0) return;

    setIsSaving(true);
    try {
      const rowUpdates = new Map<string, Record<string, unknown>>();
      for (const [, cell] of dirtyCells) {
        if (!rowUpdates.has(cell.rowId)) {
          rowUpdates.set(cell.rowId, {});
        }
        rowUpdates.get(cell.rowId)![cell.column] = cell.newValue;
      }

      const promises = Array.from(rowUpdates.entries()).map(([rowId, properties]) =>
        updateFeatureProperties(layerId, getFeatureId(rowId), properties)
      );
      await Promise.all(promises);

      setDirtyCells(new Map());
      mutate();
      toast.success(t("changes_saved", { defaultValue: "Changes saved" }));
    } catch (error) {
      toast.error(t("error_saving_changes", { defaultValue: "Failed to save changes" }));
      console.error("Save error:", error);
    } finally {
      setIsSaving(false);
    }
  };

  // --- Discard Changes ---

  const handleDiscard = () => {
    setDirtyCells(new Map());
    setEditingCell(null);
  };

  // --- Rename Field ---

  const handleRenameField = async () => {
    if (!renameFieldName.trim() || !renameFieldOriginal) return;
    try {
      await renameColumn(layerId, renameFieldOriginal, renameFieldName.trim());
      setRenameFieldOpen(false);
      setRenameFieldName("");
      setRenameFieldOriginal("");
      mutate();
      toast.success(t("field_renamed", { defaultValue: "Field renamed" }));
    } catch (error) {
      toast.error(t("error_renaming_field", { defaultValue: "Failed to rename field" }));
      console.error("Rename field error:", error);
    }
  };

  // --- Delete Column ---

  const handleDeleteColumn = async (columnName: string) => {
    try {
      await deleteColumn(layerId, columnName);
      mutate();
      toast.success(t("column_deleted", { defaultValue: "Column deleted" }));
    } catch (error) {
      toast.error(t("error_deleting_column", { defaultValue: "Failed to delete column" }));
      console.error("Delete column error:", error);
    }
  };

  // --- Delete Selected ---

  const getFeatureId = (rowId: string): string => {
    // rowId format is `${feature.id}-${page}-${index}` — extract just the feature ID
    const parts = rowId.split("-");
    // Remove last two parts (page and index)
    return parts.slice(0, -2).join("-");
  };

  const handleDeleteSelected = async () => {
    if (!selectedRowId) return;
    try {
      await deleteFeaturesBulk(layerId, [getFeatureId(selectedRowId)]);
      setSelectedRowId(null);
      mutate();
      toast.success(t("rows_deleted", { defaultValue: "{{count}} row(s) deleted", count: 1 }));
    } catch (error) {
      toast.error(t("error_deleting_rows", { defaultValue: "Failed to delete rows" }));
      console.error("Delete error:", error);
    }
  };

  // --- Column Header Menu ---

  const handleColumnMenuOpen = (event: React.MouseEvent<HTMLElement>, fieldName: string) => {
    event.preventDefault();
    event.stopPropagation();
    setColumnMenuAnchor(event.currentTarget);
    setColumnMenuField(fieldName);
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
          gap: 0.5,
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

        {/* Unsaved changes */}
        {dirtyCells.size > 0 && (
          <>
            <Divider orientation="vertical" flexItem sx={{ mx: 0.5 }} />
            <Typography variant="caption" color="text.secondary" noWrap>
              {dirtyCells.size} {t("unsaved", { defaultValue: "unsaved" })}
            </Typography>
            <IconButton size="small" onClick={handleDiscard}>
              <UndoIcon fontSize="small" />
            </IconButton>
            <Button
              size="small"
              variant="contained"
              startIcon={isSaving ? <CircularProgress size={14} /> : <SaveIcon />}
              onClick={handleSave}
              disabled={isSaving}
              sx={{ textTransform: "none", minWidth: "auto" }}>
              {t("save", { defaultValue: "Save" })}
            </Button>
          </>
        )}

        {selectedRowId && (
          <>
            <Divider orientation="vertical" flexItem sx={{ mx: 0.5 }} />
            <Tooltip title={t("delete_selected", { defaultValue: "Delete selected" })}>
              <IconButton size="small" onClick={handleDeleteSelected} color="error">
                <DeleteIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </>
        )}

        <Box sx={{ flex: 1 }} />

        {/* Right: action buttons + utility icons */}
        <Button
          size="small"
          variant="outlined"
          startIcon={<AddIcon />}
          disabled
          sx={{ textTransform: "none", whiteSpace: "nowrap" }}>
          {t("add_field", { defaultValue: "Add a field" })}
        </Button>
        <Button
          size="small"
          variant="outlined"
          startIcon={<AddIcon />}
          disabled
          sx={{ textTransform: "none", whiteSpace: "nowrap" }}>
          {t("add_feature", { defaultValue: "Add a feature" })}
        </Button>

        <Divider orientation="vertical" flexItem sx={{ mx: 0.5 }} />

        {searchOpen ? (
          <TextField
            autoFocus
            size="small"
            placeholder={t("search", { defaultValue: "Search..." })}
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Escape") {
                setSearchOpen(false);
                setSearchText("");
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
          <span>
            <IconButton size="small" disabled>
              <FilterListIcon fontSize="small" />
            </IconButton>
          </span>
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

      {/* Rename Field Inline Form */}
      {renameFieldOpen && (
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            gap: 1,
            px: 1,
            py: 0.5,
            borderBottom: "1px solid",
            borderColor: "divider",
            backgroundColor: "action.hover",
            flexShrink: 0,
          }}>
          <Typography variant="caption" color="text.secondary" noWrap>
            {t("rename_field", { defaultValue: "Rename" })} &quot;{renameFieldOriginal}&quot;:
          </Typography>
          <TextField
            autoFocus
            size="small"
            value={renameFieldName}
            onChange={(e) => setRenameFieldName(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") handleRenameField();
              if (e.key === "Escape") setRenameFieldOpen(false);
            }}
            sx={{ width: 160, "& .MuiInputBase-root": { height: 30, fontSize: "0.8rem" } }}
          />
          <Button size="small" variant="contained" onClick={handleRenameField} sx={{ textTransform: "none", minWidth: "auto", height: 30 }}>
            {t("rename", { defaultValue: "Rename" })}
          </Button>
          <IconButton size="small" onClick={() => setRenameFieldOpen(false)}>
            <CloseIcon sx={{ fontSize: 14 }} />
          </IconButton>
        </Box>
      )}

      {/* Table */}
      <TableContainer sx={{ flex: 1, minHeight: 0, overflow: "auto" }}>
        {(isLoading || areFieldsLoading) && !collectionData ? (
          <Box sx={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100%" }}>
            <CircularProgress size={32} />
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
                backgroundColor: "background.paper",
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
                    zIndex: 3,
                    backgroundColor: "background.paper",
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
                    sx={{
                      cursor: "pointer",
                      backgroundColor: isRowDirty ? "rgba(255, 193, 7, 0.08)" : undefined,
                    }}>
                    <TableCell
                      sx={{
                        position: "sticky",
                        left: 0,
                        zIndex: 1,
                        backgroundColor: isSelected ? "action.selected" : "background.paper",
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
                      const isDirty = dirtyCells.has(`${rowId}:${field.name}`);

                      return (
                        <TableCell
                          key={field.name}
                          sx={{
                            ...(columnWidths[field.name] ? { width: columnWidths[field.name], minWidth: columnWidths[field.name], maxWidth: columnWidths[field.name] } : {}),
                            cursor: "text",
                            backgroundColor: isDirty ? "rgba(255, 193, 7, 0.12)" : undefined,
                            p: isEditing ? 0 : undefined,
                          }}
                          onClick={(e) => {
                            e.stopPropagation();
                            setSelectedRowId(rowId);
                            if (!isEditing) handleCellClick(rowId, field.name, originalValue);
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
                                  borderColor: "primary.main",
                                  borderWidth: 2,
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
              setRenameFieldOriginal(columnMenuField);
              setRenameFieldName(columnMenuField);
              setRenameFieldOpen(true);
            }
            handleColumnMenuClose();
          }}>
          <ListItemIcon>
            <EditIcon />
          </ListItemIcon>
          <ListItemText>{t("edit_field", { defaultValue: "Edit field" })}</ListItemText>
        </MenuItem>
        <MenuItem disabled>
          <ListItemIcon>
            <BarChartIcon />
          </ListItemIcon>
          <ListItemText>{t("view_stats", { defaultValue: "View stats" })}</ListItemText>
        </MenuItem>
        <MenuItem disabled>
          <ListItemIcon>
            <FilterListIcon />
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
            if (columnMenuField) handleDeleteColumn(columnMenuField);
            handleColumnMenuClose();
          }}
          sx={{ color: "error.main" }}>
          <ListItemIcon>
            <DeleteIcon sx={{ color: "error.main" }} />
          </ListItemIcon>
          <ListItemText>{t("delete_column", { defaultValue: "Delete column" })}</ListItemText>
        </MenuItem>
      </Menu>
    </Box>
  );
};

export default EditableDataTable;
