"use client";

import { Add as AddIcon, Description as ReportIcon } from "@mui/icons-material";
import {
  Alert,
  Box,
  Button,
  Checkbox,
  CircularProgress,
  Divider,
  FormControlLabel,
  IconButton,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Stack,
  Switch,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import { styled } from "@mui/material/styles";
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Trans, useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import {
  createReportLayout,
  deleteReportLayout,
  duplicateReportLayout,
  updateReportLayout,
  useReportLayouts,
} from "@/lib/api/reportLayouts";
import { useLayerQueryables } from "@/lib/api/layers";
import { useProjectInitialViewState } from "@/lib/api/projects";
import { PAGE_SIZES, type PageSize } from "@/lib/print/units";
import type { Project, ProjectLayer } from "@/lib/validations/project";
import type {
  AtlasConfig,
  AtlasFeatureCoverage,
  PageConfig,
  ReportLayout,
  ReportLayoutConfig,
} from "@/lib/validations/reportLayout";
import type { SelectorItem } from "@/types/map/common";

import { useAtlasFeatures } from "@/hooks/reports/useAtlasFeatures";
import { usePrintConfig } from "@/hooks/reports/usePrintConfig";
import { type ExportFormat, useExportReport } from "@/hooks/useExportReport";

import MoreMenu from "@/components/common/PopperMenu";
import type { PopperMenuItem } from "@/components/common/PopperMenu";
import { SIDE_PANEL_WIDTH, SidePanelContainer } from "@/components/common/SidePanel";
import SectionHeader from "@/components/map/panels/common/SectionHeader";
import SectionOptions from "@/components/map/panels/common/SectionOptions";
import Selector from "@/components/map/panels/common/Selector";
import ConfirmModal from "@/components/modals/Confirm";
import ReportLayoutRenameModal from "@/components/modals/ReportLayoutRename";
import ReportTemplatePickerModal, { type ReportTemplate } from "@/components/modals/ReportTemplatePicker";

const PanelContainer = styled(SidePanelContainer)(({ theme }) => ({
  width: SIDE_PANEL_WIDTH,
  minWidth: SIDE_PANEL_WIDTH,
  height: "100%",
  maxHeight: "100%",
  boxShadow: "none",
  borderRight: `1px solid ${theme.palette.background.paper}`,
  display: "flex",
  flexDirection: "column",
  overflow: "hidden",
  position: "relative",
  zIndex: 1,
}));

interface ReportsConfigPanelProps {
  project?: Project;
  projectLayers?: ProjectLayer[];
  selectedReport: ReportLayout | null;
  onSelectReport: (report: ReportLayout | null) => void;
}

const ReportsConfigPanel: React.FC<ReportsConfigPanelProps> = ({
  project,
  projectLayers = [],
  selectedReport,
  onSelectReport,
}) => {
  const { t } = useTranslation("common");

  // Fetch report layouts from API
  const { reportLayouts, isLoading, mutate } = useReportLayouts(project?.id);

  // Fetch project's initial view state for template maps
  const { initialView } = useProjectInitialViewState(project?.id ?? "");

  // Selected report ID (local, synced with parent)
  const [selectedReportId, setSelectedReportId] = useState<string | null>(null);

  // Loading states
  const [isCreating, setIsCreating] = useState(false);
  const [isSaving, setIsSaving] = useState(false);

  // Section collapsed state
  const [pageSettingsCollapsed, setPageSettingsCollapsed] = useState(false);

  // Local state for settings (derived from selected report)
  const [pageSize, setPageSize] = useState<PageConfig["size"]>("A4");
  const [orientation, setOrientation] = useState<PageConfig["orientation"]>("portrait");
  const [snapToGuides, setSnapToGuides] = useState<boolean>(false);
  const [showRulers, setShowRulers] = useState<boolean>(false);
  const [dpi, setDpi] = useState<number>(300);
  const [exportFormat, setExportFormat] = useState<string>("pdf");

  // Atlas settings state
  const [atlasEnabled, setAtlasEnabled] = useState<boolean>(false);
  const [atlasSettingsCollapsed, setAtlasSettingsCollapsed] = useState(false);
  const [atlasLayerId, setAtlasLayerId] = useState<number | null>(null);
  const [atlasPageName, setAtlasPageName] = useState<string>("");
  const [atlasSortBy, setAtlasSortBy] = useState<string>("");
  const [atlasSortOrder, setAtlasSortOrder] = useState<"asc" | "desc">("asc");
  const [atlasFileNameTemplate, setAtlasFileNameTemplate] = useState<string>("");
  const [customTemplateInput, setCustomTemplateInput] = useState<string>("");
  const [atlasFilterToCurrentFeature, setAtlasFilterToCurrentFeature] = useState(false);
  const [atlasHiddenCoverageLayer, setAtlasHiddenCoverageLayer] = useState(false);

  // Modal states
  const [deleteModalOpen, setDeleteModalOpen] = useState(false);
  const [renameModalOpen, setRenameModalOpen] = useState(false);
  const [templatePickerOpen, setTemplatePickerOpen] = useState(false);
  const [actionLayoutId, setActionLayoutId] = useState<string | null>(null);
  const [actionLayoutName, setActionLayoutName] = useState<string>("");

  // Track if we've already shown the template picker automatically
  const hasShownTemplatePickerRef = useRef(false);

  // Memoized selector items
  const pageSizeItems: SelectorItem[] = useMemo(
    () => [
      { label: "A4", value: "A4" },
      { label: "A3", value: "A3" },
      { label: "Letter", value: "Letter" },
      { label: "Legal", value: "Legal" },
      { label: "Tabloid", value: "Tabloid" },
    ],
    []
  );

  const orientationItems: SelectorItem[] = useMemo(
    () => [
      { label: t("vertical"), value: "portrait" },
      { label: t("horizontal"), value: "landscape" },
    ],
    [t]
  );

  const dpiItems: SelectorItem[] = useMemo(
    () => [
      { label: "72 (Screen)", value: 72 },
      { label: "150 (Low)", value: 150 },
      { label: "300 (High)", value: 300 },
      { label: "600 (Print)", value: 600 },
    ],
    []
  );

  const exportFormatItems: SelectorItem[] = useMemo(
    () => [
      { label: "PDF", value: "pdf" },
      { label: "PNG", value: "png" },
      { label: "JPEG", value: "jpeg" },
    ],
    []
  );

  // Coverage layer items for atlas
  const coverageLayerItems: SelectorItem[] = useMemo(
    () =>
      projectLayers.map((layer) => ({
        label: layer.name,
        value: layer.id,
      })),
    [projectLayers]
  );

  // Fetch layer attributes for the selected coverage layer
  const coverageLayerObj = useMemo(
    () => projectLayers.find((l) => l.id === atlasLayerId) ?? null,
    [atlasLayerId, projectLayers]
  );
  const { queryables } = useLayerQueryables(coverageLayerObj?.layer_id ?? "");

  // Attribute items for Page Name and Sort By selectors
  const attributeItems: SelectorItem[] = useMemo(() => {
    if (!queryables?.properties) return [];
    return Object.values(queryables.properties).map((field) => ({
      label: field.name,
      value: field.name,
    }));
  }, [queryables]);

  const sortOrderItems: SelectorItem[] = useMemo(
    () => [
      { label: t("ascending"), value: "asc" },
      { label: t("descending"), value: "desc" },
    ],
    [t]
  );

  // Track previous report ID to avoid re-notifying parent on SWR cache updates
  const prevSelectedReportIdRef = useRef<string | null>(null);

  // Notify parent and load settings only when the selected report ID changes
  useEffect(() => {
    // Skip if the ID hasn't actually changed (e.g. SWR cache mutation)
    if (selectedReportId === prevSelectedReportIdRef.current) return;
    prevSelectedReportIdRef.current = selectedReportId;

    if (selectedReportId && reportLayouts) {
      const report = reportLayouts.find((r) => r.id === selectedReportId);
      if (report) {
        onSelectReport(report);
        // Page settings
        setPageSize(report.config.page.size);
        setOrientation(report.config.page.orientation);
        setSnapToGuides(report.config.page.snapToGuides ?? false);
        setShowRulers(report.config.page.showRulers ?? false);
        setDpi(report.config.page.dpi ?? 300);
        setExportFormat(report.config.page.export_format ?? "pdf");
        // Atlas settings loading
        const atlas = report.config.atlas;
        setAtlasEnabled(atlas?.enabled ?? false);
        if (atlas?.coverage?.type === "feature") {
          setAtlasLayerId(atlas.coverage.layer_project_id);
          setAtlasSortBy(atlas.coverage.sort_by ?? "");
          setAtlasSortOrder(atlas.coverage.sort_order ?? "asc");
          setAtlasFilterToCurrentFeature(atlas.coverage.filter_to_current_feature ?? false);
          setAtlasHiddenCoverageLayer(atlas.coverage.hidden_coverage_layer ?? false);
        } else {
          setAtlasLayerId(null);
          setAtlasSortBy("");
          setAtlasSortOrder("asc");
          setAtlasFilterToCurrentFeature(false);
          setAtlasHiddenCoverageLayer(false);
        }
        // Page label
        const pageLabel = atlas?.page_label;
        if (pageLabel?.template) {
          // Extract attribute name from template like "{{@feature.NAME}}"
          const match = pageLabel.template.match(/\{\{@feature\.([^}]+)\}\}/);
          setAtlasPageName(match ? match[1] : "");
        } else {
          setAtlasPageName("");
        }
        // File name template - detect if saved value matches a preset or is custom
        const savedTemplate = atlas?.file_name_template ?? "";
        setAtlasFileNameTemplate(savedTemplate);
        // customTemplateInput will be set after fileNameTemplateItems recalculates
        setCustomTemplateInput(savedTemplate);
      }
    } else {
      onSelectReport(null);
    }
  }, [selectedReportId, reportLayouts, onSelectReport]);

  // Auto-select first report when layouts load, or show template picker if none exist
  useEffect(() => {
    if (reportLayouts && reportLayouts.length > 0 && !selectedReportId) {
      setSelectedReportId(reportLayouts[0].id);
    } else if (
      reportLayouts &&
      reportLayouts.length === 0 &&
      !isLoading &&
      !hasShownTemplatePickerRef.current
    ) {
      // No layouts exist, show template picker automatically
      hasShownTemplatePickerRef.current = true;
      setTemplatePickerOpen(true);
    }
  }, [reportLayouts, selectedReportId, isLoading]);

  // Handle template selection - create layout from template
  const handleSelectTemplate = useCallback(
    async (template: ReportTemplate) => {
      if (!project?.id) return;

      setIsCreating(true);
      try {
        const newLayout = await createReportLayout(project.id, {
          name: `${template.name} ${(reportLayouts?.length || 0) + 1}`,
          is_default: false,
          config: template.config,
        });
        await mutate();
        setSelectedReportId(newLayout.id);
      } catch (error) {
        console.error("Failed to create report layout from template:", error);
      } finally {
        setIsCreating(false);
      }
    },
    [project?.id, reportLayouts?.length, mutate]
  );

  const handleAddReport = useCallback(async () => {
    // Show template picker instead of creating blank layout directly
    setTemplatePickerOpen(true);
  }, []);

  const handleDeleteReport = useCallback(
    async (reportId: string) => {
      if (!project?.id) return;

      try {
        await deleteReportLayout(project.id, reportId);
        await mutate();
        if (selectedReportId === reportId) {
          setSelectedReportId(null);
        }
      } catch (error) {
        console.error("Failed to delete report layout:", error);
      }
    },
    [project?.id, selectedReportId, mutate]
  );

  const handleDuplicateReport = useCallback(
    async (reportId: string) => {
      if (!project?.id) return;

      try {
        const duplicated = await duplicateReportLayout(project.id, reportId);
        await mutate();
        setSelectedReportId(duplicated.id);
      } catch (error) {
        console.error("Failed to duplicate report layout:", error);
      }
    },
    [project?.id, mutate]
  );

  const handleRenameReport = useCallback(
    async (newName: string) => {
      if (!project?.id || !actionLayoutId) return;

      try {
        // Only send name — don't send config to avoid overwriting with stale SWR cache data
        await updateReportLayout(project.id, actionLayoutId, {
          name: newName,
        });
        // Optimistic cache update: preserve selectedReport.config for the active report
        // to avoid the useEffect replacing parent state with stale data
        mutate(
          reportLayouts?.map((r) =>
            r.id === actionLayoutId
              ? {
                  ...r,
                  name: newName,
                  // Use selectedReport.config if this is the active report (has unsaved local changes)
                  ...(actionLayoutId === selectedReport?.id && { config: selectedReport.config }),
                }
              : r
          ),
          { revalidate: false }
        );
      } catch (error) {
        console.error("Failed to rename report layout:", error);
      }
    },
    [project?.id, actionLayoutId, reportLayouts, mutate, selectedReport]
  );

  // Save settings to database when they change
  const handleSettingChange = useCallback(
    async (updates: Partial<ReportLayoutConfig["page"]>) => {
      if (!project?.id || !selectedReport) return;

      setIsSaving(true);
      try {
        const updatedConfig: ReportLayoutConfig = {
          ...selectedReport.config,
          page: {
            ...selectedReport.config.page,
            ...updates,
          },
        };
        const updatedReport = { ...selectedReport, config: updatedConfig };
        await updateReportLayout(project.id, selectedReport.id, {
          config: updatedConfig,
        });
        // Optimistic cache update to avoid re-fetch replacing parent's local state
        mutate(
          reportLayouts?.map((r) =>
            r.id === selectedReport.id ? updatedReport : r
          ),
          { revalidate: false }
        );
        // Notify parent so selectedReport stays in sync
        onSelectReport(updatedReport);
      } catch (error) {
        console.error("Failed to update report layout:", error);
      } finally {
        setIsSaving(false);
      }
    },
    [project?.id, selectedReport, reportLayouts, mutate, onSelectReport]
  );

  const handlePageSizeChange = (newSize: PageConfig["size"]) => {
    setPageSize(newSize);
    handleSettingChange({ size: newSize });
  };

  const handleOrientationChange = (newOrientation: PageConfig["orientation"]) => {
    setOrientation(newOrientation);
    handleSettingChange({ orientation: newOrientation });
  };

  const handleSnapToGuidesChange = (enabled: boolean) => {
    setSnapToGuides(enabled);
    handleSettingChange({ snapToGuides: enabled });
  };

  const handleShowRulersChange = (enabled: boolean) => {
    setShowRulers(enabled);
    handleSettingChange({ showRulers: enabled });
  };

  // Atlas settings save
  const handleAtlasSettingChange = useCallback(
    async (updates: Partial<AtlasConfig>) => {
      if (!project?.id || !selectedReport) return;

      setIsSaving(true);
      try {
        const currentAtlas = selectedReport.config.atlas || { enabled: false };
        const updatedConfig: ReportLayoutConfig = {
          ...selectedReport.config,
          atlas: {
            ...currentAtlas,
            ...updates,
          },
        };
        const updatedReport = { ...selectedReport, config: updatedConfig };
        await updateReportLayout(project.id, selectedReport.id, {
          config: updatedConfig,
        });
        // Optimistic cache update to avoid re-fetch replacing parent's local state
        mutate(
          reportLayouts?.map((r) =>
            r.id === selectedReport.id ? updatedReport : r
          ),
          { revalidate: false }
        );
        // Notify parent so selectedReport stays in sync
        onSelectReport(updatedReport);
      } catch (error) {
        console.error("Failed to update atlas settings:", error);
      } finally {
        setIsSaving(false);
      }
    },
    [project?.id, selectedReport, reportLayouts, mutate, onSelectReport]
  );

  const handleAtlasEnabledChange = (enabled: boolean) => {
    setAtlasEnabled(enabled);
    handleAtlasSettingChange({ enabled });
  };

  const handleAtlasLayerChange = (layerId: number | null) => {
    setAtlasLayerId(layerId);
    // Reset dependent fields
    setAtlasSortBy("");
    setAtlasPageName("");
    setAtlasFilterToCurrentFeature(false);
    setAtlasHiddenCoverageLayer(false);
    if (layerId) {
      const coverage: AtlasFeatureCoverage = {
        type: "feature",
        layer_project_id: layerId,
        sort_by: undefined,
        sort_order: atlasSortOrder,
      };
      handleAtlasSettingChange({ coverage, page_label: { enabled: false, template: "" } });
    }
  };

  const handleAtlasSortByChange = (sortBy: string) => {
    setAtlasSortBy(sortBy);
    if (!atlasLayerId) return;
    const coverage: AtlasFeatureCoverage = {
      type: "feature",
      layer_project_id: atlasLayerId,
      sort_by: sortBy || undefined,
      sort_order: atlasSortOrder,
      filter_to_current_feature: atlasFilterToCurrentFeature,
      hidden_coverage_layer: atlasHiddenCoverageLayer,
    };
    handleAtlasSettingChange({ coverage });
  };

  const handleAtlasSortOrderChange = (order: "asc" | "desc") => {
    setAtlasSortOrder(order);
    if (!atlasLayerId) return;
    const coverage: AtlasFeatureCoverage = {
      type: "feature",
      layer_project_id: atlasLayerId,
      sort_by: atlasSortBy || undefined,
      sort_order: order,
      filter_to_current_feature: atlasFilterToCurrentFeature,
      hidden_coverage_layer: atlasHiddenCoverageLayer,
    };
    handleAtlasSettingChange({ coverage });
  };

  const handleAtlasFilterToCurrentFeatureChange = (checked: boolean) => {
    setAtlasFilterToCurrentFeature(checked);
    if (!atlasLayerId) return;
    const coverage: AtlasFeatureCoverage = {
      type: "feature",
      layer_project_id: atlasLayerId,
      sort_by: atlasSortBy || undefined,
      sort_order: atlasSortOrder,
      filter_to_current_feature: checked,
      hidden_coverage_layer: atlasHiddenCoverageLayer,
    };
    handleAtlasSettingChange({ coverage });
  };

  const handleAtlasHiddenCoverageLayerChange = (checked: boolean) => {
    setAtlasHiddenCoverageLayer(checked);
    if (!atlasLayerId) return;
    const coverage: AtlasFeatureCoverage = {
      type: "feature",
      layer_project_id: atlasLayerId,
      sort_by: atlasSortBy || undefined,
      sort_order: atlasSortOrder,
      filter_to_current_feature: atlasFilterToCurrentFeature,
      hidden_coverage_layer: checked,
    };
    handleAtlasSettingChange({ coverage });
  };

  const handleAtlasPageNameChange = (attrName: string) => {
    setAtlasPageName(attrName);
    const template = attrName ? `{{@feature.${attrName}}}` : "";
    handleAtlasSettingChange({
      page_label: { enabled: !!attrName, template },
    });
  };

  // Predefined file name template options
  const CUSTOM_TEMPLATE_VALUE = "__custom__";
  const fileNameTemplateItems: SelectorItem[] = useMemo(() => {
    const items: SelectorItem[] = [
      { label: `${t("layout_name")} + ${t("page_number")}`, value: "{{@layout_name}}_{{@page_number}}" },
      { label: t("page_number_only"), value: "{{@page_number}}" },
      { label: t("page_x_of_y"), value: "{{@page_number}}_of_{{@total_pages}}" },
    ];
    if (atlasPageName) {
      items.push(
        { label: `${t("layout_name")} + ${t("page_name")}`, value: `{{@layout_name}}_{{@feature.${atlasPageName}}}` },
        { label: t("page_name_only"), value: `{{@feature.${atlasPageName}}}` },
      );
    }
    items.push({ label: t("custom"), value: CUSTOM_TEMPLATE_VALUE });
    return items;
  }, [t, atlasPageName]);

  // Determine if current template is a preset or custom
  const isCustomTemplate = useMemo(() => {
    if (!atlasFileNameTemplate) return false;
    if (atlasFileNameTemplate === CUSTOM_TEMPLATE_VALUE) return true;
    return !fileNameTemplateItems.some(
      (item) => item.value !== CUSTOM_TEMPLATE_VALUE && item.value === atlasFileNameTemplate
    );
  }, [atlasFileNameTemplate, fileNameTemplateItems]);

  const selectedFileNameTemplateItem = useMemo(() => {
    if (!atlasFileNameTemplate) return fileNameTemplateItems[0]; // default
    if (isCustomTemplate) return fileNameTemplateItems.find((item) => item.value === CUSTOM_TEMPLATE_VALUE);
    return fileNameTemplateItems.find((item) => item.value === atlasFileNameTemplate);
  }, [atlasFileNameTemplate, fileNameTemplateItems, isCustomTemplate]);

  // Validate template placeholders (only relevant for custom input)
  const templateValidationError = useMemo(() => {
    if (!isCustomTemplate || !customTemplateInput) return "";
    // Find all {{...}} placeholders
    const placeholders = customTemplateInput.match(/\{\{[^}]*\}\}/g);
    if (!placeholders) return "";
    const validPatterns = [
      /^\{\{@page_number\}\}$/,
      /^\{\{@total_pages\}\}$/,
      /^\{\{@layout_name\}\}$/,
      /^\{\{@feature\.[^}]+\}\}$/,
    ];
    for (const ph of placeholders) {
      if (!validPatterns.some((p) => p.test(ph))) {
        return t("invalid_placeholder", { placeholder: ph });
      }
    }
    return "";
  }, [customTemplateInput, isCustomTemplate, t]);

  const handleFileNameTemplateSelect = (item: SelectorItem | SelectorItem[] | undefined) => {
    if (!item || Array.isArray(item)) return;
    if (item.value === CUSTOM_TEMPLATE_VALUE) {
      // Switch to custom mode with empty input
      setCustomTemplateInput("");
      setAtlasFileNameTemplate(CUSTOM_TEMPLATE_VALUE);
    } else {
      const value = item.value as string;
      setAtlasFileNameTemplate(value);
      setCustomTemplateInput("");
      handleAtlasSettingChange({ file_name_template: value || undefined });
    }
  };

  const handleCustomTemplateBlur = () => {
    // Only persist on blur to avoid focus loss
    if (customTemplateInput && !templateValidationError) {
      handleAtlasSettingChange({ file_name_template: customTemplateInput || undefined });
    }
  };

  // Menu items for layout actions
  const getLayoutMenuItems = useCallback(
    (report: ReportLayout): PopperMenuItem[] => [
      {
        id: "rename",
        label: t("rename"),
        icon: ICON_NAME.EDIT,
        onClick: () => {
          setActionLayoutId(report.id);
          setActionLayoutName(report.name);
          setRenameModalOpen(true);
        },
      },
      {
        id: "duplicate",
        label: t("duplicate"),
        icon: ICON_NAME.COPY,
        onClick: () => handleDuplicateReport(report.id),
      },
      {
        id: "delete",
        label: t("delete"),
        icon: ICON_NAME.TRASH,
        color: "error.main",
        onClick: () => {
          setActionLayoutId(report.id);
          setActionLayoutName(report.name);
          setDeleteModalOpen(true);
        },
      },
    ],
    [t, handleDuplicateReport]
  );

  // Print hook (server-side PDF generation via Playwright)
  const { isBusy: isPrinting, exportReport } = useExportReport();

  // Print config (atlas limits from backend)
  const { atlasMaxPages } = usePrintConfig();

  // Get atlas info for print job and UI display
  const { totalPages: atlasTotalPages, wasTruncated: atlasWasTruncated, totalFeatureCount: atlasTotalFeatureCount } =
    useAtlasFeatures({
      atlasConfig: selectedReport?.config?.atlas,
      projectLayers,
      atlasMaxPages,
    });

  // Example preview for the active template
  const templatePreview = useMemo(() => {
    const template = isCustomTemplate ? customTemplateInput : atlasFileNameTemplate;
    if (!template || template === CUSTOM_TEMPLATE_VALUE) return "";
    const layoutName = selectedReport?.name || "Layout";
    const sampleAttr = atlasPageName ? "Berlin" : "";
    const example = template
      .replace(/\{\{@layout_name\}\}/g, layoutName.replace(/\s+/g, "_"))
      .replace(/\{\{@page_number\}\}/g, "1")
      .replace(/\{\{@total_pages\}\}/g, String(atlasTotalPages || 10))
      .replace(/\{\{@feature\.[^}]+\}\}/g, sampleAttr || "value");
    return example ? `${example}.${exportFormat}` : "";
  }, [atlasFileNameTemplate, customTemplateInput, isCustomTemplate, exportFormat, selectedReport?.name, atlasTotalPages, atlasPageName, CUSTOM_TEMPLATE_VALUE]);

  const handlePrintReport = useCallback(async () => {
    if (!project?.id || !selectedReport) return;

    // Calculate paper dimensions based on size and orientation
    const pageConfig = selectedReport.config.page;
    const sizeKey = (pageConfig.size === "Custom" ? "A4" : pageConfig.size) as PageSize;
    const size = PAGE_SIZES[sizeKey] || PAGE_SIZES.A4;
    const paperWidthMm = pageConfig.orientation === "landscape" ? size.height : size.width;
    const paperHeightMm = pageConfig.orientation === "landscape" ? size.width : size.height;

    try {
      await exportReport({
        projectId: project.id,
        layoutId: selectedReport.id,
        layoutName: selectedReport.name,
        format: exportFormat as ExportFormat,
        totalAtlasPages: atlasTotalPages > 0 ? atlasTotalPages : undefined,
        dpi,
        paperWidthMm,
        paperHeightMm,
        fileNameTemplate: selectedReport.config.atlas?.file_name_template || undefined,
      });
    } catch (error) {
      console.error("Failed to print report:", error);
    }
  }, [project?.id, selectedReport, exportReport, exportFormat, atlasTotalPages, dpi]);

  return (
    <PanelContainer>
      {/* Layouts Section */}
      <Box
        sx={{ flexShrink: 1, minHeight: 80, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        {/* Layouts Header */}
        <Stack
          direction="row"
          alignItems="center"
          justifyContent="space-between"
          sx={{ p: 2, pb: 0, mb: 2, flexShrink: 0 }}>
          <Typography variant="subtitle1" fontWeight={600}>
            {t("reports")}
          </Typography>
          <Button
            variant="contained"
            size="small"
            startIcon={isCreating ? <CircularProgress size={16} color="inherit" /> : <AddIcon />}
            onClick={handleAddReport}
            disabled={isCreating || !project?.id}
            sx={{ textTransform: "none" }}>
            {t("add_layout")}
          </Button>
        </Stack>

        {/* Layouts List - Scrollable */}
        <Box
          sx={{
            flex: "1 1 auto",
            minHeight: 0,
            maxHeight: 200,
            overflowY: "auto",
            px: 2,
            "&::-webkit-scrollbar": {
              width: "6px",
            },
            "&::-webkit-scrollbar-thumb": {
              background: "#2836484D",
              borderRadius: "3px",
              "&:hover": {
                background: "#28364880",
              },
            },
          }}>
          {isLoading ? (
            <Box sx={{ display: "flex", justifyContent: "center", py: 4 }}>
              <CircularProgress size={24} />
            </Box>
          ) : (
            <List dense sx={{ mx: -2 }}>
              {reportLayouts?.map((report) => (
                <ListItem
                  key={report.id}
                  disablePadding
                  secondaryAction={
                    <MoreMenu
                      menuItems={getLayoutMenuItems(report)}
                      disablePortal={false}
                      menuButton={
                        <Tooltip title={t("more_options")} placement="top">
                          <IconButton edge="end" size="small">
                            <Icon iconName={ICON_NAME.MORE_VERT} style={{ fontSize: "15px" }} />
                          </IconButton>
                        </Tooltip>
                      }
                    />
                  }>
                  <ListItemButton
                    selected={selectedReportId === report.id}
                    onClick={() => setSelectedReportId(report.id)}>
                    <ListItemIcon sx={{ minWidth: 36 }}>
                      <ReportIcon fontSize="small" />
                    </ListItemIcon>
                    <ListItemText primary={report.name} primaryTypographyProps={{ fontSize: "0.875rem" }} />
                  </ListItemButton>
                </ListItem>
              ))}
              {(!reportLayouts || reportLayouts.length === 0) && (
                <Box sx={{ py: 2, px: 2, textAlign: "center" }}>
                  <Typography variant="body2" color="text.secondary">
                    {t("no_layouts_yet")}
                  </Typography>
                </Box>
              )}
            </List>
          )}
        </Box>
      </Box>

      <Divider sx={{ flexShrink: 0 }} />

      {/* Settings Section */}
      <Box sx={{ flex: "1 1 0", minHeight: 0, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        {/* Settings Header */}
        <Stack
          direction="row"
          alignItems="center"
          justifyContent="space-between"
          sx={{ px: 3, py: 2, flexShrink: 0 }}>
          <Typography variant="body1" fontWeight="bold">
            {t("settings")}
          </Typography>
        </Stack>
        <Divider sx={{ flexShrink: 0 }} />
        {/* Settings Body - Scrollable */}
        <Box
          sx={{
            flex: "1 1 0",
            minHeight: 0,
            overflowY: "auto",
            px: 3,
            py: 2,
            "&::-webkit-scrollbar": {
              width: "6px",
            },
            "&::-webkit-scrollbar-thumb": {
              background: "#2836484D",
              borderRadius: "3px",
              "&:hover": {
                background: "#28364880",
              },
            },
          }}>
          <Stack spacing={2}>
            {/* Page Settings Section */}
            <Box sx={{ overflow: "hidden" }}>
              <SectionHeader
                active={true}
                alwaysActive
                label={t("page_settings")}
                icon={ICON_NAME.SETTINGS}
                collapsed={pageSettingsCollapsed}
                setCollapsed={setPageSettingsCollapsed}
                disableAdvanceOptions
              />
              <SectionOptions
                active={!pageSettingsCollapsed}
                baseOptions={
                  <Stack spacing={3}>
                    {/* Page Size */}
                    <Selector
                      label={t("size")}
                      selectedItems={pageSizeItems.find((item) => item.value === pageSize)}
                      setSelectedItems={(item: SelectorItem | SelectorItem[] | undefined) => {
                        if (item && !Array.isArray(item)) {
                          handlePageSizeChange(item.value as PageConfig["size"]);
                        }
                      }}
                      items={pageSizeItems}
                      disabled={!selectedReport || isSaving}
                    />

                    {/* Orientation */}
                    <Selector
                      label={t("orientation")}
                      selectedItems={orientationItems.find((item) => item.value === orientation)}
                      setSelectedItems={(item: SelectorItem | SelectorItem[] | undefined) => {
                        if (item && !Array.isArray(item)) {
                          handleOrientationChange(item.value as PageConfig["orientation"]);
                        }
                      }}
                      items={orientationItems}
                      disabled={!selectedReport || isSaving}
                    />

                    {/* DPI */}
                    <Selector
                      label={t("dpi")}
                      selectedItems={dpiItems.find((item) => item.value === dpi)}
                      setSelectedItems={(item: SelectorItem | SelectorItem[] | undefined) => {
                        if (item && !Array.isArray(item)) {
                          const newDpi = item.value as number;
                          setDpi(newDpi);
                          handleSettingChange({ dpi: newDpi });
                        }
                      }}
                      items={dpiItems}
                      disabled={!selectedReport || isSaving}
                    />

                    {/* Export Format */}
                    <Selector
                      label={t("format_export")}
                      selectedItems={exportFormatItems.find((item) => item.value === exportFormat)}
                      setSelectedItems={(item: SelectorItem | SelectorItem[] | undefined) => {
                        if (item && !Array.isArray(item)) {
                          const newFormat = item.value as string;
                          setExportFormat(newFormat);
                          handleSettingChange({ export_format: newFormat as "pdf" | "png" | "jpeg" });
                        }
                      }}
                      items={exportFormatItems}
                      disabled={!selectedReport || isSaving}
                    />

                    {/* Snap to Guides */}
                    <FormControlLabel
                      control={
                        <Switch
                          checked={snapToGuides}
                          onChange={(e) => handleSnapToGuidesChange(e.target.checked)}
                          disabled={!selectedReport || isSaving}
                          size="small"
                        />
                      }
                      label={<Typography variant="body2">{t("snap_to_guides")}</Typography>}
                      sx={{ ml: 0 }}
                    />

                    {/* Show Rulers */}
                    <FormControlLabel
                      control={
                        <Switch
                          checked={showRulers}
                          onChange={(e) => handleShowRulersChange(e.target.checked)}
                          disabled={!selectedReport || isSaving}
                          size="small"
                        />
                      }
                      label={<Typography variant="body2">{t("show_rulers")}</Typography>}
                      sx={{ ml: 0 }}
                    />
                  </Stack>
                }
              />
            </Box>

            {/* Atlas / Map Series Section */}
            <Box sx={{ overflow: "hidden" }}>
              <SectionHeader
                active={atlasEnabled}
                label={t("atlas_map_series")}
                icon={ICON_NAME.LAYERS}
                collapsed={atlasSettingsCollapsed}
                setCollapsed={setAtlasSettingsCollapsed}
                onToggleChange={(e) => handleAtlasEnabledChange(e.target.checked)}
                disableAdvanceOptions
              />
              <SectionOptions
                active={atlasEnabled && !atlasSettingsCollapsed}
                baseOptions={
                  <Stack spacing={3}>
                    {/* Coverage Layer */}
                    <Selector
                      label={t("coverage_layer")}
                      selectedItems={coverageLayerItems.find((item) => item.value === atlasLayerId)}
                      setSelectedItems={(item: SelectorItem | SelectorItem[] | undefined) => {
                        if (item && !Array.isArray(item)) {
                          handleAtlasLayerChange(item.value as number);
                        } else {
                          handleAtlasLayerChange(null);
                        }
                      }}
                      items={coverageLayerItems}
                      placeholder={t("select_layer")}
                      disabled={!selectedReport || isSaving}
                    />

                    {/* Controls only shown when a coverage layer is selected */}
                    {atlasLayerId && (
                      <>
                        {/* Page Name (attribute-based label) */}
                        <Selector
                          label={t("page_name")}
                          selectedItems={attributeItems.find((item) => item.value === atlasPageName)}
                          setSelectedItems={(item: SelectorItem | SelectorItem[] | undefined) => {
                            if (item && !Array.isArray(item)) {
                              handleAtlasPageNameChange(item.value as string);
                            } else {
                              handleAtlasPageNameChange("");
                            }
                          }}
                          items={attributeItems}
                          placeholder={t("select_attribute")}
                          disabled={!selectedReport || isSaving}
                        />

                        {/* Sort By */}
                        <Selector
                          label={t("sort_by")}
                          selectedItems={attributeItems.find((item) => item.value === atlasSortBy)}
                          setSelectedItems={(item: SelectorItem | SelectorItem[] | undefined) => {
                            if (item && !Array.isArray(item)) {
                              handleAtlasSortByChange(item.value as string);
                            } else {
                              handleAtlasSortByChange("");
                            }
                          }}
                          items={attributeItems}
                          placeholder={t("select_attribute")}
                          disabled={!selectedReport || isSaving}
                        />

                        {/* Sort Order */}
                        <Selector
                          label={t("sort_order")}
                          selectedItems={sortOrderItems.find((item) => item.value === atlasSortOrder)}
                          setSelectedItems={(item: SelectorItem | SelectorItem[] | undefined) => {
                            if (item && !Array.isArray(item)) {
                              handleAtlasSortOrderChange(item.value as "asc" | "desc");
                            }
                          }}
                          items={sortOrderItems}
                          disabled={!selectedReport || isSaving}
                        />

                        {/* Coverage layer rendering options */}
                        <FormControlLabel
                          control={
                            <Checkbox
                              size="small"
                              checked={atlasFilterToCurrentFeature}
                              onChange={(e) => handleAtlasFilterToCurrentFeatureChange(e.target.checked)}
                              disabled={!selectedReport || isSaving || atlasHiddenCoverageLayer}
                            />
                          }
                          label={<Typography variant="body2">{t("atlas_filter_to_current_feature")}</Typography>}
                          sx={{ ml: 0 }}
                        />
                        <FormControlLabel
                          control={
                            <Checkbox
                              size="small"
                              checked={atlasHiddenCoverageLayer}
                              onChange={(e) => handleAtlasHiddenCoverageLayerChange(e.target.checked)}
                              disabled={!selectedReport || isSaving}
                            />
                          }
                          label={<Typography variant="body2">{t("atlas_hidden_coverage_layer")}</Typography>}
                          sx={{ ml: 0 }}
                        />

                        {/* Output File Name Template - only for image formats */}
                        {exportFormat !== "pdf" && (
                          <Stack spacing={1.5}>
                            <Selector
                              label={t("atlas_file_name_template")}
                              tooltip={t("atlas_file_name_template_help")}
                              selectedItems={selectedFileNameTemplateItem}
                              setSelectedItems={handleFileNameTemplateSelect}
                              items={fileNameTemplateItems}
                              disabled={!selectedReport || isSaving}
                            />
                            {isCustomTemplate && (
                              <TextField
                                size="small"
                                value={customTemplateInput}
                                onChange={(e) => setCustomTemplateInput(e.target.value)}
                                onBlur={handleCustomTemplateBlur}
                                placeholder="{{@layout_name}}_{{@page_number}}"
                                disabled={!selectedReport || isSaving}
                                error={!!templateValidationError}
                                helperText={templateValidationError}
                                fullWidth
                              />
                            )}
                            {templatePreview && (
                              <Typography variant="caption" color="text.secondary" sx={{ fontStyle: "italic" }}>
                                {t("example")}: {templatePreview}
                              </Typography>
                            )}
                          </Stack>
                        )}

                      </>
                    )}

                    {/* Page count info */}
                    {atlasEnabled && atlasTotalPages > 0 && (
                      <Typography variant="body2" color="text.secondary">
                        {t("atlas_pages_count", { count: atlasTotalPages })}
                      </Typography>
                    )}

                    {/* Truncation warning + filter hint */}
                    {atlasWasTruncated && (
                      <Alert severity="warning" sx={{ py: 0, "& .MuiAlert-message": { py: 0.5 } }}>
                        <Typography variant="caption">
                          {t("atlas_page_limit_warning", {
                            max: atlasMaxPages,
                            count: atlasTotalFeatureCount,
                          })}
                          {" "}
                          {t("atlas_filter_hint")}
                        </Typography>
                      </Alert>
                    )}
                  </Stack>
                }
              />
            </Box>
          </Stack>
        </Box>
      </Box>

      {/* Action Buttons - Fixed at bottom */}
      <Box
        sx={{
          px: 2,
          py: 3,
          borderTop: 1,
          borderColor: "divider",
          flexShrink: 0,
          backgroundColor: "background.default",
        }}>
        <Stack spacing={1.5}>
          {/* Print Button (server-side PDF generation) */}
          <Button
            variant="contained"
            fullWidth
            onClick={handlePrintReport}
            disabled={!selectedReport || isPrinting}
            startIcon={
              isPrinting ? (
                <CircularProgress size={16} color="inherit" />
              ) : (
                <Icon iconName={ICON_NAME.PRINT} style={{ fontSize: "16px" }} />
              )
            }
            sx={{
              textTransform: "none",
              py: 1.5,
            }}>
            {isPrinting ? t("printing") : t("print_layout")}
          </Button>
        </Stack>
      </Box>

      {/* Delete Confirmation Modal */}
      {deleteModalOpen && (
        <ConfirmModal
          open={deleteModalOpen}
          title={t("delete_layout")}
          body={
            <Trans
              i18nKey="common:delete_layout_confirmation"
              values={{ name: actionLayoutName }}
              components={{ b: <b /> }}
            />
          }
          onClose={() => {
            setDeleteModalOpen(false);
            setActionLayoutId(null);
            setActionLayoutName("");
          }}
          closeText={t("cancel")}
          confirmText={t("delete")}
          onConfirm={async () => {
            if (actionLayoutId) {
              await handleDeleteReport(actionLayoutId);
            }
            setDeleteModalOpen(false);
            setActionLayoutId(null);
            setActionLayoutName("");
          }}
        />
      )}

      {/* Rename Modal */}
      <ReportLayoutRenameModal
        open={renameModalOpen}
        layoutName={actionLayoutName}
        onClose={() => {
          setRenameModalOpen(false);
          setActionLayoutId(null);
          setActionLayoutName("");
        }}
        onRename={handleRenameReport}
      />

      {/* Template Picker Modal */}
      <ReportTemplatePickerModal
        open={templatePickerOpen}
        onClose={() => setTemplatePickerOpen(false)}
        onSelectTemplate={handleSelectTemplate}
        initialViewState={initialView}
      />
    </PanelContainer>
  );
};

export default ReportsConfigPanel;
