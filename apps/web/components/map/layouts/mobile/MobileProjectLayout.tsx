import { Global } from "@emotion/react";
import type { Theme } from "@mui/material";
import { Box, IconButton, Stack, SwipeableDrawer, Typography, alpha, styled, useTheme } from "@mui/material";
import React, { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import "swiper/css";
import "swiper/css/pagination";
import { Pagination } from "swiper/modules";
import { Swiper, SwiperSlide } from "swiper/react";
import { v4 } from "uuid";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { MAPBOX_TOKEN } from "@/lib/constants";
import { DEFAULT_BASEMAP } from "@/lib/constants/basemaps";
import { setActiveRightPanel, setGeocoderResult } from "@/lib/store/map/slice";
import type { PopupProperties } from "@/lib/validations/layer";
import type { CustomBasemap } from "@/lib/validations/project";
import { projectSchema } from "@/lib/validations/project";

import { MapSidebarItemID } from "@/types/map/common";

import { useLayerStyleChange } from "@/hooks/map/LayerStyleHooks";
import { useBasemap } from "@/hooks/map/MapHooks";
import { useCustomBasemapMutations } from "@/hooks/map/useCustomBasemapMutations";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import WidgetWrapper from "@/components/builder/widgets/WidgetWrapper";
import { ProjectInfo } from "@/components/builder/widgets/information/ProjectInfo";
import Header from "@/components/header/Header";
import AttributionControl from "@/components/map/controls/Attribution";
import { BaseMapSelectorList, BasemapSelectorButton } from "@/components/map/controls/BasemapSelector";
import { CustomBasemapDialog } from "@/components/map/controls/CustomBasemapDialog";
import Geocoder from "@/components/map/controls/Geocoder";
import Scalebar from "@/components/map/controls/Scalebar";
import { UserLocation } from "@/components/map/controls/UserLocation";
import { Zoom } from "@/components/map/controls/Zoom";
import type { PublicProjectLayoutProps } from "@/components/map/layouts/desktop/PublicProjectLayout";
import PropertiesPanel from "@/components/map/panels/properties/Properties";
import { buildLayerIcon } from "@/components/map/panels/layer/legend/LayerIcon";
import { seedPopupFromInteraction } from "@/components/map/panels/style/popup/seedFromLegacy";
import SimpleLayerStyle from "@/components/map/panels/style/SimpleLayerStyle";
import { PopupContent } from "@/components/map/popover/MapFeaturePopover";

import "@/styles/swiper.css";

// --- Constants ---
const drawerBleeding = 56;

// --- Types ---
type DrawerView = "default" | "layerInfo" | "basemapSelector" | "layerSettings";

// --- GlobalSwiperStyles (Unchanged) ---
const GlobalSwiperStyles = ({ open, drawerView }: { open: boolean; drawerView: DrawerView }) => (
  <Global
    styles={(theme: Theme) => ({
      "#swiper-pagination-container": {
        position: "absolute",
        top: "50%", // Centered vertically within the puller area
        transform: "translateY(-50%)",
        left: 0,
        right: 0,
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
        zIndex: 1, // Below Puller, above drawer content
        pointerEvents: "auto", // Allow clicking bullets
        transition: "opacity 0.3s ease-in-out, visibility 0.3s ease-in-out",
        // Show only when drawer is open AND in default view
        opacity: open && drawerView === "default" ? 1 : 0,
        visibility: open && drawerView === "default" ? "visible" : "hidden",
        ".swiper-pagination-bullet": {
          backgroundColor: theme.palette.mode === "dark" ? theme.palette.grey[400] : theme.palette.grey[600],
          opacity: 0.7,
          margin: "0 4px !important",
        },
        ".swiper-pagination-bullet-active": {
          backgroundColor: theme.palette.primary.main,
          opacity: 1,
        },
      },
      ".MuiDrawer-root > .MuiPaper-root": {
        // Adjusted height calculation if needed, but 60% seems reasonable
        height: `calc(60% - ${drawerBleeding}px)`,
        overflow: "visible",
      },
    })}
  />
);

// --- Puller (Unchanged) ---
const Puller = styled("div")(({ theme }) => ({
  width: 30,
  height: 6,
  backgroundColor: theme.palette.grey[300],
  borderRadius: 3,
  position: "absolute",
  top: 8,
  left: "calc(50% - 15px)",
  zIndex: 2, // Ensure puller is above pagination
  ...theme.applyStyles("dark", {
    backgroundColor: theme.palette.grey[700],
  }),
}));

// --- InfoHeader ---
interface InfoHeaderProps {
  title: string;
  onClose: () => void;
  iconName?: ICON_NAME;
  /**
   * Optional custom icon node — e.g. a `<LayerIcon>` built from the
   * layer's style so the feature popup's header matches what the
   * Layers panel shows. Takes precedence over `iconName`.
   */
  icon?: React.ReactNode;
}
export const InfoHeader: React.FC<InfoHeaderProps> = ({ title, iconName, icon, onClose }) => {
  return (
    <Stack sx={{ pl: 4, pr: 2, pt: 2 }} direction="row" alignItems="center" justifyContent="space-between">
      <Stack direction="row" spacing={2} alignItems="center" sx={{ width: "90%" }}>
        {icon ? icon : iconName && <Icon iconName={iconName} fontSize="small" />}
        <Typography variant="body1" fontWeight="bold" noWrap>
          {title}
        </Typography>
      </Stack>
      <IconButton onClick={onClose} sx={{ pointerEvents: "all" }}>
        <Icon iconName={ICON_NAME.XCLOSE} />
      </IconButton>
    </Stack>
  );
};

// --- MobileProjectLayout component ---
const MobileProjectLayout = ({
  projectLayers = [],
  projectLayerGroups = [],
  project: _project,
  onProjectUpdate,
  viewOnly,
}: PublicProjectLayoutProps) => {
  const { t, i18n } = useTranslation("common");
  const theme = useTheme();
  const dispatch = useAppDispatch();

  // Layer style change hook
  const { handleStyleChange } = useLayerStyleChange(projectLayers, viewOnly);
  const project = useMemo(() => {
    const parsedProject = projectSchema.safeParse(_project);
    if (parsedProject.success) {
      return parsedProject.data;
    } else {
      console.error("Invalid project data:", parsedProject.error);
      return undefined;
    }
  }, [_project]);

  // --- State ---
  const [open, setOpen] = useState(false); // Drawer open/closed state
  const [drawerView, setDrawerView] = useState<DrawerView>("default"); // Explicit view state
  // Snapshot of the drawer's open state taken when the user enters a
  // non-default view (layerInfo / basemapSelector / layerSettings).
  // `handleCloseView` restores this so closing a special view returns
  // the drawer to its prior state instead of forcing it to stay open
  // on the default view.
  const openBeforeSpecialViewRef = useRef<boolean>(false);

  // --- Redux State ---
  const layerInfo = useAppSelector((state) => state.map.popupInfo);
  const highlightedFeature = useAppSelector((state) => state.map.highlightedFeature);
  const selectedLayerIds = useAppSelector((state) => state.layers.selectedLayerIds || []);
  const activeRightPanel = useAppSelector((state) => state.map.activeRightPanel);

  // Layer settings logic for mobile
  const activeLayer = useMemo(() => {
    if (selectedLayerIds.length === 1) {
      return projectLayers.find((l) => l.id === selectedLayerIds[0]);
    }
    return null;
  }, [selectedLayerIds, projectLayers]);

  // Resolve the popup config + layer for the currently-clicked feature.
  // Mirrors the resolution used by `MapViewer` / `MapFixedPopupSlot`:
  //  - prefer the new `popup` block on the layer
  //  - fall back to a default seeded from the legacy `interaction`
  // Layers without either still get a sensible "show all fields"
  // popup courtesy of seedPopupFromInteraction.
  const clickedPopupLayer = useMemo(() => {
    if (!layerInfo?.layerId) return undefined;
    const target = layerInfo.layerId;
    return projectLayers.find(
      (l) =>
        (l as { layer_id?: string }).layer_id === target ||
        l.id.toString() === target,
    );
  }, [layerInfo?.layerId, projectLayers]);
  const activePopupConfig = useMemo<PopupProperties | undefined>(() => {
    if (!clickedPopupLayer) return undefined;
    const props = clickedPopupLayer.properties as
      | {
          popup?: PopupProperties;
          interaction?: { type?: string; content?: never[] };
        }
      | undefined;
    if (props?.popup) return props.popup;
    return seedPopupFromInteraction(props?.interaction);
  }, [clickedPopupLayer]);
  // Layer-style icon for the popup header (same look as the Layers panel
  // and the desktop popup).
  const clickedPopupLayerIcon = useMemo(
    () => buildLayerIcon(clickedPopupLayer),
    [clickedPopupLayer],
  );

  // --- Hooks ---
  const { translatedBaseMaps, activeBasemap, setActiveBasemap } = useBasemap(project);
  // Ephemeral preview of a non-active basemap while its edit dialog is open;
  // restored on close (never persisted).
  const basemapBeforeEdit = useRef<string | null>(null);
  const mapMode = useAppSelector((state) => state.map.mapMode);
  const editable = mapMode !== "public";
  const { addCustomBasemap, editCustomBasemap, deleteCustomBasemap } =
    useCustomBasemapMutations(project, onProjectUpdate);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editing, setEditing] = useState<CustomBasemap | null>(null);
  const scrollableContentRef = useRef<HTMLDivElement | null>(null);

  // --- Panel appearance (mobile) ---
  // The bottom drawer is a single shared surface that pages through the
  // builder panels via Swiper. Mirror the desktop PanelContainer so the
  // configured panel background color (e.g. a branded yellow) is honored
  // on mobile too, instead of always falling back to background.paper.
  const panels = useMemo(
    () =>
      (project?.builder_config?.interface ?? []).filter(
        (item) => item.type === "panel" && (item?.widgets?.length ?? 0) > 0
      ),
    [project?.builder_config?.interface]
  );
  const [activeSlideIndex, setActiveSlideIndex] = useState(0);
  const getPanelBgColor = (panel: (typeof panels)[number] | undefined) =>
    alpha(
      panel?.config?.appearance?.backgroundColor || theme.palette.background.paper,
      panel?.config?.appearance?.opacity ?? 1
    );
  // Color for the shared drawer chrome (puller + content area). Use the
  // active panel's color while showing panels; fall back to the theme
  // surface for utility views (basemap selector, layer settings, popup).
  const drawerBgColor =
    drawerView === "default"
      ? getPanelBgColor(panels[activeSlideIndex] ?? panels[0])
      : theme.palette.background.paper;

  // --- Effects ---
  // Effect to handle changes in layerInfo (feature selection/deselection)
  useEffect(() => {
    if (layerInfo) {
      // Clear activeRightPanel when switching to layerInfo to prevent conflicts
      if (activeRightPanel) {
        dispatch(setActiveRightPanel(undefined));
      }

      // Snapshot the drawer state on the first transition from
      // default into any special view, so we can restore it on close.
      // Don't overwrite mid-flight (e.g. layerInfo → basemap → close).
      if (drawerView === "default") {
        openBeforeSpecialViewRef.current = open;
      }
      // When a feature is selected, switch to layerInfo view and open drawer
      setDrawerView("layerInfo");
      setOpen(true);
    } else {
      // When a feature is deselected externally (e.g. user clicks
      // empty map area → handlePopoverClose), drop back to default
      // and restore the pre-view open state.
      if (drawerView === "layerInfo") {
        setDrawerView("default");
        setOpen(openBeforeSpecialViewRef.current);
      }
    }
    // Only react to changes in layerInfo itself
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [layerInfo]);

  // Effect to handle layer settings panel requests from ProjectLayerTree three dots menu
  useEffect(() => {
    if (activeRightPanel && activeLayer) {
      // Clear any existing layer info selection
      if (layerInfo) {
        layerInfo.onClose();
      }

      if (activeRightPanel === MapSidebarItemID.PROPERTIES || activeRightPanel === MapSidebarItemID.STYLE) {
        if (drawerView === "default") {
          openBeforeSpecialViewRef.current = open;
        }
        setDrawerView("layerSettings");
        setOpen(true);
      }
    }
  }, [activeRightPanel, activeLayer, layerInfo]);

  useEffect(() => {
    const isLayerSettingsPanel =
      activeRightPanel === MapSidebarItemID.PROPERTIES || activeRightPanel === MapSidebarItemID.STYLE;

    if (drawerView === "layerSettings" && (!activeLayer || !isLayerSettingsPanel)) {
      if (activeRightPanel) {
        dispatch(setActiveRightPanel(undefined));
      }
      setDrawerView("default");
      setOpen(openBeforeSpecialViewRef.current);
    }
  }, [drawerView, activeLayer, activeRightPanel, dispatch]);

  // --- Visibility Helper ---
  const hasControl = (key: string): boolean => {
    const positions = project?.builder_config?.settings?.control_positions;
    if (!positions) return false;
    return Object.values(positions).some((arr) => (arr as string[]).includes(key));
  };

  // --- Handlers ---
  const toggleDrawer = (newOpen: boolean) => () => {
    setOpen(newOpen);
  };

  // Handles clicks on the close ('X') button in the InfoHeader
  const handleCloseView = () => {
    // Always clear map state if layerInfo was active
    if (drawerView === "layerInfo" && layerInfo) {
      layerInfo.onClose(); // This should set layerInfo to null via Redux
    }

    // Clear activeRightPanel when closing any view to ensure clean state
    if (activeRightPanel) {
      dispatch(setActiveRightPanel(undefined));
    }

    // Reset the view to default and restore the drawer's pre-view
    // open state. If the user tapped a feature / opened basemap from
    // a closed drawer, the drawer collapses again now; if the default
    // view was already up, it stays up.
    setDrawerView("default");
    setOpen(openBeforeSpecialViewRef.current);
  };

  // Handles clicks on the BasemapSelectorButton
  const handleOpenBasemapSelector = () => {
    // Clear any existing layer info selection first
    if (layerInfo) {
      layerInfo.onClose();
    }

    // Clear activeRightPanel to prevent conflicts with layer settings
    if (activeRightPanel) {
      dispatch(setActiveRightPanel(undefined));
    }

    if (drawerView === "default") {
      openBeforeSpecialViewRef.current = open;
    }
    setDrawerView("basemapSelector");
    setOpen(true);
  };

  // Stops propagation for internal scrolling
  const handleContentTouchMove = (event: React.TouchEvent) => {
    event.stopPropagation();
  };

  // --- Derived State ---
  // Determine the title and icon for the current view in the header area
  const currentHeaderInfo = useMemo(() => {
    switch (drawerView) {
      case "layerInfo":
        return layerInfo ? { title: layerInfo.title, icon: ICON_NAME.LAYERS } : null;
      case "basemapSelector":
        return { title: t("map_style"), icon: ICON_NAME.MAP };
      case "layerSettings":
        if (activeLayer && activeRightPanel) {
          const isProperties = activeRightPanel === MapSidebarItemID.PROPERTIES;
          const title = isProperties
            ? `${t("data_source_info")}: ${activeLayer.name || t("layer")}`
            : `${t("style")}: ${activeLayer.name || t("layer")}`;
          const icon = isProperties ? ICON_NAME.LAYERS : ICON_NAME.STYLE;
          return { title, icon };
        }
        return null;
      case "default":
      default:
        return null; // No header needed for default view (only pagination)
    }
  }, [drawerView, layerInfo, activeLayer, activeRightPanel, t]);

  return (
    <>
      {/* Pass drawerView to GlobalSwiperStyles */}
      <GlobalSwiperStyles open={open} drawerView={drawerView} />
      <Box sx={{ height: `calc(100% - 56px)`, width: "100%", display: "flex", flexDirection: "column" }}>
        {/* Header and Map Controls (Unchanged structure) */}
        {project?.builder_config?.settings?.toolbar && (
          <Header showHambugerMenu={false} mapHeader={true} project={project} viewOnly />
        )}
        <Box
          display="flex"
          sx={{
            zIndex: 1,
            position: "relative",
            height: "100%",
            flexGrow: 1,
            justifyContent: "space-between",
            pointerEvents: "none",
          }}>
          {hasControl("location") && (
            <Box
              sx={{
                position: "absolute",
                left: 0,
                top: 0,
                m: 2,
                zIndex: 2,
                pointerEvents: "all",
              }}>
              <Geocoder
                accessToken={MAPBOX_TOKEN}
                bbox={project?.max_extent ?? undefined}
                language={i18n.language}
                placeholder={t("enter_an_address")}
                tooltip={t("search")}
                onSelect={(result) => {
                  dispatch(setGeocoderResult(result));
                }}
              />
            </Box>
          )}
          {/* Top-Right Controls (Unchanged) */}
          <Box
            sx={{
              position: "absolute",
              right: 0,
              top: 0,
              m: 2,
              zIndex: 2,
              pointerEvents: "all",
              display: "flex",
              flexDirection: "column",
              gap: 1,
            }}>
            {project && hasControl("project_info") && (
              <ProjectInfo project={project} viewOnly={viewOnly} onProjectUpdate={onProjectUpdate} />
            )}
            {hasControl("zoom_controls") && (
              <Zoom tooltipZoomIn={t("zoom_in")} tooltipZoomOut={t("zoom_out")} />
            )}
            {hasControl("find_my_location") && (
              <UserLocation tooltip={t("find_location")} />
            )}
            {hasControl("basemap") && (
              <BasemapSelectorButton
                // Use `open` state derived from drawerView for visual indication (optional)
                open={drawerView === "basemapSelector"}
                // Use specific handler to switch view
                setOpen={handleOpenBasemapSelector}
              />
            )}
          </Box>

          {/* Bottom-Left Controls */}
          <Box
            sx={{
              position: "absolute",
              left: 0,
              zIndex: 2,
              bottom: 0,
              pointerEvents: "all",
            }}>
            {project?.builder_config?.settings?.scalebar !== false && (
              <Box sx={{ m: 2 }}>
                <Scalebar />
              </Box>
            )}
            <AttributionControl
              extraAttribution={
                activeBasemap.source === "custom" ? activeBasemap.attribution : null
              }
            />
          </Box>
        </Box>

        {/* Drawer */}
        <SwipeableDrawer
          anchor="bottom"
          open={open}
          hideBackdrop={true}
          ModalProps={{
            sx: {
              pointerEvents: "none", // Allow interactions with map behind
              "& .MuiDialog-container": { pointerEvents: "none" },
              "& .MuiPaper-root": { pointerEvents: "auto" }, // Drawer itself is interactive
            },
            keepMounted: true, // Keep content mounted for transitions/state
          }}
          onClose={toggleDrawer(false)}
          onOpen={toggleDrawer(true)}
          swipeAreaWidth={drawerBleeding}
          disableSwipeToOpen={false}
          disableDiscovery={false}>
          {/* Puller Area */}
          <Box
            sx={{
              position: "absolute",
              top: -drawerBleeding,
              borderTopLeftRadius: 8,
              borderTopRightRadius: 8,
              visibility: "visible",
              right: 0,
              left: 0,
              height: drawerBleeding,
              cursor: "grab",
              backgroundColor: drawerBgColor,
              boxShadow: "0 -3px 6px -2px rgba(0,0,0,0.15)",
            }}>
            <Puller />
            {/* Dynamic Header Content */}
            <Box sx={{ pt: currentHeaderInfo ? 1 : 0 }}>
              {currentHeaderInfo && (
                <InfoHeader
                  title={currentHeaderInfo.title}
                  // For the feature popup view, pass the layer's own
                  // style icon (same affordance as the desktop popup
                  // and the Layers panel) instead of the generic
                  // ICON_NAME so users can recognize the layer at a
                  // glance. Other drawer views keep using `iconName`.
                  {...(drawerView === "layerInfo" && clickedPopupLayerIcon
                    ? { icon: clickedPopupLayerIcon }
                    : { iconName: currentHeaderInfo.icon })}
                  onClose={handleCloseView}
                />
              )}
              {/* Pagination container, visibility controlled by GlobalSwiperStyles */}
              <div id="swiper-pagination-container" />
            </Box>
          </Box>

          {/* Content Area */}
          <Box
            sx={{
              height: "100%",
              overflow: "auto", // Hide overflow, specific content scrolls internally
              borderTopLeftRadius: 8,
              borderTopRightRadius: 8,
              backgroundColor: drawerBgColor,
            }}
            // Attach touch move handler to prevent drawer swipe when scrolling content
            onTouchMove={handleContentTouchMove}>
            {/* Conditional Rendering based on drawerView */}
            {drawerView === "layerInfo" && layerInfo && activePopupConfig && (
              <PopupContent
                // Force re-mount when the clicked feature changes so the
                // block list refreshes against the new feature properties.
                key={highlightedFeature?.id ?? layerInfo.title ?? v4()}
                layerId={layerInfo.layerId ?? ""}
                popup={activePopupConfig}
                properties={
                  (layerInfo.featureProperties ??
                    highlightedFeature?.properties ??
                    layerInfo.properties ??
                    {}) as Record<string, unknown>
                }
                // The drawer already owns its scrollable content area,
                // so neutralize PopupContent's internal scroll/maxHeight
                // affordance — the surrounding drawer body provides it.
                sx={{ overflowY: "visible", maxHeight: "none", px: 1.5, py: 1 }}
              />
            )}

            {drawerView === "basemapSelector" && (
              <BaseMapSelectorList
                styles={translatedBaseMaps}
                active={activeBasemap.value}
                editable={editable}
                basemapChange={async (basemap) => {
                  await onProjectUpdate?.("basemap", basemap);
                  // Optionally close drawer or switch back to default view after selection
                  // setOpen(false);
                  // setDrawerView('default');
                }}
                onAdd={() => {
                  setEditing(null);
                  setDialogOpen(true);
                }}
                onEdit={(id) => {
                  const target =
                    (project?.custom_basemaps as CustomBasemap[] | undefined)?.find(
                      (c) => c.id === id
                    ) ?? null;
                  if (target && project?.basemap !== id) {
                    // Ephemeral preview only — reverted on close, never persisted.
                    basemapBeforeEdit.current = project?.basemap ?? DEFAULT_BASEMAP;
                    setActiveBasemap(id);
                  }
                  setEditing(target);
                  setDialogOpen(true);
                }}
                // Close drawer on item click for immediate map feedback
                onClick={() => setOpen(false)}
                hideHeader // Header is now in the puller area
              />
            )}

            {drawerView === "layerSettings" && activeLayer && activeRightPanel && (
              <Box
                sx={{
                  p: 3,
                  minHeight: "200px",
                  display: "flex",
                  flexDirection: "column",
                  gap: 2,
                }}>
                {activeRightPanel === MapSidebarItemID.PROPERTIES && (
                  <PropertiesPanel activeLayer={activeLayer} />
                )}
                {activeRightPanel === MapSidebarItemID.STYLE && (
                  <SimpleLayerStyle activeLayer={activeLayer} onStyleChange={handleStyleChange} />
                )}
              </Box>
            )}

            {drawerView === "default" && project?.builder_config?.interface && (
              <Swiper
                pagination={{
                  clickable: true,
                  el: "#swiper-pagination-container", // Target the div in puller area
                }}
                modules={[Pagination]}
                style={{ height: "100%", width: "100%" }}
                // Ensure Swiper doesn't conflict with drawer swipe
                touchStartPreventDefault={false} // Let drawer handle swipe if needed
                // Track the active panel so the shared drawer chrome
                // (puller + content area) can match its background color.
                onSlideChange={(swiper: { activeIndex: number }) =>
                  setActiveSlideIndex(swiper.activeIndex)
                }>
                {panels.map((item, index) => (
                  <SwiperSlide key={index} style={{ height: "100%", boxSizing: "border-box" }}>
                    {/* Scrollable container for widgets */}
                    <Box
                      ref={scrollableContentRef}
                      sx={{
                        display: "flex",
                        flexDirection: "column",
                        height: "100%",
                        width: "100%",
                        // Each slide carries its own configured panel color so
                        // adjacent panels render correctly mid-swipe.
                        backgroundColor: getPanelBgColor(item),
                        overflowY: "auto", // Enable vertical scrolling for widgets
                        overflowX: "hidden",
                        boxSizing: "border-box",
                        pb: 4, // Padding at the bottom
                        touchAction: "pan-y", // Hint for browser scroll handling
                      }}
                      // No touch handlers needed here anymore, handled by parent Box
                    >
                      {item?.widgets?.map((widget) => (
                        <Box key={widget.id} sx={{ p: 2, width: "100%", flexShrink: 0 }}>
                          <WidgetWrapper
                            widget={widget}
                            projectLayers={projectLayers}
                            projectLayerGroups={projectLayerGroups}
                            viewOnly
                          />
                        </Box>
                      ))}
                    </Box>
                  </SwiperSlide>
                ))}
              </Swiper>
            )}
          </Box>
        </SwipeableDrawer>
      </Box>
      <CustomBasemapDialog
        open={dialogOpen}
        initial={editing}
        projectLayers={projectLayers}
        onClose={() => {
          setDialogOpen(false);
          if (basemapBeforeEdit.current !== null) {
            setActiveBasemap(basemapBeforeEdit.current);
            basemapBeforeEdit.current = null;
          }
        }}
        onSubmit={async (payload) => {
          if (editing) {
            await editCustomBasemap(editing.id, payload);
          } else {
            await addCustomBasemap(payload, /* selectAfterAdd */ true);
          }
        }}
        onDelete={
          editing
            ? async () => {
                await deleteCustomBasemap(editing.id, DEFAULT_BASEMAP);
              }
            : undefined
        }
      />
    </>
  );
};

export default MobileProjectLayout;
