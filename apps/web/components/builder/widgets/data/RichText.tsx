import { Box, Divider, Paper, Portal, Stack, Typography, styled } from "@mui/material";
import { debounce } from "@mui/material/utils";
import Color from "@tiptap/extension-color";
import FontFamily from "@tiptap/extension-font-family";
import Link from "@tiptap/extension-link";
import Subscript from "@tiptap/extension-subscript";
import Superscript from "@tiptap/extension-superscript";
import TextAlign from "@tiptap/extension-text-align";
import { TextStyle } from "@tiptap/extension-text-style";
import type { Editor } from "@tiptap/react";
import { useEditor, useEditorState } from "@tiptap/react";
import StarterKit from "@tiptap/starter-kit";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useMap } from "react-map-gl/maplibre";
import { useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { useProjectLayerAggregationStats } from "@/lib/api/projects";
import InfoChip from "@/lib/extensions/info-chip";
import VariableChip from "@/lib/extensions/variable-chip";
import FontSize from "@/lib/extensions/font-size";
import LineHeight from "@/lib/extensions/line-height";
import { formatNumber } from "@/lib/utils/format-number";
import { getMapExtentCQL } from "@/lib/utils/map/navigate";
import type { AggregationStatsQueryParams, ProjectLayer } from "@/lib/validations/project";
import type { RichTextDataSchema, RichTextVariableSchema } from "@/lib/validations/widget";

import { useTemporaryFilters } from "@/hooks/map/DashboardBuilderHooks";
import { useAppSelector } from "@/hooks/store/ContextHooks";


import { TipTapEditorContent } from "@/components/builder/widgets/elements/text/Text";
import { AlignSelect } from "@/components/builder/widgets/elements/text/AlignSelect";
import LineHeightSelect from "@/components/builder/widgets/elements/text/LineHeightSelect";
import MenuButton from "@/components/builder/widgets/elements/text/MenuButton";
import { InfoChipEditPopover, InfoChipViewPopover } from "@/components/builder/widgets/data/InfoChipPopover";
import LinkPopover from "@/components/builder/widgets/data/LinkPopover";
import RichTextFontSizeSelect from "@/components/builder/widgets/data/RichTextFontSizeSelect";
import VariableInsertMenu from "@/components/builder/widgets/data/VariableInsertMenu";

const extensions = [
  StarterKit,
  Subscript,
  Superscript,
  TextStyle,
  Color,
  FontFamily,
  FontSize,
  LineHeight,
  TextAlign.configure({
    types: ["heading", "paragraph"],
  }),
  Link.configure({
    openOnClick: false,
    HTMLAttributes: { target: "_blank", rel: "noopener noreferrer" },
  }),
  VariableChip,
  InfoChip,
];

const ToolbarContainer = styled(Paper)(({ theme }) => ({
  display: "flex",
  alignItems: "center",
  padding: theme.spacing(2),
  borderRadius: theme.shape.borderRadius * 2,
  boxShadow: theme.shadows[4],
  backgroundColor: theme.palette.background.paper,
}));

/**
 * Styled wrapper for variable chips in the editor (edit mode).
 */
const RichTextEditorContent = styled(TipTapEditorContent)(({ theme }) => ({
  "& .ProseMirror .variable-chip": {
    display: "inline-flex",
    alignItems: "center",
    backgroundColor: theme.palette.action.selected,
    color: theme.palette.primary.main,
    borderRadius: theme.shape.borderRadius,
    padding: "0 6px",
    fontFamily: "monospace",
    fontSize: "0.85em",
    fontWeight: "normal",
    fontStyle: "normal",
    lineHeight: 1.6,
    whiteSpace: "nowrap",
    userSelect: "none",
    cursor: "default",
  },
  "& .ProseMirror .info-chip": {
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    width: 14,
    height: 14,
    borderRadius: "50%",
    border: `1.5px solid ${theme.palette.grey[500]}`,
    backgroundColor: "transparent",
    color: theme.palette.grey[500],
    fontSize: 9,
    fontWeight: 700,
    fontStyle: "normal",
    fontFamily: "serif",
    lineHeight: 1,
    cursor: "pointer",
    userSelect: "none",
    verticalAlign: "middle",
    marginInline: "2px",
    boxSizing: "border-box",
  },
  "& .ProseMirror a": {
    color: theme.palette.primary.main,
    textDecoration: "underline",
    cursor: "pointer",
  },
}));

/**
 * Replace variable chip spans with styled resolved values.
 * Style attributes (bold, italic, font-size) are read from the chip's data attributes
 * which are set directly in the TipTap editor via the toolbar.
 */
function resolveVariablesInHtml(
  html: string,
  variables: RichTextVariableSchema[],
  resolvedValues: Record<string, string | number | null>
): string {
  return html.replace(
    /<span[^>]*data-variable="([^"]*)"[^>]*>[\s\S]*?<\/span>/g,
    (match, variableName: string) => {
      const variable = variables.find((v) => v.name === variableName);
      const value = resolvedValues[variableName];
      const text = value !== undefined && value !== null ? String(value) : variable ? "..." : "???";

      // Extract style attributes from the chip's HTML
      const styles: string[] = [];
      if (match.includes('data-bold="true"')) styles.push("font-weight:700");
      if (match.includes('data-italic="true"')) styles.push("font-style:italic");
      const fontSizeMatch = match.match(/data-font-size="([^"]*)"/);
      if (fontSizeMatch) styles.push(`font-size:${fontSizeMatch[1]}`);
      // Carry over any existing inline style from the chip
      const styleMatch = match.match(/style="([^"]*)"/);
      if (styleMatch) styles.push(styleMatch[1]);

      if (styles.length === 0) return text;
      return `<span style="${styles.join(";")}">${text}</span>`;
    }
  );
}

/**
 * Preview renderer that makes info chips and links interactive.
 */
const RichTextPreview = ({ html }: { html: string }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const [viewPopover, setViewPopover] = useState<{
    anchorEl: HTMLElement;
    text: string;
    url?: string;
  } | null>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const handleClick = (e: MouseEvent) => {
      const target = e.target as HTMLElement;
      // Handle info chip clicks
      const chip = target.closest(".info-chip") as HTMLElement | null;
      if (chip) {
        e.preventDefault();
        e.stopPropagation();
        const text = chip.getAttribute("data-info-text") || "";
        const url = chip.getAttribute("data-info-url") || undefined;
        setViewPopover({ anchorEl: chip, text, url });
        return;
      }
      // Handle link clicks — open in new tab
      const link = target.closest("a") as HTMLAnchorElement | null;
      if (link?.href) {
        e.preventDefault();
        e.stopPropagation();
        window.open(link.href, "_blank", "noopener,noreferrer");
      }
    };

    el.addEventListener("click", handleClick);
    return () => el.removeEventListener("click", handleClick);
  }, []);

  return (
    <>
      <Box
        ref={containerRef}
        sx={{
          height: "100%",
          fontFamily: "inherit",
          "& p, & h1, & h2, & h3, & h4, & h5, & h6": { margin: 0 },
          "& a": { color: "primary.main", textDecoration: "underline" },
          "& .info-chip": {
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            width: 14,
            height: 14,
            borderRadius: "50%",
            border: "1.5px solid",
            borderColor: "grey.500",
            backgroundColor: "transparent",
            color: "grey.500",
            fontSize: 9,
            fontWeight: 700,
            fontStyle: "normal",
            fontFamily: "serif",
            lineHeight: 1,
            cursor: "pointer",
            verticalAlign: "middle",
            mx: "2px",
            boxSizing: "border-box",
          },
        }}
        dangerouslySetInnerHTML={{ __html: html || "<p></p>" }}
      />
      {viewPopover && (
        <InfoChipViewPopover
          anchorEl={viewPopover.anchorEl}
          text={viewPopover.text}
          url={viewPopover.url}
          onClose={() => setViewPopover(null)}
        />
      )}
    </>
  );
};

/**
 * Wrapper that resolves variables then renders via RichTextPreview (with interactive info chips/links).
 */
const RichTextPreviewResolved = ({
  config,
  resolvedValues,
}: {
  config: RichTextDataSchema;
  resolvedValues: Record<string, string | number | null>;
}) => {
  const variables = config.setup?.variables ?? [];
  const resolvedHtml = useMemo(
    () => resolveVariablesInHtml(config.setup?.text || "", variables, resolvedValues),
    [config.setup?.text, variables, resolvedValues]
  );
  return <RichTextPreview html={resolvedHtml} />;
};

interface RichTextDataWidgetProps {
  config: RichTextDataSchema;
  projectLayers: ProjectLayer[];
  viewOnly?: boolean;
  onConfigChange?: (nextConfig: RichTextDataSchema) => void;
}


/* ------------------------------------------------------------------ */
/*  Editable rich-text editor                                          */
/* ------------------------------------------------------------------ */

const RichTextEditable = ({
  config,
  onConfigChange,
  resolvedValues,
}: {
  config: RichTextDataSchema;
  onConfigChange?: (nextConfig: RichTextDataSchema) => void;
  resolvedValues: Record<string, string | number | null>;
}) => {
  const [isEditMode, setIsEditMode] = useState(false);
  const mouseDownPos = useRef<{ x: number; y: number } | null>(null);

  const variables: RichTextVariableSchema[] = config.setup?.variables ?? [];

  // Resolved HTML for preview (when not in edit mode)
  const resolvedHtml = useMemo(
    () => resolveVariablesInHtml(config.setup?.text || "", variables, resolvedValues),
    [config.setup?.text, variables, resolvedValues]
  );

  const editor = useEditor({
    extensions,
    content: config.setup?.text || "",
    immediatelyRender: true,
    shouldRerenderOnTransaction: false,
    editable: isEditMode,
  });

  // Update editor editable state when isEditMode changes
  useEffect(() => {
    if (editor) {
      editor.setEditable(isEditMode);
    }
  }, [editor, isEditMode]);

  // Sync editor content when config changes externally
  useEffect(() => {
    if (editor && config.setup?.text !== undefined && !isEditMode) {
      const currentContent = editor.getHTML();
      if (currentContent !== config.setup.text) {
        editor.commands.setContent(config.setup.text || "");
      }
    }
  }, [editor, config.setup?.text, isEditMode]);

  const [toolbarOpen, setToolbarOpen] = useState(false);
  const [activeDropdown, setActiveDropdown] = useState<string | null>(null);
  const activeDropdownRef = useRef<string | null>(null);
  const toolbarClickedRef = useRef(false);

  const updateActiveDropdown = useCallback((value: string | null) => {
    setActiveDropdown(value);
    activeDropdownRef.current = value;
  }, []);

  useEffect(() => {
    if (!editor) return;

    const handleFocus = () => setToolbarOpen(true);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const handleBlur = ({ event }: any) => {
      if (event?.relatedTarget && (event.relatedTarget as HTMLElement).closest(".tiptap-toolbar")) return;
      if (activeDropdownRef.current) return;
      if (toolbarClickedRef.current) return;
      setToolbarOpen(false);
      setIsEditMode(false);
    };

    editor.on("focus", handleFocus);
    editor.on("blur", handleBlur);

    return () => {
      editor.off("focus", handleFocus);
      editor.off("blur", handleBlur);
    };
  }, [editor]);

  // Mouse handling to distinguish click from drag
  const handleMouseDown = (e: React.MouseEvent) => {
    mouseDownPos.current = { x: e.clientX, y: e.clientY };
  };

  const handleMouseUp = (e: React.MouseEvent) => {
    if (!mouseDownPos.current) return;

    const dx = e.clientX - mouseDownPos.current.x;
    const dy = e.clientY - mouseDownPos.current.y;
    const distance = Math.sqrt(dx * dx + dy * dy);

    if (distance < 5 && !isEditMode) {
      setIsEditMode(true);
      setTimeout(() => {
        editor?.commands.focus();
      }, 0);
    }

    mouseDownPos.current = null;
  };

  // Popover state for link and info chip editing
  const [linkAnchorEl, setLinkAnchorEl] = useState<HTMLElement | null>(null);
  const [infoChipAnchorEl, setInfoChipAnchorEl] = useState<HTMLElement | null>(null);

  const editorState = useEditorState({
    editor,
    selector: ({ editor }: { editor: Editor }) => {
      const node = editor.state.doc.nodeAt(editor.state.selection.from);
      const isChipSelected = node?.type.name === "variableChip";
      const isInfoChipSelected = node?.type.name === "infoChip";
      return {
        isBold: isChipSelected ? !!node?.attrs.bold : editor.isActive("bold"),
        isItalic: isChipSelected ? !!node?.attrs.italic : editor.isActive("italic"),
        isUnderline: editor.isActive("underline"),
        isStrike: editor.isActive("strike"),
        isBulletList: editor.isActive("bulletList"),
        isOrderedList: editor.isActive("orderedList"),
        isLink: editor.isActive("link"),
        isChipSelected,
        isInfoChipSelected,
      };
    },
  });

  // Open info chip popover when an info chip is selected via click
  useEffect(() => {
    if (!editor || !isEditMode) return;
    const handleClick = (event: MouseEvent) => {
      const target = event.target as HTMLElement;
      const chipEl = target.closest(".info-chip") as HTMLElement | null;
      if (chipEl) {
        // Select the info chip node in the editor
        const pos = editor.view.posAtDOM(chipEl, 0);
        const resolvedPos = editor.state.doc.resolve(pos);
        const node = resolvedPos.parent.type.name === "infoChip" ? resolvedPos.parent : editor.state.doc.nodeAt(pos);
        if (node?.type.name === "infoChip") {
          editor.chain().focus().setNodeSelection(pos).run();
          setInfoChipAnchorEl(chipEl);
        }
      }
    };
    const editorEl = editor.view.dom;
    editorEl.addEventListener("click", handleClick);
    return () => editorEl.removeEventListener("click", handleClick);
  }, [editor, isEditMode]);

  // Debounced config update on editor changes
  const debouncedUpdate = debounce(() => {
    if (editor && onConfigChange) {
      onConfigChange({
        ...config,
        setup: { ...config.setup, text: editor.getHTML() },
      });
    }
  }, 300);

  useEffect(() => {
    if (!editor) return;

    const handleUpdate = () => {
      debouncedUpdate();
    };

    editor.on("update", handleUpdate);

    return () => {
      editor.off("update", handleUpdate);
    };
  }, [editor, debouncedUpdate]);

  // Toolbar positioning
  const containerRef = useRef<HTMLDivElement>(null);
  const toolbarRef = useRef<HTMLDivElement>(null);
  const [toolbarPosition, setToolbarPosition] = useState({ top: 0, left: 0, width: 0 });

  useEffect(() => {
    if (!toolbarOpen || !containerRef.current) return;

    const updatePosition = () => {
      const rect = containerRef.current?.getBoundingClientRect();
      if (rect) {
        const centeredLeft = rect.left + rect.width / 2;
        const toolbarWidth = toolbarRef.current?.offsetWidth ?? 0;
        const halfToolbar = toolbarWidth / 2;
        const padding = 8;
        // Clamp so toolbar stays within viewport
        const clampedLeft = halfToolbar > 0
          ? Math.max(halfToolbar + padding, Math.min(centeredLeft, window.innerWidth - halfToolbar - padding))
          : centeredLeft;
        setToolbarPosition({
          top: rect.top - 8,
          left: clampedLeft,
          width: rect.width,
        });
      }
    };

    updatePosition();

    let animationId: number;
    const animate = () => {
      updatePosition();
      animationId = requestAnimationFrame(animate);
    };
    animationId = requestAnimationFrame(animate);

    return () => cancelAnimationFrame(animationId);
  }, [toolbarOpen]);

  return (
    <Box
      ref={containerRef}
      onMouseDown={handleMouseDown}
      onMouseUp={handleMouseUp}
      sx={{ height: "100%", width: "100%", minHeight: 60, cursor: isEditMode ? "text" : "default" }}>
      {/* Toolbar rendered via Portal to escape overflow:hidden */}
      {toolbarOpen && (
        <Portal>
          <Box
            ref={toolbarRef}
            className="tiptap-toolbar"
            onMouseDown={(e) => {
              e.stopPropagation();
              toolbarClickedRef.current = true;
              setTimeout(() => {
                toolbarClickedRef.current = false;
              }, 200);
            }}
            onMouseUp={(e) => e.stopPropagation()}
            onClick={(e) => e.stopPropagation()}
            onPointerDown={(e) => {
              e.stopPropagation();
            }}
            onPointerUp={(e) => e.stopPropagation()}
            onTouchStart={(e) => e.stopPropagation()}
            sx={{
              position: "fixed",
              top: toolbarPosition.top,
              left: toolbarPosition.left,
              transform: "translate(-50%, -100%)",
              zIndex: 1400,
              pointerEvents: "auto",
            }}>
            <ToolbarContainer>
              {editor && (
                <Stack direction="row" spacing={1} alignItems="center">
                  <RichTextFontSizeSelect
                    editor={editor}
                    onOpen={() => updateActiveDropdown("fontSize")}
                    onClose={() => updateActiveDropdown(null)}
                    forceClose={activeDropdown !== "fontSize" && activeDropdown !== null}
                  />
                  <Divider flexItem orientation="vertical" />
                  <Stack direction="row" spacing={0.5} alignItems="center">
                    <MenuButton
                      value="bold"
                      iconName={ICON_NAME.BOLD}
                      selected={editorState?.isBold}
                      onClick={() => {
                        if (editorState?.isChipSelected) {
                          editor.chain().focus().toggleVariableChipBold().run();
                        } else {
                          editor.chain().focus().toggleBold().run();
                        }
                      }}
                    />
                    <MenuButton
                      value="italic"
                      iconName={ICON_NAME.ITALIC}
                      selected={editorState?.isItalic}
                      onClick={() => {
                        if (editorState?.isChipSelected) {
                          editor.chain().focus().toggleVariableChipItalic().run();
                        } else {
                          editor.chain().focus().toggleItalic().run();
                        }
                      }}
                    />
                    <MenuButton
                      value="underline"
                      iconName={ICON_NAME.UNDERLINE}
                      selected={editorState?.isUnderline}
                      onClick={() => editor.chain().focus().toggleUnderline().run()}
                    />
                    <MenuButton
                      value="strike"
                      iconName={ICON_NAME.STRIKETHROUGH}
                      selected={editorState?.isStrike}
                      onClick={() => editor.chain().focus().toggleStrike().run()}
                    />
                  </Stack>
                  <Divider flexItem orientation="vertical" />
                  <Stack direction="row" spacing={0.5} alignItems="center">
                    <MenuButton
                      value="bulletList"
                      iconName={ICON_NAME.BULLET_LIST}
                      selected={editorState?.isBulletList}
                      onClick={() => editor.chain().focus().toggleBulletList().run()}
                    />
                    <MenuButton
                      value="orderedList"
                      iconName={ICON_NAME.NUMBERED_LIST}
                      selected={editorState?.isOrderedList}
                      onClick={() => editor.chain().focus().toggleOrderedList().run()}
                    />
                  </Stack>
                  <Divider flexItem orientation="vertical" />
                  <AlignSelect
                    editor={editor}
                    onOpen={() => updateActiveDropdown("align")}
                    onClose={() => updateActiveDropdown(null)}
                    forceClose={activeDropdown !== "align" && activeDropdown !== null}
                  />
                  <LineHeightSelect
                    editor={editor}
                    onOpen={() => updateActiveDropdown("lineHeight")}
                    onClose={() => updateActiveDropdown(null)}
                    forceClose={activeDropdown !== "lineHeight" && activeDropdown !== null}
                  />
                  <Divider flexItem orientation="vertical" />
                  <Stack direction="row" spacing={0.5} alignItems="center">
                    <MenuButton
                      value="link"
                      iconName={ICON_NAME.LINK}
                      selected={editorState?.isLink}
                      onClick={(e) => {
                        if (editorState?.isLink) {
                          editor.chain().focus().extendMarkRange("link").unsetLink().run();
                        } else {
                          setLinkAnchorEl(e.currentTarget as HTMLElement);
                        }
                      }}
                    />
                    <MenuButton
                      value="infoChip"
                      iconName={ICON_NAME.INFO}
                      selected={false}
                      onClick={() => {
                        editor.chain().focus().insertInfoChip().run();
                        // The chip is inserted at the current selection position.
                        // After insert, the cursor moves past the chip, so the chip is at cursor - 1.
                        setTimeout(() => {
                          const { selection } = editor.state;
                          const pos = selection.from - 1;
                          const domAtPos = editor.view.domAtPos(pos);
                          const chipEl = (domAtPos.node as HTMLElement).closest?.(".info-chip")
                            || (domAtPos.node as HTMLElement).querySelector?.(".info-chip")
                            || (domAtPos.node.parentElement as HTMLElement)?.closest?.(".info-chip");
                          if (chipEl) {
                            editor.chain().setNodeSelection(pos).run();
                            setInfoChipAnchorEl(chipEl as HTMLElement);
                          }
                        }, 50);
                      }}
                    />
                  </Stack>
                  <Divider flexItem orientation="vertical" />
                  <VariableInsertMenu
                    editor={editor}
                    variables={variables}
                    onOpen={() => updateActiveDropdown("variable")}
                    onClose={() => updateActiveDropdown(null)}
                    forceClose={activeDropdown !== "variable" && activeDropdown !== null}
                  />
                </Stack>
              )}
            </ToolbarContainer>
          </Box>
        </Portal>
      )}

      {/* Link editing popover */}
      {editor && linkAnchorEl && (
        <LinkPopover editor={editor} anchorEl={linkAnchorEl} onClose={() => setLinkAnchorEl(null)} />
      )}

      {/* Info chip editing popover */}
      {editor && infoChipAnchorEl && (
        <InfoChipEditPopover editor={editor} anchorEl={infoChipAnchorEl} onClose={() => setInfoChipAnchorEl(null)} />
      )}

      {/* Edit mode: show TipTap editor with variable chips */}
      <RichTextEditorContent
        editor={editor}
        sx={{
          height: "100%",
          display: isEditMode ? "block" : "none",
          overflowY: "auto",
          "& .ProseMirror": {
            minHeight: 40,
          },
        }}
      />

      {/* Preview mode: show resolved values */}
      {!isEditMode && (
        <RichTextPreview html={resolvedHtml} />
      )}
    </Box>
  );
};

/* ------------------------------------------------------------------ */
/*  Headless variable resolver                                         */
/* ------------------------------------------------------------------ */

/**
 * Headless component that resolves a single variable value via the aggregation API.
 * Renders nothing — just fires the onResolved callback when the value is ready.
 * Applies cross-filter, viewport filter, and layer base filter — same as chart widgets.
 */
const VariableResolver: React.FC<{
  variable: RichTextVariableSchema;
  projectLayers: ProjectLayer[];
  filterByViewport?: boolean;
  crossFilter?: boolean;
  onResolved: (name: string, value: string | number) => void;
}> = ({ variable, projectLayers, filterByViewport, crossFilter = true, onResolved }) => {
  const { i18n } = useTranslation("common");
  const { map } = useMap();

  // Resolve layer_project_id (numeric) to layer_id (UUID) and get layer object
  const layer = useMemo(() => {
    if (!variable.layer_project_id) return undefined;
    return projectLayers.find((l) => l.id === variable.layer_project_id);
  }, [variable.layer_project_id, projectLayers]);

  const layerId = layer?.layer_id;

  // Get layer's base filter and temporary (cross) filters
  const layerBaseFilter = useMemo(() => {
    return layer?.query?.cql as Record<string, unknown> | undefined;
  }, [layer?.query?.cql]);

  const tempFilters = useTemporaryFilters({ layerId: variable.layer_project_id });

  // Build CQL filter combining base filter, cross-filter, and viewport filter
  const buildFilter = useCallback(() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let cqlQuery: any = layerBaseFilter ? JSON.parse(JSON.stringify(layerBaseFilter)) : undefined;

    if (crossFilter && tempFilters) {
      const cf = JSON.parse(JSON.stringify(tempFilters));
      cqlQuery = cqlQuery ? { op: "and", args: [cqlQuery, cf] } : cf;
    }

    if (filterByViewport && map) {
      const extentRaw = getMapExtentCQL(map);
      if (extentRaw) {
        const extent = JSON.parse(extentRaw);
        if (cqlQuery) {
          if (cqlQuery.op === "and" && cqlQuery.args) {
            cqlQuery.args.push(extent);
          } else {
            cqlQuery = { op: "and", args: [cqlQuery, extent] };
          }
        } else {
          cqlQuery = extent;
        }
      }
    }

    return cqlQuery ? JSON.stringify(cqlQuery) : undefined;
  }, [layerBaseFilter, crossFilter, tempFilters, filterByViewport, map]);

  const [filter, setFilter] = useState(() => buildFilter());

  useEffect(() => {
    setFilter(buildFilter());
  }, [buildFilter]);

  // Update on map moves if viewport filtering is enabled
  useEffect(() => {
    if (!map || !filterByViewport) return;
    const onMoveEnd = () => setFilter(buildFilter());
    map.on("moveend", onMoveEnd);
    return () => { map.off("moveend", onMoveEnd); };
  }, [map, filterByViewport, buildFilter]);

  const queryParams = useMemo((): AggregationStatsQueryParams | undefined => {
    if (!variable.layer_project_id || !variable.operation_type) return undefined;
    // operation_value is required for all operations except count
    if (variable.operation_type !== "count" && !variable.operation_value) return undefined;
    return {
      operation_type: variable.operation_type,
      operation_value: variable.operation_value,
      query: filter,
    } as AggregationStatsQueryParams;
  }, [variable.layer_project_id, variable.operation_type, variable.operation_value, filter]);

  const { aggregationStats } = useProjectLayerAggregationStats(layerId, queryParams);

  useEffect(() => {
    if (aggregationStats?.items?.[0]) {
      const raw = aggregationStats.items[0].operation_value;
      const formatted = typeof raw === "string" ? raw : formatNumber(raw, variable.format, i18n.language);
      onResolved(variable.name, formatted);
    }
  }, [aggregationStats, variable.format, variable.name, i18n.language, onResolved]);

  return null;
};

/* ------------------------------------------------------------------ */
/*  Public component                                                   */
/* ------------------------------------------------------------------ */

export const RichTextDataWidget = ({
  config: rawConfig,
  projectLayers,
  viewOnly,
  onConfigChange,
}: RichTextDataWidgetProps) => {
  const { t } = useTranslation("common");
  const [resolvedValues, setResolvedValues] = useState<Record<string, string | number | null>>({});
  const variables = rawConfig.setup?.variables ?? [];
  const options = rawConfig.options ?? {};

  // Check if any variable's layer has active cross-filters
  const { temporaryFilters } = useAppSelector((state) => state.map);
  const variableLayerIds = useMemo(
    () => variables.map((v) => v.layer_project_id).filter(Boolean),
    [variables]
  );
  const hasActiveFilters = useMemo(() => {
    if (variableLayerIds.length === 0) return false;
    return temporaryFilters.some(
      (f) =>
        variableLayerIds.includes(f.layer_id) ||
        f.additional_targets?.some((t) => variableLayerIds.includes(t.layer_id))
    );
  }, [temporaryFilters, variableLayerIds]);

  const handleVariableResolved = useCallback(
    (name: string, value: string | number) => {
      setResolvedValues((prev) => {
        if (prev[name] === value) return prev;
        return { ...prev, [name]: value };
      });
    },
    []
  );

  // Determine what to show when no filter is active
  const showNoFilterState = !hasActiveFilters && variables.length > 0;
  const shouldHide = showNoFilterState && !!options.hide_when_no_filter;
  const fallbackText = showNoFilterState && !shouldHide ? options.no_filter_text : undefined;

  // In public view, the WidgetWrapper handles hiding entirely.
  // In builder, show a placeholder so the widget is still visible and configurable.
  if (shouldHide) {
    return (
      <Box
        sx={{
          height: "100%",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          p: 1,
          opacity: 0.5,
          border: "1px dashed",
          borderColor: "divider",
          borderRadius: 1,
        }}>
        <Typography variant="body2" color="text.secondary" fontStyle="italic">
          {t("hidden_when_no_filter", { defaultValue: "Hidden until a filter is applied" })}
        </Typography>
      </Box>
    );
  }

  return (
    <>
      {/* Headless variable resolvers */}
      {variables.map((v) => (
        <VariableResolver
          key={v.id}
          variable={v}
          projectLayers={projectLayers}
          filterByViewport={options.filter_by_viewport}
          crossFilter={options.cross_filter}
          onResolved={handleVariableResolved}
        />
      ))}

      {fallbackText ? (
        <Box sx={{ height: "100%", display: "flex", alignItems: "center", p: 1 }}>
          <Typography variant="body2" color="text.secondary">
            {fallbackText}
          </Typography>
        </Box>
      ) : viewOnly ? (
        <RichTextPreviewResolved config={rawConfig} resolvedValues={resolvedValues} />
      ) : (
        <RichTextEditable config={rawConfig} onConfigChange={onConfigChange} resolvedValues={resolvedValues} />
      )}
    </>
  );
};
