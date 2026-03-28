import { Box, Divider, Paper, Portal, Stack, styled } from "@mui/material";
import { debounce } from "@mui/material/utils";
import Color from "@tiptap/extension-color";
import FontFamily from "@tiptap/extension-font-family";
import Subscript from "@tiptap/extension-subscript";
import Superscript from "@tiptap/extension-superscript";
import TextAlign from "@tiptap/extension-text-align";
import { TextStyle } from "@tiptap/extension-text-style";
import type { Editor } from "@tiptap/react";
import { useEditor, useEditorState } from "@tiptap/react";
import StarterKit from "@tiptap/starter-kit";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { useProjectLayerAggregationStats } from "@/lib/api/projects";
import VariableChip from "@/lib/extensions/variable-chip";
import FontSize from "@/lib/extensions/font-size";
import LineHeight from "@/lib/extensions/line-height";
import { formatNumber } from "@/lib/utils/format-number";
import type { AggregationStatsQueryParams, ProjectLayer } from "@/lib/validations/project";
import type { RichTextDataSchema, RichTextVariableSchema } from "@/lib/validations/widget";


import { TipTapEditorContent } from "@/components/builder/widgets/elements/text/Text";
import { AlignSelect } from "@/components/builder/widgets/elements/text/AlignSelect";
import LineHeightSelect from "@/components/builder/widgets/elements/text/LineHeightSelect";
import MenuButton from "@/components/builder/widgets/elements/text/MenuButton";
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
  VariableChip,
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
    // Bold/italic set via inline style from node attributes override these defaults
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

interface RichTextDataWidgetProps {
  config: RichTextDataSchema;
  projectLayers: ProjectLayer[];
  viewOnly?: boolean;
  onConfigChange?: (nextConfig: RichTextDataSchema) => void;
}

/* ------------------------------------------------------------------ */
/*  View-only renderer                                                 */
/* ------------------------------------------------------------------ */

const RichTextViewOnly = ({
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

  const viewExtensions = useMemo(
    () => [
      StarterKit,
      Subscript,
      Superscript,
      TextStyle,
      Color,
      FontFamily,
      FontSize,
      LineHeight,
      TextAlign.configure({ types: ["heading", "paragraph"] }),
    ],
    []
  );

  const editor = useEditor({
    extensions: viewExtensions,
    content: resolvedHtml,
    immediatelyRender: true,
    shouldRerenderOnTransaction: false,
    editable: false,
  });

  useEffect(() => {
    if (editor) {
      const current = editor.getHTML();
      if (current !== resolvedHtml) {
        editor.commands.setContent(resolvedHtml);
      }
    }
  }, [editor, resolvedHtml]);

  return <TipTapEditorContent editor={editor} />;
};

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

  const editorState = useEditorState({
    editor,
    selector: ({ editor }: { editor: Editor }) => {
      // Check if a variable chip is selected
      const node = editor.state.doc.nodeAt(editor.state.selection.from);
      const isChipSelected = node?.type.name === "variableChip";
      return {
        isBold: isChipSelected ? !!node?.attrs.bold : editor.isActive("bold"),
        isItalic: isChipSelected ? !!node?.attrs.italic : editor.isActive("italic"),
        isUnderline: editor.isActive("underline"),
        isStrike: editor.isActive("strike"),
        isBulletList: editor.isActive("bulletList"),
        isOrderedList: editor.isActive("orderedList"),
        isChipSelected,
      };
    },
  });

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
  const [toolbarPosition, setToolbarPosition] = useState({ top: 0, left: 0, width: 0 });

  useEffect(() => {
    if (!toolbarOpen || !containerRef.current) return;

    const updatePosition = () => {
      const rect = containerRef.current?.getBoundingClientRect();
      if (rect) {
        setToolbarPosition({
          top: rect.top - 8,
          left: rect.left + rect.width / 2,
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
        <Box
          sx={{
            height: "100%",
            pointerEvents: "none",
            userSelect: "none",
            fontFamily: "inherit",
            "& p, & h1, & h2, & h3, & h4, & h5, & h6": { margin: 0 },
          }}
          dangerouslySetInnerHTML={{ __html: resolvedHtml || "<p></p>" }}
        />
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
 */
const VariableResolver: React.FC<{
  variable: RichTextVariableSchema;
  projectLayers: ProjectLayer[];
  onResolved: (name: string, value: string | number) => void;
}> = ({ variable, projectLayers, onResolved }) => {
  const { i18n } = useTranslation("common");

  // Resolve layer_project_id (numeric) to layer_id (UUID) needed by the aggregation API
  const layerId = useMemo(() => {
    if (!variable.layer_project_id) return undefined;
    const layer = projectLayers.find((l) => l.id === variable.layer_project_id);
    return layer?.layer_id;
  }, [variable.layer_project_id, projectLayers]);

  const queryParams = useMemo((): AggregationStatsQueryParams | undefined => {
    if (!variable.layer_project_id || !variable.operation_type) return undefined;
    return {
      operation_type: variable.operation_type,
      operation_value: variable.operation_value,
    } as AggregationStatsQueryParams;
  }, [variable.layer_project_id, variable.operation_type, variable.operation_value]);

  const { aggregationStats } = useProjectLayerAggregationStats(layerId, queryParams);

  useEffect(() => {
    if (aggregationStats?.items?.[0]) {
      const raw = aggregationStats.items[0].operation_value;
      const formatted = formatNumber(raw, variable.format, i18n.language);
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
  const [resolvedValues, setResolvedValues] = useState<Record<string, string | number | null>>({});
  const variables = rawConfig.setup?.variables ?? [];

  const handleVariableResolved = useCallback(
    (name: string, value: string | number) => {
      setResolvedValues((prev) => {
        if (prev[name] === value) return prev; // Avoid unnecessary re-renders
        return { ...prev, [name]: value };
      });
    },
    []
  );

  return (
    <>
      {/* Headless variable resolvers */}
      {variables.map((v) => (
        <VariableResolver
          key={v.id}
          variable={v}
          projectLayers={projectLayers}
          onResolved={handleVariableResolved}
        />
      ))}

      {viewOnly ? (
        <RichTextViewOnly config={rawConfig} resolvedValues={resolvedValues} />
      ) : (
        <RichTextEditable config={rawConfig} onConfigChange={onConfigChange} resolvedValues={resolvedValues} />
      )}
    </>
  );
};
