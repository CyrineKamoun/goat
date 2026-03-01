"use client";

import { Delete as DeleteIcon, ContentCopy as DuplicateIcon } from "@mui/icons-material";
import { Box, Divider, IconButton, Paper, Stack, Tooltip, useTheme } from "@mui/material";
import { styled } from "@mui/material/styles";
import Subscript from "@tiptap/extension-subscript";
import Superscript from "@tiptap/extension-superscript";
import TextAlign from "@tiptap/extension-text-align";
import Underline from "@tiptap/extension-underline";
import type { Editor } from "@tiptap/react";
import { EditorContent, useEditor, useEditorState } from "@tiptap/react";
import StarterKit from "@tiptap/starter-kit";
import { type NodeProps, NodeResizer, NodeToolbar, Position, useOnViewportChange } from "@xyflow/react";
import React, { memo, useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { useDispatch, useSelector } from "react-redux";
import { v4 as uuidv4 } from "uuid";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import type { AppDispatch } from "@/lib/store";
import { selectNodes } from "@/lib/store/workflow/selectors";
import { addNode, removeNodes, updateNode } from "@/lib/store/workflow/slice";
import { rgbToHex } from "@/lib/utils/helpers";
import type { TextAnnotationNodeData } from "@/lib/validations/workflow";

import type { RGBColor } from "@/types/map/color";

import { ArrowPopper } from "@/components/ArrowPoper";
import { AlignSelect } from "@/components/builder/widgets/elements/text/AlignSelect";
import { BlockTypeSelect } from "@/components/builder/widgets/elements/text/BlockTypeSelect";
import MenuButton from "@/components/builder/widgets/elements/text/MenuButton";
import SingleColorSelector from "@/components/map/panels/style/color/SingleColorSelector";

// TipTap extensions
const extensions = [
  StarterKit,
  Subscript,
  Superscript,
  Underline,
  TextAlign.configure({
    types: ["heading", "paragraph"],
  }),
];

const TipTapEditorContent = styled(EditorContent)(({ theme }) => ({
  flexGrow: 1,
  height: "100%",
  overflow: "hidden",
  wordBreak: "break-word",
  overflowWrap: "break-word",
  // Use theme text color for proper light/dark mode support
  color: theme.palette.text.primary,
  "& .ProseMirror": {
    wordBreak: "break-word",
    overflowWrap: "break-word",
    padding: 0,
    margin: 0,
    height: "100%",
    boxSizing: "border-box",
    outline: "none",
    color: theme.palette.text.primary,
    "& p, & h1, & h2, & h3, & h4, & h5, & h6": {
      margin: 0,
      color: theme.palette.text.primary,
    },
    "& h2": {
      fontSize: "1.5rem",
      fontWeight: 600,
      marginBottom: "0.5rem",
    },
    "& p:first-of-type, & h1:first-of-type, & h2:first-of-type, & h3:first-of-type": {
      marginTop: 0,
    },
  },
  "& > .ProseMirror-focused": {
    outline: "none",
  },
}));

const NodeContainer = styled(Box, {
  shouldForwardProp: (prop) => prop !== "selected" && prop !== "backgroundColor",
})<{ selected?: boolean; backgroundColor?: string }>(({ theme, selected, backgroundColor }) => {
  const baseBgColor = backgroundColor || "#F2CE58";

  return {
    padding: theme.spacing(2),
    borderRadius: theme.shape.borderRadius,
    // 5% opacity background
    backgroundColor: `${baseBgColor}0D`, // 0D = 5% opacity in hex
    // Visible border using the background color at higher opacity
    border: `2px solid ${baseBgColor}`,
    // Box-shadow for selection indicator (blue glow like in the example)
    boxShadow: selected ? `0 0 0 4px ${theme.palette.primary.main}40` : "none",
    height: "100%",
    width: "100%",
    boxSizing: "border-box",
    overflow: "hidden",
    transition: "box-shadow 0.2s ease",
  };
});

const ActionButton = styled(IconButton)(({ theme }) => ({
  width: 36,
  height: 36,
  "&:hover": {
    backgroundColor: theme.palette.action.hover,
  },
}));

const ToolbarContainer = styled(Paper)(({ theme }) => ({
  display: "flex",
  alignItems: "center",
  padding: theme.spacing(1),
  borderRadius: theme.shape.borderRadius * 2,
  boxShadow: theme.shadows[4],
  backgroundColor: theme.palette.background.paper,
  border: `1px solid ${theme.palette.divider}`,
}));

const ColorPickerButton = styled(Box, {
  shouldForwardProp: (prop) => prop !== "buttonColor",
})<{ buttonColor: string }>(({ theme, buttonColor }) => ({
  width: 24,
  height: 24,
  borderRadius: 4,
  backgroundColor: buttonColor,
  border: `1px solid ${theme.palette.divider}`,
  cursor: "pointer",
  transition: "transform 0.1s ease",
  "&:hover": {
    transform: "scale(1.1)",
  },
}));

interface TextAnnotationNodeProps extends NodeProps {
  data: TextAnnotationNodeData;
}

const TextAnnotationNode: React.FC<TextAnnotationNodeProps> = ({ id, data, selected }) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const dispatch = useDispatch<AppDispatch>();
  const nodes = useSelector(selectNodes);

  const [isEditMode, setIsEditMode] = useState(false);
  const [toolbarOpen, setToolbarOpen] = useState(false);
  const [activeDropdown, setActiveDropdown] = useState<string | null>(null);
  const [colorPickerOpen, setColorPickerOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  // Close color picker when viewport changes (pan/zoom)
  useOnViewportChange({
    onChange: useCallback(() => {
      if (colorPickerOpen) {
        setColorPickerOpen(false);
      }
    }, [colorPickerOpen]),
  });

  const editor = useEditor({
    extensions,
    content: data.text || "<p></p>",
    immediatelyRender: true,
    shouldRerenderOnTransaction: false,
    editable: isEditMode,
    onUpdate: ({ editor }) => {
      // Save content on change
      dispatch(
        updateNode({
          id,
          changes: { data: { ...data, text: editor.getHTML() } },
        })
      );
    },
  });

  // Update editor editable state
  useEffect(() => {
    if (editor) {
      editor.setEditable(isEditMode);
    }
  }, [editor, isEditMode]);

  // Sync editor content when data changes externally
  useEffect(() => {
    if (editor && data.text !== undefined && !isEditMode) {
      const currentContent = editor.getHTML();
      if (currentContent !== data.text) {
        editor.commands.setContent(data.text || "<p></p>");
      }
    }
  }, [editor, data.text, isEditMode]);

  // Show toolbar when node is selected, but don't auto-enable edit mode
  // Edit mode is entered via double-click
  useEffect(() => {
    if (selected) {
      setToolbarOpen(true);
    } else {
      setToolbarOpen(false);
      setIsEditMode(false);
      setColorPickerOpen(false); // Close color picker when deselected
    }
  }, [selected]);

  // Handle double-click to enter edit mode
  const handleDoubleClick = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      if (!isEditMode) {
        setIsEditMode(true);
        // Focus editor after enabling edit mode
        setTimeout(() => {
          editor?.commands.focus();
        }, 0);
      }
    },
    [isEditMode, editor]
  );

  // Handle editor focus/blur
  useEffect(() => {
    if (!editor) return;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const handleBlur = ({ event }: any) => {
      // Don't close toolbar if clicking on toolbar or color picker
      if (
        event?.relatedTarget &&
        ((event.relatedTarget as HTMLElement).closest(".tiptap-toolbar") ||
          (event.relatedTarget as HTMLElement).closest(".color-picker-popper"))
      ) {
        return;
      }
      // Don't close if color picker is open or node is selected
      if (colorPickerOpen || selected) return;
      setToolbarOpen(false);
      setIsEditMode(false);
    };

    editor.on("blur", handleBlur);

    return () => {
      editor.off("blur", handleBlur);
    };
  }, [editor, colorPickerOpen, selected]);

  const editorState = useEditorState({
    editor,
    selector: ({ editor }: { editor: Editor }) => ({
      isBold: editor.isActive("bold"),
      isItalic: editor.isActive("italic"),
      isUnderline: editor.isActive("underline"),
      isStrike: editor.isActive("strike"),
      isSuperscript: editor.isActive("superscript"),
      isSubscript: editor.isActive("subscript"),
    }),
  });

  // Handle duplicate
  const handleDuplicate = useCallback(
    (event: React.MouseEvent) => {
      event.stopPropagation();
      const node = nodes.find((n) => n.id === id);
      if (!node) return;

      dispatch(
        addNode({
          ...node,
          id: `text-${uuidv4()}`,
          position: {
            x: node.position.x + 50,
            y: node.position.y + 50,
          },
        })
      );
    },
    [id, nodes, dispatch]
  );

  // Handle delete
  const handleDelete = useCallback(
    (event: React.MouseEvent) => {
      event.stopPropagation();
      dispatch(removeNodes([id]));
    },
    [id, dispatch]
  );

  // Handle background color change from SingleColorSelector
  const handleColorChange = useCallback(
    (rgb: RGBColor) => {
      const hexColor = rgbToHex(rgb);
      dispatch(
        updateNode({
          id,
          changes: { data: { ...data, backgroundColor: hexColor } },
        })
      );
    },
    [id, data, dispatch]
  );

  // Handle resize
  const handleResize = useCallback(
    (_: unknown, params: { width: number; height: number }) => {
      dispatch(
        updateNode({
          id,
          changes: { data: { ...data, width: params.width, height: params.height } },
        })
      );
    },
    [id, data, dispatch]
  );

  // Get current color as hex for the color picker
  const currentColorHex = data.backgroundColor || "#F2CE58";

  return (
    <>
      <NodeResizer
        isVisible={selected}
        minWidth={200}
        minHeight={100}
        onResize={handleResize}
        handleStyle={{
          width: 10,
          height: 10,
          backgroundColor: theme.palette.primary.main,
          borderRadius: 2,
          zIndex: 20,
        }}
        lineStyle={{
          // Use transparent line - we handle selection with box-shadow on the NodeContainer
          borderColor: "transparent",
          borderWidth: 0,
        }}
      />

      <Box
        ref={containerRef}
        onDoubleClick={handleDoubleClick}
        className={isEditMode ? "nodrag" : ""}
        sx={{
          width: data.width || 400,
          height: data.height || 200,
          position: "relative",
          cursor: isEditMode ? "text" : "grab",
          "&:active": {
            cursor: isEditMode ? "text" : "grabbing",
          },
        }}>
        <NodeContainer selected={selected} backgroundColor={data.backgroundColor}>
          <TipTapEditorContent
            editor={editor}
            className={isEditMode ? "nodrag nowheel" : ""}
            sx={{
              overflowY: isEditMode ? "auto" : "hidden",
              // Prevent text selection and let pointer events pass through when not in edit mode
              // This allows dragging the node without accidentally selecting text
              userSelect: isEditMode ? "auto" : "none",
              pointerEvents: isEditMode ? "auto" : "none",
              "& .ProseMirror": {
                userSelect: isEditMode ? "auto" : "none",
                pointerEvents: isEditMode ? "auto" : "none",
              },
            }}
          />
        </NodeContainer>
      </Box>

      {/* Floating toolbar */}
      <NodeToolbar
        isVisible={toolbarOpen}
        position={Position.Top}
        offset={8}
        className="tiptap-toolbar nodrag nowheel">
        <Box
          onMouseDown={(e) => e.stopPropagation()}
          onMouseUp={(e) => e.stopPropagation()}
          onClick={(e) => e.stopPropagation()}
          onPointerDown={(e) => e.stopPropagation()}
          onPointerUp={(e) => e.stopPropagation()}
          onTouchStart={(e) => e.stopPropagation()}
          sx={{
            display: "flex",
            alignItems: "center",
            gap: 1,
          }}>
          <ToolbarContainer>
            {editor && (
              <Stack direction="row" spacing={1} alignItems="center">
                <BlockTypeSelect
                  editor={editor}
                  onOpen={() => setActiveDropdown("blockType")}
                  onClose={() => setActiveDropdown(null)}
                  forceClose={activeDropdown !== "blockType" && activeDropdown !== null}
                />
                <Divider flexItem orientation="vertical" />
                <Stack direction="row" spacing={0.5} alignItems="center">
                  <MenuButton
                    value="bold"
                    iconName={ICON_NAME.BOLD}
                    selected={editorState?.isBold}
                    onClick={() => editor.chain().focus().toggleBold().run()}
                  />
                  <MenuButton
                    value="italic"
                    iconName={ICON_NAME.ITALIC}
                    selected={editorState?.isItalic}
                    onClick={() => editor.chain().focus().toggleItalic().run()}
                  />
                  <MenuButton
                    value="underline"
                    iconName={ICON_NAME.UNDERLINE}
                    selected={editorState?.isUnderline}
                    onClick={() => editor.chain().focus().toggleUnderline().run()}
                  />
                </Stack>
                <Divider flexItem orientation="vertical" />
                <AlignSelect
                  editor={editor}
                  onOpen={() => setActiveDropdown("align")}
                  onClose={() => setActiveDropdown(null)}
                  forceClose={activeDropdown !== "align" && activeDropdown !== null}
                />
                <Divider flexItem orientation="vertical" />
                {/* Background color picker */}
                <ArrowPopper
                  open={colorPickerOpen}
                  placement="bottom"
                  arrow={false}
                  disablePortal={false}
                  isClickAwayEnabled={true}
                  onClose={() => setColorPickerOpen(false)}
                  content={
                    <Paper
                      className="color-picker-popper"
                      sx={{
                        py: 3,
                        boxShadow: "rgba(0, 0, 0, 0.16) 0px 6px 12px 0px",
                        width: "235px",
                        maxHeight: "500px",
                      }}>
                      <SingleColorSelector
                        selectedColor={currentColorHex}
                        onSelectColor={handleColorChange}
                      />
                    </Paper>
                  }>
                  <ColorPickerButton
                    buttonColor={currentColorHex}
                    onClick={() => setColorPickerOpen(!colorPickerOpen)}
                    title={t("color")}
                  />
                </ArrowPopper>
              </Stack>
            )}
          </ToolbarContainer>
          {/* Separate action buttons group */}
          <ToolbarContainer>
            <Tooltip title={t("duplicate")} placement="top" arrow>
              <ActionButton onClick={handleDuplicate}>
                <DuplicateIcon fontSize="small" />
              </ActionButton>
            </Tooltip>
            <Tooltip title={t("delete")} placement="top" arrow>
              <ActionButton onClick={handleDelete}>
                <DeleteIcon fontSize="small" color="error" />
              </ActionButton>
            </Tooltip>
          </ToolbarContainer>
        </Box>
      </NodeToolbar>
    </>
  );
};

export default memo(TextAnnotationNode);
