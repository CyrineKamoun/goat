import { mergeAttributes, Node } from "@tiptap/core";

export interface VariableChipOptions {
  HTMLAttributes: Record<string, unknown>;
}

declare module "@tiptap/core" {
  interface Commands<ReturnType> {
    variableChip: {
      insertVariable: (name: string) => ReturnType;
      toggleVariableChipBold: () => ReturnType;
      toggleVariableChipItalic: () => ReturnType;
      setVariableChipFontSize: (size: string | null) => ReturnType;
    };
  }
}

const VariableChip = Node.create<VariableChipOptions>({
  name: "variableChip",
  group: "inline",
  inline: true,
  atom: true,
  selectable: true,

  addOptions() {
    return {
      HTMLAttributes: {},
    };
  },

  addAttributes() {
    return {
      variableName: {
        default: null,
        parseHTML: (element) => element.getAttribute("data-variable"),
        renderHTML: (attributes) => ({
          "data-variable": attributes.variableName,
        }),
      },
      bold: {
        default: false,
        parseHTML: (element) => element.getAttribute("data-bold") === "true",
        renderHTML: (attributes) => {
          if (!attributes.bold) return {};
          return { "data-bold": "true" };
        },
      },
      italic: {
        default: false,
        parseHTML: (element) => element.getAttribute("data-italic") === "true",
        renderHTML: (attributes) => {
          if (!attributes.italic) return {};
          return { "data-italic": "true" };
        },
      },
      fontSize: {
        default: null,
        parseHTML: (element) => element.getAttribute("data-font-size"),
        renderHTML: (attributes) => {
          if (!attributes.fontSize) return {};
          return { "data-font-size": attributes.fontSize };
        },
      },
    };
  },

  parseHTML() {
    return [
      {
        tag: "span[data-variable]",
      },
    ];
  },

  renderHTML({ node, HTMLAttributes }) {
    // Always output all style properties so TipTap fully replaces the style attribute on re-render
    const styles: string[] = [
      `font-weight:${node.attrs.bold ? "700" : "normal"}`,
      `font-style:${node.attrs.italic ? "italic" : "normal"}`,
    ];
    if (node.attrs.fontSize) styles.push(`font-size:${node.attrs.fontSize}`);

    const extraAttrs: Record<string, string> = {
      "data-variable": node.attrs.variableName,
      class: "variable-chip",
      style: styles.join(";"),
    };

    return [
      "span",
      mergeAttributes(this.options.HTMLAttributes, HTMLAttributes, extraAttrs),
      `{{@${node.attrs.variableName}}}`,
    ];
  },

  addCommands() {
    return {
      insertVariable:
        (name: string) =>
        ({ commands }) => {
          return commands.insertContent({
            type: this.name,
            attrs: { variableName: name },
          });
        },
      toggleVariableChipBold:
        () =>
        ({ tr, state, dispatch }) => {
          const { selection } = state;
          const node = state.doc.nodeAt(selection.from);
          if (!node || node.type.name !== "variableChip") return false;
          if (dispatch) {
            tr.setNodeMarkup(selection.from, undefined, {
              ...node.attrs,
              bold: !node.attrs.bold,
            });
            dispatch(tr);
          }
          return true;
        },
      toggleVariableChipItalic:
        () =>
        ({ tr, state, dispatch }) => {
          const { selection } = state;
          const node = state.doc.nodeAt(selection.from);
          if (!node || node.type.name !== "variableChip") return false;
          if (dispatch) {
            tr.setNodeMarkup(selection.from, undefined, {
              ...node.attrs,
              italic: !node.attrs.italic,
            });
            dispatch(tr);
          }
          return true;
        },
      setVariableChipFontSize:
        (size: string | null) =>
        ({ tr, state, dispatch }) => {
          const { selection } = state;
          const node = state.doc.nodeAt(selection.from);
          if (!node || node.type.name !== "variableChip") return false;
          if (dispatch) {
            tr.setNodeMarkup(selection.from, undefined, {
              ...node.attrs,
              fontSize: size,
            });
            dispatch(tr);
          }
          return true;
        },
    };
  },
});

export default VariableChip;
