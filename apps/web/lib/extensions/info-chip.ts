import { mergeAttributes, Node } from "@tiptap/core";

export interface InfoChipOptions {
  HTMLAttributes: Record<string, unknown>;
}

declare module "@tiptap/core" {
  interface Commands<ReturnType> {
    infoChip: {
      insertInfoChip: () => ReturnType;
      updateInfoChip: (attrs: { text?: string; url?: string }) => ReturnType;
      deleteInfoChip: () => ReturnType;
    };
  }
}

const InfoChip = Node.create<InfoChipOptions>({
  name: "infoChip",
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
      infoId: {
        default: null,
        parseHTML: (element) => element.getAttribute("data-info-id"),
        renderHTML: (attributes) => ({
          "data-info-id": attributes.infoId,
        }),
      },
      text: {
        default: "",
        parseHTML: (element) => element.getAttribute("data-info-text"),
        renderHTML: (attributes) => ({
          "data-info-text": attributes.text,
        }),
      },
      url: {
        default: "",
        parseHTML: (element) => element.getAttribute("data-info-url"),
        renderHTML: (attributes) => {
          if (!attributes.url) return {};
          return { "data-info-url": attributes.url };
        },
      },
    };
  },

  parseHTML() {
    return [
      {
        tag: "span[data-info-id]",
      },
    ];
  },

  renderHTML({ HTMLAttributes }) {
    return [
      "span",
      mergeAttributes(this.options.HTMLAttributes, HTMLAttributes, {
        class: "info-chip",
      }),
      "i",
    ];
  },

  addCommands() {
    return {
      insertInfoChip:
        () =>
        ({ commands }) => {
          const id = `info_${Date.now()}`;
          return commands.insertContent({
            type: this.name,
            attrs: { infoId: id, text: "", url: "" },
          });
        },
      updateInfoChip:
        (attrs) =>
        ({ tr, state, dispatch }) => {
          const { selection } = state;
          const node = state.doc.nodeAt(selection.from);
          if (!node || node.type.name !== "infoChip") return false;
          if (dispatch) {
            tr.setNodeMarkup(selection.from, undefined, {
              ...node.attrs,
              ...attrs,
            });
            dispatch(tr);
          }
          return true;
        },
      deleteInfoChip:
        () =>
        ({ tr, state, dispatch }) => {
          const { selection } = state;
          const node = state.doc.nodeAt(selection.from);
          if (!node || node.type.name !== "infoChip") return false;
          if (dispatch) {
            tr.delete(selection.from, selection.from + node.nodeSize);
            dispatch(tr);
          }
          return true;
        },
    };
  },
});

export default InfoChip;
