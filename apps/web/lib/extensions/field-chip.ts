// Mirrors variable-chip.ts but for popup field tokens.
// Renders `{{name}}` (no `@` scope prefix). No per-chip styling — the
// surrounding rich-text marks (bold, italic, etc.) handle that.
// TODO future: merge with variable-chip.ts into a shared TokenChip.
import { mergeAttributes, Node } from "@tiptap/core";

export interface FieldChipOptions {
  HTMLAttributes: Record<string, unknown>;
}

declare module "@tiptap/core" {
  interface Commands<ReturnType> {
    fieldChip: {
      insertField: (name: string) => ReturnType;
    };
  }
}

const FieldChip = Node.create<FieldChipOptions>({
  name: "fieldChip",
  group: "inline",
  inline: true,
  atom: true,
  selectable: true,

  addOptions() {
    return { HTMLAttributes: {} };
  },

  addAttributes() {
    return {
      fieldName: {
        default: null,
        parseHTML: (el) => el.getAttribute("data-field"),
        renderHTML: (attrs) => ({ "data-field": attrs.fieldName }),
      },
    };
  },

  parseHTML() {
    return [{ tag: "span[data-field]" }];
  },

  renderHTML({ node, HTMLAttributes }) {
    return [
      "span",
      mergeAttributes(this.options.HTMLAttributes, HTMLAttributes, {
        "data-field": node.attrs.fieldName,
        class: "field-chip",
      }),
      `{{${node.attrs.fieldName}}}`,
    ];
  },

  addCommands() {
    return {
      insertField:
        (name: string) =>
        ({ commands }) =>
          commands.insertContent({ type: this.name, attrs: { fieldName: name } }),
    };
  },
});

export default FieldChip;
