import { Box } from "@mui/material";
import ReactMarkdown from "react-markdown";

/**
 * Markdown at the app's text scale.
 *
 * `ReactMarkdown` emits plain `<p>`/`<ul>`/`<a>`, which would inherit the
 * document's 16px; `typography: "body2"` puts them at the 14px the values
 * beside them are set in. `overflowWrap: "anywhere"` is what breaks the long
 * unbroken strings these records carry — a slash-joined authority name
 * ("…/Tiefbauamt/Leitung/Dokumentation") or a bare URL has no space to wrap at.
 */
const PROSE_SX = {
  overflowWrap: "anywhere",
  typography: "body2",
  /**
   * Providers indent lines in their records, and CommonMark reads four spaces
   * or a tab as an indented code block, so a paragraph of ordinary prose
   * arrives as `<pre>`, which does not wrap on its own. De-indenting the
   * source instead would flatten the nested lists that make up most of the
   * other indented cases.
   */
  "& pre": { whiteSpace: "pre-wrap", overflowWrap: "anywhere" },
  "& :first-of-type": { marginTop: 0 },
  "& :last-child": { marginBottom: 0 },
  "& img": { maxWidth: "100%" },
} as const;

const MarkdownProse = ({ children }: { children: string }) => (
  <Box sx={PROSE_SX}>
    <ReactMarkdown
      components={{
        img: ({ node: _, ...props }) => {
          const hasSize =
            props.width !== undefined ||
            props.height !== undefined ||
            (props.style && (props.style.width || props.style.height));

          const style = hasSize ? props.style : { width: "100%" };

          // eslint-disable-next-line jsx-a11y/alt-text
          return <img {...props} style={style} />;
        },
        a: ({ node: _, href, children, ...props }) => (
          <a href={href} target="_blank" rel="noopener noreferrer" {...props}>
            {children}
          </a>
        ),
      }}>
      {children}
    </ReactMarkdown>
  </Box>
);

export default MarkdownProse;
