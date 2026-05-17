import DOMPurify from "dompurify";

const ALLOWED_TAGS = [
  // text nodes (required when explicit ALLOWED_TAGS is set with KEEP_CONTENT: false)
  "#text",
  // structure
  "div", "span", "br", "hr",
  "h1", "h2", "h3", "h4", "h5", "h6",
  "p", "blockquote",
  // inline
  "strong", "b", "em", "i", "u", "s", "code", "small", "sub", "sup",
  // links + images
  "a", "img",
  // lists
  "ul", "ol", "li",
  // tables
  "table", "thead", "tbody", "tr", "th", "td", "caption",
  // definition lists (field list "list" layout)
  "dl", "dt", "dd",
];

const ALLOWED_ATTRS = [
  "href", "target", "rel",
  "src", "alt", "width", "height",
  "class", "style",
  "colspan", "rowspan",
];

export function sanitizePopupHtml(dirty: string): string {
  return DOMPurify.sanitize(dirty, {
    ALLOWED_TAGS,
    ALLOWED_ATTR: ALLOWED_ATTRS,
    FORBID_ATTR: ["onerror", "onload", "onclick", "onmouseover", "onfocus", "onblur"],
    KEEP_CONTENT: false,
  }) as string;
}
