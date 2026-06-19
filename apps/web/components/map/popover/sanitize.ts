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
  // disclosure (expand/collapse without JS)
  "details", "summary",
  // Inline SVG — a presentational subset only (shapes, paths, gradients,
  // text). Deliberately EXCLUDES the SVG XSS vectors: <script>,
  // <foreignObject> (embeds arbitrary HTML), <use>/xlink (external refs),
  // and SMIL <animate>/<set> (can fire script via events). Those tags are
  // simply not in this allowlist, so DOMPurify drops them.
  "svg", "g", "path", "circle", "ellipse", "rect", "line",
  "polyline", "polygon", "title", "text", "tspan",
  "defs", "linearGradient", "radialGradient", "stop", "clipPath",
];

const ALLOWED_ATTRS = [
  "href", "target", "rel",
  "src", "alt", "width", "height",
  "class", "style",
  "colspan", "rowspan",
  "open",
  // SVG presentational attributes
  "viewbox", "xmlns", "fill", "stroke", "stroke-width", "stroke-linecap",
  "stroke-linejoin", "stroke-dasharray", "fill-rule", "clip-rule", "clip-path",
  "d", "points", "cx", "cy", "r", "rx", "ry", "x", "y", "x1", "y1", "x2", "y2",
  "transform", "opacity", "offset", "stop-color", "stop-opacity",
  "gradientunits", "gradienttransform", "preserveaspectratio",
];

export function sanitizePopupHtml(dirty: string): string {
  return DOMPurify.sanitize(dirty, {
    ALLOWED_TAGS,
    ALLOWED_ATTR: ALLOWED_ATTRS,
    // Defense-in-depth: even though these aren't in ALLOWED_TAGS, forbid the
    // dangerous SVG/script tags explicitly so a future allowlist edit can't
    // silently re-enable an XSS vector.
    FORBID_TAGS: ["script", "foreignObject", "use", "animate", "animateTransform", "animateMotion", "set"],
    FORBID_ATTR: ["onerror", "onload", "onclick", "onmouseover", "onfocus", "onblur", "xlink:href"],
    KEEP_CONTENT: false,
  }) as string;
}
