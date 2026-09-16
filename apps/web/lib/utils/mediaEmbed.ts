/** Video containers a browser can play from a direct file URL. */
const VIDEO_EXTENSIONS = ["mp4", "webm", "ogv", "m4v", "mov"];

/** Poster images accepted alongside a video. */
const IMAGE_EXTENSIONS = ["jpg", "jpeg", "png", "webp", "avif", "gif"];

/**
 * Only https. A plain http source is blocked as mixed content once GOAT is
 * served over https, so accepting it would render a silently broken player.
 */
const isHttpsUrl = (raw: string): URL | null => {
  try {
    const url = new URL(raw.trim());
    return url.protocol === "https:" ? url : null;
  } catch {
    return null;
  }
};

/** Extension of the path, ignoring any query string — sources are often ?blob=… */
const extensionOf = (url: URL): string => {
  const path = url.pathname.toLowerCase();
  const dot = path.lastIndexOf(".");
  return dot < 0 ? "" : path.slice(dot + 1);
};

export const isVideoUrl = (raw: string): boolean => {
  const url = isHttpsUrl(raw);
  return !!url && VIDEO_EXTENSIONS.includes(extensionOf(url));
};

export const isImageUrl = (raw: string): boolean => {
  const url = isHttpsUrl(raw);
  return !!url && IMAGE_EXTENSIONS.includes(extensionOf(url));
};

export type VideoEmbed = {
  src: string;
  /** Frame shown before playback, from the markdown title if one was given. */
  poster?: string;
};

/**
 * Read a video embed out of a markdown link or image target.
 *
 * The optional markdown title carries the poster — standard syntax, no
 * invented directive:
 *
 *   ![](https://host/clip.mp4 "https://host/poster.jpg")
 *
 * Returns null for anything that is not a playable https video, so callers fall
 * back to rendering an ordinary link.
 */
export const parseVideoEmbed = (src: string | undefined, title?: string | null): VideoEmbed | null => {
  if (!src || !isVideoUrl(src)) return null;
  const poster = title && isImageUrl(title) ? title.trim() : undefined;
  return { src: src.trim(), ...(poster ? { poster } : {}) };
};

/**
 * A paragraph holding nothing but a video URL, which is how an author embeds
 * one without markdown syntax. Without remark-gfm a bare URL stays plain text,
 * so this reads the paragraph's own text rather than a link node.
 */
export const parseBareVideoParagraph = (text: string): VideoEmbed | null => {
  const trimmed = text.trim();
  if (/\s/.test(trimmed)) return null;
  return parseVideoEmbed(trimmed);
};

/** Standalone media URLs, for stripping out of text-only renderings. */
export const stripMediaUrls = (input: string): string =>
  input
    .split(/(\s+)/)
    .filter((token) => !isVideoUrl(token))
    .join("")
    .replace(/\s+/g, " ")
    .trim();

/**
 * The video id of a YouTube URL in any of its shapes — `watch?v=`, `youtu.be/`,
 * `/embed/`, `/shorts/` — or null for anything else. Only https, as above.
 */
export const youtubeVideoId = (raw: string): string | null => {
  const url = isHttpsUrl(raw);
  if (!url) return null;
  const host = url.hostname.replace(/^www\./, "").replace(/^m\./, "");
  let id: string | null = null;
  if (host === "youtu.be") {
    id = url.pathname.slice(1).split("/")[0] || null;
  } else if (host === "youtube.com" || host === "youtube-nocookie.com") {
    if (url.pathname === "/watch") id = url.searchParams.get("v");
    else {
      const match = /^\/(?:embed|shorts|v)\/([^/?]+)/.exec(url.pathname);
      id = match?.[1] ?? null;
    }
  }
  return id && /^[\w-]{6,}$/.test(id) ? id : null;
};

/**
 * The privacy-enhanced embed for a YouTube id. It autoplays, so callers only
 * navigate to it after a click; muted by default, unmuted where that click
 * was explicitly "watch this video".
 */
export const youtubeEmbedUrl = (id: string, { muted = true }: { muted?: boolean } = {}): string =>
  `https://www.youtube-nocookie.com/embed/${id}?autoplay=1&mute=${muted ? 1 : 0}&rel=0&modestbranding=1`;

/** YouTube's own poster frame for an id. */
export const youtubePosterUrl = (id: string): string => `https://img.youtube.com/vi/${id}/maxresdefault.jpg`;
