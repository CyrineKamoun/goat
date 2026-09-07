/** The largest thumbnail a picker takes, in megabytes. */
export const THUMBNAIL_MAX_SIZE_MB = 2;

/** The image types a thumbnail may be. */
export const THUMBNAIL_MIME_TYPES = ["image/png", "image/jpeg", "image/webp"];

/** The same list as a file input's `accept`. */
export const THUMBNAIL_ACCEPT = THUMBNAIL_MIME_TYPES.join(",");

/** Why a picked file cannot be a thumbnail — a translation key and its
 * values, for the caller to show however it shows failures — or `null` when
 * the file is one. */
export const thumbnailFileRejection = (
  file: File
): { key: string; values?: Record<string, unknown> } | null => {
  if (!THUMBNAIL_MIME_TYPES.includes(file.type)) return { key: "thumbnail_invalid_type" };
  if (file.size > THUMBNAIL_MAX_SIZE_MB * 1024 * 1024)
    return { key: "thumbnail_too_large", values: { mb: THUMBNAIL_MAX_SIZE_MB } };
  return null;
};
