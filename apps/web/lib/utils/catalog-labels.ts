const BRACKETED_SUFFIX_RE = /\s*\[([^\]]+)\]\s*$/;

export const formatCatalogGroupName = (name: string | undefined): string => {
  if (!name) {
    return "";
  }

  const cleaned = name.replace(BRACKETED_SUFFIX_RE, "").trim();
  return cleaned || name;
};

export const formatCatalogLayerName = (name: string | undefined): string => {
  if (!name) {
    return "";
  }

  const match = name.match(BRACKETED_SUFFIX_RE);
  if (!match) {
    return name;
  }

  const base = name.replace(BRACKETED_SUFFIX_RE, "").trim();
  const rawIdentifier = match[1].trim();
  const shortIdentifier = rawIdentifier.includes(":")
    ? rawIdentifier.split(":").pop() || rawIdentifier
    : rawIdentifier;

  if (!base) {
    return shortIdentifier;
  }

  return `${base} (${shortIdentifier})`;
};