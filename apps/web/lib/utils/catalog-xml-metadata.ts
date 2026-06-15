type CatalogXmlMetadata = {
  title?: string;
  abstract?: string;
  keywords: string[];
  distributorName?: string;
  distributorEmail?: string;
  bbox?: { west: number; south: number; east: number; north: number };
};

const firstTagText = (xml: string, tags: string[]): string | undefined => {
  for (const tag of tags) {
    const regex = new RegExp(`<[^>]*:?${tag}[^>]*>([\\s\\S]*?)<\\/[^>]*:?${tag}>`, "i");
    const match = xml.match(regex);
    if (match?.[1]) {
      const value = match[1].replace(/<[^>]+>/g, "").trim();
      if (value) {
        return value;
      }
    }
  }
  return undefined;
};

const parseFloatTag = (xml: string, tag: string): number | undefined => {
  const text = firstTagText(xml, [tag]);
  if (!text) {
    return undefined;
  }
  const value = Number.parseFloat(text);
  return Number.isFinite(value) ? value : undefined;
};

export const parseCatalogXmlMetadata = (xmlMetadata?: string): CatalogXmlMetadata | null => {
  if (!xmlMetadata) {
    return null;
  }

  const title = firstTagText(xmlMetadata, ["title", "CharacterString", "Anchor"]);
  const abstract = firstTagText(xmlMetadata, ["abstract", "CharacterString", "Anchor"]);

  const keywordRegex = /<[^>]*:?keyword[^>]*>([\s\S]*?)<\/[^>]*:?keyword>/gi;
  const keywords: string[] = [];
  let keywordMatch: RegExpExecArray | null = keywordRegex.exec(xmlMetadata);
  while (keywordMatch) {
    const value = keywordMatch[1].replace(/<[^>]+>/g, "").trim();
    if (value && !keywords.includes(value)) {
      keywords.push(value);
    }
    keywordMatch = keywordRegex.exec(xmlMetadata);
  }

  const distributorName = firstTagText(xmlMetadata, ["organisationName"]);
  const distributorEmail = firstTagText(xmlMetadata, ["electronicMailAddress"]);

  const west = parseFloatTag(xmlMetadata, "westBoundLongitude");
  const south = parseFloatTag(xmlMetadata, "southBoundLatitude");
  const east = parseFloatTag(xmlMetadata, "eastBoundLongitude");
  const north = parseFloatTag(xmlMetadata, "northBoundLatitude");

  const bbox =
    west !== undefined && south !== undefined && east !== undefined && north !== undefined
      ? { west, south, east, north }
      : undefined;

  return {
    title,
    abstract,
    keywords,
    distributorName,
    distributorEmail,
    bbox,
  };
};
