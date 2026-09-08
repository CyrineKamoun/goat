import { useCallback, useMemo } from "react";
import { useTranslation } from "react-i18next";

import type { CatalogKind } from "@/lib/catalog/kind";
import type { CatalogPeriod } from "@/lib/catalog/period";
import type {
  CatalogCollection,
  CatalogItem,
  CatalogLink,
  CatalogTheme,
} from "@/lib/validations/catalog";

/** Turning catalog values into text a person reads. */
export const useCatalogLabels = () => {
  const { t, i18n } = useTranslation(["common", "countries", "languages"]);

  /** Translate if the vocabulary has the value; otherwise show what was served. */
  const translate = useCallback(
    (path: string, raw: string) => (i18n.exists(path) ? t(path) : raw),
    [i18n, t]
  );

  const year = (iso: string) => String(new Date(iso).getUTCFullYear());
  const sameDay = (a: string, b: string) => a === b;
  /** A year-precision date: 1 January, midnight, UTC — how the harvester writes one. */
  const yearOnly = (iso: string) => {
    const at = new Date(iso);
    return (
      at.getUTCMonth() === 0 &&
      at.getUTCDate() === 1 &&
      at.getUTCHours() === 0 &&
      at.getUTCMinutes() === 0
    );
  };
  const exactDay = (iso: string, locale: string) =>
    new Date(iso).toLocaleDateString(locale, {
      day: "numeric",
      month: "short",
      year: "numeric",
    });

  return useMemo(
    () => ({
      /** A dataset's kind, from `catalogKindOf` — never from `goat:layerType` directly, which is inherited from the collection. */
      kindLabel: (kind: CatalogKind) => {
        if (kind === "bundle") return t("common:catalog_bundle");
        if (kind === "unknown") return undefined;
        const key = kind === "vector" ? "feature" : kind;
        return translate(`common:metadata.type.${key}`, key);
      },

      /** A theme concept id as a category name. */
      conceptLabel: (concept?: string | null) =>
        concept
          ? translate(`common:metadata.data_category.${concept}`, concept)
          : undefined,

      /** The data category: the first concept of the first theme block. */
      categoryLabel: (themes?: CatalogTheme[] | null) => {
        const concept = themes?.[0]?.concepts?.[0]?.id;
        if (!concept) return undefined;
        return translate(`common:metadata.data_category.${concept}`, concept);
      },

      /** A `goat:geographical_code` (ISO 3166-1 alpha-2) as a country name. */
      regionLabel: (code?: string | null) =>
        code ? translate(`countries:${code.toUpperCase()}`, code) : undefined,

      languageLabel: (code?: string | null) =>
        code ? translate(`languages:${code}`, code) : undefined,

      geometryLabel: (geometryType?: string | null) =>
        geometryType
          ? translate(`common:metadata.geometry_type.${geometryType}`, geometryType)
          : undefined,

      /** A licence, where the row states one. */
      licenseLabel: (license?: string | null) => {
        if (!license) return undefined;
        const value = license.trim();
        if (/^(other|proprietary)$/i.test(value)) return undefined;
        if (!/^https?:\/\//i.test(value)) return value;
        const tail = value.replace(/\/+$/, "").split("/").pop();
        return tail ? tail.toUpperCase() : undefined;
      },

      /** Date only: catalog timestamps are harvest clocks, so the time misleads. */
      formatDate: (iso?: string | null) =>
        iso ? new Date(iso).toLocaleDateString(i18n.language) : undefined,

      /** A period as one line, for a card: "10 Jun 2001", "2001", "2014 – 2021". */
      formatPeriod: (period?: CatalogPeriod) => {
        if (!period) return undefined;
        const { start, end } = period;
        if (!start && !end) return undefined;
        if (start && end) {
          if (year(start) !== year(end)) return `${year(start)} – ${year(end)}`;
          if (!sameDay(start, end)) return year(start);
          return yearOnly(start) ? year(start) : exactDay(start, i18n.language);
        }
        return start
          ? t("common:catalog_period_since", { year: year(start) })
          : t("common:catalog_period_until", { year: year(end as string) });
      },

      /** The same period for a detail sidebar, with the heading it deserves. */
      periodField: (period?: CatalogPeriod) => {
        if (!period) return undefined;
        const { start, end } = period;
        if (!start && !end) return undefined;
        if (start && end && year(start) === year(end)) {
          return {
            labelKey: "common:metadata.headings.data_reference_year",
            value: year(start),
          };
        }
        const value =
          start && end
            ? `${year(start)} – ${year(end)}`
            : start
              ? t("common:catalog_period_since", { year: year(start) })
              : t("common:catalog_period_until", { year: year(end as string) });
        return { labelKey: "common:catalog_datetime", value };
      },

      formatCount: (count?: number | null) =>
        typeof count === "number" ? count.toLocaleString(i18n.language) : undefined,
    }),
    [i18n.language, t, translate]
  );
};

/** The href of a link relation, if the catalog published one. */
export const linkHref = (links: CatalogLink[] | undefined, rel: string) =>
  links?.find((link) => link.rel === rel)?.href;

/** A dataset's descriptive text and keywords. */
export const describedBy = (item?: CatalogItem, collection?: CatalogCollection) => ({
  description: item?.properties.description || collection?.description || undefined,
  keywords: item?.properties.keywords?.length
    ? item.properties.keywords
    : (collection?.keywords ?? undefined),
});
