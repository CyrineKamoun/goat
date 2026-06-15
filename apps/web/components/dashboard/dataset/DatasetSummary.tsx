import { Divider, Link, Stack, Typography, styled, useTheme } from "@mui/material";
import React from "react";
import { useTranslation } from "react-i18next";
import ReactMarkdown from "react-markdown";

import { Icon } from "@p4b/ui/components/Icon";

import { type Layer, datasetMetadataAggregated } from "@/lib/validations/layer";
import type { ProjectLayer } from "@/lib/validations/project";

import { useGetMetadataValueTranslation } from "@/hooks/map/DatasetHooks";

import { METADATA_HEADER_ICONS } from "@/components/dashboard/catalog/CatalogDatasetCard";

interface DatasetSummaryProps {
  dataset: Layer | ProjectLayer;
  hideEmpty?: boolean; // Prop to control empty field display
  hideMainSection?: boolean; // Prop to control main section display
}

const ContainerWrapper = styled("div")({
  containerType: "inline-size",
  width: "100%",
});

const LayoutContainer = styled("div")({
  display: "flex",
  flexDirection: "row",
  gap: "16px",
  width: "100%",
  "@container (max-width: 600px)": {
    flexDirection: "column",
  },
});

const MetadataSection = styled("div")({
  flex: 4,
  order: 1,

  "@container (max-width: 600px)": {
    order: 2,
    flex: "1 1 100%",
  },
});

const MainContentSection = styled("div")({
  flex: 1,
  order: 2,

  "@container (max-width: 600px)": {
    order: 1,
    flex: "1 1 100%",
  },
});

const DatasetSummary: React.FC<DatasetSummaryProps> = ({
  dataset,
  hideEmpty = false,
  hideMainSection = false,
}) => {
  const theme = useTheme();
  const { t, i18n } = useTranslation(["common", "countries"]);
  const getMetadataValueTranslation = useGetMetadataValueTranslation();
  const metadataSummaryFields = [
    {
      field: "description",
      heading: t("metadata.headings.description"),
      noMetadataAvailable: t("metadata.no_metadata_available.description"),
      type: "markdown",
    },
    {
      field: "data_category",
      heading: t("metadata.headings.data_category"),
      noMetadataAvailable: t("metadata.no_metadata_available.data_category"),
      type: "text",
    },
    {
      field: "geographical_code",
      heading: t("metadata.headings.geographical_code"),
      noMetadataAvailable: t("metadata.no_metadata_available.geographical_code"),
      type: "text",
    },
    {
      field: "language_code",
      heading: t("metadata.headings.language_code"),
      noMetadataAvailable: t("metadata.no_metadata_available.language_code"),
      type: "text",
    },
    {
      field: "data_reference_year",
      heading: t("metadata.headings.data_reference_year"),
      noMetadataAvailable: t("metadata.no_metadata_available.data_reference_year"),
      type: "text",
    },
    {
      field: "lineage",
      heading: t("metadata.headings.lineage"),
      noMetadataAvailable: t("metadata.no_metadata_available.lineage"),
      type: "markdown",
    },
    {
      field: "positional_accuracy",
      heading: t("metadata.headings.positional_accuracy"),
      noMetadataAvailable: t("metadata.no_metadata_available.positional_accuracy"),
      type: "text",
    },
    {
      field: "attribute_accuracy",
      heading: t("metadata.headings.attribute_accuracy"),
      noMetadataAvailable: t("metadata.no_metadata_available.attribute_accuracy"),
      type: "text",
    },
    {
      field: "completeness",
      heading: t("metadata.headings.completeness"),
      noMetadataAvailable: t("metadata.no_metadata_available.completeness"),
      type: "text",
    },
    {
      field: "license",
      heading: t("metadata.headings.license"),
      noMetadataAvailable: t("metadata.no_metadata_available.license"),
      type: "text",
    },
    {
      field: "distributor_name",
      heading: t("metadata.headings.distributor_name"),
      noMetadataAvailable: t("metadata.no_metadata_available.distributor_name"),
      type: "text",
    },
    {
      field: "distributor_email",
      heading: t("metadata.headings.distributor_email"),
      noMetadataAvailable: t("metadata.no_metadata_available.distributor_email"),
      type: "email",
    },
    {
      field: "distribution_url",
      heading: t("metadata.headings.distribution_url"),
      noMetadataAvailable: t("metadata.no_metadata_available.distribution_url"),
      type: "url",
    },
    {
      field: "attribution",
      heading: t("metadata.headings.attribution"),
      noMetadataAvailable: t("metadata.no_metadata_available.attribution"),
      type: "text",
    },
  ];

  // Catalog layers keep their metadata in record_jsonb (OGC API Records); the flat
  // columns are a legacy fallback that harvested layers leave empty. Resolve each
  // display field from record_jsonb.properties first, then the flat column — same
  // pattern as DatasetGroupOverview, so the detail page matches the catalog cards.
  const props = (dataset as { record_jsonb?: { properties?: Record<string, unknown> } }).record_jsonb
    ?.properties;
  const fromRecord = (field: string): unknown => {
    if (!props) return undefined;
    switch (field) {
      case "data_category":
        return (props.themes as { concepts?: { id?: string }[] }[] | undefined)?.[0]?.concepts?.[0]?.id;
      case "language_code":
        return props.language;
      case "data_reference_year":
        return ((props.extent as { temporal?: { interval?: unknown[][] } } | undefined)?.temporal
          ?.interval)?.[0]?.[0];
      case "distributor_name":
        return (props.publisher as { name?: string } | undefined)?.name;
      case "distributor_email":
        return (props.publisher as { email?: string } | undefined)?.email;
      case "distribution_url": {
        // Prefer the source PACKAGE page (CKAN "via" link, e.g. ckan.govdata.de/dataset/…),
        // then a resource download (enclosure), then publisher URL.
        const links = (props.links as { rel?: string; href?: string }[] | undefined) ?? [];
        return (
          links.find((l) => l.rel === "via")?.href ??
          links.find((l) => l.rel === "enclosure")?.href ??
          (props.publisher as { url?: string } | undefined)?.url
        );
      }
      default:
        return props[field]; // description, license, geographical_code, lineage, attribution, …
    }
  };
  const getField = (field: string): string => {
    const v = fromRecord(field);
    return (v !== undefined && v !== null && v !== "" ? v : dataset[field]) as string;
  };

  const hasAnyMetadata = metadataSummaryFields.some(({ field }) => !!getField(field));
  const shouldRenderMetadataSection = !hideEmpty || hasAnyMetadata;

  return (
    <ContainerWrapper>
      <LayoutContainer>
        {shouldRenderMetadataSection && (
          <MetadataSection>
            <Stack spacing={4} sx={{ width: "100%" }}>
              {metadataSummaryFields.map(({ field, heading, noMetadataAvailable, type }) => {
                if (hideEmpty && !getField(field)) return null;
                return (
                  <Stack key={field} spacing={1}>
                    <Typography variant="caption">{heading}</Typography>
                    <Divider />
                    {!getField(field) && (
                      <Typography variant="body2" sx={{ fontStyle: "italic" }}>
                        {noMetadataAvailable}
                      </Typography>
                    )}
                    {type === "markdown" && getField(field) && (
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
                        {getField(field)}
                      </ReactMarkdown>
                    )}
                    {type === "email" && getField(field) && (
                      <Link href={`mailto:${getField(field)}`} target="_blank" rel="noopener noreferrer">
                        {getField(field)}
                      </Link>
                    )}
                    {type === "url" && getField(field) && (
                      <Link href={getField(field)} target="_blank" rel="noopener noreferrer">
                        {getField(field)}
                      </Link>
                    )}
                    {type === "text" && getField(field) && <Typography>{getField(field)}</Typography>}
                  </Stack>
                );
              })}
            </Stack>
          </MetadataSection>
        )}

        {!hideMainSection && (
          <MainContentSection>
            <Stack spacing={2}>
              {Object.keys(datasetMetadataAggregated.shape).map((key) => (
                <div key={key} style={{ display: "flex", gap: "8px", alignItems: "center" }}>
                  <Icon
                    iconName={METADATA_HEADER_ICONS[key]}
                    style={{ fontSize: 14, flexShrink: 0 }}
                    htmlColor={theme.palette.text.secondary}
                  />
                  <div style={{ minWidth: 0 }}>
                    <Typography variant="caption" noWrap>
                      {i18n.exists(`common:metadata.headings.${key}`)
                        ? t(`common:metadata.headings.${key}`)
                        : key}
                    </Typography>
                    <Typography variant="body2" fontWeight="bold" noWrap>
                      {getMetadataValueTranslation(key, getField(key))}
                    </Typography>
                  </div>
                </div>
              ))}
            </Stack>
          </MainContentSection>
        )}
      </LayoutContainer>
    </ContainerWrapper>
  );
};

export default DatasetSummary;
