"use client";

import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  Button,
  Container,
  Divider,
  Grid,
  Pagination,
  Paper,
  Skeleton,
  Stack,
  Typography,
  debounce,
  useTheme,
} from "@mui/material";
import { useRouter } from "next/navigation";
import { parseAsArrayOf, parseAsInteger, parseAsString, useQueryState } from "nuqs";
import { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useCatalogLayers, useMetadataAggregated } from "@/lib/api/layers";
import type { PaginatedQueryParams } from "@/lib/validations/common";
import type { GetDatasetSchema } from "@/lib/validations/layer";

import EmptySection from "@/components/common/EmptySection";
import CatalogDatasetCard, { METADATA_HEADER_ICONS } from "@/components/dashboard/catalog/CatalogDatasetCard";
import FilterPanel from "@/components/dashboard/catalog/FilterPanel";
import SpatialFilterSearch from "@/components/dashboard/catalog/SpatialFilterSearch";
import ContentSearchBar from "@/components/dashboard/common/ContentSearchbar";

const CATALOG_FILTER_ORDER = [
  "type",
  "data_category",
  "language_code",
  "distributor_name",
  "license",
] as const;

const Catalog = () => {
  const { t } = useTranslation("common");
  const router = useRouter();
  const theme = useTheme();
  const useQueryStateArray = (key: string) => useQueryState(key, parseAsArrayOf(parseAsString));
  const [typeValue, setTypeValue] = useQueryStateArray("type");
  const [dataCategoryValue, setDataCategoryValue] = useQueryStateArray("data_category");
  const [distributorNameValue, setDistributorNameValue] = useQueryStateArray("distributor_name");
  const [languageCodeValue, setLanguageCodeValue] = useQueryStateArray("language_code");
  const [licenseValue, setLicenseValue] = useQueryStateArray("license");
  const [searchText, setSearchText] = useQueryState("search", parseAsString);
  const [bbox, setBbox] = useQueryState("bbox", parseAsString);
  const [bboxLabel, setBboxLabel] = useQueryState("bbox_label", parseAsString);

  const filterOptions = useMemo(
    () => ({
      type: {
        value: typeValue,
        setValue: setTypeValue,
      },
      data_category: {
        value: dataCategoryValue,
        setValue: setDataCategoryValue,
      },
      distributor_name: {
        value: distributorNameValue,
        setValue: setDistributorNameValue,
      },
      language_code: {
        value: languageCodeValue,
        setValue: setLanguageCodeValue,
      },
      license: {
        value: licenseValue,
        setValue: setLicenseValue,
      },
    }),
    [
      typeValue,
      setTypeValue,
      dataCategoryValue,
      setDataCategoryValue,
      distributorNameValue,
      setDistributorNameValue,
      languageCodeValue,
      setLanguageCodeValue,
      licenseValue,
      setLicenseValue,
    ]
  );
  const datasetSchemaValues = useMemo(() => {
    const keys = CATALOG_FILTER_ORDER;
    const base: GetDatasetSchema = { in_catalog: true };
    if (bbox) base.spatial_search = bbox;
    return keys.reduce((acc, key) => {
      if (filterOptions[key].value && filterOptions[key].value.length > 0) {
        acc[key] = filterOptions[key].value;
      }
      return acc;
    }, base);
  }, [filterOptions, bbox]);

  const [queryParamPage, setQueryParamPage] = useQueryState("page", parseAsInteger.withDefault(1));

  const [datasetSchema, setDatasetSchema] = useState<GetDatasetSchema>(datasetSchemaValues);
  const [queryParams, setQueryParams] = useState<PaginatedQueryParams>({
    order: "descendent",
    order_by: "updated_at",
    size: 10,
    page: queryParamPage || 1,
  });
  const { metadata, isLoading: filtersLoading } = useMetadataAggregated(datasetSchema);
  const { layers: datasets, isLoading: datasetsLoading } = useCatalogLayers(queryParams, datasetSchema);

  const resetPage = useCallback(() => {
    setQueryParamPage(1);
    setQueryParams({
      ...queryParams,
      page: 1,
    });
  }, [queryParams, setQueryParamPage]);

  const handleToggle = useCallback(
    (filterType: string, value: string) => {
      resetPage();
      const setFilterValues = filterOptions[filterType].setValue;
      const filterValues = filterOptions[filterType].value || [];
      const currentIndex = filterValues.indexOf(value);
      const newChecked = [...filterValues];
      if (currentIndex === -1) {
        newChecked.push(value);
      } else {
        newChecked.splice(currentIndex, 1);
      }
      setFilterValues(newChecked?.length ? newChecked : null);
      const newDatasetSchema = { ...datasetSchema };
      if (newChecked?.length > 0) {
        newDatasetSchema[filterType] = newChecked;
      } else {
        delete newDatasetSchema[filterType];
      }
      setDatasetSchema(newDatasetSchema);
    },
    [datasetSchema, filterOptions, resetPage]
  );

  const debouncedSetSearchText = debounce((value) => {
    resetPage();
    setSearchText(value || null);
    const newDatasetSchema = { ...datasetSchema };
    if (value) {
      newDatasetSchema.search = value;
    } else {
      delete newDatasetSchema.search;
    }
    setDatasetSchema(newDatasetSchema);
  }, 500);

  const handleBboxChange = useCallback(
    (newBbox: string | null, label: string) => {
      resetPage();
      setBbox(newBbox);
      setBboxLabel(label || null);
      const newDatasetSchema = { ...datasetSchema };
      if (newBbox) {
        newDatasetSchema.spatial_search = newBbox;
      } else {
        delete newDatasetSchema.spatial_search;
      }
      setDatasetSchema(newDatasetSchema);
    },
    [datasetSchema, resetPage, setBbox, setBboxLabel]
  );

  return (
    <Container sx={{ py: 10, px: 10 }} maxWidth="xl">
      <Box
        sx={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          mb: 8,
        }}>
        <Typography variant="h6">{t("catalog")}</Typography>
      </Box>
      <Grid container justifyContent="space-between" spacing={4}>
        <Grid item xs={3}>
          <Paper elevation={3}>
            <Stack sx={{ mt: 0 }}>
              <Accordion elevation={0} disableGutters defaultExpanded>
                <AccordionSummary
                  expandIcon={<ExpandMoreIcon />}
                  aria-controls="region-panel-content"
                  id="region-panel-header">
                  <Stack direction="row" sx={{ py: 0, pl: 1 }} alignItems="center" spacing={4}>
                    <Icon
                      sx={{ ml: 2 }}
                      iconName={ICON_NAME.GLOBE}
                      fontSize="small"
                      htmlColor={theme.palette.text.secondary}
                    />
                    <Typography variant="body1">{t("region")}</Typography>
                  </Stack>
                </AccordionSummary>
                <Divider sx={{ my: 0, py: 0 }} />
                <AccordionDetails sx={{ p: 2 }}>
                  <SpatialFilterSearch
                    bbox={bbox}
                    bboxLabel={bboxLabel || ""}
                    onBboxChange={handleBboxChange}
                  />
                </AccordionDetails>
              </Accordion>
              <Divider sx={{ py: 0, my: 0 }} />
              {CATALOG_FILTER_ORDER.map((key, index) => {
                return (
                  <Stack key={key}>
                    {index !== 0 && <Divider sx={{ py: 0, my: 0 }} />}
                    <FilterPanel
                      filterValues={filterOptions[key].value}
                      onToggle={(value) => handleToggle(key, value)}
                      filterType={key}
                      values={metadata ? metadata[key] : []}
                      isLoading={filtersLoading}
                      icon={METADATA_HEADER_ICONS[key]}
                    />
                  </Stack>
                );
              })}
            </Stack>
          </Paper>
        </Grid>
        <Grid item xs={9}>
          <Stack spacing={2}>
            <ContentSearchBar
              contentType="layer"
              searchText={searchText || ""}
              onSearchTextChange={(text) => {
                debouncedSetSearchText(text);
              }}
            />
            <Stack direction="row">
              {datasets && (
                <>
                  <Typography variant="body1" fontWeight="bold">
                    {`${datasets?.total} ${t("datasets")}`}
                  </Typography>
                </>
              )}
            </Stack>

            {datasets && datasets?.total > 0 && <Divider />}

            {datasets && datasets?.total === 0 && (
              <Stack sx={{ mt: 10 }} alignItems="center" spacing={4}>
                <EmptySection label={t("no_catalog_dataset_found")} icon={ICON_NAME.DATABASE} />
                <Typography variant="body1">{t("try_different_filters")}</Typography>
                <Stack spacing={2} direction="column">
                  <Divider />
                  <Typography variant="body1">{t("common:no_catalog_dataset_found_description")}</Typography>
                </Stack>
                <Button
                  variant="outlined"
                  color="primary"
                  sx={{ mt: 2 }}
                  onClick={() => {
                    window.open("https://plan4better.de/en/contact/", "_blank");
                  }}>
                  <Typography variant="body1" fontWeight="bold" color="inherit">
                    {t("contact_us")}
                  </Typography>
                </Button>
              </Stack>
            )}

            {datasetsLoading && !datasets && (
              <Stack spacing={4} direction="column" width="100%">
                {Array(queryParams.size)
                  .fill(0)
                  .map((_, index) => (
                    <Skeleton key={index} variant="rectangular" height={200} />
                  ))}
              </Stack>
            )}
            <Stack direction="column" spacing={4}>
              {!datasetsLoading &&
                datasets &&
                datasets?.items.length > 0 &&
                datasets.items.map((dataset) => (
                  <CatalogDatasetCard
                    key={dataset.id}
                    dataset={dataset}
                    onClick={(selectedDataset) => {
                      const dists = ((selectedDataset.other_properties as Record<string, unknown> | undefined)
                        ?.distributions as Array<{ id?: string }> | undefined) ?? [];
                      if (dists.length > 1) {
                        router.push(`/datasets/${selectedDataset.id}`);
                      } else {
                        const layerId = dists[0]?.id || selectedDataset.id;
                        router.push(`/datasets/${layerId}`);
                      }
                    }}
                  />
                ))}

              {!datasetsLoading && datasets && datasets?.items.length > 0 && (
                <Stack direction="row" justifyContent="center" alignItems="center" sx={{ p: 4 }}>
                  <Pagination
                    count={datasets.pages || 1}
                    size="large"
                    page={queryParams.page || 1}
                    onChange={(_e, page) => {
                      setQueryParamPage(page);
                      setQueryParams({
                        ...queryParams,
                        page,
                      });
                    }}
                  />
                </Stack>
              )}
            </Stack>
          </Stack>
        </Grid>
      </Grid>
    </Container>
  );
};

export default Catalog;
