import { zodResolver } from "@hookform/resolvers/zod";
import { Box, Divider, Stack, TextField, Typography } from "@mui/material";
import { useEffect, useState } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";
import { mutate } from "swr";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { type BundleDatasetMetadata, isBundleTile, updateBundle, useBundle } from "@/lib/api/bundles";
import { matchesContentListKey } from "@/lib/api/datasets";
import { updateDataset } from "@/lib/api/layers";
import { PROJECTS_API_BASE_URL, updateProject } from "@/lib/api/projects";
import { BUNDLE_METADATA_KEYS, type BundleMetadata, bundleMetadataSchema } from "@/lib/validations/bundle";
import { layerMetadataSchema } from "@/lib/validations/layer";

import type { ContentDialogBaseProps } from "@/types/dashboard/content";

import { useContentMetadataHooks } from "@/hooks/map/ContentMetadataHooks";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";
import { RhfAutocompleteField } from "@/components/common/form-inputs/AutocompleteField";

interface MetadataDialogProps extends ContentDialogBaseProps {}

const Metadata: React.FC<MetadataDialogProps> = ({ open, onClose, content, type }) => {
  const { t } = useTranslation("common");
  const [isBusy, setIsBusy] = useState(false);
  // A layer, a project and a bundle all edit name and description here; only a
  // bundle also states where its data came from, so the form is typed on the
  // widest of the three and the provenance inputs render for bundles alone.
  const isBundle = isBundleTile(content);
  const {
    handleSubmit,
    register,
    reset,
    formState: { errors, isValid },
    control,
  } = useForm<BundleMetadata>({
    mode: "onChange",
    resolver: zodResolver(isBundle ? bundleMetadataSchema : layerMetadataSchema),
    // The form is flat; a stored row carries the document. Spreading it over
    // the top level seeds the inputs without the form knowing either shape.
    defaultValues: {
      ...content,
      ...((content as { dataset_metadata?: Record<string, unknown> }).dataset_metadata ?? {}),
    },
  });

  // Callers pass whatever they hold, and a content tile carries no provenance
  // (the grid listing omits it), so the authoritative row is fetched and the
  // form re-seeded. Only the fields this form owns: resetting folder_id would
  // make every save look like a folder move.
  const { bundle } = useBundle(isBundle ? content.id : null);
  useEffect(() => {
    if (!bundle) return;
    // The form is flat for both kinds; a bundle's provenance is stored as one
    // document, so it is unpacked here and packed again on submit.
    const provenance = bundle.dataset_metadata ?? {};
    reset({
      name: bundle.name,
      description: bundle.description ?? undefined,
      geographical_code: provenance.geographical_code ?? undefined,
      data_reference_year: provenance.data_reference_year ?? undefined,
      lineage: provenance.lineage ?? undefined,
      license: (provenance.license ?? undefined) as BundleMetadata["license"],
      attribution: provenance.attribution ?? undefined,
      distributor_name: provenance.distributor_name ?? undefined,
      distributor_email: provenance.distributor_email ?? undefined,
      distribution_url: provenance.distribution_url ?? undefined,
    });
  }, [bundle, reset]);

  const { geographicalCodeOptions } = useContentMetadataHooks();

  const onSubmit = async (data: BundleMetadata) => {
    try {
      setIsBusy(true);
      const cleanedData = Object.fromEntries(
        Object.entries(data).filter(([_, value]) => value !== null && value !== undefined && value !== "")
      );
      // Name and description are the row's own columns. Provenance is a bundle
      // concept — an importer fills it from what the source states about itself
      // — so only the bundle branch below sends it, as a document the API merges
      // into what is stored rather than replacing.
      // Every provenance field the form owns, with an emptied one sent as null:
      // the API merges the document, so a key that is simply absent is a key
      // that keeps its old value, and there would be no way to clear one.
      const provenance = Object.fromEntries(
        BUNDLE_METADATA_KEYS.filter((key) => key in data).map((key) => {
          const value = (data as Record<string, unknown>)[key];
          return [key, value === "" || value === undefined ? null : value];
        })
      );
      const identity = {
        ...(cleanedData.name !== undefined ? { name: cleanedData.name as string } : {}),
        ...(cleanedData.description !== undefined ? { description: cleanedData.description as string } : {}),
      };
      if (isBundle) {
        await updateBundle(content.id, {
          ...identity,
          dataset_metadata: provenance as BundleDatasetMetadata,
        });
        // The detail page reads a single bundle; the grids read the listing.
        mutate(matchesContentListKey);
      } else if (type === "layer") {
        // A layer is its name, description and tags. Publishing one to the
        // catalog will be its own job, not a set of metadata fields here.
        await updateDataset(content.id, {
          folder_id: content.folder_id,
          ...identity,
        });
        mutate(matchesContentListKey);
      } else {
        await updateProject(content.id, {
          folder_id: content.folder_id,
          ...cleanedData,
        });
        mutate((key) => Array.isArray(key) && key[0] === PROJECTS_API_BASE_URL);
      }
      toast.success(t("metadata_updated_success"));
    } catch (error) {
      toast.error(t("metadata_updated_error"));
    } finally {
      setIsBusy(false);
      onClose && onClose();
    }
  };
  return (
    <AppDialog
      open={open}
      onClose={() => onClose?.()}
      icon={ICON_NAME.EDITPEN}
      title={t("edit_metadata")}
      maxWidth={600}
      footer={
        <AppDialogFooter
          onCancel={onClose}
          primaryLabel={t("update")}
          onPrimary={handleSubmit(onSubmit)}
          primaryDisabled={!isValid}
          primaryLoading={isBusy}
        />
      }>
      <Box component="form" onSubmit={handleSubmit(onSubmit)} sx={{ mt: 1, maxHeight: "500px" }}>
        <Stack spacing={4}>
          {type === "layer" && (
            <>
              <Divider />
              <Box>
                <Typography variant="body1" fontWeight="bold">
                  {t("common:metadata.heading_titles.basic")}
                </Typography>
              </Box>
              <Divider />
            </>
          )}

          <TextField
            fullWidth
            label={t("name")}
            {...register("name")}
            error={!!errors.name}
            helperText={errors.name?.message}
          />
          <TextField
            fullWidth
            multiline
            rows={6}
            label={t("description")}
            {...register("description")}
            error={!!errors.description}
            helperText={errors.description?.message}
          />
          {isBundle && (
            <>
              <RhfAutocompleteField
                options={geographicalCodeOptions}
                control={control}
                name="geographical_code"
                label={t("common:metadata.headings.geographical_code")}
              />
              <TextField
                fullWidth
                label={t("common:metadata.headings.data_reference_year")}
                type="number"
                {...register("data_reference_year", {
                  setValueAs: (v) => (v === "" ? undefined : parseInt(v, 10)),
                })}
                error={!!errors.data_reference_year}
                helperText={errors.data_reference_year?.message}
              />
              <Divider />
              <Box>
                <Typography variant="body1" fontWeight="bold">
                  {t("common:metadata.heading_titles.data_quality")}
                </Typography>
              </Box>
              <Divider />
              <TextField
                fullWidth
                multiline
                rows={6}
                label={t("common:metadata.headings.lineage")}
                {...register("lineage")}
                error={!!errors.lineage}
                helperText={errors.lineage?.message}
              />
              <Divider />
              <Box>
                <Typography variant="body1" fontWeight="bold">
                  {t("common:metadata.heading_titles.distribution")}
                </Typography>
              </Box>
              <Divider />
              <TextField
                fullWidth
                label={t("common:metadata.headings.distributor_name")}
                {...register("distributor_name")}
                error={!!errors.distributor_name}
                helperText={errors.distributor_name?.message}
              />
              <TextField
                fullWidth
                label={t("common:metadata.headings.distributor_email")}
                {...register("distributor_email", {
                  setValueAs: (v) => (!v ? undefined : v),
                })}
                error={!!errors.distributor_email}
                helperText={errors.distributor_email?.message}
              />
              <TextField
                fullWidth
                label={t("common:metadata.headings.distribution_url")}
                {...register("distribution_url", {
                  setValueAs: (v) => (!v ? undefined : v),
                })}
                error={!!errors.distribution_url}
                helperText={errors.distribution_url?.message}
              />
              <TextField
                fullWidth
                label={t("common:metadata.headings.license")}
                placeholder="DL-DE-BY-2.0"
                {...register("license")}
                error={!!errors.license}
                helperText={errors.license?.message}
              />
              <TextField
                fullWidth
                label={t("common:metadata.headings.attribution")}
                {...register("attribution")}
                error={!!errors.attribution}
                helperText={errors.attribution?.message}
              />
            </>
          )}
        </Stack>
      </Box>
    </AppDialog>
  );
};

export default Metadata;
