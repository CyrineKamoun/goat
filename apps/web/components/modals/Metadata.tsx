import { zodResolver } from "@hookform/resolvers/zod";
import { LoadingButton } from "@mui/lab";
import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import { useState } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";
import { mutate } from "swr";

import { LAYERS_API_BASE_URL, updateDataset } from "@/lib/api/layers";
import { PROJECTS_API_BASE_URL, updateProject } from "@/lib/api/projects";
import { type LayerMetadata, layerMetadataSchema } from "@/lib/validations/layer";

import type { ContentDialogBaseProps } from "@/types/dashboard/content";

import { useContentMetadataHooks } from "@/hooks/map/ContentMetadataHooks";

import { RhfAutocompleteField } from "@/components/common/form-inputs/AutocompleteField";

interface MetadataDialogProps extends ContentDialogBaseProps {}

type RecordJsonb = {
  time?: { interval?: unknown[][] };
  links?: { rel?: string; href?: string }[];
  properties?: Record<string, unknown>;
};

// Published layers keep their record-owned columns NULL — the record is the
// metadata truth, so the edit form prefills from it (columns as fallback).
const recordDefaults = (content: Record<string, unknown>): Partial<LayerMetadata> => {
  const record = content.record_jsonb as RecordJsonb | undefined;
  const props = record?.properties;
  if (!props) return {};
  const contacts = (props.contacts as { name?: string; roles?: string[]; emails?: { value?: string }[] }[]) ?? [];
  const publisher = contacts.find((c) => (c.roles ?? []).includes("publisher")) ?? contacts[0];
  const t0 = record?.time?.interval?.[0]?.[0];
  const themes = props.themes as { concepts?: { id?: string }[] }[] | undefined;
  const enclosure = record?.links?.find((l) => l.rel === "enclosure")?.href;
  const out = {
    description: props.description,
    license: props.license,
    language_code: (props.language as { code?: string } | undefined)?.code,
    distributor_name: publisher?.name,
    distributor_email: publisher?.emails?.[0]?.value,
    distribution_url: enclosure,
    data_reference_year: typeof t0 === "string" ? Number(t0.slice(0, 4)) : undefined,
    tags: props.keywords,
    data_category: themes?.[0]?.concepts?.[0]?.id,
    geographical_code: props["goat:geographical_code"],
  };
  return Object.fromEntries(
    Object.entries(out).filter(([, v]) => v !== undefined && v !== null)
  ) as Partial<LayerMetadata>;
};

const Metadata: React.FC<MetadataDialogProps> = ({ open, onClose, content, type }) => {
  const { t } = useTranslation("common");
  const [isBusy, setIsBusy] = useState(false);
  const {
    handleSubmit,
    register,
    formState: { errors, isValid },
    control,
  } = useForm<LayerMetadata>({
    mode: "onChange",
    resolver: zodResolver(layerMetadataSchema),
    defaultValues: { ...content, ...recordDefaults(content as Record<string, unknown>) },
  });

  const { dataCategoryOptions, geographicalCodeOptions, licenseOptions, languageCodeOptions } =
    useContentMetadataHooks();

  const onSubmit = async (data: LayerMetadata) => {
    try {
      setIsBusy(true);
      const postMethod = type === "layer" ? updateDataset : updateProject;
      const cleanedData = Object.fromEntries(
        Object.entries(data).filter(([_, value]) => value !== null && value !== undefined && value !== "")
      );
      await postMethod(content.id, {
        folder_id: content.folder_id,
        ...cleanedData,
      });
      const mutateUrl = type === "layer" ? LAYERS_API_BASE_URL : PROJECTS_API_BASE_URL;
      mutate((key) => Array.isArray(key) && key[0] === mutateUrl);
      toast.success(t("metadata_updated_success"));
    } catch (error) {
      toast.error(t("metadata_updated_error"));
    } finally {
      setIsBusy(false);
      onClose && onClose();
    }
  };
  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>{t("edit_metadata")}</DialogTitle>
      <DialogContent>
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
            {type === "layer" && (
              <>
                <RhfAutocompleteField
                  options={dataCategoryOptions}
                  control={control}
                  name="data_category"
                  label={t("common:metadata.headings.data_category")}
                />
                <RhfAutocompleteField
                  options={geographicalCodeOptions}
                  control={control}
                  name="geographical_code"
                  label={t("common:metadata.headings.geographical_code")}
                />
                <RhfAutocompleteField
                  options={languageCodeOptions}
                  control={control}
                  name="language_code"
                  label={t("common:metadata.headings.language_code")}
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
                <TextField
                  fullWidth
                  label={t("common:metadata.headings.positional_accuracy")}
                  {...register("positional_accuracy")}
                  error={!!errors.positional_accuracy}
                  helperText={errors.positional_accuracy?.message}
                />
                <TextField
                  fullWidth
                  label={t("common:metadata.headings.attribute_accuracy")}
                  {...register("attribute_accuracy")}
                  error={!!errors.attribute_accuracy}
                  helperText={errors.attribute_accuracy?.message}
                />
                <TextField
                  fullWidth
                  label={t("common:metadata.headings.completeness")}
                  {...register("completeness")}
                  error={!!errors.completeness}
                  helperText={errors.completeness?.message}
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
                <RhfAutocompleteField
                  options={licenseOptions}
                  control={control}
                  name="license"
                  label={t("common:metadata.headings.license")}
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
      </DialogContent>
      <DialogActions
        disableSpacing
        sx={{
          pb: 2,
          mt: 4,
        }}>
        <Button onClick={onClose} variant="text">
          <Typography variant="body2" fontWeight="bold">
            {t("cancel")}
          </Typography>
        </Button>
        <LoadingButton
          variant="contained"
          disabled={!isValid}
          loading={isBusy}
          onClick={handleSubmit(onSubmit)}>
          <Typography variant="body2" fontWeight="bold" color="inherit">
            {t("update")}
          </Typography>
        </LoadingButton>
      </DialogActions>
    </Dialog>
  );
};

export default Metadata;
