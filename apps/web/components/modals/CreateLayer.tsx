import { zodResolver } from "@hookform/resolvers/zod";
import LoadingButton from "@mui/lab/LoadingButton";
import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Stack,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
  Typography,
  useTheme,
} from "@mui/material";
import { useState } from "react";
import { Controller, useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { createEmptyLayer } from "@/lib/api/layers";
import { useJobs } from "@/lib/api/processes";
import { setRunningJobIds } from "@/lib/store/jobs/slice";
import type { CreateEmptyLayerInput, FieldDefinition } from "@/lib/validations/layer";
import { createEmptyLayerSchema } from "@/lib/validations/layer";

import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import FieldEditor from "@/components/common/FieldEditor";

interface CreateLayerModalProps {
  open: boolean;
  onClose: () => void;
  projectId: string;
}

type GeometryToggleValue = "point" | "line" | "polygon" | "table";

const geometryOptions: { value: GeometryToggleValue; icon: ICON_NAME; label: string }[] = [
  { value: "point", icon: ICON_NAME.POINT_FEATURE, label: "Point" },
  { value: "line", icon: ICON_NAME.LINE_FEATURE, label: "Line" },
  { value: "polygon", icon: ICON_NAME.POLYGON_FEATURE, label: "Polygon" },
  { value: "table", icon: ICON_NAME.TABLE, label: "Table" },
];

const CreateLayerModal: React.FC<CreateLayerModalProps> = ({ open, onClose, projectId }) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const dispatch = useAppDispatch();
  const runningJobIds = useAppSelector((state) => state.jobs.runningJobIds);
  const { mutate } = useJobs({ read: false });

  const [step, setStep] = useState<0 | 1>(0);
  const [isBusy, setIsBusy] = useState(false);
  const [selectedFieldId, setSelectedFieldId] = useState<string | null>(null);

  const {
    register,
    control,
    handleSubmit,
    setValue,
    watch,
    reset,
    formState: { errors, isValid },
  } = useForm<CreateEmptyLayerInput>({
    mode: "onChange",
    resolver: zodResolver(createEmptyLayerSchema),
    defaultValues: {
      name: t("untitled_layer"),
      geometryType: "point",
      fields: [],
    },
  });

  const fields = watch("fields");
  // Track toggle value separately since MUI ToggleButtonGroup can't handle null
  const geometryType = watch("geometryType");
  const toggleValue: GeometryToggleValue = geometryType === null ? "table" : geometryType;

  const handleToggleChange = (_: React.MouseEvent<HTMLElement>, newValue: GeometryToggleValue | null) => {
    // ToggleButtonGroup can send null when clicking the already-selected button
    if (newValue === null) return;
    setValue("geometryType", newValue === "table" ? null : newValue, { shouldValidate: true });
  };

  const handleFieldsChange = (updatedFields: FieldDefinition[]) => {
    setValue("fields", updatedFields, { shouldValidate: true });
  };

  const handleClose = () => {
    setStep(0);
    setIsBusy(false);
    setSelectedFieldId(null);
    reset();
    onClose();
  };

  const onSubmit = async (data: CreateEmptyLayerInput) => {
    try {
      setIsBusy(true);
      const response = await createEmptyLayer(
        {
          name: data.name,
          geometry_type: data.geometryType,
          fields: data.fields.map((f) => ({ name: f.name, type: f.type })),
        },
        projectId
      );

      const jobId = response?.jobID;
      if (jobId) {
        mutate();
        dispatch(setRunningJobIds([...runningJobIds, jobId]));
      }
      toast.info(t("creating_layer"));
    } catch (error) {
      toast.error(t("error_creating_layer"));
      console.error("error", error);
    } finally {
      handleClose();
    }
  };

  return (
    <>
      {/* Step 1: Name & Geometry Type */}
      {step === 0 && (
        <Dialog open={open} onClose={handleClose} fullWidth maxWidth="sm">
          <DialogTitle>{t("create_layer")}</DialogTitle>
          <DialogContent>
            <Stack spacing={6} sx={{ mt: 1 }}>
              <Box>
                <Typography variant="body2" fontWeight="bold" sx={{ mb: 1 }}>
                  {t("layer_name")}
                </Typography>
                <TextField
                  fullWidth
                  required
                  size="small"
                  {...register("name")}
                  error={!!errors.name}
                  helperText={errors.name?.message}
                  autoFocus
                />
              </Box>

              <Box>
                <Typography variant="body2" fontWeight="bold" sx={{ mb: 1 }}>
                  {t("geometry_type")}
                </Typography>
                <Controller
                  name="geometryType"
                  control={control}
                  render={() => (
                    <ToggleButtonGroup
                      value={toggleValue}
                      exclusive
                      onChange={handleToggleChange}
                      fullWidth
                      size="small">
                      {geometryOptions.map((opt) => (
                        <ToggleButton
                          key={opt.value}
                          value={opt.value}
                          sx={{ textTransform: "none", py: 1 }}>
                          <Stack direction="row" spacing={1} alignItems="center">
                            <Icon
                              iconName={opt.icon}
                              style={{ fontSize: 14 }}
                              htmlColor={
                                toggleValue === opt.value
                                  ? theme.palette.primary.main
                                  : theme.palette.text.secondary
                              }
                            />
                            <Typography variant="body2">{opt.label}</Typography>
                          </Stack>
                        </ToggleButton>
                      ))}
                    </ToggleButtonGroup>
                  )}
                />
              </Box>
            </Stack>
          </DialogContent>
          <DialogActions sx={{ px: 3, pb: 2 }}>
            <Button onClick={handleClose} variant="text">
              <Typography variant="body2" fontWeight="bold">
                {t("cancel")}
              </Typography>
            </Button>
            <Button
              onClick={() => {
                // Auto-add a default "name" field if no fields exist yet
                if (fields.length === 0) {
                  const defaultField: FieldDefinition = {
                    id: crypto.randomUUID(),
                    name: "name",
                    type: "string",
                  };
                  setValue("fields", [defaultField], { shouldValidate: true });
                  setSelectedFieldId(defaultField.id);
                }
                setStep(1);
              }}
              variant="outlined"
              color="primary"
              disabled={!watch("name")}>
              <Typography variant="body2" fontWeight="bold" color="inherit">
                {t("next")}
              </Typography>
            </Button>
          </DialogActions>
        </Dialog>
      )}

      {/* Step 2: Define Fields */}
      {step === 1 && (
        <Dialog open={open} onClose={handleClose} fullWidth maxWidth="md">
          <DialogTitle>{t("define_fields")}</DialogTitle>
          <DialogContent sx={{ p: 0 }}>
            <FieldEditor
              fields={fields}
              onChange={handleFieldsChange}
              selectedFieldId={selectedFieldId}
              onSelectField={setSelectedFieldId}
            />
          </DialogContent>
          <DialogActions
            sx={{
              "&.MuiDialogActions-root": {
                px: 3,
                py: 2,
                borderTop: `1px solid ${theme.palette.divider}`,
              },
              justifyContent: "space-between",
            }}>
            <Button variant="text" onClick={() => setStep(0)}>
              <Typography variant="body2" fontWeight="bold">
                {t("back")}
              </Typography>
            </Button>
            <Stack direction="row" spacing={2}>
              <Button onClick={handleClose} variant="text">
                <Typography variant="body2" fontWeight="bold">
                  {t("cancel")}
                </Typography>
              </Button>
              <LoadingButton
                onClick={handleSubmit(onSubmit)}
                disabled={!isValid || isBusy}
                loading={isBusy}
                variant="contained"
                color="primary">
                <Typography variant="body2" fontWeight="bold" color="inherit">
                  {t("create_layer")}
                </Typography>
              </LoadingButton>
            </Stack>
          </DialogActions>
        </Dialog>
      )}
    </>
  );
};

export default CreateLayerModal;
