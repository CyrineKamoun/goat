"use client";

import { Stack, TextField, Typography } from "@mui/material";
import type { FieldErrors, UseFormRegister } from "react-hook-form";
import { useTranslation } from "react-i18next";

import type { CustomDomainCreate } from "@/lib/validations/customDomain";

interface StepEnterProps {
  /** The `id` the dialog's footer Continue button submits via `primaryForm`. */
  formId: string;
  register: UseFormRegister<CustomDomainCreate>;
  errors: FieldErrors<CustomDomainCreate>;
  /** Defensive only: a submit-type button associated by `form` (rather than
   * DOM nesting) is what Enter and a click both drive, so this native
   * `onSubmit` is not expected to fire — kept as a fallback all the same. */
  onSubmit: () => void;
}

export function StepEnter({ formId, register, errors, onSubmit }: StepEnterProps) {
  const { t } = useTranslation("common");

  return (
    <Stack
      component="form"
      id={formId}
      spacing={3}
      onSubmit={(event) => {
        event.preventDefault();
        onSubmit();
      }}>
      <Typography variant="body2" color="text.secondary">
        {t(
          "white_label_add_domain_enter_description",
          "Enter the subdomain you want to point at your dashboard."
        )}
      </Typography>
      <TextField
        autoFocus
        fullWidth
        label={t("white_label_add_domain_domain_name", "Domain name")}
        placeholder="dashboards.example.com"
        {...register("base_domain")}
        error={!!errors.base_domain}
        helperText={errors.base_domain?.message}
      />
    </Stack>
  );
}
