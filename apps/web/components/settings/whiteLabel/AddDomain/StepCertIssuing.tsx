"use client";

import { Alert, Stack, Typography } from "@mui/material";
import { useTranslation } from "react-i18next";

/** The cert-issuing notice alone — "Close" now lives in the dialog's shared
 * footer. */
export function StepCertIssuing() {
  const { t } = useTranslation("common");
  return (
    <Stack spacing={3}>
      <Alert severity="success" variant="outlined">
        <Typography variant="body2">
          {t(
            "white_label_add_domain_cert_issuing",
            "DNS verified. Issuing SSL certificate from Let's Encrypt — usually under 2 minutes."
          )}
        </Typography>
      </Alert>
    </Stack>
  );
}
