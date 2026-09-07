"use client";

import { Alert, Stack, Typography } from "@mui/material";
import { useTranslation } from "react-i18next";

import type { CustomDomain } from "@/lib/validations/customDomain";

import { DnsRecordCard } from "./DnsRecordCard";

interface StepConfigureDnsProps {
  domain: CustomDomain;
}

/** The DNS instructions alone — "Recheck now" and "Done" now live in the
 * dialog's shared footer. */
export function StepConfigureDns({ domain }: StepConfigureDnsProps) {
  const { t } = useTranslation("common");

  return (
    <Stack spacing={3}>
      <Typography variant="body2" color="text.secondary">
        {t(
          "white_label_add_domain_configure_description",
          "Add this CNAME record at your DNS provider. We'll automatically detect it once it propagates."
        )}
      </Typography>
      <DnsRecordCard domain={domain.base_domain} />
      <Alert severity="warning" variant="outlined">
        {t(
          "white_label_add_domain_checking_status",
          "Checking DNS every 30 seconds. This can take a few minutes after you update your records."
        )}
      </Alert>
      {domain.dns_status_message && (
        <Typography variant="caption" color="text.secondary">
          {domain.dns_status_message}
        </Typography>
      )}
    </Stack>
  );
}
