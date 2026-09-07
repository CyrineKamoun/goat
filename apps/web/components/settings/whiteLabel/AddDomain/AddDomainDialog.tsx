"use client";

import { zodResolver } from "@hookform/resolvers/zod";
import { LoadingButton } from "@mui/lab";
import { Typography } from "@mui/material";
import { useEffect, useState } from "react";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { createCustomDomain, recheckCustomDomain, useOrganizationDomain } from "@/lib/api/customDomains";
import { customDomainCreateSchema } from "@/lib/validations/customDomain";
import type { CustomDomain, CustomDomainCreate } from "@/lib/validations/customDomain";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

import { StepCertIssuing } from "./StepCertIssuing";
import { StepConfigureDns } from "./StepConfigureDns";
import { StepEnter } from "./StepEnter";

type Step = "enter" | "configure_dns" | "cert_issuing";

/** The `<form>` `StepEnter` renders, submitted by the footer's primary via
 * `primaryForm` — the button lives outside it, in the shared footer. */
const ENTER_FORM_ID = "add-domain-enter-form";

interface AddDomainDialogProps {
  open: boolean;
  onClose: () => void;
  organizationId: string;
  /** Called after the dialog creates a domain so the parent can mutate its list. */
  onCreated?: () => void;
}

export function AddDomainDialog({ open, onClose, organizationId, onCreated }: AddDomainDialogProps) {
  const { t } = useTranslation("common");
  const [step, setStep] = useState<Step>("enter");
  const [createdDomain, setCreatedDomain] = useState<CustomDomain | null>(null);
  const [isBusy, setIsBusy] = useState(false);
  const [isRechecking, setIsRechecking] = useState(false);

  // Owned here rather than by `StepEnter` itself, since its Continue button
  // now lives in the shared footer and needs `handleSubmit`/`isValid` to
  // wire `onPrimary`/`primaryDisabled`.
  const {
    register,
    handleSubmit,
    reset: resetEnterFields,
    formState: { errors, isValid },
  } = useForm<CustomDomainCreate>({
    mode: "onChange",
    resolver: zodResolver(customDomainCreateSchema),
    defaultValues: { base_domain: "" },
  });

  // Poll the single-domain endpoint while the user is configuring DNS or
  // waiting on the cert. SWR ignores `null` keys, so we just gate on the
  // domain id (passing undefined when not needed).
  const shouldPoll = open && createdDomain !== null && step !== "enter";
  const { domain: polledDomain } = useOrganizationDomain(
    shouldPoll ? organizationId : undefined,
    shouldPoll ? createdDomain?.id : undefined,
    { polling: true }
  );

  // Drive transitions off of the polled domain.
  useEffect(() => {
    if (!polledDomain) return;
    setCreatedDomain(polledDomain);
    if (step === "configure_dns" && polledDomain.dns_status === "verified") {
      setStep("cert_issuing");
    }
  }, [polledDomain, step]);

  const reset = () => {
    setStep("enter");
    setCreatedDomain(null);
    setIsBusy(false);
    setIsRechecking(false);
    resetEnterFields();
  };

  const handleClose = () => {
    onClose();
    // Defer state reset to avoid flashing step 1 during dialog close animation.
    window.setTimeout(reset, 200);
  };

  const handleCreate = async (data: CustomDomainCreate) => {
    setIsBusy(true);
    try {
      const created = await createCustomDomain(organizationId, data.base_domain);
      setCreatedDomain(created);
      onCreated?.();
      // If the backend already has a verified DNS (rare; normally pending),
      // skip straight to the cert-issuing screen.
      if (created.dns_status === "verified") {
        setStep("cert_issuing");
      } else {
        setStep("configure_dns");
      }
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : t("white_label_add_domain_create_failed", "Failed to add domain");
      toast.error(message);
    } finally {
      setIsBusy(false);
    }
  };

  const handleRecheck = async () => {
    if (!createdDomain) return;
    setIsRechecking(true);
    try {
      await recheckCustomDomain(organizationId, createdDomain.id);
      // The polled SWR hook will tick on its next interval; nothing else to
      // do here.
    } catch {
      toast.error(t("white_label_add_domain_recheck_failed", "Failed to recheck DNS"));
    } finally {
      setIsRechecking(false);
    }
  };

  const title = (() => {
    switch (step) {
      case "enter":
        return t("white_label_add_domain_title", "Add custom domain");
      case "configure_dns":
        return t("white_label_add_domain_configure_dns", "Configure DNS");
      case "cert_issuing":
        return t("white_label_add_domain_dns_verified", "DNS verified");
    }
  })();

  const footer = (() => {
    if (step === "enter") {
      return (
        <AppDialogFooter
          onCancel={handleClose}
          primaryLabel={t("white_label_add_domain_continue", "Continue")}
          onPrimary={handleSubmit(handleCreate)}
          primaryType="submit"
          primaryForm={ENTER_FORM_ID}
          primaryDisabled={!isValid}
          primaryLoading={isBusy}
        />
      );
    }
    if (step === "configure_dns") {
      return (
        <AppDialogFooter
          primaryLabel={t("done", "Done")}
          onPrimary={handleClose}
          extra={
            <LoadingButton variant="text" loading={isRechecking} onClick={() => void handleRecheck()}>
              <Typography variant="body2" fontWeight="bold">
                {t("white_label_add_domain_recheck_now", "Recheck now")}
              </Typography>
            </LoadingButton>
          }
        />
      );
    }
    return <AppDialogFooter primaryLabel={t("close", "Close")} onPrimary={handleClose} />;
  })();

  return (
    <AppDialog
      open={open}
      onClose={handleClose}
      icon={ICON_NAME.LINK}
      title={title}
      maxWidth={600}
      closeDisabled={isBusy}
      bodySx={{ pt: 2 }}
      footer={footer}>
      {step === "enter" && (
        <StepEnter
          formId={ENTER_FORM_ID}
          register={register}
          errors={errors}
          onSubmit={handleSubmit(handleCreate)}
        />
      )}
      {step === "configure_dns" && createdDomain && <StepConfigureDns domain={createdDomain} />}
      {step === "cert_issuing" && <StepCertIssuing />}
    </AppDialog>
  );
}
