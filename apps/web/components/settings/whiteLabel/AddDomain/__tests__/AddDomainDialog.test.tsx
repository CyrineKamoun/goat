import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { AddDomainDialog } from "@/components/settings/whiteLabel/AddDomain/AddDomainDialog";

const { createCustomDomainMock, recheckCustomDomainMock, useOrganizationDomainMock } = vi.hoisted(() => ({
  createCustomDomainMock: vi.fn(),
  recheckCustomDomainMock: vi.fn(),
  useOrganizationDomainMock: vi.fn(),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string, fallback?: string) => fallback ?? key }),
}));
vi.mock("react-toastify", () => ({ toast: { error: vi.fn() } }));
vi.mock("@/lib/api/customDomains", () => ({
  createCustomDomain: createCustomDomainMock,
  recheckCustomDomain: recheckCustomDomainMock,
  useOrganizationDomain: useOrganizationDomainMock,
}));
// `StepConfigureDns` mounts `DnsRecordCard`, which pulls the CNAME target
// through this hook — irrelevant to the dialog's own chrome, and its default
// fetcher reaches for a next-auth session this environment has none of.
vi.mock("@/lib/api/customDomainConfig", () => ({
  useCustomDomainConfig: () => ({ config: undefined, isLoading: false }),
}));

const createdDomain = {
  id: "d1",
  organization_id: "org-1",
  base_domain: "dashboards.example.com",
  kind: "single" as const,
  dns_status: "pending" as const,
  dns_status_message: null,
  dns_last_checked_at: null,
  cert_status: "pending" as const,
  cert_status_message: null,
  created_at: "2026-01-01T00:00:00Z",
};

describe("AddDomainDialog", () => {
  beforeEach(() => {
    createCustomDomainMock.mockReset();
    recheckCustomDomainMock.mockReset().mockResolvedValue(undefined);
    useOrganizationDomainMock.mockReset().mockReturnValue({ domain: undefined });
  });

  it("opens on the enter step with the domain form's Continue disabled until a valid hostname is typed", async () => {
    render(<AddDomainDialog open onClose={vi.fn()} organizationId="org-1" />);

    expect(screen.getByText("Add custom domain")).toBeInTheDocument();
    const continueButton = screen.getByRole("button", { name: "Continue" }) as HTMLButtonElement;
    expect(continueButton.disabled).toBe(true);

    await userEvent.type(screen.getByLabelText("Domain name"), "dashboards.example.com");
    expect(continueButton.disabled).toBe(false);
  });

  it("advances to the Configure DNS step once a domain is created", async () => {
    createCustomDomainMock.mockResolvedValue(createdDomain);

    render(<AddDomainDialog open onClose={vi.fn()} organizationId="org-1" />);

    await userEvent.type(screen.getByLabelText("Domain name"), "dashboards.example.com");
    await userEvent.click(screen.getByRole("button", { name: "Continue" }));

    expect(await screen.findByText("Configure DNS")).toBeInTheDocument();
    expect(createCustomDomainMock).toHaveBeenCalledWith("org-1", "dashboards.example.com");
  });

  it("runs Recheck from the footer without leaving the Configure DNS step, then Done closes", async () => {
    createCustomDomainMock.mockResolvedValue(createdDomain);
    const onClose = vi.fn();

    render(<AddDomainDialog open onClose={onClose} organizationId="org-1" />);

    await userEvent.type(screen.getByLabelText("Domain name"), "dashboards.example.com");
    await userEvent.click(screen.getByRole("button", { name: "Continue" }));
    expect(await screen.findByText("Configure DNS")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Recheck now" }));
    expect(recheckCustomDomainMock).toHaveBeenCalledWith("org-1", "d1");
    // Recheck alone never closes the dialog or advances the step.
    expect(onClose).not.toHaveBeenCalled();
    expect(screen.getByText("Configure DNS")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Done" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("submits the Continue button on Enter in the domain field", async () => {
    createCustomDomainMock.mockResolvedValue(createdDomain);

    render(<AddDomainDialog open onClose={vi.fn()} organizationId="org-1" />);

    await userEvent.type(screen.getByLabelText("Domain name"), "dashboards.example.com{Enter}");

    expect(await screen.findByText("Configure DNS")).toBeInTheDocument();
    expect(createCustomDomainMock).toHaveBeenCalledTimes(1);
  });
});
