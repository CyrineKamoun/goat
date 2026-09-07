import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import WhiteLabelAnalyticsPage from "@/app/(dashboard)/settings/organization/white-label/analytics/page";

const { useOrganizationMock, useOrganizationAnalyticsMock, useOrganizationAnalyticsDashboardsMock } =
  vi.hoisted(() => ({
    useOrganizationMock: vi.fn(),
    useOrganizationAnalyticsMock: vi.fn(),
    useOrganizationAnalyticsDashboardsMock: vi.fn(),
  }));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string, fallback?: string) => fallback ?? key }),
}));
vi.mock("react-toastify", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));
vi.mock("@/lib/api/users", () => ({ useOrganization: () => useOrganizationMock() }));
vi.mock("@/lib/api/organizationAnalytics", () => ({
  useOrganizationAnalytics: (...args: unknown[]) => useOrganizationAnalyticsMock(...args),
  useOrganizationAnalyticsDashboards: (...args: unknown[]) => useOrganizationAnalyticsDashboardsMock(...args),
  createOrganizationAnalytics: vi.fn(),
  updateOrganizationAnalytics: vi.fn(),
  deleteOrganizationAnalytics: vi.fn(),
  setAnalyticsDashboards: vi.fn(),
}));

beforeEach(() => {
  useOrganizationMock.mockReset().mockReturnValue({ organization: { id: "org-1" }, isLoading: false });
  useOrganizationAnalyticsMock.mockReset().mockReturnValue({
    analyticsList: [],
    isLoading: false,
    mutate: vi.fn(),
  });
  useOrganizationAnalyticsDashboardsMock.mockReset().mockReturnValue({
    dashboards: [],
    isLoading: false,
    mutate: vi.fn(),
  });
});

describe("WhiteLabelAnalyticsPage", () => {
  it("opens the Add analytics dialog with Save disabled until the required fields are valid", async () => {
    render(<WhiteLabelAnalyticsPage />);

    await userEvent.click(screen.getByRole("button", { name: "Add analytics" }));

    expect(screen.getByText("Add analytics", { selector: "div" })).toBeInTheDocument();
    const save = screen.getByRole("button", { name: "Save" }) as HTMLButtonElement;
    expect(save.disabled).toBe(true);

    await userEvent.type(screen.getByLabelText(/^Name/), "Client XY Matomo");
    await userEvent.type(screen.getByLabelText(/^Matomo URL/), "https://matomo.example.org/");
    await userEvent.type(screen.getByLabelText(/^Site ID/), "5");

    expect(save.disabled).toBe(false);
  });
});
