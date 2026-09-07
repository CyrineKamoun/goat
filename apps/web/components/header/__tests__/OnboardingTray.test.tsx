import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import OnboardingTray from "@/components/header/OnboardingTray";

const { useHomeStageMock, useHomeCreateMock, pushMock } = vi.hoisted(() => ({
  useHomeStageMock: vi.fn(),
  useHomeCreateMock: vi.fn(),
  pushMock: vi.fn(),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock("next/navigation", () => ({ useRouter: () => ({ push: pushMock }) }));
vi.mock("@/hooks/dashboard/home/useHomeStage", async () => {
  const actual = await vi.importActual<typeof import("@/hooks/dashboard/home/useHomeStage")>(
    "@/hooks/dashboard/home/useHomeStage"
  );
  return { ...actual, useHomeStage: useHomeStageMock };
});
vi.mock("@/hooks/dashboard/home/useHomeCreate", () => ({ useHomeCreate: useHomeCreateMock }));

const baseCreate = { newProject: vi.fn(), addDataset: vi.fn(), browseCatalog: vi.fn(), dialogs: null };

const stage = (overrides: Partial<ReturnType<typeof useHomeStageMock>> = {}) => ({
  stage: "getting_started",
  isLoading: false,
  done: [],
  skipped: false,
  skip: vi.fn(),
  refresh: vi.fn(),
  facts: undefined,
  ...overrides,
});

beforeEach(() => {
  useHomeStageMock.mockReset();
  useHomeCreateMock.mockReset().mockReturnValue(baseCreate);
  pushMock.mockReset();
});

/** The trigger's accessible name is the ring's own "n/6" label, whatever n
 * currently is — matching it by a fixed count would break the moment a test
 * varies `done`. */
const openTray = () => fireEvent.click(screen.getByRole("button", { name: /^\d\/6$/ }));

describe("OnboardingTray", () => {
  it("renders nothing while facts/preferences are still loading", () => {
    useHomeStageMock.mockReturnValue(stage({ stage: undefined, isLoading: true }));

    const { container } = render(<OnboardingTray />);

    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing once established", () => {
    useHomeStageMock.mockReturnValue(
      stage({
        stage: "established",
        done: ["project", "data", "catalog", "analysis", "workflow", "team"],
      })
    );

    const { container } = render(<OnboardingTray />);

    expect(container).toBeEmptyDOMElement();
  });

  it("shows the ring as n of the total step count", () => {
    useHomeStageMock.mockReturnValue(stage({ done: ["project"] }));

    render(<OnboardingTray />);

    expect(screen.getByText("1/6")).toBeInTheDocument();
  });

  it("shows the header's completed-of-total label when open", () => {
    useHomeStageMock.mockReturnValue(stage({ done: ["project"] }));

    render(<OnboardingTray />);
    openTray();

    expect(screen.getByText("steps_completed")).toBeInTheDocument();
  });

  it("renders skip as a plain text link, not a framed MUI button", () => {
    useHomeStageMock.mockReturnValue(stage());

    render(<OnboardingTray />);
    openTray();
    const skipButton = screen.getByRole("button", { name: "skip_onboarding" });

    expect(skipButton.className).not.toMatch(/MuiButton-(contained|outlined)/);
  });

  it("calls skip when the footer's skip button is clicked", () => {
    const skip = vi.fn();
    useHomeStageMock.mockReturnValue(stage({ skip }));

    render(<OnboardingTray />);
    openTray();
    fireEvent.click(screen.getByText("skip_onboarding"));

    expect(skip).toHaveBeenCalled();
  });

  it("strikes through a done step and does not let it be clicked again", () => {
    const newProject = vi.fn();
    useHomeCreateMock.mockReturnValue({ ...baseCreate, newProject });
    useHomeStageMock.mockReturnValue(stage({ done: ["project"] }));

    render(<OnboardingTray />);
    openTray();
    fireEvent.click(screen.getByText("step_project_title"));

    expect(newProject).not.toHaveBeenCalled();
  });

  it("routes an open step's click to its action", () => {
    const newProject = vi.fn();
    useHomeCreateMock.mockReturnValue({ ...baseCreate, newProject });
    useHomeStageMock.mockReturnValue(stage({ done: [] }));

    render(<OnboardingTray />);
    openTray();
    fireEvent.click(screen.getByText("step_project_title"));

    expect(newProject).toHaveBeenCalled();
  });

  it("closes the popper after clicking an open step", async () => {
    useHomeCreateMock.mockReturnValue(baseCreate);
    useHomeStageMock.mockReturnValue(stage({ done: [] }));

    render(<OnboardingTray />);
    openTray();
    fireEvent.click(screen.getByText("step_project_title"));

    await waitFor(() => expect(screen.queryByText("step_data_title")).not.toBeInTheDocument());
  });

  it("routes the team step to Settings > Teams", () => {
    useHomeStageMock.mockReturnValue(stage({ done: ["project", "data", "catalog"] }));

    render(<OnboardingTray />);
    openTray();
    fireEvent.click(screen.getByText("step_team_title"));

    expect(pushMock).toHaveBeenCalledWith("/settings/teams");
  });

  it("routes the analysis step to the Catalog Templates tab", () => {
    useHomeStageMock.mockReturnValue(stage({ done: ["project", "data", "catalog"] }));

    render(<OnboardingTray />);
    openTray();
    fireEvent.click(screen.getByText("run_first_analysis_title"));

    expect(pushMock).toHaveBeenCalledWith("/catalog?tab=templates");
  });

  it("routes the workflow step to the Catalog Templates tab", () => {
    useHomeStageMock.mockReturnValue(stage({ done: ["project", "data", "catalog"] }));

    render(<OnboardingTray />);
    openTray();
    fireEvent.click(screen.getByText("build_workflow_title"));

    expect(pushMock).toHaveBeenCalledWith("/catalog?tab=templates");
  });
});
