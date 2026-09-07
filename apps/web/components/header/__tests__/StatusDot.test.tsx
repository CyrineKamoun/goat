import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { StatusFeed } from "@/lib/validations/home";

import StatusDot from "@/components/header/StatusDot";

const { useStatusFeedMock } = vi.hoisted(() => ({ useStatusFeedMock: vi.fn() }));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key, i18n: { language: "en" } }),
}));
vi.mock("@/lib/api/status", () => ({ useStatusFeed: useStatusFeedMock }));

const feed = (overrides: Partial<StatusFeed> = {}): StatusFeed => ({
  generatedAt: "2026-09-04T10:00:00.000Z",
  overall: "operational",
  url: "https://status.dev.plan4better.de/goat",
  systems: [],
  incidents: [],
  ...overrides,
});

beforeEach(() => {
  useStatusFeedMock.mockReset();
});

describe("StatusDot", () => {
  it("renders nothing when the feed is unset", () => {
    useStatusFeedMock.mockReturnValue({ status: undefined });

    const { container } = render(<StatusDot />);

    expect(container).toBeEmptyDOMElement();
  });

  it("shows a quiet dot with no label while operational", () => {
    useStatusFeedMock.mockReturnValue({ status: feed() });

    render(<StatusDot />);

    expect(screen.queryByText("status_operational")).not.toBeInTheDocument();
    expect(screen.getByLabelText("status_operational")).toBeInTheDocument();
  });

  it("shows the outage label linking to the status page otherwise", () => {
    useStatusFeedMock.mockReturnValue({ status: feed({ overall: "outage" }) });

    render(<StatusDot />);

    const link = screen.getByRole("link", { name: /status_outage/ }) as HTMLAnchorElement;
    // Read the DOM properties rather than jest-dom's `toHaveAttribute`: the
    // repo's ESLint setup reads it as Playwright's (async) matcher of the
    // same name.
    expect(link.getAttribute("href")).toBe("https://status.dev.plan4better.de/goat");
    expect(link.target).toBe("_blank");
  });
});
