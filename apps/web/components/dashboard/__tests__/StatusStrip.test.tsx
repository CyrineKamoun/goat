import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { StatusFeed } from "@/lib/validations/home";

import StatusStrip from "@/components/dashboard/StatusStrip";

const { useStatusFeedMock } = vi.hoisted(() => ({ useStatusFeedMock: vi.fn() }));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key, i18n: { language: "en" } }),
}));
vi.mock("@/lib/api/status", () => ({ useStatusFeed: useStatusFeedMock }));

/** `severity` values as actually emitted by the status site: "outage",
 * "disrupted" or "notice" — never the literal string "maintenance", which is
 * a system status, not an incident severity (see plan4better/status
 * `SEVERITIES`/`Status`). "disrupted" is the one that maps to the strip's
 * warning tier. */
const incident = (
  overrides: Partial<StatusFeed["incidents"][number]> = {}
): StatusFeed["incidents"][number] => ({
  id: "inc-1",
  severity: "disrupted",
  phase: "monitoring",
  title: "Workspace saves are delayed",
  startedAt: "2026-09-04T10:00:00.000Z",
  affected: ["workspace"],
  latestUpdate: { at: "2026-09-04T10:00:00.000Z", body: "We are monitoring the fix." },
  ...overrides,
});

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
  window.localStorage.clear();
});

describe("StatusStrip", () => {
  it("renders nothing when everything is operational", () => {
    useStatusFeedMock.mockReturnValue({ status: feed() });

    const { container } = render(<StatusStrip />);

    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing when the feed is unset", () => {
    useStatusFeedMock.mockReturnValue({ status: undefined });

    const { container } = render(<StatusStrip />);

    expect(container).toBeEmptyDOMElement();
  });

  it("shows the strip with the incident title for a disrupted incident", () => {
    useStatusFeedMock.mockReturnValue({
      status: feed({ overall: "notice", incidents: [incident()] }),
    });

    render(<StatusStrip />);

    expect(screen.getByText("Workspace saves are delayed")).toBeInTheDocument();
    expect(screen.getByText("status_disrupted")).toBeInTheDocument();
  });

  it("shows an outage incident under the outage label", () => {
    useStatusFeedMock.mockReturnValue({
      status: feed({ overall: "outage", incidents: [incident({ id: "inc-outage", severity: "outage" })] }),
    });

    render(<StatusStrip />);

    expect(screen.getByText("status_outage")).toBeInTheDocument();
  });

  it("hides after the dismiss button is clicked", () => {
    useStatusFeedMock.mockReturnValue({
      status: feed({ overall: "notice", incidents: [incident()] }),
    });

    render(<StatusStrip />);
    fireEvent.click(screen.getByRole("button", { name: "hide_until_next_update" }));

    expect(screen.queryByText("Workspace saves are delayed")).not.toBeInTheDocument();
    expect(window.localStorage.getItem("goat.status.dismissed")).toBe("inc-1");
  });

  it("shows again for a different incident id once the previous one was dismissed", () => {
    window.localStorage.setItem("goat.status.dismissed", "inc-1");
    useStatusFeedMock.mockReturnValue({
      status: feed({
        overall: "notice",
        incidents: [incident({ id: "inc-2", title: "A new disruption" })],
      }),
    });

    render(<StatusStrip />);

    expect(screen.getByText("A new disruption")).toBeInTheDocument();
  });

  it("stays hidden for the same dismissed incident id", () => {
    window.localStorage.setItem("goat.status.dismissed", "inc-1");
    useStatusFeedMock.mockReturnValue({
      status: feed({ overall: "notice", incidents: [incident()] }),
    });

    const { container } = render(<StatusStrip />);

    expect(container).toBeEmptyDOMElement();
  });
});
