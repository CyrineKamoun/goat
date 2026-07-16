import { act, fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("react-i18next", () => {
  // Stable `t` reference (real react-i18next keeps `t` stable across renders);
  // a fresh `t` each call would churn the component's memoized callbacks.
  const t = (k: string) => k;
  return { useTranslation: () => ({ t }) };
});

const fakeMap = {
  getStyle: () => ({ sources: { osm: { attribution: "© MapTiler © OpenStreetMap contributors" } } }),
  on: vi.fn(),
  off: vi.fn(),
};
vi.mock("react-map-gl/maplibre", () => ({
  useMap: () => ({ map: fakeMap }),
}));

import AttributionControl from "../Attribution";

// Force overflow: capture the ResizeObserver callback and make scrollWidth > clientWidth.
let roCallback: (() => void) | null = null;
beforeEach(() => {
  roCallback = null;
  vi.stubGlobal(
    "ResizeObserver",
    class {
      constructor(cb: () => void) {
        roCallback = cb;
      }
      observe() {}
      disconnect() {}
      unobserve() {}
    },
  );
});

describe("AttributionControl", () => {
  it("renders the localized GOAT credit and 'Data from' with the source credit, without MapLibre", () => {
    render(<AttributionControl />);
    // `t` is mocked to echo the key, so the branding label renders as its key.
    expect(screen.getByText(/made_with_goat/)).toBeInTheDocument();
    expect(screen.getByText(/data_from/)).toBeInTheDocument();
    expect(screen.getByText(/OpenStreetMap contributors/)).toBeInTheDocument();
    expect(screen.queryByText(/MapLibre/i)).not.toBeInTheDocument();
  });

  it("shows a 'more' link on overflow that opens the attributions modal", () => {
    const { container } = render(<AttributionControl />);
    const textEl = container.querySelector("[data-testid='attribution-text']") as HTMLElement;
    Object.defineProperty(textEl, "scrollWidth", { configurable: true, value: 500 });
    Object.defineProperty(textEl, "clientWidth", { configurable: true, value: 100 });
    act(() => {
      roCallback?.();
    });

    const moreLink = screen.getByText("show_more");
    expect(moreLink).toBeInTheDocument();
    fireEvent.click(moreLink);

    // Dialog open: title + credit listed
    expect(screen.getByText("attributions")).toBeInTheDocument();
    expect(screen.getAllByText(/OpenStreetMap contributors/).length).toBeGreaterThan(0);
  });
});
