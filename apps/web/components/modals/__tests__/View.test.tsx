import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import ViewModal from "@/components/modals/View";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key }) }));

describe("View", () => {
  it("shows the caller's title and children behind a close action", () => {
    render(
      <ViewModal open title="data_source_info" closeText="Got it" onClose={() => {}}>
        <p>Some detail</p>
      </ViewModal>
    );

    expect(screen.getByText("data_source_info")).toBeInTheDocument();
    expect(screen.getByText("Some detail")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Got it" })).toBeInTheDocument();
  });

  it("closes through the footer's action", () => {
    const onClose = vi.fn();
    render(
      <ViewModal open title="data_source_info" closeText="Got it" onClose={onClose}>
        <p>Some detail</p>
      </ViewModal>
    );

    fireEvent.click(screen.getByRole("button", { name: "Got it" }));

    expect(onClose).toHaveBeenCalled();
  });
});
