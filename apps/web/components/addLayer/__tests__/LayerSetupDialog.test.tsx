import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import LayerSetupDialog from "@/components/addLayer/LayerSetupDialog";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (k: string) => k }),
  Trans: ({ i18nKey }: { i18nKey: string }) => <span>{i18nKey}</span>,
}));

describe("LayerSetupDialog", () => {
  it("renders the title, the confirm action and the settings it is handed", () => {
    render(
      <LayerSetupDialog open fileName="stops.xlsx" onClose={vi.fn()} onSave={vi.fn()}>
        <div>column settings</div>
      </LayerSetupDialog>
    );

    expect(screen.getByText("upload_set_up_columns")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "confirm" })).toBeInTheDocument();
    expect(screen.getByText("column settings")).toBeInTheDocument();
  });

  it("saves from the confirm action and closes from cancel", () => {
    const onSave = vi.fn();
    const onClose = vi.fn();
    render(
      <LayerSetupDialog open fileName="stops.xlsx" onClose={onClose} onSave={onSave}>
        <div>column settings</div>
      </LayerSetupDialog>
    );

    fireEvent.click(screen.getByRole("button", { name: "confirm" }));
    expect(onSave).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole("button", { name: "cancel" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
