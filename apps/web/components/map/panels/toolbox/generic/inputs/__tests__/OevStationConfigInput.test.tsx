import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import OevStationConfigInput from "@/components/map/panels/toolbox/generic/inputs/OevStationConfigInput";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (k: string) => k }) }));

const input = { name: "station_config", title: "Station config" };

describe("OevStationConfigInput", () => {
  it("opens the configuration dialog with its title and the apply action", () => {
    render(<OevStationConfigInput input={input} value={undefined} onChange={vi.fn()} />);

    fireEvent.click(screen.getByRole("button", { name: "Haltestellenkonfiguration" }));
    expect(screen.getByText("Verkehrsmittelgruppen")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "apply" })).toBeInTheDocument();
  });

  it("hands the draft configuration back on apply", () => {
    const onChange = vi.fn();
    render(<OevStationConfigInput input={input} value={undefined} onChange={onChange} />);

    fireEvent.click(screen.getByRole("button", { name: "Haltestellenkonfiguration" }));
    fireEvent.click(screen.getByRole("button", { name: "apply" }));
    expect(onChange).toHaveBeenCalledTimes(1);
    expect(onChange.mock.calls[0][0]).toHaveProperty("time_frequency");
  });
});
