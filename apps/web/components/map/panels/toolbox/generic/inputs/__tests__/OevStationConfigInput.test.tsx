import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { getOevStationConfigPreset } from "@/lib/constants/oev-gueteklassen";
import { stationConfigSchema } from "@/lib/validations/tools";

import OevStationConfigInput from "@/components/map/panels/toolbox/generic/inputs/OevStationConfigInput";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (k: string) => k }) }));

const input = { name: "station_config", title: "Station config" };

describe("OevStationConfigInput", () => {
  it("opens the configuration dialog with its title and the apply action", () => {
    render(<OevStationConfigInput input={input} value={undefined} onChange={vi.fn()} />);

    fireEvent.click(screen.getByRole("button", { name: "oev_station_config" }));
    expect(screen.getByText("oev_station_config_transport_groups")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "apply" })).toBeInTheDocument();
  });

  it("hands the draft configuration back on apply", () => {
    const onChange = vi.fn();
    render(<OevStationConfigInput input={input} value={undefined} onChange={onChange} />);

    fireEvent.click(screen.getByRole("button", { name: "oev_station_config" }));
    fireEvent.click(screen.getByRole("button", { name: "apply" }));
    expect(onChange).toHaveBeenCalledTimes(1);
    expect(onChange.mock.calls[0][0]).toHaveProperty("time_frequency");
  });

  it("keeps the presets valid and rejects a config the tool cannot style", () => {
    (["compact_60", "standard_120", "extended_210"] as const).forEach((preset) => {
      expect(stationConfigSchema.safeParse(getOevStationConfigPreset(preset)).success).toBe(true);
    });

    const tooFewClasses = {
      ...getOevStationConfigPreset("standard_120"),
      time_frequency: [5],
      categories: [{ A: 1, B: 1, C: 2 }],
      classification: { "1": { 300: "1" }, "2": { 300: "2" } },
    };
    expect(stationConfigSchema.safeParse(tooFewClasses).success).toBe(false);
  });
});
