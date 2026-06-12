import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { popupProperties } from "@/lib/validations/layer";

import { PopupAppearanceSettings } from "../PopupAppearanceSettings";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

describe("PopupAppearanceSettings", () => {
  it("renders width/max_height and emits numeric patches", () => {
    const onChange = vi.fn();
    render(<PopupAppearanceSettings popup={popupProperties.parse({})} onChange={onChange} />);

    const width = screen.getByLabelText("popup_width") as HTMLInputElement;
    fireEvent.change(width, { target: { value: "420" } });
    expect(onChange).toHaveBeenCalledWith({ width: 420 });

    const height = screen.getByLabelText("popup_max_height") as HTMLInputElement;
    fireEvent.change(height, { target: { value: "0" } });
    expect(onChange).toHaveBeenCalledWith({ max_height: undefined });
  });
});
