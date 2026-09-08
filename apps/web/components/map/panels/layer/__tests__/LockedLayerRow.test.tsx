import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { LockedLayerRow } from "@/components/map/panels/layer/LockedLayerRow";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

describe("LockedLayerRow", () => {
  it("shows the lock icon, the layer's name and the locked hint", () => {
    render(<LockedLayerRow name="Confidential parcels" />);

    expect(screen.getByTestId("locked-layer-row-icon")).toBeTruthy();
    expect(screen.getByText("Confidential parcels")).toBeTruthy();
    expect(screen.getByText("layer_locked_hint")).toBeTruthy();
  });

  it("offers no style, table, zoom or edit affordance", () => {
    render(<LockedLayerRow name="Confidential parcels" />);

    expect(screen.queryByRole("button")).toBeNull();
    ["style", "open_data_table", "zoom_to", "edit_features", "duplicate", "rename"].forEach(
      (key) => {
        expect(screen.queryByText(key)).toBeNull();
      }
    );
  });
});
