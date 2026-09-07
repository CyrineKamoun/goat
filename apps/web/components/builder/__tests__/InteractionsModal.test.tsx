import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import InteractionsModal from "@/components/builder/InteractionsModal";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (k: string) => k }) }));

describe("InteractionsModal", () => {
  it("renders the title and the empty state, and adds a rule from the primary action", () => {
    const onChange = vi.fn();
    render(
      <InteractionsModal
        open
        onClose={vi.fn()}
        interactions={[]}
        onChange={onChange}
        panels={[]}
        projectLayers={[]}
        projectLayerGroups={[]}
      />
    );

    expect(screen.getByText("interactions")).toBeInTheDocument();
    expect(screen.getByText("no_interactions_yet")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "add_interaction" }));
    expect(onChange).toHaveBeenCalledTimes(1);
    expect(onChange.mock.calls[0][0]).toHaveLength(1);
  });

  it("closes from the header close button", () => {
    const onClose = vi.fn();
    render(
      <InteractionsModal
        open
        onClose={onClose}
        interactions={[]}
        onChange={vi.fn()}
        panels={[]}
        projectLayers={[]}
        projectLayerGroups={[]}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "close" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
