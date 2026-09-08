import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import ContentFeedSkeleton from "@/components/dashboard/content/ContentFeedSkeleton";

describe("ContentFeedSkeleton", () => {
  it("mirrors the grid: two headings, four folder cards, six tiles", () => {
    const { container } = render(<ContentFeedSkeleton layout="tiles" />);

    // Four folder cards.
    expect(container.querySelectorAll(".MuiSkeleton-rounded")).toHaveLength(4);
    // Six tile thumbnails, each with a creator avatar under it.
    expect(container.querySelectorAll(".MuiSkeleton-rectangular")).toHaveLength(6);
    expect(container.querySelectorAll(".MuiSkeleton-circular")).toHaveLength(6);
    // Two section headings plus a name and a date line per tile.
    expect(container.querySelectorAll(".MuiSkeleton-text")).toHaveLength(14);
  });

  it("mirrors the list: a header row and six rows", () => {
    const { container } = render(<ContentFeedSkeleton layout="list" />);

    expect(container.querySelectorAll(".MuiSkeleton-rounded")).toHaveLength(7);
    expect(container.querySelectorAll(".MuiSkeleton-text")).toHaveLength(0);
  });
});
