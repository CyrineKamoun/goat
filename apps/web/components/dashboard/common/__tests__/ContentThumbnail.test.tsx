import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import ContentThumbnail from "@/components/dashboard/common/ContentThumbnail";

const fallback = <div data-testid="drawn-fallback">drawn</div>;

describe("ContentThumbnail", () => {
  it("shows the image when it has an href", () => {
    const { container } = render(<ContentThumbnail kind="project" href="https://example.test/a.png" />);

    expect(container.querySelector("img")?.getAttribute("src")).toBe("https://example.test/a.png");
    expect(screen.queryByTestId("content-thumbnail-mark")).not.toBeInTheDocument();
  });

  it("draws the kind's texture mark when there is no href and no fallback", () => {
    render(<ContentThumbnail kind="vector" geometryType="point" />);

    expect(screen.getByTestId("content-thumbnail-mark")).toBeInTheDocument();
  });

  it("renders the fallback instead of the texture mark when there is no href", () => {
    render(<ContentThumbnail kind="template" fallback={fallback} />);

    expect(screen.getByTestId("drawn-fallback")).toBeInTheDocument();
    expect(screen.queryByTestId("content-thumbnail-mark")).not.toBeInTheDocument();
  });

  it("renders the fallback when the image fails to load", () => {
    const { container } = render(
      <ContentThumbnail kind="template" href="https://example.test/gone.png" fallback={fallback} />
    );

    expect(screen.queryByTestId("drawn-fallback")).not.toBeInTheDocument();

    fireEvent.error(container.querySelector("img")!);

    expect(container.querySelector("img")).not.toBeInTheDocument();
    expect(screen.getByTestId("drawn-fallback")).toBeInTheDocument();
    expect(screen.queryByTestId("content-thumbnail-mark")).not.toBeInTheDocument();
  });

  it("falls back to the texture mark when a dead image has no fallback given", () => {
    const { container } = render(<ContentThumbnail kind="project" href="https://example.test/gone.png" />);

    fireEvent.error(container.querySelector("img")!);

    expect(screen.getByTestId("content-thumbnail-mark")).toBeInTheDocument();
  });
});
