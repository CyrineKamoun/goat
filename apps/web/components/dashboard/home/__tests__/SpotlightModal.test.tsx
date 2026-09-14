import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { ReleaseEntry } from "@/lib/validations/home";

import SpotlightModal from "@/components/dashboard/home/SpotlightModal";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key, i18n: { language: "en" } }),
}));

const entry = (spotlight?: ReleaseEntry["spotlight"]): ReleaseEntry => ({
  id: "catalog",
  date: "2026-09-14T00:00:00.000Z",
  tag: "improved",
  title: "Search more than 30,000 datasets in the Catalog",
  summary: "The Catalog grew from 60 to more than 30,000 datasets.",
  url: "https://www.plan4better.de/en/goat/changelog/2026-09#catalog",
  thumbnail: "https://www.plan4better.de/_astro/05-catalog.webp",
  spotlight,
});

describe("SpotlightModal", () => {
  it("uses the spotlight headline, image and url, with Learn more leading and Skip dismissing", () => {
    const onClose = vi.fn();
    render(
      <SpotlightModal
        entry={entry({
          headline: "More than 30,000 datasets",
          image: "https://www.plan4better.de/_astro/spot.webp",
          highlights: ["One", "Two"],
          cta: { url: "https://www.plan4better.de/en/post/goat-v3" },
        })}
        onClose={onClose}
      />
    );

    expect(screen.getByRole("heading", { name: "More than 30,000 datasets" })).toBeInTheDocument();
    expect(screen.getByRole("img").getAttribute("src")).toBe("https://www.plan4better.de/_astro/spot.webp");
    expect(screen.getByRole("link", { name: "learn_more" }).getAttribute("href")).toBe(
      "https://www.plan4better.de/en/post/goat-v3"
    );
    expect(screen.getAllByRole("listitem").map((li) => li.textContent)).toEqual(["One", "Two"]);
    expect(screen.queryByText("release_improved")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "skip" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("falls back to the entry's title, thumbnail and link without a spotlight element", () => {
    render(<SpotlightModal entry={entry(undefined)} onClose={vi.fn()} />);

    expect(
      screen.getByRole("heading", { name: "Search more than 30,000 datasets in the Catalog" })
    ).toBeInTheDocument();
    expect(screen.getByRole("img").getAttribute("src")).toBe(
      "https://www.plan4better.de/_astro/05-catalog.webp"
    );
    expect(screen.getByRole("link", { name: "learn_more" }).getAttribute("href")).toBe(
      "https://www.plan4better.de/en/goat/changelog/2026-09#catalog"
    );
  });

  it("leads with Watch video when the spotlight has one, keeping Learn more as the secondary action", () => {
    render(
      <SpotlightModal
        entry={entry({
          headline: "H",
          highlights: [],
          video: "https://videos.plan4better.de/x/play_720p.mp4",
        })}
        onClose={vi.fn()}
      />
    );

    expect(screen.getByRole("button", { name: "watch_video" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "learn_more" })).toBeInTheDocument();
  });
});
