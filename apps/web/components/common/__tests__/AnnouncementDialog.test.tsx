import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import AnnouncementDialog from "@/components/common/AnnouncementDialog";

const base = {
  open: true,
  headline: "Welcome to GOAT",
  body: ["First paragraph.", "Second paragraph."],
  dismissLabel: "Skip",
};

describe("AnnouncementDialog", () => {
  it("renders headline, body and actions without any media", () => {
    const onDismiss = vi.fn();
    render(<AnnouncementDialog {...base} primary={{ label: "Learn more" }} onDismiss={onDismiss} />);

    expect(screen.getByRole("heading", { name: "Welcome to GOAT" })).toBeInTheDocument();
    expect(screen.getByText("First paragraph.")).toBeInTheDocument();
    expect(screen.queryByRole("img")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Skip" }));
    expect(onDismiss).toHaveBeenCalledTimes(1);
  });

  it("shows the screenshot and dismisses after the primary action", () => {
    const onDismiss = vi.fn();
    const onClick = vi.fn();
    render(
      <AnnouncementDialog
        {...base}
        media={{ kind: "image", src: "https://example.org/shot.webp", alt: "The catalog" }}
        primary={{ label: "Learn more", href: "https://example.org/changelog#x", onClick }}
        onDismiss={onDismiss}
      />
    );

    expect(screen.getByRole("img", { name: "The catalog" }).getAttribute("src")).toBe(
      "https://example.org/shot.webp"
    );
    const link = screen.getByRole("link", { name: "Learn more" });
    expect(link.getAttribute("href")).toBe("https://example.org/changelog#x");
    fireEvent.click(link);
    expect(onClick).toHaveBeenCalledTimes(1);
    expect(onDismiss).toHaveBeenCalledTimes(1);
  });

  it("opens a YouTube video in the lightbox on the primary action and dismisses when it closes", () => {
    const onDismiss = vi.fn();
    render(
      <AnnouncementDialog
        {...base}
        media={{ kind: "video", src: "https://www.youtube.com/watch?v=_clsR386b9w" }}
        primary={{ label: "Watch video", playsVideo: true }}
        onDismiss={onDismiss}
      />
    );

    // Poster first, taken from YouTube, with the play overlay.
    expect(screen.getByRole("img").getAttribute("src")).toBe(
      "https://img.youtube.com/vi/_clsR386b9w/maxresdefault.jpg"
    );
    expect(screen.getByRole("button", { name: "play" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Watch video" }));

    const frame = document.querySelector("iframe");
    expect(frame?.getAttribute("src")).toContain("youtube-nocookie.com/embed/_clsR386b9w");
    expect(frame?.getAttribute("src")).toContain("mute=0");
    expect(onDismiss).not.toHaveBeenCalled();
    // Watching counts as acting on the announcement: closing the video dismisses it.
    fireEvent.click(screen.getByRole("button", { name: "close" }));
    expect(onDismiss).toHaveBeenCalledTimes(1);
  });

  it("opens a native player in the lightbox for a direct video file", () => {
    render(
      <AnnouncementDialog
        {...base}
        media={{
          kind: "video",
          src: "https://example.org/clip.mp4",
          poster: "https://example.org/poster.jpg",
        }}
        primary={{ label: "Watch video", playsVideo: true }}
        onDismiss={vi.fn()}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "play" }));

    const video = document.querySelector("video");
    expect(video?.getAttribute("src")).toBe("https://example.org/clip.mp4");
    expect(video?.hasAttribute("controls")).toBe(true);
    expect(video?.hasAttribute("autoplay")).toBe(true);
  });
});
