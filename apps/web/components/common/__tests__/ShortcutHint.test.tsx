import { render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import ShortcutHint from "@/components/common/ShortcutHint";

const setPlatform = (value: string) =>
  Object.defineProperty(window.navigator, "platform", { value, configurable: true });

describe("ShortcutHint", () => {
  const original = window.navigator.platform;
  beforeEach(() => vi.restoreAllMocks());
  afterEach(() => setPlatform(original));

  it("shows the Command symbol on a Mac", () => {
    setPlatform("MacIntel");
    render(<ShortcutHint letter="K" />);
    expect(screen.getByText("⌘K")).toBeInTheDocument();
  });

  it("shows Ctrl on Windows, where there is no Command key to press", () => {
    setPlatform("Win32");
    render(<ShortcutHint letter="K" />);
    expect(screen.getByText("Ctrl+K")).toBeInTheDocument();
    expect(screen.queryByText("⌘K")).not.toBeInTheDocument();
  });

  it("shows Ctrl on Linux", () => {
    setPlatform("Linux x86_64");
    render(<ShortcutHint letter="K" />);
    expect(screen.getByText("Ctrl+K")).toBeInTheDocument();
  });

  it("renders a kbd element, so assistive tech announces it as a key", () => {
    setPlatform("Win32");
    const { container } = render(<ShortcutHint letter="K" />);
    expect(container.querySelector("kbd")).not.toBeNull();
  });
});
