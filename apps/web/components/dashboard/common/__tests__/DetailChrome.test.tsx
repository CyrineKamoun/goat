import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { DetailHeader, MetaSidebar } from "@/components/dashboard/common/DetailChrome";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key, i18n: { language: "en" } }),
}));

describe("DetailHeader", () => {
  it("shows no back button where nothing passed one", () => {
    render(<DetailHeader title="Bus stops" />);

    expect(screen.getByText("Bus stops")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "back" })).not.toBeInTheDocument();
  });

  it("navigates back through the handler it was given", async () => {
    const onBack = vi.fn();
    render(<DetailHeader title="Bus stops" onBack={onBack} />);

    await userEvent.click(screen.getByRole("button", { name: "back" }));

    expect(onBack).toHaveBeenCalledTimes(1);
  });
});

describe("MetaSidebar", () => {
  const fields = [{ icon: ICON_NAME.USER, label: "Owner", value: "Ada Lovelace" }];

  it("draws a card around the field list by default", () => {
    render(<MetaSidebar fields={fields} />);

    const list = screen.getByText("Owner").closest("aside")?.querySelector("aside > div > div");
    expect(getComputedStyle(list as Element).borderStyle).toBe("solid");
  });

  it("drops the card where the host is already a surface", () => {
    render(<MetaSidebar fields={fields} flat />);

    const list = screen.getByText("Owner").closest("aside")?.querySelector("aside > div > div");
    expect(getComputedStyle(list as Element).borderStyle).toBe("");
  });
});
