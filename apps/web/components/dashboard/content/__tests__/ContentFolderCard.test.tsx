import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { ContentItem, Space } from "@/lib/validations/content";

import ContentFolderCard from "@/components/dashboard/content/ContentFolderCard";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const space: Space = {
  id: "s1",
  kind: "personal",
  name: "My Content",
  default_role: "viewer",
  my_role: "owner",
  team_id: null,
  organization_id: null,
} as Space;

const folder: ContentItem = {
  type: "folder",
  id: "f1",
  name: "Surveys",
  space_id: "s1",
  folder_id: null,
  updated_at: new Date().toISOString(),
  created_at: new Date().toISOString(),
  my_role: "owner",
  created_by: null,
  shared_with: null,
  thumbnail_url: null,
  is_public: false,
  layer_type: null,
  feature_layer_geometry_type: null,
  is_shortcut: false,
  restricted: false,
  restricted_inherited: false,
} as ContentItem;

const renderCard = (props: Partial<React.ComponentProps<typeof ContentFolderCard>> = {}) =>
  render(
    <ContentFolderCard
      item={folder}
      space={space}
      selected={false}
      anySelected={false}
      onToggleSelect={() => {}}
      onOpen={() => {}}
      menuItems={[]}
      onMenuSelect={() => {}}
      {...props}
    />
  );

describe("ContentFolderCard", () => {
  it("offers a select circle by default", () => {
    const onToggleSelect = vi.fn();
    renderCard({ onToggleSelect });

    const circle = screen.getByRole("checkbox");
    fireEvent.click(circle);
    expect(onToggleSelect).toHaveBeenCalledWith("f1");
  });

  it("renders no select circle when it is not selectable", () => {
    renderCard({ selectable: false });

    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
    expect(screen.getByText("Surveys")).toBeInTheDocument();
  });

  it("renders no kebab without menu items", () => {
    renderCard();

    expect(screen.queryByRole("button", { name: "more" })).not.toBeInTheDocument();
  });
});
