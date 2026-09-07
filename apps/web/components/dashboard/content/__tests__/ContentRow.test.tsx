import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { ContentItem, Space } from "@/lib/validations/content";

import ContentRow from "@/components/dashboard/content/ContentRow";

const { useUserProfileMock } = vi.hoisted(() => ({ useUserProfileMock: vi.fn() }));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock("@/i18n/utils", () => ({ useDateFnsLocale: () => undefined }));
vi.mock("@/lib/api/users", () => ({ useUserProfile: useUserProfileMock }));

useUserProfileMock.mockReturnValue({
  userProfile: { id: "u-me", firstname: "Marco", lastname: "Albrecht", avatar: "" },
});

const me = { id: "u-me", name: "Marco Albrecht", avatar: null };
const lena = { id: "u-lena", name: "Lena Schmidt", avatar: null };

const personalSpace: Space = {
  id: "s1",
  kind: "personal",
  name: "My Content",
  default_role: "viewer",
  my_role: "owner",
  team_id: null,
  organization_id: null,
};

const teamSpace: Space = { ...personalSpace, id: "t1", kind: "team", name: "Design QA", team_id: "team-1" };

const layer: ContentItem = {
  type: "layer",
  id: "l1",
  name: "Bike network — segments",
  space_id: "s1",
  folder_id: null,
  updated_at: new Date().toISOString(),
  created_at: new Date().toISOString(),
  my_role: "owner",
  created_by: me,
  shared_with: null,
  thumbnail_url: null,
  is_public: false,
  layer_type: "feature",
  feature_layer_geometry_type: "line",
  is_shortcut: false,
  restricted: false,
  restricted_inherited: false,
};

const renderRow = (props?: {
  item?: Partial<ContentItem>;
  space?: Space;
  variant?: "space" | "shared_with_me" | "recent";
  location?: string;
}) =>
  render(
    <ContentRow
      item={{ ...layer, ...props?.item }}
      space={props?.space ?? personalSpace}
      variant={props?.variant ?? "space"}
      location={props?.location}
      selected={false}
      anySelected={false}
      onToggleSelect={() => {}}
      onOpen={() => {}}
      menuItems={[]}
      onMenuSelect={() => {}}
    />
  );

describe("ContentRow", () => {
  it("appends where the item lives to its type line when a location is given", () => {
    renderRow({ location: "Field surveys › Round 2 QA" });
    expect(screen.getByText(/Field surveys › Round 2 QA/)).toBeInTheDocument();
  });

  it("shows creator, audience and updated columns while browsing a space", () => {
    renderRow();

    // Creator: the caller themselves, written out as "You" in a list.
    expect(screen.getByText("you")).toBeInTheDocument();
    // Audience: stated even for an unshared item, so the column never
    // disappears from under the rows below it.
    expect(screen.getByText("private_content")).toBeInTheDocument();
    // Updated: date-fns' relative distance for a just-touched item.
    expect(screen.getByText(/less than a minute/i)).toBeInTheDocument();
  });

  it("names another member as the creator of a team space's content", () => {
    renderRow({ space: teamSpace, item: { space_id: "t1", created_by: lena } });

    expect(screen.getByText("Lena Schmidt")).toBeInTheDocument();
    expect(screen.queryByText("you")).not.toBeInTheDocument();
    expect(screen.getByText("in_space")).toBeInTheDocument();
  });

  it("leaves the creator column empty once the creating account is gone", () => {
    renderRow({ item: { created_by: null } });

    expect(screen.queryByText("you")).not.toBeInTheDocument();
    expect(screen.getByText("private_content")).toBeInTheDocument();
  });

  it("says Public for a published project, over any grant", () => {
    renderRow({
      item: {
        type: "project",
        is_public: true,
        shared_with: { organizations: [{ role: "viewer", id: "o1" }], teams: [], users: [] },
      },
    });

    expect(screen.getByText("public")).toBeInTheDocument();
    expect(screen.queryByText("shared_with_organization")).not.toBeInTheDocument();
  });

  it("keeps the creator, origin space and granted role in Shared with me", () => {
    renderRow({ variant: "shared_with_me", space: teamSpace, item: { my_role: "editor", created_by: lena } });

    expect(screen.getByText("Lena Schmidt")).toBeInTheDocument();
    expect(screen.getByText("Design QA")).toBeInTheDocument();
    expect(screen.getByText("editor")).toBeInTheDocument();
  });

  it("adds the space column to Recent, alongside creator and audience", () => {
    renderRow({ variant: "recent", space: teamSpace, item: { space_id: "t1" } });

    expect(screen.getByText("you")).toBeInTheDocument();
    expect(screen.getByText("Design QA")).toBeInTheDocument();
    expect(screen.getByText("in_space")).toBeInTheDocument();
  });

  it("renders no select circle when selectable is false", () => {
    render(
      <ContentRow
        item={layer}
        space={personalSpace}
        variant="recent"
        selected={false}
        anySelected={false}
        onToggleSelect={() => {}}
        onOpen={() => {}}
        menuItems={[]}
        onMenuSelect={() => {}}
        selectable={false}
      />
    );

    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  });
});
