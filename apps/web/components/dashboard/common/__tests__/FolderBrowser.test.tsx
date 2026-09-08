import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { Space } from "@/lib/validations/content";
import type { Folder } from "@/lib/validations/folder";

import FolderBrowser from "@/components/dashboard/common/FolderBrowser";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, vars?: Record<string, unknown>) => (vars ? `${key}:${JSON.stringify(vars)}` : key),
  }),
}));

const space: Space = {
  id: "space-1",
  kind: "team",
  name: "Design Team",
  default_role: "viewer",
  my_role: "owner",
  team_id: "team-1",
  organization_id: null,
};

const folders: Folder[] = [
  {
    id: "home-1",
    name: "home",
    parent_id: null,
    space_id: "space-1",
    depth: 0,
    is_owned: true,
    restricted: false,
  },
  {
    id: "a-1",
    name: "A",
    parent_id: null,
    space_id: "space-1",
    depth: 0,
    is_owned: true,
    restricted: false,
  },
  {
    id: "b-1",
    name: "B",
    parent_id: "a-1",
    space_id: "space-1",
    depth: 1,
    is_owned: true,
    restricted: false,
  },
  {
    id: "x-1",
    name: "X",
    parent_id: null,
    space_id: "space-other",
    depth: 0,
    is_owned: true,
    restricted: false,
  },
];

const noop = () => undefined;

describe("FolderBrowser", () => {
  it("leads the breadcrumb with the space, and lists only that space's root folders", () => {
    render(
      <FolderBrowser space={space} folders={folders} homeFolderId="home-1" value={null} onChange={noop} />
    );

    expect(screen.getByText("Design Team")).toBeInTheDocument();
    expect(screen.getByText("A")).toBeInTheDocument();
    // The space's own `home` root is what the space crumb stands for, and a
    // folder in another space is not this browser's to offer.
    expect(screen.queryByText("home")).not.toBeInTheDocument();
    expect(screen.queryByText("X")).not.toBeInTheDocument();
    // One level at a time: a grandchild only shows once its parent is open.
    expect(screen.queryByText("B")).not.toBeInTheDocument();
  });

  it("reports the folder id when the author browses into one", () => {
    const onChange = vi.fn();
    render(
      <FolderBrowser space={space} folders={folders} homeFolderId="home-1" value={null} onChange={onChange} />
    );

    fireEvent.click(screen.getByText("A"));

    expect(onChange).toHaveBeenCalledWith("a-1");
  });

  it("shows the children of the browsed folder, and reports null back at the space root", () => {
    const onChange = vi.fn();
    render(
      <FolderBrowser space={space} folders={folders} homeFolderId="home-1" value="a-1" onChange={onChange} />
    );

    expect(screen.getByText("B")).toBeInTheDocument();
    // The crumb the author came from is the space itself.
    fireEvent.click(screen.getByText("Design Team"));
    expect(onChange).toHaveBeenCalledWith(null);
  });

  it("stands at the space root when the value is the space's home folder", () => {
    render(
      <FolderBrowser space={space} folders={folders} homeFolderId="home-1" value="home-1" onChange={noop} />
    );

    // Browsing has not left the root, so the root's own folders are listed.
    expect(screen.getByText("A")).toBeInTheDocument();
    expect(screen.queryByText("B")).not.toBeInTheDocument();
  });

  it("never offers a hidden folder", () => {
    render(
      <FolderBrowser
        space={space}
        folders={folders}
        homeFolderId="home-1"
        value={null}
        onChange={noop}
        hiddenFolderIds={new Set(["a-1"])}
      />
    );

    expect(screen.queryByText("A")).not.toBeInTheDocument();
    expect(screen.getByText("no_folders_here")).toBeInTheDocument();
  });

  it("labels itself above the list and captions itself under it", () => {
    render(
      <FolderBrowser
        space={space}
        folders={folders}
        homeFolderId="home-1"
        value={null}
        onChange={noop}
        label="folder"
        helperText="Everyone who can open this folder can use the template."
      />
    );

    expect(screen.getByText("folder")).toBeInTheDocument();
    const caption = screen.getByText("Everyone who can open this folder can use the template.");
    expect(caption.className).toContain("MuiTypography-caption");
  });
});
