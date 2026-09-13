import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { TemplateInput } from "@/lib/validations/template";

import TemplateCatalogSwitch from "@/components/templates/TemplateCatalogSwitch";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) => (opts ? `${key}:${JSON.stringify(opts)}` : key),
  }),
}));
vi.mock("@/i18n/utils", () => ({ useDateFnsLocale: () => undefined }));

const shipped = (name: string, flags: Partial<TemplateInput> = {}): TemplateInput => ({
  key: name,
  label: name,
  mode: "ship",
  layer_id: "00000000-0000-0000-0000-000000000001",
  layer_type: "feature",
  geometry_type: null,
  from_catalog: false,
  public_read: false,
  ...flags,
});

describe("TemplateCatalogSwitch", () => {
  it("says a published template is in the catalog and stays there on update", () => {
    render(
      <TemplateCatalogSwitch
        published
        publishedAt="2026-09-06T10:00:00Z"
        checked
        onChange={() => {}}
        updating
      />
    );
    expect(screen.getByText(/published_in_catalog_since/)).toBeInTheDocument();
    expect(screen.getByText(/catalog_stays_published/)).toBeInTheDocument();
  });

  it("warns that switching a published template off unpublishes on save", () => {
    render(<TemplateCatalogSwitch published checked={false} onChange={() => {}} />);
    expect(screen.getByText("unpublish_on_save")).toBeInTheDocument();
  });

  it("names the datasets that publishing makes public", () => {
    render(
      <TemplateCatalogSwitch
        published={false}
        checked
        onChange={() => {}}
        becomingPublic={[shipped("Parks"), shipped("Trees")]}
      />
    );
    expect(screen.getByText("becomes_public")).toBeInTheDocument();
    expect(screen.getByText('publish_makes_datasets_public:{"names":"Parks, Trees"}')).toBeInTheDocument();
  });

  it("shows no consequence when every shipped dataset is public or catalog already", () => {
    render(<TemplateCatalogSwitch published={false} checked onChange={() => {}} becomingPublic={[]} />);
    expect(screen.queryByText(/publish_makes_datasets_public|becomes_public/)).not.toBeInTheDocument();
  });

  it("shows no status for an unpublished template with the switch off", () => {
    render(<TemplateCatalogSwitch published={false} checked={false} onChange={() => {}} />);
    expect(
      screen.queryByText(/unpublish_on_save|published_in_catalog|publish_makes_datasets_public/)
    ).not.toBeInTheDocument();
  });

  it("reports the switch change", () => {
    const onChange = vi.fn();
    render(<TemplateCatalogSwitch published={false} checked={false} onChange={onChange} />);
    screen.getByRole("checkbox", { name: "publish_to_goat_catalog" }).click();
    expect(onChange).toHaveBeenCalledWith(true);
  });
});
