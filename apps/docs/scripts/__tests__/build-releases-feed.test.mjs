import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { after, before, describe, it } from "node:test";

import { buildFeed } from "../build-releases-feed.mjs";

const write = (dir, filename, content) => {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, filename), content);
};

const enNote = (overrides = {}) => `---
id: ${overrides.id ?? "2026-09-content-spaces"}
date: ${overrides.date ?? "2026-09-04"}
tag: ${overrides.tag ?? "new"}
title: "${overrides.title ?? "Content Spaces"}"
summary: "${overrides.summary ?? "Spaces now own content."}"
---

Body text.
`;

describe("buildFeed", () => {
  let root;
  let enDir;
  let deDir;

  before(() => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), "releases-feed-"));
    enDir = path.join(root, "releases");
    deDir = path.join(root, "i18n", "de", "docusaurus-plugin-content-blog-releases");
  });

  after(() => {
    fs.rmSync(root, { recursive: true, force: true });
  });

  it("reads both locales and returns their entries", () => {
    write(enDir, "2026-09-04-content-spaces.md", enNote());
    write(
      deDir,
      "2026-09-04-content-spaces.md",
      `---
id: 2026-09-content-spaces
date: 2026-09-04
tag: new
title: "Content Spaces"
summary: "Inhalte gehören jetzt Bereichen."
---

Textkörper.
`
    );

    const feed = buildFeed({ enDir, deDir });

    assert.equal(feed.entries.en.length, 1);
    assert.equal(feed.entries.de.length, 1);
    assert.equal(feed.entries.en[0].title, "Content Spaces");
    assert.equal(feed.entries.de[0].summary, "Inhalte gehören jetzt Bereichen.");
  });

  it("carries a generatedAt timestamp", () => {
    const feed = buildFeed({ enDir, deDir });

    assert.equal(typeof feed.generatedAt, "string");
    assert.ok(!Number.isNaN(new Date(feed.generatedAt).getTime()));
  });

  it("sorts entries by date, newest first", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "releases-feed-sort-"));
    write(dir, "2026-01-01-older.md", enNote({ id: "older", date: "2026-01-01", title: "Older" }));
    write(dir, "2026-09-04-newer.md", enNote({ id: "newer", date: "2026-09-04", title: "Newer" }));

    const feed = buildFeed({ enDir: dir, deDir: path.join(dir, "missing-de") });

    assert.deepEqual(
      feed.entries.en.map((entry) => entry.id),
      ["newer", "older"]
    );

    fs.rmSync(dir, { recursive: true, force: true });
  });

  it("derives the url from the site domain, locale and slug", () => {
    const feed = buildFeed({ enDir, deDir });

    assert.equal(feed.entries.en[0].url, "https://docs.plan4better.de/releases/content-spaces");
    assert.equal(feed.entries.de[0].url, "https://docs.plan4better.de/de/releases/content-spaces");
  });

  it("uses a frontmatter slug over the filename-derived one when given", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "releases-feed-slug-"));
    write(
      dir,
      "2026-09-05-some-file.md",
      `---
id: custom-slug-entry
date: 2026-09-05
tag: improved
title: "Custom slug"
summary: "Uses a frontmatter slug."
slug: custom-slug
---

Body.
`
    );

    const feed = buildFeed({ enDir: dir, deDir: path.join(dir, "missing-de") });

    assert.equal(feed.entries.en[0].url, "https://docs.plan4better.de/releases/custom-slug");

    fs.rmSync(dir, { recursive: true, force: true });
  });

  it("passes spotlight through, including a nested cta", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "releases-feed-spotlight-"));
    write(
      dir,
      "2026-09-06-spotlighted.md",
      `---
id: spotlighted
date: 2026-09-06
tag: new
title: "Spotlighted"
summary: "Has a spotlight block."
spotlight:
  headline: "Big news"
  cta:
    label: "Read more"
    url: https://docs.plan4better.de/releases/spotlighted
---

Body.
`
    );

    const feed = buildFeed({ enDir: dir, deDir: path.join(dir, "missing-de") });

    assert.deepEqual(feed.entries.en[0].spotlight, {
      headline: "Big news",
      cta: { label: "Read more", url: "https://docs.plan4better.de/releases/spotlighted" },
    });

    fs.rmSync(dir, { recursive: true, force: true });
  });

  it("omits spotlight for entries without the block", () => {
    const feed = buildFeed({ enDir, deDir });

    assert.equal(feed.entries.de[0].spotlight, undefined);
  });

  it("returns an empty locale list when its directory does not exist", () => {
    const feed = buildFeed({ enDir, deDir: path.join(root, "does-not-exist") });

    assert.deepEqual(feed.entries.de, []);
  });
});
