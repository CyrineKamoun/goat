#!/usr/bin/env node
// Builds `static/releases.json` — the feed the web app's `useReleases` reads
// (Home H7) — from the release notes authored as Docusaurus blog posts in
// `releases/` (en) and `i18n/de/docusaurus-plugin-content-blog-releases/`
// (de). Runs before both `dev` and `build` so the static file is never
// stale relative to the markdown it is generated from.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const SITE_ORIGIN = "https://docs.plan4better.de";

/** Splits a Docusaurus post into its `---`-delimited frontmatter and body.
 * Every note here carries frontmatter, so a missing block is a source error
 * worth failing loudly on rather than silently skipping the file. */
const splitFrontmatter = (source) => {
  const match = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?([\s\S]*)$/.exec(source);
  if (!match) {
    throw new Error("Expected a frontmatter block delimited by '---' lines");
  }
  return { frontmatter: match[1], body: match[2] };
};

/** A value is either a quoted or bare scalar. Quotes are stripped so
 * `title: "Content Spaces"` and `date: 2026-09-04` both come back as plain
 * strings, matching the feed schema's `z.string()` fields. */
const parseScalar = (raw) => {
  const trimmed = raw.trim();
  const quoted =
    (trimmed.startsWith('"') && trimmed.endsWith('"')) || (trimmed.startsWith("'") && trimmed.endsWith("'"));
  return quoted ? trimmed.slice(1, -1) : trimmed;
};

/**
 * A small hand-written parser for the flat/nested `key: value` YAML this
 * frontmatter uses — no arrays, one indentation width (2 spaces) per
 * nesting level. Neither `js-yaml` nor `gray-matter` is installed in this
 * workspace, and pulling one in for three keys' worth of nesting is not
 * worth a new dependency.
 */
const parseFrontmatter = (yaml) => {
  const lines = yaml.split(/\r?\n/).filter((line) => line.trim() !== "");
  let cursor = 0;

  const parseBlock = (indent) => {
    const result = {};
    while (cursor < lines.length) {
      const line = lines[cursor];
      const lineIndent = line.length - line.trimStart().length;
      if (lineIndent < indent) break;
      if (lineIndent > indent) {
        throw new Error(`Unexpected indentation in frontmatter line: "${line}"`);
      }
      const separator = line.indexOf(":");
      if (separator === -1) {
        throw new Error(`Expected "key: value" in frontmatter line: "${line}"`);
      }
      const key = line.slice(0, separator).trim();
      const rawValue = line.slice(separator + 1);
      cursor += 1;
      result[key] = rawValue.trim() === "" ? parseBlock(indent + 2) : parseScalar(rawValue);
    }
    return result;
  };

  return parseBlock(0);
};

/** The filename without its leading `YYYY-MM-DD-` date prefix and its
 * extension — Docusaurus's own blog slug rule — unless the frontmatter
 * names an explicit `slug`. */
const slugFor = (filename, frontmatter) => {
  if (typeof frontmatter.slug === "string") return frontmatter.slug;
  const base = filename.replace(/\.mdx?$/, "");
  const match = /^\d{4}-\d{2}-\d{2}-(.+)$/.exec(base);
  return match ? match[1] : base;
};

const urlFor = (locale, slug) =>
  locale === "de" ? `${SITE_ORIGIN}/de/releases/${slug}` : `${SITE_ORIGIN}/releases/${slug}`;

/** One locale directory's markdown files as feed entries, or `[]` when the
 * directory does not exist — the de translation for a note is optional
 * until someone writes it. */
const entriesFor = (dir, locale) => {
  if (!fs.existsSync(dir)) return [];

  return fs
    .readdirSync(dir)
    .filter((filename) => /\.mdx?$/.test(filename))
    .map((filename) => {
      const source = fs.readFileSync(path.join(dir, filename), "utf8");
      const { frontmatter: yaml } = splitFrontmatter(source);
      const frontmatter = parseFrontmatter(yaml);
      const slug = slugFor(filename, frontmatter);

      const entry = {
        id: frontmatter.id,
        date: frontmatter.date,
        tag: frontmatter.tag,
        title: frontmatter.title,
        summary: frontmatter.summary,
        url: urlFor(locale, slug),
      };
      if (frontmatter.spotlight) entry.spotlight = frontmatter.spotlight;
      return entry;
    })
    .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
};

/** Builds the feed object from an en directory and a de directory. Exported
 * for the test suite, which points both at temporary fixture directories
 * instead of the real `releases/` tree. */
export const buildFeed = ({ enDir, deDir }) => ({
  generatedAt: new Date().toISOString(),
  entries: {
    en: entriesFor(enDir, "en"),
    de: entriesFor(deDir, "de"),
  },
});

const isMainModule = () => {
  const invoked = process.argv[1];
  return Boolean(invoked) && fileURLToPath(import.meta.url) === path.resolve(invoked);
};

if (isMainModule()) {
  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
  const docsRoot = path.resolve(scriptDir, "..");
  const feed = buildFeed({
    enDir: path.join(docsRoot, "releases"),
    deDir: path.join(docsRoot, "i18n", "de", "docusaurus-plugin-content-blog-releases"),
  });
  const outFile = path.join(docsRoot, "static", "releases.json");
  fs.mkdirSync(path.dirname(outFile), { recursive: true });
  fs.writeFileSync(outFile, `${JSON.stringify(feed, null, 2)}\n`);
}
