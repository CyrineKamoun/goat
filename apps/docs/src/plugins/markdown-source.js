// @ts-check
const fs = require("fs/promises");
const path = require("path");

const FRONT_MATTER = /^---\r?\n[\s\S]*?\r?\n---\r?\n/;
const MDX_IMPORT = /^import\s.+\sfrom\s+["'][^"']+["'];?\s*$/;
const FENCE = /^\s*(```|~~~)/;
const HEADING_1 = /^#\s+\S/m;

/**
 * The Markdown a reader copies: the source without its front matter and
 * without the MDX import lines, which only matter to the site build. A doc
 * whose title comes from front matter gets it back as a heading.
 * @param {string} source
 * @param {string} title
 */
function readableMarkdown(source, title) {
  const body = source.replace(FRONT_MATTER, "");
  let inFence = false;
  const lines = body.split(/\r?\n/).filter((line) => {
    if (FENCE.test(line)) {
      inFence = !inFence;
      return true;
    }
    return inFence || !MDX_IMPORT.test(line);
  });
  const markdown = lines.join("\n").replace(/^\s*\n/, "");
  return HEADING_1.test(markdown) ? markdown : `# ${title}\n\n${markdown}`;
}

/**
 * Where a doc's Markdown is written inside the build output. Mirrors
 * `markdownSourceUrl` in `src/components/CopyPageButton`.
 * @param {string} permalink
 * @param {string} baseUrl
 */
function markdownOutputPath(permalink, baseUrl) {
  const relative = permalink.startsWith(baseUrl)
    ? permalink.slice(baseUrl.length)
    : permalink.replace(/^\//, "");
  if (relative === "" || relative.endsWith("/")) {
    return `${relative}index.md`;
  }
  return `${relative}.md`;
}

/**
 * Writes the Markdown source of every doc (all docs plugin instances, in the
 * locale being built) next to its page as `<permalink>.md`. The "Copy page"
 * button fetches it from there. It exists only in production builds; the dev
 * server does not run `postBuild`.
 * @type {import('@docusaurus/types').PluginModule}
 */
module.exports = function markdownSourcePlugin() {
  return {
    name: "markdown-source",
    async postBuild({ plugins, outDir, siteDir, baseUrl }) {
      const docsPlugins = plugins.filter((plugin) => plugin.name === "docusaurus-plugin-content-docs");
      const writes = [];
      for (const plugin of docsPlugins) {
        /** @type {any} */
        const content = plugin.content;
        for (const version of content?.loadedVersions ?? []) {
          for (const doc of version.docs) {
            const sourceFile = path.join(siteDir, doc.source.replace(/^@site\//, ""));
            const target = path.join(outDir, markdownOutputPath(doc.permalink, baseUrl));
            writes.push(
              (async () => {
                const source = await fs.readFile(sourceFile, "utf8");
                await fs.mkdir(path.dirname(target), { recursive: true });
                await fs.writeFile(target, readableMarkdown(source, doc.title));
              })()
            );
          }
        }
      }
      await Promise.all(writes);
    },
  };
};
