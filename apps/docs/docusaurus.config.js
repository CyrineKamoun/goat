// @ts-nocheck
// Note: type annotations allow type checking and IDEs autocompletion

const { themes: prismThemes } = require("prism-react-renderer");
const { sidebarItemsGenerator } = require("./src/sidebar/sections");

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: "GOAT DOCS",
  tagline: "GOATs are cool",
  favicon: "img/favicon.ico",
  url: "https://goat.plan4better.de",
  baseUrl: "/docs/",
  organizationName: "plan4better",
  projectName: "goat",
  onBrokenLinks: "warn",
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: "warn",
    },
  },
  i18n: {
    defaultLocale: "en",
    locales: ["en", "de"],
    path: "i18n",
    localeConfigs: {
      en: {
        label: "English",
      },
      de: {
        label: "Deutsch",
      },
    },
  },
  clientModules: [require.resolve("./src/matomo.js")],
  presets: [
    [
      "classic",
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          routeBasePath: "/",
          sidebarPath: require.resolve("./sidebars.js"),
          sidebarItemsGenerator,
          editUrl: ({ locale, versionDocsDirPath, docPath }) => {
            const translation = locale || 'en';
            if (translation !== 'en') {
              return `https://github.com/plan4better/goat/edit/main/apps/docs/i18n/${translation}/docusaurus-plugin-content-docs/current/${docPath}`;
            }
            return `https://github.com/plan4better/goat/edit/main/apps/docs/docs/${docPath}`;
          },
          lastVersion: "current",
          versions: {
            current: {
              path: "",
            },
          },
        },
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
      }),
    ],
  ],
  plugins: [
    require.resolve("./src/plugins/markdown-source.js"),
    [
      "@docusaurus/plugin-content-docs",
      {
        id: "tutorials",
        path: "tutorials",
        routeBasePath: "tutorials",
        sidebarPath: require.resolve("./sidebarsTutorials.js"),
        editUrl: ({ locale, docPath }) => {
          const translation = locale || 'en';
          if (translation !== 'en') {
            return `https://github.com/plan4better/goat/edit/main/apps/docs/i18n/${translation}/docusaurus-plugin-content-docs-tutorials/current/${docPath}`;
          }
          return `https://github.com/plan4better/goat/edit/main/apps/docs/tutorials/${docPath}`;
        },
      },
    ],
    [
      "@docusaurus/plugin-content-blog",
      {
        id: "releases",
        routeBasePath: "releases",
        path: "./releases",
        blogTitle: "Release notes",
        blogSidebarTitle: "Release notes",
        showReadingTime: false,
        feedOptions: { type: null },
      },
    ],
    [
      "@docusaurus/plugin-client-redirects",
      {
        createRedirects(existingPath) {
          // Redirect old /2.0/ versioned URLs to the new unversioned paths
          return [`/2.0${existingPath}`];
        },
      },
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Replace with your project's social card
      image: "img/GOAT_logo_white_green_crop_b.png",
      navbar: {
        logo: {
          alt: "GOAT by Plan4Better",
          src: "img/goat-lockup.svg",
        },
        items: [
          {
            type: "docSidebar",
            sidebarId: "tutorialSidebar",
            position: "right",
            label: "Docs",
          },
          {
            to: "/tutorials",
            label: "Tutorials",
            position: "right",
            activeBaseRegex: `/tutorials/`,
          },
          {
            to: "https://plan4better.de/en/blog/",
            label: "Blog",
            position: "right",
          },
          {
            type: "localeDropdown",
            position: "right"
          },
          // Re-enable when multiple doc versions exist:
          // {
          //   type: "docsVersionDropdown",
          //   position: "right",
          //   dropdownActiveClassDisabled: true,
          // },
          {
            href: "https://github.com/plan4better/goat",
            label: "GitHub",
            position: "right",
            className: "header-github-link",
            "aria-label": "GitHub",
          },
        ],
      },
      footer: {
        links: [
          {
            title: "Community",
            items: [
              {
                label: "LinkedIn",
                href: "https://www.linkedin.com/company/plan4better/",
              },
              {
                label: "GitHub",
                href: "https://github.com/plan4better",
              },
            ],
          },
          {
            title: "More",
            items: [
              {
                label: "Plan4Better",
                to: "https://plan4better.de/en/",
              },
              {
                label: "Blog",
                to: "https://plan4better.de/en/blog/",
              },
              {
                label: "References",
                href: "https://plan4better.de/en/references/",
              },
              {
                label: "Privacy",
                to: "/privacy",
              },
              {
                label: "Imprint",
                href: "https://plan4better.de/en/about-us/imprint",
              },
            ],
          },
        ],
        copyright: `Plan4Better GmbH 2026 | All Rights Reserved`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.vsDark,
      },
      algolia: {
        indexName: 'goat-plan4better',
        appId: 'LLUCN6LJ7S',
        apiKey: '638cac0d311f215315b3313f679af50a',
        contextualSearch: true,
      },
    }),
};

module.exports = config;
