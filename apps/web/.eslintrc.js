module.exports = {
  root: true,
  extends: ["p4b"],
  rules: {
    // The package root re-exports all 2,000+ icons: importing it takes ~2.6 s,
    // paid by every test file that reaches it directly or through a component.
    "no-restricted-imports": [
      "error",
      {
        paths: [
          {
            name: "@mui/icons-material",
            message: 'Import the icon by path, e.g. `import Close from "@mui/icons-material/Close"`.',
          },
        ],
      },
    ],
  },
};
