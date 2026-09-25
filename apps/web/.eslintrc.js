module.exports = {
  root: true,
  extends: ["p4b"],
  rules: {
    // The package root re-exports every icon, so importing it is many times
    // slower than importing one icon by path, a cost paid by every test file
    // that reaches it directly or through a component.
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
