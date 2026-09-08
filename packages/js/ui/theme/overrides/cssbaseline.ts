import type { Theme } from "@mui/material/styles";

/**
 * The toast palette is deliberately NOT here.
 *
 * A page may mount a second `ThemeProvider` in a fixed mode — the print
 * preview wraps its paper in a light theme, public dashboards do the same —
 * and each one emits this CssBaseline again, later in the stylesheet. Anything
 * declared here about the toast therefore wins globally, so a dark-mode user
 * got a white toast on those pages. `ToastProvider` mounts once at the root
 * and owns it.
 */
const CssBaseline = (theme: Theme) => {
  return {
    MuiCssBaseline: {
      styleOverrides: {
        html: {
          "& ::-webkit-scrollbar": {
            width: "5px",
            height: "5px",
          },
          "& ::-webkit-scrollbar-track": {
            background: "transparent",
          },
          "& ::-webkit-scrollbar-thumb": {
            backgroundColor:
              theme.palette.mode === "dark"
                ? "#374A62"
                : theme.palette.grey[400],
            borderRadius: "5px",
          },
        },
        body: {
          overflow: "hidden",
          width: "100%",
          height: "100%",
          margin: 0,
          padding: 0,
          position: "fixed",
        },
      },
    },
  };
};

export default CssBaseline;
