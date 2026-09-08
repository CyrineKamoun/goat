"use client";

import { GlobalStyles, useTheme } from "@mui/material";
import { ToastContainer, Zoom } from "react-toastify";

import ToastCloseButton from "@/components/common/ToastCloseButton";

interface ToastProviderProps {
  children: React.ReactNode;
}

export default function ToastProvider({ children }: ToastProviderProps) {
  const theme = useTheme();
  return (
    <>
      {children}
      {/**
       * Toastify's palette, in the app's colours.
       *
       * `theme={mode}` below only picks one of the library's two built-in palettes, and both are
       * hardcoded: the dark one paints the toast `#121212` against a `#18202B` page, draws the
       * progress bar in Material purple and spins a light-mode spinner. Remapping the variables
       * is what actually themes the toast — the alternative is restyling every part by class.
       *
       * This is the ONLY place that declares them. They used to be in the shared MUI
       * CssBaseline override as well, which every `ThemeProvider` emits: a page mounting a
       * second one in a fixed mode (the print preview's white paper, public dashboards)
       * re-declared the palette later in the sheet and won, so a dark-mode user got a white
       * toast there while the body colour below still came from the dark theme.
       */}
      <GlobalStyles
        styles={{
          ":root": {
            "--toastify-color-light": theme.palette.background.paper,
            "--toastify-color-dark": theme.palette.background.paper,
            "--toastify-text-color-light": theme.palette.text.primary,
            "--toastify-text-color-dark": theme.palette.text.primary,
            "--toastify-font-family": "inherit",
            "--toastify-color-progress-light": theme.palette.primary.main,
            "--toastify-color-progress-dark": theme.palette.primary.main,
            // The upload toasts drive a real progress bar, so the per-type
            // colours are not decoration.
            "--toastify-color-progress-info": theme.palette.info.main,
            "--toastify-color-progress-success": theme.palette.success.main,
            "--toastify-color-progress-warning": theme.palette.warning.main,
            "--toastify-color-progress-error": theme.palette.error.main,
            "--toastify-color-info": theme.palette.info.main,
            "--toastify-color-success": theme.palette.success.main,
            "--toastify-color-warning": theme.palette.warning.main,
            "--toastify-color-error": theme.palette.error.main,
            "--toastify-spinner-color": theme.palette.primary.main,
            "--toastify-spinner-color-empty-area": theme.palette.divider,
          },
          ".Toastify__toast": {
            borderRadius: `${theme.shape.borderRadius}px`,
            // The library's shadow is `rgba(0, 0, 0, 0.1)`, which is nothing against a dark page:
            // the toast lost its edge and read as a hole rather than a surface above the app.
            border: `1px solid ${theme.palette.divider}`,
            boxShadow: theme.shadows[8],
          },
          // Toastify runs its toasts edge to edge on small screens, where a border and rounded
          // corners would draw a frame around the full width of the viewport.
          "@media only screen and (max-width: 480px)": {
            ".Toastify__toast": {
              borderRadius: 0,
              border: "none",
              borderBottom: `1px solid ${theme.palette.divider}`,
            },
          },
        }}
      />
      {/* react-toastify v9 ships its defaults via `ToastContainer.defaultProps`, which React 19
       * ignores on function components. Without them `autoClose` is undefined, the progress bar
       * renders as "controlled" at 0 and never fires the animationend that dismisses the toast.
       * Pass them explicitly until the library is upgraded to a React 19 compatible version. */}
      <ToastContainer
        transition={Zoom}
        position="top-center"
        hideProgressBar
        theme={theme.palette.mode}
        autoClose={5000}
        closeOnClick
        pauseOnHover
        pauseOnFocusLoss
        draggable
        draggablePercent={80}
        draggableDirection="x"
        role="alert"
        closeButton={ToastCloseButton}
        /**
         * The app's body scale, not toastify's own.
         *
         * A toast passed a bare string renders at the library's 16px, while one built from
         * `Typography` renders at the theme's 14px — two sizes for the same kind of message,
         * depending only on how the call site happened to be written. Setting it here fixes
         * every toast at once rather than rewriting each `toast.success("…")`.
         */
        bodyStyle={{
          fontSize: theme.typography.body2.fontSize,
          lineHeight: theme.typography.body2.lineHeight,
          color: theme.palette.text.primary,
        }}
      />
    </>
  );
}
