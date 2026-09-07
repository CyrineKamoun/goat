"use client";

import { Box, alpha, useTheme } from "@mui/material";
import { Fragment } from "react";

interface HighlightedTextProps {
  /** The label as it should read. */
  text: string;
  /** What the user typed. Matched case-insensitively; only the first hit is marked. */
  query: string;
}

/**
 * Marks where a result label answers the query, so a row's relevance is
 * readable at a glance rather than inferred. Renders the label unchanged when
 * the query is empty or does not occur in it.
 */
const HighlightedText = ({ text, query }: HighlightedTextProps) => {
  const theme = useTheme();
  const needle = query.trim();
  const at = needle ? text.toLowerCase().indexOf(needle.toLowerCase()) : -1;

  if (at < 0) return <Fragment>{text}</Fragment>;

  return (
    <Fragment>
      {text.slice(0, at)}
      <Box
        component="mark"
        sx={{
          backgroundColor: alpha(theme.palette.primary.main, 0.16),
          color: theme.palette.text.primary,
          borderRadius: "3px",
          px: "1px",
        }}>
        {text.slice(at, at + needle.length)}
      </Box>
      {text.slice(at + needle.length)}
    </Fragment>
  );
};

export default HighlightedText;
