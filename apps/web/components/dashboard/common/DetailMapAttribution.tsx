import { Box } from "@mui/material";

import AttributionControl from "@/components/map/controls/Attribution";

/** The credit strip for the dashboard's detail maps, in place of maplibre's own control. */
const DetailMapAttribution = ({
  /** Share of the map's width the strip may take. Full width by default. */
  maxWidth = "100%",
}: {
  maxWidth?: string;
}) => (
  <Box
    // Measured by hosts that place something above it — see the preview map's
    // pinned panel, which stops short of this strip.
    data-catalog-attribution
    sx={{
      position: "absolute",
      right: 0,
      bottom: 0,
      maxWidth,
      display: "flex",
      justifyContent: "flex-end",
      // Both needed for the cap to bite: a flex item will not shrink below its content without `minWidth: 0`, so the strip would otherwise overhang to the left of this box — clipped at its start, with no "more" link, since the text inside was never the thing being cut.
      overflow: "hidden",
      "& > *": { minWidth: 0 },
    }}>
    <AttributionControl />
  </Box>
);

export default DetailMapAttribution;
