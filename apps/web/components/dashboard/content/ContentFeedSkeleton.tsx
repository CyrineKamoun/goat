"use client";

import { Box, Skeleton, useTheme } from "@mui/material";

import { sectionGridSx } from "@/components/dashboard/content/sectionGrid";

/** The feed's loading state, in the shape of the layout the reader chose:
 * folder and tile cards for the grid, rows for the list. Sized from the same
 * numbers the real cards and rows are drawn with, so nothing shifts when the
 * data lands. */

/** `ContentFolderCard`: a 38px mark inside 14px of vertical padding. */
const FOLDER_CARD_HEIGHT = 66;
/** `ContentCard`: a bordered surface holding the thumbnail, then a body with
 * the name row (28px, 9px above it) and the creator row (22px avatar and a
 * date, 13px below it). */
const TILE_THUMBNAIL_HEIGHT = { desktop: 132, mobile: 96 };
/** `ContentRow`'s own height, from its 11px padding around a 22px line. */
const ROW_HEIGHT = 44;

const SectionHeadingSkeleton = () => <Skeleton variant="text" width={72} sx={{ fontSize: 15, mb: "12px" }} />;

const ContentFeedSkeleton = ({ layout, mobile = false }: { layout: "tiles" | "list"; mobile?: boolean }) => {
  const theme = useTheme();
  if (layout === "list") {
    return (
      <Box>
        <Skeleton variant="rounded" height={36} sx={{ borderRadius: "8px", mb: "8px" }} />
        <Box sx={{ display: "flex", flexDirection: "column", gap: "8px" }}>
          {Array.from({ length: 6 }).map((_, index) => (
            <Skeleton key={index} variant="rounded" height={ROW_HEIGHT} sx={{ borderRadius: "8px" }} />
          ))}
        </Box>
      </Box>
    );
  }

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: "26px" }}>
      <Box>
        <SectionHeadingSkeleton />
        <Box sx={sectionGridSx("folders", mobile)}>
          {Array.from({ length: 4 }).map((_, index) => (
            <Skeleton
              key={index}
              variant="rounded"
              height={FOLDER_CARD_HEIGHT}
              sx={{ borderRadius: "12px" }}
            />
          ))}
        </Box>
      </Box>

      <Box>
        <SectionHeadingSkeleton />
        <Box sx={sectionGridSx("cards", mobile)}>
          {Array.from({ length: 6 }).map((_, index) => (
            <Box
              key={index}
              sx={{
                border: `1px solid ${theme.palette.divider}`,
                borderRadius: "12px",
                overflow: "hidden",
              }}>
              <Skeleton
                variant="rectangular"
                height={mobile ? TILE_THUMBNAIL_HEIGHT.mobile : TILE_THUMBNAIL_HEIGHT.desktop}
              />
              <Box sx={{ px: "11px", pt: "9px", pb: "13px" }}>
                <Box sx={{ height: 28, display: "flex", alignItems: "center" }}>
                  <Skeleton variant="text" width="72%" sx={{ fontSize: 14 }} />
                </Box>
                <Box sx={{ mt: "4px", display: "flex", alignItems: "center", gap: "8px" }}>
                  <Skeleton variant="circular" width={22} height={22} />
                  <Skeleton variant="text" width={70} sx={{ fontSize: 12 }} />
                </Box>
              </Box>
            </Box>
          ))}
        </Box>
      </Box>
    </Box>
  );
};

export default ContentFeedSkeleton;
