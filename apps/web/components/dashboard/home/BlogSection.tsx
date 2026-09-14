"use client";

import {
  Box,
  Button,
  Card,
  CardContent,
  CardMedia,
  Grid,
  Skeleton,
  Stack,
  Typography,
  useTheme,
} from "@mui/material";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { type BlogPost, GOAT_CATEGORY, useBlogPosts } from "@/lib/api/blog";
import { FEEDS_ENABLED, blogIndexUrl, formatFeedDate } from "@/lib/api/feeds";

import HomeSection from "@/components/dashboard/home/HomeSection";

/** How many posts the grid holds at its widest (lg shows three, sm/md four). */
const POST_COUNT = 4;

/**
 * H13: the newest GOAT-tagged posts of the website's blog, from its RSS feed.
 * Returns null when no website URL is configured, the feed fails, or no post
 * carries the GOAT category — a strip with nothing in it is worse than none.
 */
const BlogSection = () => {
  const theme = useTheme();
  const { t, i18n } = useTranslation("common");
  const locale = i18n.language === "de" ? "de" : "en";

  const { posts, isLoading, isError } = useBlogPosts(locale, { category: GOAT_CATEGORY, limit: POST_COUNT });

  if (!FEEDS_ENABLED || isError) return null;
  if (!isLoading && posts.length === 0) return null;

  const items: (BlogPost | undefined)[] = isLoading ? Array.from({ length: POST_COUNT }) : posts;

  return (
    <Box sx={{ pt: "32px", borderTop: `1px solid ${theme.palette.divider}` }}>
      <HomeSection
        title={t("from_our_blog")}
        action={
          <Button
            variant="text"
            size="small"
            component="a"
            href={blogIndexUrl(locale)}
            target="_blank"
            rel="noopener noreferrer"
            endIcon={<Icon iconName={ICON_NAME.EXTERNAL_LINK} style={{ fontSize: 12 }} />}
            sx={{ borderRadius: 0 }}>
            {t("visit_blog")}
          </Button>
        }>
        <Grid container spacing={5}>
          {items.map((item, index) => (
            <Grid
              item
              key={item?.id ?? index}
              xs={12}
              sm={6}
              md={6}
              lg={4}
              display={{
                sm: index > 3 ? "none" : "block",
                md: index > 3 ? "none" : "block",
                lg: index > 2 ? "none" : "block",
              }}>
              {!item ? (
                <Stack spacing={2}>
                  <Skeleton variant="rectangular" height={220} />
                  <Skeleton width="40%" />
                  <Skeleton width="80%" />
                </Stack>
              ) : (
                <Card
                  component="a"
                  href={item.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  variant="outlined"
                  sx={{
                    backgroundColor: "transparent",
                    border: "none",
                    boxShadow: "none",
                    textDecoration: "none",
                    color: "inherit",
                    height: "100%",
                    display: "flex",
                    flexDirection: "column",
                    "&:hover": {
                      cursor: "pointer",
                      "& img": { boxShadow: theme.shadows[4] },
                      "& .blog-title": { color: theme.palette.primary.main },
                    },
                  }}>
                  <CardMedia
                    component="img"
                    image={item.thumbnail ?? undefined}
                    alt=""
                    sx={{
                      height: 220,
                      objectFit: "cover",
                      backgroundColor: theme.palette.action.hover,
                      transition: theme.transitions.create(["box-shadow", "transform"], {
                        duration: theme.transitions.duration.standard,
                      }),
                    }}
                  />
                  <CardContent sx={{ flexGrow: 1, px: 0 }}>
                    <Stack spacing={2}>
                      <Typography gutterBottom variant="caption">
                        {formatFeedDate(item.date, locale)}
                      </Typography>
                      <Typography
                        className="blog-title"
                        sx={{
                          transition: theme.transitions.create(["color", "transform"], {
                            duration: theme.transitions.duration.standard,
                          }),
                        }}
                        fontWeight="bold">
                        {item.title}
                      </Typography>
                    </Stack>
                  </CardContent>
                </Card>
              )}
            </Grid>
          ))}
        </Grid>
      </HomeSection>
    </Box>
  );
};

export default BlogSection;
