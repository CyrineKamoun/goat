"use client";

import { Box, Button, Skeleton, useMediaQuery, useTheme } from "@mui/material";
import { useRouter } from "next/navigation";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useContent, useSpaces } from "@/lib/api/content";
import { useFavoriteStars } from "@/lib/api/favorites";
import type { ContentItem } from "@/lib/validations/content";

import { contentPath } from "@/hooks/dashboard/content/useContentPageState";

import type { PopperMenuItem } from "@/components/common/PopperMenu";
import ContentCard from "@/components/dashboard/content/ContentCard";
import HomeSection from "@/components/dashboard/home/HomeSection";

/** At most this many cards, regardless of how many the feed and the pins
 * together would offer (H4). Also how many skeleton cards render on first
 * load. */
const CARD_COUNT = 4;

/** One grid cell's worth of loading placeholder — a rounded rectangle at the
 * thumbnail height `ContentCard` uses, plus its two text lines. */
const CardSkeleton = ({ mobile }: { mobile: boolean }) => (
  <Box>
    <Skeleton variant="rectangular" height={mobile ? 96 : 132} sx={{ borderRadius: "11px" }} />
    <Skeleton variant="text" width="70%" sx={{ fontSize: 14, mt: "9px" }} />
    <Skeleton variant="text" width="45%" sx={{ fontSize: 12, mt: "2px" }} />
  </Box>
);

/** H1/H4/H14: up to four project cards — the caller's pinned projects (in
 * pin order), then the rest by last opened (falling back to `updated_at`,
 * via `order_by=last_opened_at` on the feed). Renders nothing once loaded
 * when the feed has no projects; `HomePage` decides whether that band exists
 * at all. While the first load is in flight (no cached page yet — SWR keeps
 * the previous page across a revalidation, so a refetch never re-triggers
 * this), the header still renders with skeleton cards in its place so the
 * page doesn't pop in once every band's data lands. */
const JumpBackIn = () => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const mobile = useMediaQuery(theme.breakpoints.down("md"));
  const router = useRouter();

  const { page, isLoading } = useContent({
    view: "recent",
    types: "project",
    order_by: "last_opened_at",
    // Wider than the four cards shown so a pinned project that was not opened
    // recently still reaches the band; the feed has no id filter yet.
    size: 24,
  });
  const { spaces } = useSpaces();
  const { starred, toggleStar } = useFavoriteStars("project");

  const items = useMemo(() => {
    const feed = page?.items ?? [];
    const byId = new Map(feed.map((item) => [item.id, item]));
    // `starred` keys come back newest-pinned-first (see `useFavoriteStars`),
    // so that order is the pin order H4 asks for.
    const pinned = Object.keys(starred)
      .map((id) => byId.get(id))
      .filter((item): item is ContentItem => item !== undefined);
    const rest = feed.filter((item) => !starred[item.id]);
    return [...pinned, ...rest].slice(0, CARD_COUNT);
  }, [page, starred]);

  const showSkeleton = isLoading && !page;
  if (!showSkeleton && items.length === 0) return null;

  const menuItemsFor = (item: ContentItem): PopperMenuItem[] => [
    { id: "open", label: t("open"), icon: ICON_NAME.EXTERNAL_LINK },
    {
      id: starred[item.id] ? "unpin_from_home" : "pin_to_home",
      label: t(starred[item.id] ? "unpin_from_home" : "pin_to_home"),
      icon: ICON_NAME.BOOKMARK,
    },
    { id: "show_in_content", label: t("show_in_content"), icon: ICON_NAME.FOLDER, group: "content" },
  ];

  const handleMenuSelect = (menuItem: PopperMenuItem, item: ContentItem) => {
    if (menuItem.id === "open") router.push(`/map/${item.id}`);
    else if (menuItem.id === "pin_to_home" || menuItem.id === "unpin_from_home") toggleStar(item.id);
    else if (menuItem.id === "show_in_content") router.push(contentPath({ spaceId: item.space_id }));
  };

  return (
    <HomeSection
      title={t("jump_back_in")}
      action={
        <Button
          variant="text"
          size="small"
          endIcon={<Icon iconName={ICON_NAME.CHEVRON_RIGHT} style={{ fontSize: 12 }} />}
          onClick={() => router.push(contentPath({ view: "recent" }))}
          sx={{ borderRadius: 0 }}>
          {t("all_projects")}
        </Button>
      }>
      <Box
        sx={{
          display: "grid",
          gridTemplateColumns: mobile
            ? "repeat(auto-fill, minmax(min(100%, 168px), 1fr))"
            : "repeat(auto-fill, minmax(210px, 1fr))",
          gap: mobile ? "10px" : "16px",
        }}>
        {showSkeleton
          ? Array.from({ length: CARD_COUNT }).map((_, index) => <CardSkeleton key={index} mobile={mobile} />)
          : items.map((item) => (
              <ContentCard
                key={item.id}
                item={item}
                space={spaces.find((space) => space.id === item.space_id)}
                selected={false}
                anySelected={false}
                onToggleSelect={() => {}}
                onOpen={(openedItem) => router.push(`/map/${openedItem.id}`)}
                menuItems={menuItemsFor(item)}
                onMenuSelect={(menuItem) => handleMenuSelect(menuItem, item)}
                selectable={false}
                pinned={!!starred[item.id]}
                onTogglePin={() => toggleStar(item.id)}
                mobile={mobile}
              />
            ))}
      </Box>
    </HomeSection>
  );
};

export default JumpBackIn;
