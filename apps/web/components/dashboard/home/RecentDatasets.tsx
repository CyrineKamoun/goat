"use client";

import { Button, Skeleton, Stack, useMediaQuery, useTheme } from "@mui/material";
import dynamic from "next/dynamic";
import { useRouter } from "next/navigation";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useContent, useSpaces } from "@/lib/api/content";
import type { ContentItem } from "@/lib/validations/content";

import { contentPath } from "@/hooks/dashboard/content/useContentPageState";

import type { PopperMenuItem } from "@/components/common/PopperMenu";
import SurfaceCard from "@/components/dashboard/common/SurfaceCard";
import ContentRow from "@/components/dashboard/content/ContentRow";
import HomeSection from "@/components/dashboard/home/HomeSection";

const ContentPreviewDialog = dynamic(() => import("@/components/dashboard/content/ContentPreviewDialog"), {
  ssr: false,
});

/** How many rows show, and how many row skeletons render on first load. */
const ROW_COUNT = 5;

/** H5: the feed's Recent view, filtered to layers and bundles, five rows.
 * A dataset opens in the preview dialog, the way a Content row does; anything
 * else lands in its Content location, as every kebab action does. Renders nothing once loaded
 * when the feed has no datasets; `HomePage` decides whether this band exists
 * at all. While the first load is in flight (no cached page yet — SWR keeps
 * the previous page across a revalidation, so a refetch never re-triggers
 * this), the header still renders with row skeletons — the same shape
 * `ContentPage` uses for its own loading rows — so the page doesn't pop in
 * once every band's data lands. */
const RecentDatasets = () => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  // Below `md` the row drops its creator, audience and space columns; without
  // this the fixed-width columns force the whole page wider than the screen.
  const mobile = useMediaQuery(theme.breakpoints.down("md"));
  const router = useRouter();
  const [previewLayerId, setPreviewLayerId] = useState<string | null>(null);

  const { page, isLoading } = useContent({ view: "recent", types: "layer,bundle", size: 5 });
  const { spaces } = useSpaces();

  const items = page?.items ?? [];

  const showSkeleton = isLoading && !page;
  if (!showSkeleton && items.length === 0) return null;

  const goToContent = (item: ContentItem) => router.push(contentPath({ spaceId: item.space_id }));

  const openItem = (item: ContentItem) => {
    if (item.type === "layer") {
      setPreviewLayerId(item.id);
      return;
    }
    goToContent(item);
  };

  const menuItems: PopperMenuItem[] = [
    { id: "open", label: t("open"), icon: ICON_NAME.EXTERNAL_LINK },
    { id: "show_in_content", label: t("show_in_content"), icon: ICON_NAME.FOLDER },
  ];

  const handleMenuSelect = (menuItem: PopperMenuItem, item: ContentItem) => {
    if (menuItem.id === "open" || menuItem.id === "show_in_content") goToContent(item);
  };

  return (
    <HomeSection
      title={t("recent_datasets")}
      sx={{ flex: 1, minWidth: 0 }}
      action={
        <Button
          variant="text"
          size="small"
          endIcon={<Icon iconName={ICON_NAME.CHEVRON_RIGHT} style={{ fontSize: 12 }} />}
          onClick={() => router.push(contentPath({ view: "recent" }))}
          sx={{ borderRadius: 0 }}>
          {t("all_datasets")}
        </Button>
      }>
      {showSkeleton ? (
        <Stack spacing={1}>
          {Array.from({ length: ROW_COUNT }).map((_, index) => (
            <Skeleton key={index} variant="rectangular" height={56} sx={{ borderRadius: "10px" }} />
          ))}
        </Stack>
      ) : (
        <SurfaceCard hoverable={false} sx={{ overflow: "hidden", p: "6px" }}>
          {items.map((item) => (
            <ContentRow
              key={item.id}
              item={item}
              space={spaces.find((space) => space.id === item.space_id)}
              variant="recent"
              mobile={mobile}
              selected={false}
              anySelected={false}
              onToggleSelect={() => {}}
              onOpen={openItem}
              menuItems={menuItems}
              onMenuSelect={(menuItem) => handleMenuSelect(menuItem, item)}
              selectable={false}
              flat
            />
          ))}
        </SurfaceCard>
      )}
      {previewLayerId && (
        <ContentPreviewDialog layerId={previewLayerId} onClose={() => setPreviewLayerId(null)} />
      )}
    </HomeSection>
  );
};

export default RecentDatasets;
