"use client";

import { Box, Button, Stack, Typography, useMediaQuery, useTheme } from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { useContent } from "@/lib/api/content";
import { useFolders } from "@/lib/api/folders";
import type { ContentItem } from "@/lib/validations/content";

import { useDatasetPickerState } from "@/hooks/addLayer/useDatasetPickerState";
import { useAccumulatedPages } from "@/hooks/dashboard/content/useAccumulatedPages";

import EmptyState from "@/components/dashboard/common/EmptyState";
import LayoutToggle from "@/components/dashboard/common/LayoutToggle";
import SearchInput from "@/components/dashboard/common/SearchInput";
import SortMenu from "@/components/dashboard/common/SortMenu";
import ContentBreadcrumb from "@/components/dashboard/content/ContentBreadcrumb";
import ContentCard from "@/components/dashboard/content/ContentCard";
import ContentFeedSkeleton from "@/components/dashboard/content/ContentFeedSkeleton";
import ContentFolderCard from "@/components/dashboard/content/ContentFolderCard";
import ContentRow from "@/components/dashboard/content/ContentRow";
import ContentSection from "@/components/dashboard/content/ContentSection";
import ContentSpacesPanel from "@/components/dashboard/content/ContentSpacesPanel";
import { SORT_ITEMS } from "@/components/dashboard/content/ContentToolbar";
import type { SectionGridKind } from "@/components/dashboard/content/sectionGrid";
import { sectionGridSx } from "@/components/dashboard/content/sectionGrid";

export type DatasetPickerSelection = {
  /** The selected dataset and bundle ids, in the order they were picked. */
  ids: string[];
  /** The selected items themselves, so a caller can read their names/types
   * without listing the feed again. */
  items: ContentItem[];
  /** Takes the item rather than its id: the selection keeps its own snapshot
   * of what was picked, so it survives browsing away from the page the item
   * was listed on. */
  toggle: (item: ContentItem) => void;
  clear: () => void;
};

export type DatasetPickerBodyProps = (
  | { mode: "add"; selection: DatasetPickerSelection }
  | {
      mode: "pick";
      picked: ContentItem | null;
      /** `null` when the picked item is clicked again. */
      onPickedChange: (item: ContentItem | null) => void;
    }
) & {
  /** Fills the host's content area instead of standing at the shelf's own
   * height — what a full-screen dialog needs, so nothing is left empty
   * between the feed and the footer. */
  fullHeight?: boolean;
};

const NO_MENU: never[] = [];

/** What a single-pick host can use: a bundle is not a layer, and both
 * workflow hosts take a layer, so `pick` mode does not offer one. */
const PICK_TYPES = "folder,layer";

/**
 * One of the user's own datasets, picked from the same shelf the Content
 * page shows: spaces on the left, folders to navigate, datasets and bundles
 * to select. A shelf only — nothing opens, nothing is shared, moved or
 * deleted from here.
 */
const DatasetPickerBody = (props: DatasetPickerBodyProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const mobile = useMediaQuery(theme.breakpoints.down("md"));
  const state = useDatasetPickerState(props.mode === "pick" ? { types: PICK_TYPES } : {});
  const { page: fetchedPage } = useContent(state.feedParams);
  const { folders } = useFolders({});
  const [openSections, setOpenSections] = useState({ folders: true, datasets: true });

  // "Load more" advances the page at a fixed size, so the pages already
  // listed are accumulated rather than re-read in one ever-growing request.
  // The collection carries the sort as well as the scope: pages of two
  // different orderings must never be listed together.
  const requestedPage = state.feedParams?.page ?? 1;
  const collectionKey = state.feedParams
    ? `${state.scopeKey}|${JSON.stringify({ ...state.feedParams, page: undefined })}`
    : null;
  const accumulated = useAccumulatedPages(fetchedPage, requestedPage, collectionKey);

  // Hold the items last listed for this space, folder or view. Asking the
  // same collection for a new sort re-keys SWR, which drops the response in
  // hand, and the held list covers that window so the shelf stays as it is
  // instead of blanking to a skeleton.
  const [held, setHeld] = useState<{ scopeKey: string; items: ContentItem[]; total: number } | null>(null);
  useEffect(() => {
    if (accumulated.items.length > 0) {
      setHeld({ scopeKey: state.scopeKey, items: accumulated.items, total: accumulated.total });
    }
  }, [accumulated.items, accumulated.total, state.scopeKey]);

  const holding = accumulated.items.length === 0 && held?.scopeKey === state.scopeKey;
  const items = holding ? held.items : accumulated.items;
  const total = holding ? held.total : accumulated.total;
  const loaded = holding || accumulated.loaded;
  const folderItems = useMemo(() => items.filter((item) => item.type === "folder"), [items]);
  const datasetItems = useMemo(
    () => items.filter((item) => item.type === "layer" || item.type === "bundle"),
    [items]
  );

  const hasMore = total > items.length;

  const spaceOf = (item: ContentItem) => state.spaces.find((space) => space.id === item.space_id);
  const active = state.active;
  const activeSpace =
    active.kind === "space" ? state.spaces.find((space) => space.id === active.spaceId) : undefined;

  const isSelected = (id: string) =>
    props.mode === "add" ? props.selection.ids.includes(id) : props.picked?.id === id;
  const toggle = (item: ContentItem) => {
    if (props.mode === "add") props.selection.toggle(item);
    else props.onPickedChange(props.picked?.id === item.id ? null : item);
  };
  const anySelected = props.mode === "add" ? props.selection.ids.length > 0 : props.picked !== null;

  /** A folder is entered, never picked — and it is entered in the space it
   * actually lives in, which is not the active one for a folder shortcut or
   * a folder listed by a cross-space view. */
  const openFolder = (item: ContentItem) => state.goFolderIn(item.space_id, item.id);
  const folderProps = {
    selected: false,
    anySelected: false,
    onToggleSelect: () => {},
    selectable: false,
    onOpen: openFolder,
  };
  const pickProps = (item: ContentItem) => ({
    selected: isSelected(item.id),
    anySelected,
    onToggleSelect: () => toggle(item),
    // A click on the tile is a pick, never an open.
    onOpen: (picked: ContentItem) => toggle(picked),
  });
  const sharedProps = (item: ContentItem) => ({
    item,
    space: spaceOf(item),
    menuItems: NO_MENU,
    onMenuSelect: () => {},
    mobile,
  });
  const rowVariant = active.kind === "view" ? active.view : "space";

  const renderCard = (item: ContentItem) =>
    item.type === "folder" ? (
      <ContentFolderCard key={item.id} {...sharedProps(item)} {...folderProps} />
    ) : (
      <ContentCard key={item.id} {...sharedProps(item)} {...pickProps(item)} />
    );

  const renderRow = (item: ContentItem) =>
    item.type === "folder" ? (
      <ContentRow key={item.id} {...sharedProps(item)} variant={rowVariant} {...folderProps} />
    ) : (
      <ContentRow key={item.id} {...sharedProps(item)} variant={rowVariant} {...pickProps(item)} />
    );

  /** One group of the feed, in the layout the reader chose: the Content
   * page's tiles, or its rows. */
  const renderItems = (list: ContentItem[], kind: SectionGridKind = "cards") =>
    state.layout === "grid" ? (
      <Box sx={sectionGridSx(kind, mobile)}>{list.map((item) => renderCard(item))}</Box>
    ) : (
      <Box sx={{ display: "flex", flexDirection: "column", gap: "8px" }}>
        {list.map((item) => renderRow(item))}
      </Box>
    );

  const sortValue =
    SORT_ITEMS.find((option) => option.by === state.orderBy && option.dir === state.order)?.id ??
    SORT_ITEMS[0].id;

  return (
    <Stack
      direction="row"
      sx={{ height: props.fullHeight ? "100%" : "min(760px, 74vh)", minHeight: 0, width: "100%" }}>
      {!mobile && (
        // The rail is `flush`, which drops its own border, so the one rule
        // between it and the results column is drawn here.
        <Box sx={{ display: "flex", flexShrink: 0, borderRight: `1px solid ${theme.palette.divider}` }}>
          <ContentSpacesPanel
            flush
            spaces={state.spaces}
            active={state.active}
            onSelectSpace={state.goSpace}
            onSelectView={state.goView}
            loading={state.spacesLoading}
          />
        </Box>
      )}

      {/* The results column carries no horizontal padding of its own: the search
          row and the feed each pay for their inset, which lets the rule between
          them run from the rail to the frame. */}
      <Stack sx={{ flex: 1, minWidth: 0, minHeight: 0, pt: 4 }}>
        <Box sx={{ px: 6, pb: 3, borderBottom: `1px solid ${theme.palette.divider}` }}>
          <SearchInput
            value={state.search}
            onChange={state.setSearch}
            onClear={() => state.setSearch("")}
            placeholder={t("search_datasets")}
            fullWidth
          />
          <Stack direction="row" alignItems="center" sx={{ mt: 3, gap: 2 }}>
            <Box sx={{ minWidth: 0, flex: 1, display: "flex", alignItems: "center", gap: "8px" }}>
              <ContentBreadcrumb
                space={activeSpace}
                folders={folders ?? []}
                folderId={state.folderId}
                onNavigate={state.goFolder}
                view={active.kind === "view" ? active.view : undefined}
                loading={state.spacesLoading}
              />
            </Box>
            <LayoutToggle value={state.layout} onChange={state.setLayout} compact={mobile} />
            <SortMenu
              value={sortValue}
              label={t("sort")}
              compact={mobile}
              options={SORT_ITEMS.map((option) => ({
                value: option.id,
                label: t(option.labelKey),
                icon: option.icon,
              }))}
              onChange={(id) => {
                const picked = SORT_ITEMS.find((option) => option.id === id);
                if (picked) state.setSort(picked.by, picked.dir);
              }}
            />
          </Stack>
          {/* In `add` mode the body draws only this strip; the primary button
              lives in the frame's footer. `pick` mode draws nothing here — its
              wrapper dialog owns the button and shows no count. The count is
              of the datasets listed: the feed's total spans folders too, and
              would contradict the section headers right below it. */}
          {props.mode === "add" && (
            <Stack direction="row" alignItems="center" justifyContent="space-between" sx={{ mt: 2 }}>
              <Typography variant="caption" color="text.secondary">
                {t("n_datasets", { count: datasetItems.length })}
                {props.selection.ids.length > 0 &&
                  ` · ${t("catalog_n_selected", { count: props.selection.ids.length })}`}
              </Typography>
              {props.selection.ids.length > 0 && (
                <Typography
                  component="button"
                  variant="caption"
                  onClick={props.selection.clear}
                  sx={{
                    background: "transparent",
                    border: "none",
                    p: 0,
                    cursor: "pointer",
                    fontWeight: 600,
                    color: theme.palette.primary.main,
                  }}>
                  {t("catalog_clear_selection")}
                </Typography>
              )}
            </Stack>
          )}
        </Box>

        <Box sx={{ flex: 1, minHeight: 0, overflowY: "auto", px: 6, pt: 3, pb: 4 }}>
          {/* The skeleton stands only while there is nothing to show at all:
              the picker opens before its space is known, so it holds that
              window rather than the reader being told the space is empty. */}
          {!loaded && (
            <ContentFeedSkeleton layout={state.layout === "grid" ? "tiles" : "list"} mobile={mobile} />
          )}
          {loaded && items.length === 0 && (
            <EmptyState
              icon={ICON_NAME.FOLDER}
              title={state.search ? t("no_results_for", { q: state.search }) : t("nothing_here_yet")}
            />
          )}
          {/* A cross-space view spans every space, so it groups by nothing and
              renders as one flat list, as on the Content page. */}
          {loaded && items.length > 0 && active.kind === "view" && renderItems(items)}
          {loaded && items.length > 0 && active.kind === "space" && (
            <Stack spacing={6}>
              {folderItems.length > 0 && (
                <ContentSection
                  labelKey="folders"
                  count={folderItems.length}
                  open={openSections.folders}
                  onToggle={() => setOpenSections((prev) => ({ ...prev, folders: !prev.folders }))}>
                  {renderItems(folderItems, "folders")}
                </ContentSection>
              )}
              {datasetItems.length > 0 && (
                <ContentSection
                  labelKey="datasets"
                  count={datasetItems.length}
                  open={openSections.datasets}
                  onToggle={() => setOpenSections((prev) => ({ ...prev, datasets: !prev.datasets }))}>
                  {renderItems(datasetItems)}
                </ContentSection>
              )}
            </Stack>
          )}
          {/* The feed holds more than this page lists, in the flat views as
              well as in the sections above. */}
          {hasMore && (
            <Stack direction="row" justifyContent="center" sx={{ mt: 4 }}>
              <Button variant="outlined" onClick={state.loadMore}>
                {t("load_more")}
              </Button>
            </Stack>
          )}
        </Box>
      </Stack>
    </Stack>
  );
};

export default DatasetPickerBody;
