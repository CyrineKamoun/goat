"use client";

import { Container, Skeleton, Stack, Typography } from "@mui/material";
import { useRouter } from "next/navigation";
import { useCallback } from "react";

import { useFavoriteStars } from "@/lib/api/favorites";

import { favoriteDatasetId } from "@/lib/utils/catalogFavorites";
import { useTranslation } from "react-i18next";

import type { CatalogCollection, CatalogItem } from "@/lib/validations/catalog";

import CatalogBundleDetail from "@/components/dashboard/catalog/CatalogBundleDetail";
import CatalogLayerDetail from "@/components/dashboard/catalog/CatalogLayerDetail";

/** Detail view for one catalog entry, dispatching on what the id turned out to be: a bundle (`/stac/resolve` says `collection`) or a single dataset. */

type Props = {
  entryId: string;
  collection?: CatalogCollection;
  members?: CatalogItem[];
  item?: CatalogItem;
  parent?: CatalogCollection;
  isLoading: boolean;
  isError?: boolean;
};

const CatalogDetailView = ({
  collection,
  members = [],
  item,
  parent,
  isLoading,
  isError,
}: Props) => {
  const router = useRouter();
  const { t } = useTranslation("common");

  // The same persisted favourites the list page and the Add Layer picker use;
  // a star set here shows there and survives a reload. Both of those work in
  // DATASETS, so a layer page saves the dataset it belongs to — see
  // `favoriteDatasetId`.
  const { starred, toggleStar: toggleFavorite } = useFavoriteStars("catalog_item");
  const toggleStar = useCallback((target: CatalogItem) => toggleFavorite(target.id), [toggleFavorite]);
  const toggleAll = useCallback(
    (targets: CatalogItem[], save: boolean) => {
      for (const target of targets) {
        if (!!starred[target.id] !== save) toggleFavorite(target.id);
      }
    },
    [starred, toggleFavorite]
  );

  // A bundle member came from its bundle, so back belongs to the bundle rather
  // than to whatever the browser remembers.
  const parentIsBundle = (parent?.["goat:member_count"] ?? 1) > 1;

  /** The one layer of a single-layer dataset, if that is what this id resolved to. */
  const singleLayer =
    collection && (collection["goat:member_count"] ?? members.length) === 1
      ? members[0]
      : undefined;

  /** Which dataset a star on this page saves. */
  const savedAs = favoriteDatasetId({ parent, item }) ?? "";

  /** Back to the list, with the search that produced it intact. */
  const backToList = useCallback(() => {
    if (typeof window !== "undefined" && window.history.length > 1) {
      router.back();
      return;
    }
    router.push("/catalog");
  }, [router]);

  return (
    <Container sx={{ py: 10, px: { xs: 4, sm: 10 } }} maxWidth="xl">
      {isLoading && (
        <Stack spacing={4}>
          <Skeleton variant="text" width={120} />
          <Skeleton variant="text" width="60%" height={44} />
          <Skeleton variant="rectangular" width="100%" height={420} />
        </Stack>
      )}

      {!isLoading && isError && (
        <Typography variant="body2" color="error">
          {t("catalog_entry_not_found")}
        </Typography>
      )}

      {/* A dataset with ONE layer is that layer. */}
      {!isLoading && !isError && collection && singleLayer && (
        <CatalogLayerDetail
          item={singleLayer}
          collection={collection}
          onBack={backToList}
          starred={!!starred[collection.id]}
          onToggleStar={() => toggleFavorite(collection.id)}
        />
      )}

      {!isLoading && !isError && collection && !singleLayer && (
        <CatalogBundleDetail
          collection={collection}
          members={members}
          onBack={backToList}
          onOpenMember={(member) => router.push(`/catalog/${encodeURIComponent(member.id)}`)}
          starred={starred}
          onToggleStar={toggleStar}
          onToggleAll={toggleAll}
        />
      )}

      {!isLoading && !isError && item && (
        <CatalogLayerDetail
          item={item}
          collection={parent}
          onBack={() =>
            // A member goes up to its bundle; anything else goes back to the list it came from, filters and all.
            parentIsBundle && parent
              ? router.replace(`/catalog/${encodeURIComponent(parent.id)}`)
              : backToList()
          }
          backLabel={parentIsBundle ? (parent?.title || undefined) : undefined}
          starred={!!starred[savedAs]}
          onToggleStar={() => toggleFavorite(savedAs)}
        />
      )}
    </Container>
  );
};

export default CatalogDetailView;
