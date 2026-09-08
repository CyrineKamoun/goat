"use client";

import { Box, Checkbox, Collapse, IconButton, Stack, Typography, useTheme } from "@mui/material";
import { memo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { CatalogCardModel } from "@/lib/catalog/card";

import { useCatalogLabels } from "@/hooks/catalog/useCatalogLabels";

import CatalogBundleMembers from "@/components/dashboard/catalog/CatalogBundleMembers";
import ContentThumbnail from "@/components/dashboard/common/ContentThumbnail";
import MetaCell from "@/components/dashboard/common/MetaCell";
import SurfaceCard from "@/components/dashboard/common/SurfaceCard";
import TypeTag from "@/components/dashboard/common/TypeTag";

/** One result: thumbnail with the kind tagged over it, title with a save star, two clamped lines of
 * description, and a meta row of publisher / licence / language / period. */

export type CatalogCardSelection = {
  /** Everything this card stands for is selected. */
  selected: boolean;
  /** Some of a bundle's layers are — the checkbox reads as partial. */
  indeterminate?: boolean;
  onToggle: () => void;
  /** Per-layer selection inside the expanded bundle. */
  isMemberSelected?: (memberId: string) => boolean;
  onToggleMember?: (memberId: string) => void;
};

type CatalogCardProps = {
  card: CatalogCardModel;
  view?: "list" | "grid";
  onClick?: () => void;
  /** Opens one layer from the inline expand. */
  onOpenMember?: (memberId: string) => void;
  starred?: boolean;
  onToggleStar?: () => void;
  /**
   * Turns the card into a picker: a checkbox instead of navigation. Present only
   * in the Add Layer modal — the catalog page leaves it out and the card behaves
   * as a link, exactly as before.
   */
  selection?: CatalogCardSelection;
  /**
   * Phone layout: a square mark beside the title instead of a thumbnail band, the
   * kind as a text eyebrow, and the description clamped to one line.
   */
  compact?: boolean;
  /** A bundle's list was opened or closed. */
  onExpandedChange?: (expanded: boolean) => void;
};

const CatalogCard = ({
  card,
  view = "list",
  onClick,
  onOpenMember,
  starred = false,
  onToggleStar,
  selection,
  compact,
  onExpandedChange,
}: CatalogCardProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const labels = useCatalogLabels();
  const [hover, setHover] = useState(false);
  const [expanded, setExpanded] = useState(false);

  const { kind, memberCount } = card;
  const isBundle = kind === "bundle";
  // A phone is one column wide, so grid and list would render identically —
  // compact wins over both.
  const isGrid = view === "grid" && !compact;
  // A tile and a phone card are both about half a row wide, so their meta cells
  // pair up two per line instead of standing in one.
  const twoUp = isGrid || !!compact;

  // No tag at all when the kind is genuinely unknown: labelling every such item
  // "Feature" would be a guess presented as fact.
  const typeLabel = labels.kindLabel(kind);

  // The meta row keeps one order in every view, most-populated first, so only the
  // tail of it can be missing.
  const language = labels.languageLabel(card.languageCode);
  const period = labels.formatPeriod(card.period);
  const license = labels.licenseLabel(card.license);
  const meta = [
    card.publisher && { icon: ICON_NAME.ORGANIZATION, label: card.publisher },
    license && { icon: ICON_NAME.LICENSE, label: license },
    language && { icon: ICON_NAME.LANGUAGE, label: language },
    period && { icon: ICON_NAME.CALENDAR, label: period },
  ].filter((cell): cell is { icon: ICON_NAME; label: string } => !!cell);

  /** One save control, placed differently on a row and on a tile. */
  const star = onToggleStar ? (
    <IconButton
      size="small"
      aria-label={t("save")}
      onClick={(event) => {
        // The whole card navigates to the dataset; saving must not.
        event.stopPropagation();
        onToggleStar();
      }}
      sx={{
        flexShrink: 0,
        opacity: hover || starred ? 1 : 0.55,
        transition: theme.transitions.create("opacity"),
        // No disc behind it on a tile.
      }}>
      <Icon
        iconName={ICON_NAME.STAR}
        style={{ fontSize: 15 }}
        htmlColor={starred ? theme.palette.primary.main : theme.palette.text.secondary}
      />
    </IconButton>
  ) : null;

  /** A bundle's layers, listed the same way wherever they are shown. */
  const memberList = card.bundleId ? (
    <CatalogBundleMembers
      collectionId={card.bundleId}
      dense={isGrid}
      onOpenMember={selection ? undefined : onOpenMember}
      selection={
        selection?.onToggleMember && selection.isMemberSelected
          ? { isSelected: selection.isMemberSelected, onToggle: selection.onToggleMember }
          : undefined
      }
    />
  ) : null;

  const checkbox = selection ? (
    <Checkbox
      size="small"
      checked={selection.selected}
      indeterminate={!!selection.indeterminate}
      // The card handles the click; this must not toggle twice.
      onClick={(event) => event.stopPropagation()}
      onChange={() => selection.onToggle()}
      sx={{ flexShrink: 0, p: 0.5 }}
    />
  ) : null;

  return (
    <SurfaceCard
      selected={!!selection?.selected || !!selection?.indeterminate}
      onClick={() => (selection ? selection.onToggle() : onClick?.())}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      sx={{
        overflow: "hidden",
        cursor: onClick || selection ? "pointer" : "default",
        // A tile is a column with a floor, so a row of them stays even when one card
        // has little to show. The grid sizes the row to the tallest card in it — see
        // the grids' `gridAutoRows: max-content`, which this floor then raises.
        ...(isGrid && {
          display: "flex",
          flexDirection: "column",
          minHeight: 320,
          /**
           * A tile out of view is not laid out, styled or painted — the browser's own
           * answer to "only render what is on screen", and what keeps a scrolled-up
           * list of hundreds cheap. `containIntrinsicSize` is the height to assume
           * while skipped, so the scrollbar does not jump as tiles are passed.
           */
          contentVisibility: "auto",
          containIntrinsicSize: "auto 368px",
        }),
      }}>
      <Box
        sx={{
          /**
           * A tile stacks; a row and a phone card put the thumbnail beside the body.
           *
           * The stack is flex rather than grid on purpose: a flex child is never
           * sized below its own content, whereas the `1fr` body row this used to be
           * could be — and was, whenever a two-line title made the body taller than
           * the card's floor, which cut the meta line in half against the card's
           * clipped edge. The body still grows to fill a card taller than its
           * content, so the meta row's `mt: auto` keeps it at the bottom.
           */
          ...(isGrid
            ? {
                display: "flex",
                flexDirection: "column",
                flexGrow: 1,
                // `auto`, never `0`: a `flex: 1` basis of zero makes this contribute
                // nothing to how tall the card wants to be, and where the grid has a
                // height of its own — the picker's scrolling results column — the row
                // was then sized to the card's floor and the body spilled past the
                // clipped edge.
                flexBasis: "auto",
              }
            : {
                display: "grid",
                gridTemplateColumns: compact
                  ? "44px minmax(0, 1fr)"
                  : { xs: "minmax(0, 1fr)", sm: "200px minmax(0, 1fr)" },
              }),
          gap: compact ? "12px" : isGrid ? "10px" : "18px",
          p: compact ? 3 : isGrid ? 3 : 4,
        }}>
        <Box
          sx={{
            position: "relative",
            width: isGrid ? "100%" : "fit-content",
            flexShrink: 0,
          }}>
          <ContentThumbnail
            kind={kind}
            geometryType={card.geometryType}
            memberCount={memberCount}
            href={card.thumbnailHref}
            variant={compact ? "mark" : isGrid ? "grid" : "list"}
          />
          {typeLabel && !compact && (
            <Box sx={{ position: "absolute", top: 8, left: 8 }}>
              <TypeTag label={typeLabel} />
            </Box>
          )}
          {/* On a tile the star sits over the thumbnail: the title is two clamped lines and cannot give up the width. */}
          {isGrid && (star || checkbox) && (
            <Box sx={{ position: "absolute", top: 4, right: 4, display: "flex", alignItems: "center" }}>
              {star}
              {checkbox}
            </Box>
          )}
        </Box>

        <Stack
          sx={{ minWidth: 0, ...(isGrid && { flexGrow: 1, flexBasis: "auto" }) }}>
          <Stack
            direction="row"
            alignItems="flex-start"
            justifyContent="space-between"
            spacing={3}
            sx={{ mb: isGrid ? 2 : 0 }}>
            <Box sx={{ minWidth: 0 }}>
              {compact && typeLabel && (
                <Typography
                  sx={{
                    fontSize: 10.5,
                    fontWeight: 700,
                    letterSpacing: 0.6,
                    textTransform: "uppercase",
                    color: theme.palette.text.secondary,
                    mb: 0.5,
                  }}>
                  {typeLabel}
                </Typography>
              )}
            <Typography
              fontWeight={700}
              sx={{
                minWidth: 0,
                fontSize: compact ? 14 : isGrid ? 14 : 16,
                letterSpacing: "-0.1px",
                lineHeight: 1.3,
                overflowWrap: "anywhere",
                ...(isGrid || compact
                  ? {
                      display: "-webkit-box",
                      WebkitLineClamp: 2,
                      WebkitBoxOrient: "vertical",
                      overflow: "hidden",
                    }
                  : {}),
              }}>
              {card.title}
            </Typography>
            </Box>
            {!isGrid && (star || checkbox) && (
              <Stack direction="row" alignItems="center" sx={{ flexShrink: 0 }}>
                {star}
                {checkbox}
              </Stack>
            )}
          </Stack>

          {card.description && (
            <Typography
              color="text.secondary"
              sx={{
                mt: 1,
                mb: compact ? 1.5 : 3,
                maxWidth: 920,
                fontSize: compact ? 12 : isGrid ? 12 : 13,
                lineHeight: 1.55,
                display: "-webkit-box",
                WebkitLineClamp: compact ? 1 : 2,
                WebkitBoxOrient: "vertical",
                overflow: "hidden",
              }}>
              {card.description}
            </Typography>
          )}

          {meta.length > 0 && (
            <Box
              sx={{
                mt: compact ? 2 : "auto",
                // A clear gap above the rule even when the title fills the card.
                pt: isGrid ? 3 : 2.5,
                borderTop: `1px solid ${theme.palette.divider}`,
                // Spread across the rule, ends flush with it: the cells divide the width they actually have instead of filling the first N of four fixed columns, which left the last cell stranded three-quarters across and a quarter of the rule with nothing under it.
                display: "grid",
                gridTemplateColumns: `repeat(${meta.length}, minmax(0, auto))`,
                justifyContent: "space-between",
                columnGap: isGrid ? 1.5 : 4,
                rowGap: 1.5,
                // A tile is half the width of a row, so its cells pair up two per line instead of being squeezed into one.
                ...(twoUp && { gridTemplateColumns: "minmax(0, 1fr) auto" }),
              }}>
              {meta.map((cell, index) => (
                <MetaCell
                  key={`${cell.icon}-${cell.label}`}
                  icon={cell.icon}
                  label={cell.label}
                  sx={
                    twoUp
                      ? {
                          // Right-hand cells sit against the right edge; a final cell with no partner takes the whole line rather than half of it, which is what left "since 2010" stranded under a truncated publisher.
                          ...(index % 2 === 1 && { justifySelf: "end" }),
                          ...(index === meta.length - 1 &&
                            meta.length % 2 === 1 && { gridColumn: "1 / -1" }),
                        }
                      : undefined
                  }
                />
              ))}
            </Box>
          )}
        </Stack>
      </Box>

      {/* A bundle lists its layers in either view; the strip is the control in both. */}
      {isBundle && card.bundleId && (
        <>
          <Box
            component="button"
            type="button"
            onClick={(event: React.MouseEvent) => {
              event.stopPropagation();
              // Both computed here rather than inside the updater: React may run
              // an updater during the render phase (and runs it twice under
              // StrictMode), so telling the parent from in there is a setState
              // while rendering — "Cannot update a component (CatalogPickerCard)
              // while rendering a different component (CatalogCard)".
              const next = !expanded;
              setExpanded(next);
              onExpandedChange?.(next);
            }}
            sx={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              width: "100%",
              flexShrink: 0,
              px: isGrid ? 3 : 5,
              py: isGrid ? 2 : 2.5,
              font: "inherit",
              cursor: "pointer",
              border: "none",
              borderTop: `1px solid ${theme.palette.divider}`,
              backgroundColor: theme.palette.action.hover,
              "&:hover": { backgroundColor: theme.palette.action.selected },
            }}>
            <Typography
              variant={isGrid ? "caption" : "body2"}
              color="text.secondary"
              fontWeight={500}>
              {t("catalog_member_count", { count: memberCount })}
            </Typography>
            <Stack direction="row" spacing={1.5} alignItems="center">
              {/* The chevron carries it on a tile, where the words would crowd the
                  count they sit beside. */}
              {!isGrid && (
                <Typography variant="body2" color="primary" fontWeight={600}>
                  {expanded ? t("collapse") : t("expand")}
                </Typography>
              )}
              <Icon
                iconName={expanded ? ICON_NAME.CHEVRON_UP : ICON_NAME.CHEVRON_DOWN}
                style={{ fontSize: 14 }}
                htmlColor={theme.palette.primary.main}
              />
            </Stack>
          </Box>
          <Collapse in={expanded} unmountOnExit sx={{ flexShrink: 0 }}>
            {memberList}
          </Collapse>
        </>
      )}
    </SurfaceCard>
  );
};

export default memo(CatalogCard);
