"use client";

import { Box, Button, Skeleton, Typography, useMediaQuery, useTheme } from "@mui/material";
import { useRouter } from "next/navigation";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useSpaces } from "@/lib/api/content";
import { useFavoriteStars } from "@/lib/api/favorites";
import { useTemplates } from "@/lib/api/templates";
import { interleaveTemplateStrip, rankTemplateStrip } from "@/lib/utils/templateStrip";
import {
  TEMPLATE_EMPTY_HINT_KEY,
  templateEmptyTitle,
  templateShelfOf,
  templateSourceLabel,
} from "@/lib/utils/templates";
import type { TemplateKind, TemplateRead, TemplateSourceFilter } from "@/lib/validations/template";

import { contentPath } from "@/hooks/dashboard/content/useContentPageState";
import { templateResultHref } from "@/hooks/templates/useUseTemplate";

import EmptyState from "@/components/dashboard/common/EmptyState";
import StarterCard from "@/components/dashboard/common/StarterCard";
import HomeSection from "@/components/dashboard/home/HomeSection";
import TemplateBrowser from "@/components/templates/TemplateBrowser";
import TemplateKindFilter from "@/components/templates/TemplateKindFilter";
import TemplatePreviewDialog from "@/components/templates/TemplatePreviewDialog";
import TemplateSourceMenu from "@/components/templates/TemplateSourceMenu";
import UseTemplateFlow from "@/components/templates/UseTemplateFlow";

const GRID_SIZE = 12;
const MAX_CARDS = 6;
const MAX_CARDS_MOBILE = 4;

/** One grid cell's worth of loading placeholder — a rounded rectangle at the
 * thumbnail height `StarterCard` uses, plus its two text lines. */
const CardSkeleton = ({ mobile }: { mobile: boolean }) => (
  <Box>
    <Skeleton variant="rectangular" height={mobile ? 96 : 132} sx={{ borderRadius: "11px" }} />
    <Skeleton variant="text" width="70%" sx={{ fontSize: 14, mt: "9px" }} />
    <Skeleton variant="text" width="45%" sx={{ fontSize: 12, mt: "2px" }} />
  </Box>
);

/**
 * §4's Home band: "Start from a template" with kind/source filters shared
 * with the browser, a capped grid of `StarterCard`s (pinned first), and a
 * footer that both states the visible/total count and links out to the
 * fuller surface for whichever source is selected — the GOAT catalog's
 * Templates tab for `goat`, Content filtered to templates for `mine`/`team`/
 * `org`. Card click opens the preview dialog; "Use template" hands off to
 * `UseTemplateFlow` in the `new_project` context, same as every other
 * unfiltered entry point (T7). Renders nothing only when the feed itself
 * errors — an empty page (genuinely zero templates) still shows the header
 * and filters, per §3's "mount every stage". While the first load is in
 * flight (no cached page yet — SWR keeps the previous page across a
 * revalidation, so a refetch never re-triggers this), the header still
 * renders but the filters and footer stay hidden and a skeleton grid takes
 * the cards' place, so the page doesn't pop in once every band's data lands.
 */
const TemplateBand = () => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const router = useRouter();
  const mobile = useMediaQuery(theme.breakpoints.down("md"));

  const [kind, setKind] = useState<TemplateKind | "all">("all");
  const [source, setSource] = useState<TemplateSourceFilter>("all");
  const [selected, setSelected] = useState<TemplateRead | null>(null);
  const [using, setUsing] = useState<TemplateRead | null>(null);
  const [browserOpen, setBrowserOpen] = useState(false);

  const allKinds = kind === "all";
  // The kind-less page carries the footer's total and the single-kind grid;
  // under All the cards come from one feed per kind, interleaved, so the
  // newest kind never crowds the others off a six-card strip.
  const { page, isLoading, isError } = useTemplates({
    source,
    kind: allKinds ? undefined : kind,
    size: GRID_SIZE,
  });
  const workflows = useTemplates(allKinds ? { source, kind: "workflow", size: MAX_CARDS } : null);
  const projects = useTemplates(allKinds ? { source, kind: "project", size: MAX_CARDS } : null);
  const layouts = useTemplates(allKinds ? { source, kind: "layout", size: MAX_CARDS } : null);
  const { spaces } = useSpaces();
  const { starred, toggleStar } = useFavoriteStars("template");

  const templates = useMemo(() => {
    const limit = mobile ? MAX_CARDS_MOBILE : MAX_CARDS;
    if (!allKinds) return rankTemplateStrip(page?.items ?? [], starred, limit);
    return interleaveTemplateStrip(
      {
        workflow: workflows.page?.items,
        project: projects.page?.items,
        layout: layouts.page?.items,
      },
      starred,
      limit
    );
  }, [allKinds, page, workflows.page, projects.page, layouts.page, starred, mobile]);

  if (isError || workflows.isError || projects.isError || layouts.isError) return null;

  const perKindLoading = [workflows, projects, layouts].some((feed) => feed.isLoading && !feed.page);
  const showSkeleton = (isLoading && !page) || perKindLoading;
  const showEmpty = !showSkeleton && templates.length === 0;
  const narrowedSource = source === "mine" || source === "team" || source === "org";

  const personalSpaceId = spaces.find((space) => space.kind === "personal")?.id;
  const teamSpaceId = spaces.find((space) => space.kind === "team")?.id;
  const orgSpaceId = spaces.find((space) => space.kind === "organization")?.id;

  const footerLink = (() => {
    if (source === "goat") {
      return (
        <Button variant="text" size="small" onClick={() => router.push("/catalog?tab=templates")}>
          {t("browse_goat_templates")}
        </Button>
      );
    }
    const spaceId = source === "mine" ? personalSpaceId : source === "team" ? teamSpaceId : orgSpaceId;
    if (!narrowedSource || !spaceId) return null;
    return (
      <Button
        variant="text"
        size="small"
        onClick={() => router.push(`${contentPath({ spaceId })}?types=template`)}>
        {t("show_in_content")}
      </Button>
    );
  })();

  return (
    <>
      <HomeSection
        title={t("start_from_a_template")}
        action={
          <Button
            variant="text"
            size="small"
            endIcon={<Icon iconName={ICON_NAME.CHEVRON_RIGHT} style={{ fontSize: 12 }} />}
            onClick={() => setBrowserOpen(true)}
            sx={{ borderRadius: 0 }}>
            {t("all_templates")}
          </Button>
        }>
        <Box sx={{ display: "flex", flexDirection: "column", gap: "14px" }}>
          {!showSkeleton && (
            <Box
              sx={{
                display: "flex",
                flexDirection: "row",
                justifyContent: "space-between",
                alignItems: "center",
                gap: "10px",
              }}>
              <TemplateKindFilter kind={kind} onChange={setKind} />
              <TemplateSourceMenu source={source} onChange={setSource} spaces={spaces} compact={mobile} />
            </Box>
          )}

          {showSkeleton ? (
            <Box
              sx={{
                display: "grid",
                gridTemplateColumns: mobile
                  ? "repeat(auto-fill, minmax(min(100%, 168px), 1fr))"
                  : "repeat(auto-fill, minmax(210px, 1fr))",
                gap: mobile ? "10px" : "16px",
              }}>
              {Array.from({ length: mobile ? MAX_CARDS_MOBILE : MAX_CARDS }).map((_, index) => (
                <CardSkeleton key={index} mobile={mobile} />
              ))}
            </Box>
          ) : showEmpty ? (
            <EmptyState
              icon={ICON_NAME.CLONE}
              title={templateEmptyTitle(t, source)}
              hint={t(TEMPLATE_EMPTY_HINT_KEY[source])}
              action={
                <Button variant="outlined" size="small" onClick={() => setBrowserOpen(true)}>
                  {t("all_templates")}
                </Button>
              }
            />
          ) : (
            <Box
              sx={{
                display: "grid",
                gridTemplateColumns: mobile
                  ? "repeat(auto-fill, minmax(min(100%, 168px), 1fr))"
                  : "repeat(auto-fill, minmax(210px, 1fr))",
                gap: mobile ? "10px" : "16px",
              }}>
              {templates.map((template) => (
                <StarterCard
                  key={template.id}
                  template={template}
                  pinned={!!starred[template.id]}
                  onTogglePin={() => toggleStar(template.id)}
                  onOpen={() => setSelected(template)}
                  mobile={mobile}
                />
              ))}
            </Box>
          )}

          {!showSkeleton && (
            <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "10px" }}>
              <Typography component="div" sx={{ fontSize: 12, color: theme.palette.text.secondary }}>
                {t("showing_n_of_m_templates", { n: templates.length, m: page?.total ?? 0 })}
              </Typography>
              {footerLink}
            </Box>
          )}
        </Box>
      </HomeSection>

      <TemplatePreviewDialog
        open={!!selected}
        template={selected ?? undefined}
        sourceLabel={selected ? templateSourceLabel(templateShelfOf(selected, spaces), t) : undefined}
        onClose={() => setSelected(null)}
        onUse={() => {
          setUsing(selected);
          setSelected(null);
        }}
      />

      {browserOpen && (
        <TemplateBrowser
          mode="dialog"
          open
          onClose={() => setBrowserOpen(false)}
          onUse={(template) => {
            setBrowserOpen(false);
            setUsing(template);
          }}
        />
      )}

      {using && (
        <UseTemplateFlow
          template={using}
          context={{ kind: "outside_project" }}
          onClose={() => setUsing(null)}
          onDone={(result) => {
            const template = using;
            setUsing(null);
            router.push(templateResultHref(template, result));
          }}
        />
      )}
    </>
  );
};

export default TemplateBand;
