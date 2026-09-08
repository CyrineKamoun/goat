"use client";

import { LoadingButton } from "@mui/lab";
import { Checkbox, Skeleton, Stack, Typography, useTheme } from "@mui/material";
import { differenceInCalendarDays, formatDistance } from "date-fns";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "react-toastify";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useDateFnsLocale } from "@/i18n/utils";

import { refreshContentFeed, restoreContent, useTrash } from "@/lib/api/content";
import { iconForType, spaceDisplayName } from "@/lib/utils/content";
import type { Space, TrashItem } from "@/lib/validations/content";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";

interface TrashDialogProps {
  space: Space;
  onClose: () => void;
}

/**
 * The per-space Trash dialog: everything deleted from `space` that hasn't
 * been purged yet, each row restorable on its own or as part of a header-
 * checkbox multi-select. Opened only for a space the caller owns — the
 * trash endpoint itself is owner/admin-only.
 */
const TrashDialog = ({ space, onClose }: TrashDialogProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const dateLocale = useDateFnsLocale();

  const { items, isLoading, mutate } = useTrash(space.id);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [restoringId, setRestoringId] = useState<string | null>(null);
  const [restoringSelected, setRestoringSelected] = useState(false);

  const toggle = (id: string) =>
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  const allSelected = items.length > 0 && selected.size === items.length;
  const toggleAll = () => setSelected(allSelected ? new Set() : new Set(items.map((item) => item.id)));

  const restore = async (targets: TrashItem[]) => {
    if (targets.length === 0) return;
    try {
      await restoreContent(targets.map(({ type, id }) => ({ type, id })));
      await mutate();
      refreshContentFeed();
      toast.success(t("restore_success"));
      setSelected((prev) => {
        const next = new Set(prev);
        for (const target of targets) next.delete(target.id);
        return next;
      });
    } catch {
      toast.error(t("restore_failed"));
    }
  };

  const handleRestoreOne = async (item: TrashItem) => {
    setRestoringId(item.id);
    await restore([item]);
    setRestoringId(null);
  };

  const handleRestoreSelected = async () => {
    setRestoringSelected(true);
    await restore(items.filter((item) => selected.has(item.id)));
    setRestoringSelected(false);
  };

  return (
    <AppDialog
      open
      onClose={onClose}
      icon={ICON_NAME.TRASH}
      title={t("trash")}
      subtitle={spaceDisplayName(space, t)}
      maxWidth={600}
      fullScreenBelow="md"
      footer={
        <AppDialogFooter
          // The purge note reads alongside the way out, as it did above the
          // old action row.
          extra={
            <Typography variant="caption" color="text.secondary">
              {t("trash_note")}
            </Typography>
          }
          cancelLabel={t("close")}
          onCancel={onClose}
        />
      }>
      {isLoading && (
        <Stack spacing={1}>
          <Skeleton variant="rectangular" height={40} />
          <Skeleton variant="rectangular" height={40} />
        </Stack>
      )}

      {!isLoading && items.length === 0 && (
        <Typography variant="body2" color="text.secondary" sx={{ py: 3, textAlign: "center" }}>
          {t("trash_empty")}
        </Typography>
      )}

      {!isLoading && items.length > 0 && (
        <>
          <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
            <Checkbox
              size="small"
              checked={allSelected}
              indeterminate={selected.size > 0 && !allSelected}
              onChange={toggleAll}
              inputProps={{ "aria-label": t("select_all") }}
            />
            <LoadingButton
              size="small"
              variant="outlined"
              disabled={selected.size === 0}
              loading={restoringSelected}
              onClick={handleRestoreSelected}>
              {t("restore_selected", { count: selected.size })}
            </LoadingButton>
          </Stack>

          <Stack spacing={0}>
            {items.map((item) => {
              const goneInDays = Math.max(
                0,
                differenceInCalendarDays(new Date(item.purge_after), new Date())
              );
              return (
                <Stack
                  key={`${item.type}-${item.id}`}
                  direction="row"
                  alignItems="center"
                  spacing={1.5}
                  sx={{ py: 1, borderBottom: `1px solid ${theme.palette.divider}` }}>
                  <Checkbox
                    size="small"
                    checked={selected.has(item.id)}
                    onChange={() => toggle(item.id)}
                    inputProps={{ "aria-label": item.name }}
                  />
                  <Icon iconName={iconForType(item.type)} fontSize="small" />
                  <Stack sx={{ flexGrow: 1, minWidth: 0 }}>
                    <Typography variant="body2" noWrap>
                      {item.name}
                    </Typography>
                    <Typography variant="caption" color="text.secondary" noWrap>
                      {t("deleted_ago", {
                        time: formatDistance(new Date(item.deleted_at), new Date(), {
                          addSuffix: true,
                          locale: dateLocale,
                        }),
                      })}
                      {" · "}
                      {t("gone_in_days", { count: goneInDays })}
                    </Typography>
                  </Stack>
                  <LoadingButton
                    size="small"
                    loading={restoringId === item.id}
                    onClick={() => handleRestoreOne(item)}>
                    {t("restore")}
                  </LoadingButton>
                </Stack>
              );
            })}
          </Stack>
        </>
      )}
    </AppDialog>
  );
};

export default TrashDialog;
