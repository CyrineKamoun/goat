"use client";

import { Box, ButtonBase, Typography, useTheme } from "@mui/material";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { folderPath, parentIdOf, spaceDisplayName, spaceIconFor } from "@/lib/utils/content";
import type { Space } from "@/lib/validations/content";
import type { Folder } from "@/lib/validations/folder";

import FormLabelHelper from "@/components/common/FormLabelHelper";

/** A folder's children, one level down, from a flat `parent_id`-linked list. */
export const childrenOf = (folderId: string | null, folders: Folder[]): Folder[] =>
  folders.filter((f) => parentIdOf(f) === folderId);

interface FolderBrowserProps {
  /** The space being browsed — its icon and name lead the breadcrumb. */
  space: Space;
  /** Every folder in hand; the browser keeps the ones in `space` and drops
   * the space's `home` root, which the breadcrumb's space name stands for. */
  folders: Folder[];
  /** The space's `home` folder id — what the root crumb resolves to for a
   * caller that needs a concrete folder. `null` while it is unknown. */
  homeFolderId: string | null;
  /** The folder currently browsed into; `null` and `homeFolderId` both mean
   * the space root. */
  value: string | null;
  /** The folder browsed into, or `null` at the space root — a caller that
   * needs a folder id there maps it to `homeFolderId` itself. */
  onChange: (folderId: string | null) => void;
  /** Folders the list never offers — a folder being moved plus its own
   * descendants, which cannot receive it. */
  hiddenFolderIds?: Set<string>;
  /** A label above the browser; omitted leaves it unlabelled (the Move
   * dialog's header already says what the list is). */
  label?: string;
  /** A caption line under the list, for a caller that has something to say
   * about what picking this folder means. */
  helperText?: string;
  /** Scroll cap on the folder list. */
  maxHeight?: number;
  disabled?: boolean;
}

/**
 * The folder picker every destination dialog browses with: a breadcrumb
 * from the space root down, and the folders one level below whatever crumb
 * is active. The target is the level you are standing on, so picking the
 * space root is a click on the space's own crumb.
 */
const FolderBrowser = ({
  space,
  folders,
  homeFolderId,
  value,
  onChange,
  hiddenFolderIds,
  label,
  helperText,
  maxHeight = 220,
  disabled,
}: FolderBrowserProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();

  const spaceFolders = useMemo(
    () => folders.filter((f) => f.space_id === space.id && f.name !== "home"),
    [folders, space.id]
  );

  // The `home` folder is the space root, which the breadcrumb's space name
  // already stands for — a caller holding it is standing at the root.
  const current = value === null || value === homeFolderId ? null : value;

  const breadcrumbPath = useMemo(() => folderPath(spaceFolders, current), [spaceFolders, current]);
  const children = useMemo(
    () => childrenOf(current, spaceFolders).filter((f) => !hiddenFolderIds?.has(f.id)),
    [current, spaceFolders, hiddenFolderIds]
  );

  const spaceName = spaceDisplayName(space, t);
  const crumbSx = {
    cursor: disabled ? "default" : "pointer",
    fontSize: 12.5,
    background: "none",
    border: "none",
    padding: 0,
  } as const;

  return (
    <Box>
      {label && <FormLabelHelper label={label} color={theme.palette.text.secondary} />}

      <Box
        sx={{
          borderRadius: "10px",
          border: `1px solid ${theme.palette.divider}`,
          opacity: disabled ? 0.6 : 1,
          pointerEvents: disabled ? "none" : undefined,
        }}>
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            flexWrap: "wrap",
            gap: "5px",
            padding: "10px 12px 6px",
            flexShrink: 0,
          }}>
          <Icon iconName={spaceIconFor(space)} style={{ fontSize: 13, color: theme.palette.text.disabled }} />
          <Typography
            component="button"
            onClick={() => onChange(null)}
            sx={{
              ...crumbSx,
              fontWeight: current === null ? 800 : 600,
              color: current === null ? "text.primary" : "text.secondary",
            }}>
            {spaceName}
          </Typography>
          {breadcrumbPath.map((folder) => (
            <Box key={folder.id} sx={{ display: "inline-flex", alignItems: "center", gap: "5px" }}>
              <Icon
                iconName={ICON_NAME.CHEVRON_RIGHT}
                style={{ fontSize: 10, color: theme.palette.text.disabled }}
              />
              <Typography
                component="button"
                onClick={() => onChange(folder.id)}
                sx={{
                  ...crumbSx,
                  fontWeight: folder.id === current ? 800 : 600,
                  color: folder.id === current ? "text.primary" : "text.secondary",
                }}>
                {folder.name}
              </Typography>
            </Box>
          ))}
        </Box>

        <Box sx={{ maxHeight, overflowY: "auto", padding: "2px 6px 8px" }}>
          {children.length === 0 ? (
            <Typography sx={{ padding: "18px 12px", fontSize: 12.5, color: "text.disabled" }}>
              {t("no_folders_here")}
            </Typography>
          ) : (
            children.map((folder) => (
              <ButtonBase
                key={folder.id}
                onClick={() => onChange(folder.id)}
                sx={{
                  display: "flex",
                  width: "100%",
                  alignItems: "center",
                  gap: "10px",
                  padding: "9px 11px",
                  borderRadius: "8px",
                  fontSize: 13.5,
                  fontWeight: 600,
                  textAlign: "left",
                  "&:hover": { backgroundColor: theme.palette.action.hover },
                }}>
                <Icon
                  iconName={ICON_NAME.FOLDER}
                  style={{ fontSize: 16, color: theme.palette.text.secondary }}
                />
                <Box component="span" sx={{ flex: 1, minWidth: 0 }}>
                  {folder.name}
                </Box>
                <Icon
                  iconName={ICON_NAME.CHEVRON_RIGHT}
                  style={{ fontSize: 13, color: theme.palette.text.disabled }}
                />
              </ButtonBase>
            ))
          )}
        </Box>
      </Box>

      {helperText && (
        <Typography variant="caption" sx={{ display: "block", color: "text.secondary", mt: 0.5 }}>
          {helperText}
        </Typography>
      )}
    </Box>
  );
};

export default FolderBrowser;
