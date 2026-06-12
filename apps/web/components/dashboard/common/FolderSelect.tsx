import Autocomplete from "@mui/material/Autocomplete";
import Box from "@mui/material/Box";
import Divider from "@mui/material/Divider";
import InputAdornment from "@mui/material/InputAdornment";
import ListItem from "@mui/material/ListItem";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import React, { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { Folder } from "@/lib/validations/folder";

interface FolderSelectProps {
  folders: Folder[] | undefined;
  selectedFolder: Folder | null | undefined;
  setSelectedFolder: (folder: Folder | null) => void;
}

const GROUP_MY_FOLDERS = "__my__";
const SHARED_PREFIX = "__shared__::";

const FolderSelect = ({ folders, selectedFolder, setSelectedFolder }: FolderSelectProps) => {
  const { t } = useTranslation("common");

  const getFolderLabel = (folder: Folder) =>
    folder.name === "home" ? t("my_content") : folder.name;

  // Sort: home → owned named → shared (sorted by source name, then folder name)
  const sortedFolders = useMemo(() => {
    if (!folders) return [];
    const home = folders.filter((f) => f.name === "home");
    const ownedNamed = folders.filter((f) => f.is_owned && f.name !== "home").sort((a, b) => a.name.localeCompare(b.name));
    const shared = folders
      .filter((f) => !f.is_owned)
      .sort((a, b) => {
        const aSource = a.shared_from_name ?? "";
        const bSource = b.shared_from_name ?? "";
        return aSource.localeCompare(bSource) || a.name.localeCompare(b.name);
      });
    return [...home, ...ownedNamed, ...shared];
  }, [folders]);

  // Key of the first shared group — used to inject the outer "SHARED FOLDERS" header
  const firstSharedGroupKey = useMemo(() => {
    const first = sortedFolders.find((f) => !f.is_owned);
    return first ? `${SHARED_PREFIX}${first.shared_from_name ?? ""}` : null;
  }, [sortedFolders]);

  // All shared group keys start collapsed
  const allSharedGroupKeys = useMemo(() => {
    const keys = new Set<string>();
    sortedFolders.forEach((f) => {
      if (!f.is_owned) keys.add(`${SHARED_PREFIX}${f.shared_from_name ?? ""}`);
    });
    return keys;
  }, [sortedFolders]);

  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  const [inputValue, setInputValue] = useState("");

  // Collapse all shared groups on first load (folders arrive asynchronously)
  const hasInitialized = useRef(false);
  useEffect(() => {
    if (!hasInitialized.current && allSharedGroupKeys.size > 0) {
      hasInitialized.current = true;
      setCollapsedGroups(new Set(allSharedGroupKeys));
    }
  }, [allSharedGroupKeys]);

  const toggleGroup = (groupKey: string) => {
    setCollapsedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(groupKey)) next.delete(groupKey);
      else next.add(groupKey);
      return next;
    });
  };

  return (
    <Autocomplete
      fullWidth
      value={selectedFolder ?? null}
      onChange={(_event, newValue) => {
        setSelectedFolder(newValue);
      }}
      autoHighlight
      isOptionEqualToValue={(option, value) => option.id === value.id}
      id="folder-select"
      options={sortedFolders}
      inputValue={inputValue}
      onInputChange={(_e, value) => {
        setInputValue(value);
        if (value) {
          setCollapsedGroups(new Set());
        } else {
          setCollapsedGroups(new Set(allSharedGroupKeys));
        }
      }}
      filterOptions={(options, state) => {
        const term = state.inputValue.toLowerCase();
        return term ? options.filter((opt) => getFolderLabel(opt).toLowerCase().includes(term)) : options;
      }}
      groupBy={(option) =>
        option.is_owned ? GROUP_MY_FOLDERS : `${SHARED_PREFIX}${option.shared_from_name ?? ""}`
      }
      getOptionLabel={(option) => {
        if (typeof option === "string") return option;
        return getFolderLabel(option);
      }}
      renderGroup={(params) => {
        const isMyFolders = params.group === GROUP_MY_FOLDERS;
        const isFirstShared = params.group === firstSharedGroupKey;
        const sourceName = isMyFolders ? null : params.group.slice(SHARED_PREFIX.length);
        const isCollapsed = !isMyFolders && collapsedGroups.has(params.group);

        return (
          <Box key={params.key} component="li" sx={{ listStyle: "none" }}>
            {/* Divider + "SHARED FOLDERS" label before the first shared group */}
            {isFirstShared && (
              <>
                <Divider sx={{ my: 0.5 }} />
                <Typography
                  variant="caption"
                  sx={{
                    px: 2,
                    pt: 0.75,
                    pb: 0.25,
                    display: "block",
                    color: "text.secondary",
                    textTransform: "uppercase",
                    letterSpacing: 0.5,
                    fontSize: 10,
                    fontWeight: 600,
                  }}>
                  {t("shared_folders")}
                </Typography>
              </>
            )}

            {/* Owned folders: no header. Shared: collapsible team/org heading. */}
            {!isMyFolders && (
              <Box
                component="button"
                type="button"
                onMouseDown={(e: React.MouseEvent) => e.preventDefault()}
                onClick={() => toggleGroup(params.group)}
                sx={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  width: "100%",
                  px: 2,
                  pl: 3,
                  py: 0.5,
                  border: "none",
                  background: "none",
                  cursor: "pointer",
                  "&:hover": { bgcolor: "action.hover" },
                }}>
                <Typography
                  variant="caption"
                  sx={{ display: "block", color: "text.secondary", fontSize: 11, fontWeight: 400 }}>
                  {sourceName}
                </Typography>
                <Icon
                  iconName={ICON_NAME.CHEVRON_LEFT}
                  style={{
                    fontSize: 12,
                    transform: isCollapsed ? "rotate(-90deg)" : "rotate(-270deg)",
                    transition: "transform 0.2s",
                    color: "inherit",
                  }}
                />
              </Box>
            )}

            <ul style={{ padding: 0, margin: 0 }}>{params.children}</ul>
          </Box>
        );
      }}
      renderOption={(props, option) => {
        // Strip the `key` MUI puts in `props` before spreading — React
        // warns when a spread object includes `key`. We use the
        // folder's stable id as the key instead.
        const { key: _muiKey, ...rest } =
          props as React.HTMLAttributes<HTMLLIElement> & { key?: React.Key };
        const groupKey = option.is_owned
          ? GROUP_MY_FOLDERS
          : `${SHARED_PREFIX}${option.shared_from_name ?? ""}`;
        const hidden = !option.is_owned && collapsedGroups.has(groupKey);
        if (hidden) {
          return <li key={option.id} {...rest} style={{ display: "none" }} aria-hidden />;
        }
        return (
          <ListItem
            key={option.id}
            {...rest}
            sx={{ pl: option.is_owned ? 2 : 4 }}>
            <ListItemIcon>
              <Icon
                iconName={option.name === "home" ? ICON_NAME.HOUSE : ICON_NAME.FOLDER}
                style={{ marginLeft: 2 }}
                fontSize="small"
              />
            </ListItemIcon>
            <ListItemText
              primary={getFolderLabel(option)}
              secondary={
                !option.is_owned
                  ? option.role === "folder-editor"
                    ? t("write_access")
                    : t("read_access")
                  : undefined
              }
            />
          </ListItem>
        );
      }}
      renderInput={(params) => (
        <TextField
          {...params}
          fullWidth
          sx={{ mt: 4 }}
          InputProps={{
            ...params.InputProps,
            startAdornment: (
              <InputAdornment position="start">
                <Icon
                  iconName={selectedFolder?.name === "home" ? ICON_NAME.HOUSE : ICON_NAME.FOLDER}
                  style={{ marginLeft: 2 }}
                  fontSize="small"
                />
              </InputAdornment>
            ),
          }}
          label={t("select_folder_destination")}
        />
      )}
    />
  );
};

export default FolderSelect;
