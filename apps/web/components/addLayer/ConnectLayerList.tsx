"use client";

import {
  Box,
  ButtonBase,
  Checkbox,
  InputAdornment,
  Radio,
  Stack,
  TextField,
  Tooltip,
  Typography,
  alpha,
  useTheme,
} from "@mui/material";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import type { ConnectedService, UnsupportedReason } from "@/lib/utils/externalService";
import { serviceLayers } from "@/lib/utils/externalService";

const MONO = "ui-monospace, SFMono-Regular, Menlo, monospace";

/**
 * What a service offers, as a list to pick from: searchable, grouped the way the service
 * nests its layers, one choice or several as the service allows.
 *
 * Layers GOAT cannot draw stay in the list, greyed with the reason, rather than vanishing:
 * a layer someone came for and cannot find reads as a bug, one marked "EPSG:25832 only"
 * reads as an answer.
 */
const ConnectLayerList = ({
  service,
  selected,
  focusedId,
  onToggle,
}: {
  service: ConnectedService;
  selected: string[];
  focusedId: string | null;
  onToggle: (id: string) => void;
}) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const [query, setQuery] = useState("");
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());

  const all = serviceLayers(service);
  const hidden = all.filter((layer) => layer.unsupported?.reason === "projection").length;

  const groups = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return service.groups
      .map((group) => ({
        ...group,
        layers: needle
          ? group.layers.filter((layer) => `${layer.title} ${layer.id}`.toLowerCase().includes(needle))
          : group.layers,
      }))
      .filter((group) => group.layers.length);
  }, [service.groups, query]);

  const toggleGroup = (id: string) =>
    setCollapsed((previous) => {
      const next = new Set(previous);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  const unsupportedLabel = (unsupported: UnsupportedReason) => {
    switch (unsupported.reason) {
      case "projection":
        return {
          short: t("connect_service_only_crs", { crs: unsupported.crs[0] ?? "" }),
          long: t("connect_service_only_crs_tip", { crs: unsupported.crs.join(", ") }),
        };
      case "format":
        return { short: t("connect_service_other_format"), long: t("connect_service_other_format_tip") };
      case "grid":
        return { short: t("connect_service_other_grid"), long: t("connect_service_other_grid_tip") };
      case "no_tiles":
        return { short: t("connect_service_no_tiles"), long: t("connect_service_no_tiles_tip") };
    }
  };

  return (
    <Stack sx={{ minHeight: 0, height: "100%" }}>
      <Box sx={{ px: 4, py: 3, borderBottom: `1px solid ${theme.palette.divider}` }}>
        <TextField
          fullWidth
          size="small"
          type="search"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder={t("connect_service_search", { count: all.length })}
          inputProps={{ "aria-label": t("connect_service_search", { count: all.length }) }}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <Icon
                  iconName={ICON_NAME.SEARCH}
                  style={{ fontSize: 13 }}
                  htmlColor={theme.palette.text.secondary}
                />
              </InputAdornment>
            ),
          }}
        />
      </Box>

      <Box sx={{ flex: 1, minHeight: 0, overflowY: "auto", py: 1.5 }} role="list">
        {groups.map((group) => {
          const isCollapsed = collapsed.has(group.id) && !query;
          return (
            <Box key={group.id}>
              {group.title && (
                <ButtonBase
                  onClick={() => toggleGroup(group.id)}
                  aria-expanded={!isCollapsed}
                  sx={{
                    width: "100%",
                    justifyContent: "flex-start",
                    gap: 1.5,
                    px: 4,
                    py: 2,
                    "&:hover": { backgroundColor: theme.palette.action.hover },
                  }}>
                  <Icon
                    iconName={isCollapsed ? ICON_NAME.CHEVRON_RIGHT : ICON_NAME.CHEVRON_DOWN}
                    style={{ fontSize: 11 }}
                    htmlColor={theme.palette.text.secondary}
                  />
                  <Typography variant="caption" fontWeight={700} noWrap>
                    {group.title}
                  </Typography>
                  <Typography variant="caption" color="text.disabled">
                    {group.layers.length}
                  </Typography>
                </ButtonBase>
              )}
              {!isCollapsed &&
                group.layers.map((layer) => {
                  const checked = selected.includes(layer.id);
                  const off = !!layer.unsupported;
                  const label = layer.unsupported ? unsupportedLabel(layer.unsupported) : null;
                  const Control = service.multiple ? Checkbox : Radio;
                  return (
                    <Tooltip key={layer.id} title={label?.long ?? ""} placement="right" disableInteractive>
                      <Box role="listitem">
                        <ButtonBase
                          disabled={off}
                          onClick={() => onToggle(layer.id)}
                          aria-pressed={checked}
                          sx={{
                            width: "100%",
                            justifyContent: "flex-start",
                            textAlign: "left",
                            gap: 2,
                            py: 1.25,
                            pr: 4,
                            pl: group.title ? 7 : 3,
                            opacity: off ? 0.6 : 1,
                            backgroundColor:
                              focusedId === layer.id && !off
                                ? alpha(theme.palette.primary.main, 0.07)
                                : "transparent",
                            "&:hover": { backgroundColor: theme.palette.action.hover },
                          }}>
                          <Control
                            size="small"
                            checked={checked && !off}
                            disabled={off}
                            tabIndex={-1}
                            disableRipple
                            inputProps={{ "aria-hidden": true }}
                            sx={{ p: 0.5 }}
                          />
                          <Box sx={{ minWidth: 0, flex: 1 }}>
                            <Typography variant="body2" fontWeight={600} noWrap>
                              {layer.title}
                            </Typography>
                            <Typography
                              variant="caption"
                              color="text.secondary"
                              noWrap
                              component="div"
                              sx={{ fontFamily: MONO, fontSize: 11.5 }}>
                              {layer.id}
                            </Typography>
                          </Box>
                          {label && (
                            <Typography
                              variant="caption"
                              fontWeight={700}
                              color="text.secondary"
                              noWrap
                              sx={{
                                flexShrink: 0,
                                px: 1.5,
                                py: 0.25,
                                borderRadius: 1,
                                backgroundColor: theme.palette.action.hover,
                              }}>
                              {label.short}
                            </Typography>
                          )}
                        </ButtonBase>
                      </Box>
                    </Tooltip>
                  );
                })}
            </Box>
          );
        })}
        {!groups.length && (
          <Typography variant="body2" color="text.secondary" sx={{ textAlign: "center", py: 7, px: 4 }}>
            {t("connect_service_no_match", { query })}
          </Typography>
        )}
      </Box>

      {hidden > 0 && (
        <Typography
          variant="caption"
          color="text.secondary"
          component="div"
          sx={{ px: 4, py: 2.5, borderTop: `1px solid ${theme.palette.divider}`, lineHeight: 1.45 }}>
          {t("connect_service_hidden_note", { hidden, total: all.length })}
        </Typography>
      )}
    </Stack>
  );
};

export default ConnectLayerList;
