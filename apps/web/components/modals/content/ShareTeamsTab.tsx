"use client";

import { Box, Button, Stack, Typography, alpha, useTheme } from "@mui/material";
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { DialogGroupLabel, DialogSearchField, warningInk } from "@/components/modals/content/ContentDialogChrome";
import RolePicker, { type RolePrefix } from "@/components/modals/content/RolePicker";

interface TeamRow {
  id: string;
  name: string;
  role: string;
}

interface OrganizationRow {
  id: string;
  name: string;
  role: string;
  memberCount: number;
}

interface ShareTeamsTabProps {
  /** Omitted at the item's own org space, where "everyone here" is already
   * implied by living in that space. */
  organization?: OrganizationRow;
  /** The item's own team (when it lives in a team space) is excluded from
   * this list by the caller — sharing with your own home team is a no-op. */
  teams: TeamRow[];
  rolePrefix: RolePrefix;
  onOrganizationRoleChange: (role: string) => void;
  onTeamRoleChange: (teamId: string, role: string) => void;
  /** Only rendered for a personal-space item with a transfer handler. */
  onTransfer?: () => void;
  showTransfer: boolean;
}

/**
 * The Teams tab: a search over the groups this item can be granted to, an
 * "Everyone at {org}" row (when it applies), one row per team, and — for a
 * personal item — the dashed hand-off row to transfer ownership to a team
 * instead of sharing it.
 */
const ShareTeamsTab = ({
  organization,
  teams,
  rolePrefix,
  onOrganizationRoleChange,
  onTeamRoleChange,
  onTransfer,
  showTransfer,
}: ShareTeamsTabProps) => {
  const { t } = useTranslation("common");
  const theme = useTheme();
  const [query, setQuery] = useState("");

  const trimmed = query.trim().toLowerCase();
  const visibleTeams = useMemo(
    () => (trimmed ? teams.filter((team) => team.name.toLowerCase().includes(trimmed)) : teams),
    [teams, trimmed]
  );
  const showOrganization = !!organization && (!trimmed || organization.name.toLowerCase().includes(trimmed));

  const groupRow = (
    key: string,
    icon: ICON_NAME,
    granted: boolean,
    primary: string,
    secondary: string | undefined,
    role: string,
    onRoleChange: (role: string) => void
  ) => (
    <Box key={key} sx={{ display: "flex", alignItems: "center", gap: "11px", padding: "6px 0" }}>
      <Box
        sx={{
          width: 28,
          height: 28,
          borderRadius: "50%",
          flexShrink: 0,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          backgroundColor: granted ? alpha(theme.palette.primary.main, 0.12) : theme.palette.action.hover,
        }}>
        <Icon
          iconName={icon}
          style={{ fontSize: 13, color: granted ? theme.palette.primary.main : theme.palette.text.secondary }}
        />
      </Box>
      <Box sx={{ flex: 1, minWidth: 0 }}>
        <Typography component="div" noWrap sx={{ fontSize: 13.5, fontWeight: 600 }}>
          {primary}
        </Typography>
        {secondary && (
          <Typography component="div" noWrap sx={{ fontSize: 11.5, color: "text.secondary" }}>
            {secondary}
          </Typography>
        )}
      </Box>
      <RolePicker value={role} prefix={rolePrefix} onChange={onRoleChange} />
    </Box>
  );

  return (
    <Stack sx={{ height: "100%", minHeight: 0 }} spacing="10px">
      <DialogSearchField
        value={query}
        onChange={setQuery}
        placeholder={t("search_teams")}
        clearLabel={t("clear")}
      />

      <Box sx={{ flex: 1, minHeight: 0, overflowY: "auto" }}>
        {showOrganization && organization && (
          <>
            <DialogGroupLabel first>{t("organization")}</DialogGroupLabel>
            {groupRow(
              organization.id,
              ICON_NAME.ORGANIZATION,
              organization.role !== "",
              t("everyone_at", { name: organization.name }),
              t("n_members", { count: organization.memberCount }),
              organization.role,
              onOrganizationRoleChange
            )}
          </>
        )}
        <DialogGroupLabel first={!showOrganization}>{t("teams")}</DialogGroupLabel>
        {visibleTeams.length === 0 && (
          <Typography sx={{ fontSize: 12.5, color: "text.disabled", padding: "10px 0" }}>
            {t("no_teams_match")}
          </Typography>
        )}
        {visibleTeams.map((team) =>
          groupRow(team.id, ICON_NAME.USERS, team.role !== "", team.name, undefined, team.role, (role) =>
            onTeamRoleChange(team.id, role)
          )
        )}
        <Typography sx={{ fontSize: 11.5, color: "text.disabled", padding: "10px 0 2px", lineHeight: 1.5 }}>
          {t("group_share_note")}
        </Typography>
      </Box>

      {showTransfer && onTransfer && (
        <Box
          sx={{
            flexShrink: 0,
            display: "flex",
            alignItems: "center",
            gap: "11px",
            padding: "12px 12px 12px 14px",
            borderRadius: "10px",
            border: `1px dashed ${alpha(theme.palette.text.primary, 0.24)}`,
          }}>
          <Box
            sx={{
              width: 30,
              height: 30,
              borderRadius: "8px",
              flexShrink: 0,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              backgroundColor: alpha(theme.palette.warning.main, 0.16),
            }}>
            <Icon iconName={ICON_NAME.CROWN} style={{ fontSize: 14, color: warningInk(theme) }} />
          </Box>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography component="div" sx={{ fontSize: 13, fontWeight: 700 }}>
              {t("transfer_ownership")}
            </Typography>
            <Typography component="div" sx={{ fontSize: 11.5, color: "text.secondary", lineHeight: 1.45 }}>
              {t("transfer_hint")}
            </Typography>
          </Box>
          <Button
            variant="outlined"
            color="inherit"
            onClick={onTransfer}
            sx={{
              flexShrink: 0,
              borderRadius: "999px",
              padding: "8px 14px",
              fontSize: 12.5,
              fontWeight: 700,
              textTransform: "none",
              whiteSpace: "nowrap",
              borderColor: alpha(theme.palette.text.primary, 0.24),
            }}>
            {t("transfer_ellipsis")}
          </Button>
        </Box>
      )}
    </Stack>
  );
};

export default ShareTeamsTab;
