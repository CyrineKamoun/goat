import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { useProject } from "@/lib/api/projects";
import { useTeams } from "@/lib/api/teams";

/** Shown in the notice before the rest collapse into "+N". */
const MAX_NAMES = 3;

/**
 * D7's first guardrail: a name for the notice above the Add Layer footer —
 * "this project belongs to / is shared with X, they will see this layer too."
 *
 * Two sources, checked in order: the project's own space (a team or
 * organisation project always shows its owner), then, only for a personal
 * project, any explicit grant (`shared_with`). A personal project with no
 * grants has nobody else to warn about, so the hook returns `undefined` —
 * the dialog then renders no banner at all.
 *
 * `useProject` and `useTeams` are called unconditionally (hook order), so
 * this returns `undefined` while either is still loading rather than
 * flashing a wrong answer first.
 */
export const useShareNotice = (projectId?: string): string | undefined => {
  const { t } = useTranslation("common");
  const { project, isLoading: projectLoading } = useProject(projectId);
  const { teams, isLoading: teamsLoading } = useTeams();

  return useMemo(() => {
    if (!project || projectLoading || teamsLoading) return undefined;

    if (project.space_kind && project.space_kind !== "personal" && project.space_name) {
      return t("add_layer_space_notice", { name: project.space_name });
    }

    const teamNameById = new Map(teams.map((team) => [team.id, team.name]));

    // A team entry may carry its own name already, or only an id — in which
    // case it comes from `useTeams`. An organisation entry is never named
    // individually (D7 only needs to say "an organisation is watching", not
    // which one); a user entry is named when the grant carries one, and
    // otherwise folds into the overflow count below.
    const resolved: (string | undefined)[] = [
      ...(project.shared_with?.teams ?? []).map(
        (entry) => entry.name ?? teamNameById.get(entry.id)
      ),
      ...(project.shared_with?.organizations ?? []).map(() => t("organization")),
      ...(project.shared_with?.users ?? []).map((entry) => entry.name),
    ];

    if (resolved.length === 0) return undefined;

    const named = resolved.filter((name): name is string => !!name);
    const shown = named.slice(0, MAX_NAMES);
    const overflow = resolved.length - shown.length;
    const overflowLabel = overflow > 0 ? `+${overflow}` : "";
    const names = [shown.join(", "), overflowLabel].filter(Boolean).join(" ");

    return t("add_layer_shared_notice", { names });
  }, [project, projectLoading, teams, teamsLoading, t]);
};
