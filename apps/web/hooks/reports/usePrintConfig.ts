/**
 * Hook for fetching print configuration from the backend.
 *
 * Reads constraints (e.g. atlas_max_pages) from the print_report
 * process description schema so the frontend stays in sync with
 * backend limits without hardcoding values.
 */
import { useMemo } from "react";

import { useProcessDescription } from "@/hooks/map/useOgcProcesses";

/** Fallback used while the process description is still loading. */
const DEFAULT_ATLAS_MAX_PAGES = 120;

export interface PrintConfig {
  atlasMaxPages: number;
  isLoading: boolean;
}

export function usePrintConfig(): PrintConfig {
  const { process, isLoading } = useProcessDescription("print_report");

  const atlasMaxPages = useMemo(() => {
    const maximum = process?.inputs?.total_atlas_pages?.schema?.maximum;
    return typeof maximum === "number" ? maximum : DEFAULT_ATLAS_MAX_PAGES;
  }, [process]);

  return { atlasMaxPages, isLoading };
}
