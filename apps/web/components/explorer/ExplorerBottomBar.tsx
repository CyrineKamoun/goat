/**
 * The Explorer's bottom bar. In the full design it holds playback and the
 * trip strip (boards 3–12); for now the Explorer's one date and Finish
 * editing, the only way out. With no changes to save, Finish editing closes
 * the Explorer straight away.
 */
import LogoutIcon from "@mui/icons-material/Logout";
import { Box, Button, Stack } from "@mui/material";
import { useTranslation } from "react-i18next";

import { closeExplorer } from "@/lib/store/explorer/slice";

import { useAppDispatch } from "@/hooks/store/ContextHooks";

import { FloatingPanel } from "@/components/common/FloatingPanel";
import { AnalysisDate } from "@/components/explorer/AnalysisDate";

export function ExplorerBottomBar() {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();

  return (
    <FloatingPanel width={360} minHeight={0}>
      <Stack direction="row" spacing={1.5} alignItems="center" sx={{ px: 1.5, py: 1 }}>
        {/* The Explorer's one date: the panels show it, this sets it. */}
        <Box sx={{ width: 170 }}>
          <AnalysisDate />
        </Box>
        <Button
          variant="contained"
          color="primary"
          startIcon={<LogoutIcon fontSize="small" />}
          onClick={() => dispatch(closeExplorer())}
          sx={{ textTransform: "none", whiteSpace: "nowrap" }}>
          {t("explorer_finish_editing")}
        </Button>
      </Stack>
    </FloatingPanel>
  );
}
