/**
 * The Explorer's bottom bar. In the full design it holds playback and the
 * trip strip (boards 3–12); for now the Explorer's one date and Finish
 * editing, the only way out. With no changes to save, Finish editing closes
 * the Explorer straight away.
 */
import LogoutIcon from "@mui/icons-material/Logout";
import { Button, Stack, TextField } from "@mui/material";
import { useTranslation } from "react-i18next";

import { closeExplorer, setExplorerDate } from "@/lib/store/explorer/slice";

import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";

import { FloatingPanel } from "@/components/common/FloatingPanel";
import { useExplorer } from "@/components/explorer/ExplorerContext";

export function ExplorerBottomBar() {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const date = useAppSelector((s) => s.explorer.date);
  const { window } = useExplorer();

  return (
    <FloatingPanel width={360} minHeight={0}>
      <Stack direction="row" spacing={1.5} alignItems="center" sx={{ px: 1.5, py: 1 }}>
        {date && window && (
          <TextField
            type="date"
            size="small"
            value={date}
            inputProps={{ min: window[0], max: window[1], "aria-label": t("calendar") }}
            onChange={(e) => e.target.value && dispatch(setExplorerDate(e.target.value))}
            sx={{ width: 160 }}
          />
        )}
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
