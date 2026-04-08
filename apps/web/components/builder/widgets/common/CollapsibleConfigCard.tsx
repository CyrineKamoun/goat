import { KeyboardArrowDown, KeyboardArrowRight, Close } from "@mui/icons-material";
import { Box, Collapse, IconButton, Stack, Typography } from "@mui/material";

interface CollapsibleConfigCardProps {
  title: React.ReactNode;
  summary?: string;
  expanded: boolean;
  onToggle: () => void;
  onRemove?: () => void;
  canRemove?: boolean;
  children: React.ReactNode;
}

const CollapsibleConfigCard: React.FC<CollapsibleConfigCardProps> = ({
  title,
  summary,
  expanded,
  onToggle,
  onRemove,
  canRemove = true,
  children,
}) => {
  return (
    <Box
      sx={{
        border: "1px solid",
        borderColor: expanded ? "primary.main" : "divider",
        borderRadius: 1,
        overflow: "hidden",
      }}>
      {/* Header row — always visible */}
      <Stack
        direction="row"
        alignItems="center"
        justifyContent="space-between"
        onClick={onToggle}
        sx={{
          px: 1.5,
          py: 1,
          cursor: "pointer",
          borderBottom: expanded ? "1px solid" : "none",
          borderColor: "divider",
          "&:hover": { backgroundColor: "action.hover" },
        }}>
        <Stack direction="row" alignItems="center" spacing={1} sx={{ minWidth: 0, flex: 1 }}>
          {expanded ? (
            <KeyboardArrowDown fontSize="small" color="primary" />
          ) : (
            <KeyboardArrowRight fontSize="small" color="action" />
          )}
          <Box sx={{ minWidth: 0, flex: 1, overflow: "hidden" }}>
            <Stack direction="row" alignItems="center" spacing={0.5} sx={{ minWidth: 0, overflow: "hidden" }}>
              <Box sx={{ flexShrink: 0, minWidth: 0, maxWidth: "50%" }}>
                {typeof title === "string" ? (
                  <Typography variant="body2" noWrap>
                    {title}
                  </Typography>
                ) : (
                  title
                )}
              </Box>
              {!expanded && summary && (
                <Typography variant="caption" color="text.secondary" noWrap sx={{ minWidth: 0 }}>
                  · {summary}
                </Typography>
              )}
            </Stack>
          </Box>
        </Stack>
        {onRemove && (
          <IconButton
            size="small"
            disabled={!canRemove}
            onClick={(e) => {
              e.stopPropagation();
              onRemove();
            }}
            sx={{ ml: 0.5 }}>
            <Close fontSize="small" />
          </IconButton>
        )}
      </Stack>

      {/* Expandable content */}
      <Collapse in={expanded}>
        <Box sx={{ p: 1.5 }}>{children}</Box>
      </Collapse>
    </Box>
  );
};

export default CollapsibleConfigCard;
