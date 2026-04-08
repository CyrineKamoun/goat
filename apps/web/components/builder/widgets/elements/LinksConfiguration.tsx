import type { DragEndEvent } from "@dnd-kit/core";
import { DndContext, closestCenter } from "@dnd-kit/core";
import { restrictToVerticalAxis } from "@dnd-kit/modifiers";
import { SortableContext, useSortable, verticalListSortingStrategy } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import {
  Close as CloseIcon,
  DragIndicator as DragIndicatorIcon,
} from "@mui/icons-material";
import { Box, Button, IconButton, Stack, TextField, Typography, useTheme } from "@mui/material";
import { useCallback } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import type { LinksElementSchema } from "@/lib/validations/widget";
import { linksSeparatorTypes } from "@/lib/validations/widget";

import type { SelectorItem } from "@/types/map/common";

import SectionHeader from "@/components/map/panels/common/SectionHeader";
import SectionOptions from "@/components/map/panels/common/SectionOptions";
import Selector from "@/components/map/panels/common/Selector";
import TextFieldInput from "@/components/map/panels/common/TextFieldInput";

interface LinksConfigurationProps {
  config: LinksElementSchema;
  onChange: (config: LinksElementSchema) => void;
}

interface SortableLinkItemProps {
  id: string;
  index: number;
  label: string;
  url: string;
  onLabelChange: (index: number, value: string) => void;
  onUrlChange: (index: number, value: string) => void;
  onDelete: (index: number) => void;
}

const SortableLinkItem = ({
  id,
  index,
  label,
  url,
  onLabelChange,
  onUrlChange,
  onDelete,
}: SortableLinkItemProps) => {
  const theme = useTheme();
  const { t } = useTranslation("common");
  const { attributes, listeners, setNodeRef, transform, transition } = useSortable({ id });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
  };

  return (
    <Box
      ref={setNodeRef}
      style={style}
      sx={{
        border: "1px solid",
        borderColor: "divider",
        borderRadius: 1,
        p: 1,
        "&:hover": { borderColor: theme.palette.action.hover },
      }}>
      <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
        <Box {...attributes} {...listeners} sx={{ cursor: "grab", display: "flex" }}>
          <DragIndicatorIcon sx={{ fontSize: 14, color: "text.disabled" }} />
        </Box>
        <Typography
          variant="caption"
          fontWeight="bold"
          sx={{
            color: "text.disabled",
            bgcolor: "background.default",
            borderRadius: 0.5,
            px: 0.75,
            py: 0.25,
          }}>
          #{index + 1}
        </Typography>
        <Box sx={{ flex: 1 }} />
        <IconButton size="small" onClick={() => onDelete(index)} sx={{ p: 0.25 }}>
          <CloseIcon sx={{ fontSize: 16, color: "text.disabled", "&:hover": { color: "error.main" } }} />
        </IconButton>
      </Stack>
      <Stack spacing={1}>
        <TextField
          size="small"
          fullWidth
          placeholder={t("link_label")}
          value={label}
          onChange={(e) => onLabelChange(index, e.target.value)}
        />
        <TextField
          size="small"
          fullWidth
          placeholder="https://..."
          value={url}
          onChange={(e) => onUrlChange(index, e.target.value)}
        />
      </Stack>
    </Box>
  );
};

const LinksConfiguration = ({ config, onChange }: LinksConfigurationProps) => {
  const { t } = useTranslation("common");

  const links = config.setup?.links ?? [];
  const options = config.options ?? {};

  const handleSetupChange = useCallback(
    (key: string, value: unknown) => {
      onChange({
        ...config,
        setup: { ...config.setup, [key]: value },
      } as LinksElementSchema);
    },
    [config, onChange]
  );

  const handleOptionChange = useCallback(
    (key: string, value: unknown) => {
      onChange({
        ...config,
        options: { ...config.options, [key]: value },
      } as LinksElementSchema);
    },
    [config, onChange]
  );

  const handleLinkLabelChange = useCallback(
    (index: number, label: string) => {
      const updated = [...links];
      updated[index] = { ...updated[index], label };
      handleSetupChange("links", updated);
    },
    [links, handleSetupChange]
  );

  const handleLinkUrlChange = useCallback(
    (index: number, url: string) => {
      const updated = [...links];
      updated[index] = { ...updated[index], url };
      handleSetupChange("links", updated);
    },
    [links, handleSetupChange]
  );

  const handleDeleteLink = useCallback(
    (index: number) => {
      const updated = links.filter((_, i) => i !== index);
      handleSetupChange("links", updated);
    },
    [links, handleSetupChange]
  );

  const handleAddLink = useCallback(() => {
    handleSetupChange("links", [...links, { label: "", url: "" }]);
  }, [links, handleSetupChange]);

  const handleDragEnd = useCallback(
    (event: DragEndEvent) => {
      const { active, over } = event;
      if (!over || active.id === over.id) return;

      const oldIndex = links.findIndex((_, i) => `link-${i}` === active.id);
      const newIndex = links.findIndex((_, i) => `link-${i}` === over.id);
      if (oldIndex === -1 || newIndex === -1) return;

      const updated = [...links];
      const [moved] = updated.splice(oldIndex, 1);
      updated.splice(newIndex, 0, moved);
      handleSetupChange("links", updated);
    },
    [links, handleSetupChange]
  );

  const separatorItems = linksSeparatorTypes.options.map((sep) => ({
    label: t(`separator_${sep}`),
    value: sep,
  }));

  const selectedSeparator = separatorItems.find(
    (item) => item.value === (options.separator ?? "vertical_line")
  );

  return (
    <Stack direction="column" spacing={2} justifyContent="space-between">
      {/* Info Section */}
      <SectionHeader
        active
        alwaysActive
        label={t("info")}
        icon={ICON_NAME.CIRCLEINFO}
        disableAdvanceOptions
      />
      <SectionOptions
        active
        baseOptions={
          <>
            <TextFieldInput
              type="text"
              label={t("title")}
              placeholder={t("add_widget_title")}
              clearable={false}
              value={config.setup?.title || ""}
              onChange={(value: string) => handleSetupChange("title", value)}
            />
            <TextFieldInput
              type="text"
              label={t("description")}
              placeholder={t("add_widget_description")}
              multiline
              clearable={false}
              value={options.description || ""}
              onChange={(value: string) => handleOptionChange("description", value)}
            />
          </>
        }
      />

      {/* Links Section */}
      <SectionHeader
        active
        alwaysActive
        label={t("links")}
        icon={ICON_NAME.LINK}
        disableAdvanceOptions
      />
      <SectionOptions
        active
        baseOptions={
          <Stack spacing={2}>
            <DndContext
              collisionDetection={closestCenter}
              modifiers={[restrictToVerticalAxis]}
              onDragEnd={handleDragEnd}>
              <SortableContext
                items={links.map((_, i) => `link-${i}`)}
                strategy={verticalListSortingStrategy}>
                <Stack spacing={1}>
                  {links.map((link, index) => (
                    <SortableLinkItem
                      key={`link-${index}`}
                      id={`link-${index}`}
                      index={index}
                      label={link.label}
                      url={link.url}
                      onLabelChange={handleLinkLabelChange}
                      onUrlChange={handleLinkUrlChange}
                      onDelete={handleDeleteLink}
                    />
                  ))}
                </Stack>
              </SortableContext>
            </DndContext>
            <Button
              variant="text"
              fullWidth
              onClick={handleAddLink}>
              {t("add_link")}
            </Button>
          </Stack>
        }
      />

      {/* Options Section */}
      <SectionHeader
        active
        alwaysActive
        label={t("options")}
        icon={ICON_NAME.SLIDERS}
        disableAdvanceOptions
      />
      <SectionOptions
        active
        baseOptions={
          <Stack spacing={2}>
            <Selector
              selectedItems={selectedSeparator}
              setSelectedItems={(item: SelectorItem) => {
                handleOptionChange("separator", item.value);
              }}
              items={separatorItems}
              label={t("separator")}
            />
            <TextFieldInput
              type="text"
              label={t("secondary_text")}
              placeholder={t("secondary_text_placeholder")}
              clearable={false}
              value={options.secondary_text || ""}
              onChange={(value: string) => handleOptionChange("secondary_text", value)}
            />
          </Stack>
        }
      />
    </Stack>
  );
};

export default LinksConfiguration;
