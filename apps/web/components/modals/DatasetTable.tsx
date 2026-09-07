import { TablePagination } from "@mui/material";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME } from "@p4b/ui/components/Icon";

import { DEFAULT_ROWS_PER_PAGE_OPTIONS } from "@/lib/utils/pagination";
import type { Layer } from "@/lib/validations/layer";
import type { ProjectLayer } from "@/lib/validations/project";

import { useFeaturePage } from "@/hooks/useFeaturePage";

import AppDialog, { AppDialogFooter } from "@/components/common/AppDialog";
import FeatureTable from "@/components/common/FeatureTable";

interface DatasetTableDialogProps {
  open: boolean;
  onClose?: () => void;
  disabled?: boolean;
  dataset: ProjectLayer | Layer;
}

const DatasetTableModal: React.FC<DatasetTableDialogProps> = ({ open, onClose, dataset }) => {
  const { t } = useTranslation("common");
  const datasetId = dataset["layer_id"] || dataset["id"] || "";
  // A project layer's saved filter must narrow the preview too.
  const filter = useMemo(() => dataset["query"]?.["cql"] ?? undefined, [dataset]);

  const { fields, areFieldsLoading, data, rowsPerPage, page, totalCount, onPageChange, onRowsPerPageChange } =
    useFeaturePage(datasetId, { limit: 50, filter });

  return (
    <AppDialog
      open={open}
      onClose={() => onClose?.()}
      icon={ICON_NAME.TABLE}
      title={`${dataset.name}`}
      maxWidth={1200}
      // The table reaches the frame on both sides, as it did before.
      bleed
      bodySx={{ minHeight: "250px" }}
      footer={
        <AppDialogFooter
          // The pagination is the footer's left-hand control.
          extra={
            data ? (
              <TablePagination
                rowsPerPageOptions={DEFAULT_ROWS_PER_PAGE_OPTIONS}
                component="div"
                count={totalCount}
                rowsPerPage={rowsPerPage}
                page={page}
                onPageChange={onPageChange}
                onRowsPerPageChange={onRowsPerPageChange}
              />
            ) : undefined
          }
          cancelLabel={t("close")}
          onCancel={() => onClose?.()}
        />
      }>
      <FeatureTable fields={fields} data={data} isLoading={areFieldsLoading} />
    </AppDialog>
  );
};

export default DatasetTableModal;
