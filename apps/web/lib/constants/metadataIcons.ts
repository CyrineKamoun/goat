import { ICON_NAME } from "@p4b/ui/components/Icon";

/** The icon each metadata heading is shown with, wherever a dataset is summarised. */
export const METADATA_HEADER_ICONS = {
  type: ICON_NAME.LAYERS,
  data_category: ICON_NAME.DATA_CATEGORY,
  distributor_name: ICON_NAME.ORGANIZATION,
  // The catalog's word for the same thing; a bundle says distributor_name.
  publisher: ICON_NAME.ORGANIZATION,
  geographical_code: ICON_NAME.GLOBE,
  language_code: ICON_NAME.LANGUAGE,
  license: ICON_NAME.LICENSE,
};
