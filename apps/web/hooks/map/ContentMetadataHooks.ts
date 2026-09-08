import { countries } from "country-flag-icons";
import getUnicodeFlagIcon from "country-flag-icons/unicode";
import { useMemo } from "react";

import { useTranslation } from 'react-i18next'


/** The option lists the bundle metadata form needs. A layer has no metadata of
 * its own to pick from, so nothing else consumes these. */
export const useContentMetadataHooks = () => {
  const { t } = useTranslation(["common", "countries"]);

  const geographicalCodeOptions = useMemo(() => {
    return countries.map((countryCode) => {
      return {
        value: countryCode,
        label: `${t(`countries:${countryCode}`)}`,
        icon: getUnicodeFlagIcon(countryCode),
      };
    });
  }, [t]);

  return {
    t,
    geographicalCodeOptions,
  };
};
