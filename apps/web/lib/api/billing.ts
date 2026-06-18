import useSWR from "swr";

import { API_BASE_URL } from "@/lib/constants";

import { fetcher } from "@/lib/api/fetcher";
import type { PlansList } from "@/lib/validations/billing";

export const BILLING_API_BASE_URL = new URL("api/v2/billing", API_BASE_URL).href;

export const useAppPlans = () => {
  const { data, isLoading, error, mutate, isValidating } = useSWR<PlansList>(
    `${BILLING_API_BASE_URL}/plans`,
    fetcher
  );

  return {
    plans: data ?? { plans: [] },
    isLoading,
    isError: error,
    mutate,
    isValidating,
  };
};
