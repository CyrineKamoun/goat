import { fetcher } from "@/lib/api/fetcher";
import { useAuthedSWR } from "@/lib/api/useAuthedSWR";
import { USERS_API_BASE_URL } from "@/lib/api/users";
import type { OnboardingFacts } from "@/lib/validations/home";

/** `GET /api/v2/users/me/onboarding` — which "getting started" steps the
 * caller has already completed, for the Home checklist. */
export const useOnboardingFacts = () => {
  const { data, isLoading, error, mutate } = useAuthedSWR<OnboardingFacts>(
    `${USERS_API_BASE_URL}/me/onboarding`,
    fetcher
  );
  return { facts: data, isLoading, isError: error, mutate };
};
