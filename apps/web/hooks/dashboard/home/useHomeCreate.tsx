"use client";

import { useRouter } from "next/navigation";
import type { ReactNode } from "react";
import { useState } from "react";
import { mutate } from "swr";

import { refreshContentFeed, useSpaces } from "@/lib/api/content";
import { useFolders } from "@/lib/api/folders";
import { USERS_API_BASE_URL } from "@/lib/api/users";
import { homeFolderOf } from "@/lib/utils/content";

import AddLayerDialog from "@/components/addLayer/AddLayerDialog";
import type { NewProjectIntent } from "@/components/dashboard/common/NewProjectMenu";
import { NewProjectFlows } from "@/components/dashboard/common/NewProjectMenu";

/** Same SWR key `useOnboardingFacts` (lib/api/onboarding.ts) reads, revalidated
 * here so the checklist/tray reflect a dataset just added without waiting for
 * their own poll. */
const revalidateOnboardingFacts = () => mutate(`${USERS_API_BASE_URL}/me/onboarding`);

/**
 * H11: Home's quick actions reuse the flows Content already has — one hook so
 * the hero, the onboarding checklist and the header tray share the same
 * wiring rather than each opening its own copy of these dialogs. Both a new
 * project and a new dataset file into the caller's personal home folder,
 * since Home has no folder of its own being browsed.
 */
export const useHomeCreate = (): {
  /** The caller's personal home folder, for callers that mount their own
   * project flow (`HomeHero`'s "New project" menu). */
  homeFolderId: string | undefined;
  newProject: () => void;
  addDataset: () => void;
  browseCatalog: () => void;
  dialogs: ReactNode;
} => {
  const router = useRouter();
  const { spaces } = useSpaces();
  const { folders } = useFolders({});

  const personalSpace = spaces.find((space) => space.kind === "personal");
  const homeFolder = personalSpace ? homeFolderOf(folders ?? [], personalSpace.id) : undefined;

  const [projectIntent, setProjectIntent] = useState<NewProjectIntent | null>(null);
  const [uploadOpen, setUploadOpen] = useState(false);

  const newProject = () => setProjectIntent("blank");
  const addDataset = () => setUploadOpen(true);
  const browseCatalog = () => router.push("/catalog");

  const dialogs = (
    <>
      <NewProjectFlows
        intent={projectIntent}
        onClose={() => setProjectIntent(null)}
        location={{ folderId: homeFolder?.id }}
      />

      {uploadOpen && (
        <AddLayerDialog
          source="upload"
          defaultFolderId={homeFolder?.id}
          onClose={() => {
            setUploadOpen(false);
            refreshContentFeed();
            void revalidateOnboardingFacts();
          }}
        />
      )}
    </>
  );

  return { homeFolderId: homeFolder?.id, newProject, addDataset, browseCatalog, dialogs };
};
