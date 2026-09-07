"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

// The Content page replaced this one — send visitors there instead of
// rendering the old projects list.
const Projects = () => {
  const router = useRouter();

  useEffect(() => {
    router.replace("/content");
  }, [router]);

  return null;
};

export default Projects;
