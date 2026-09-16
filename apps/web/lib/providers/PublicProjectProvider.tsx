"use client";

import { type ReactNode, createContext, useContext } from "react";

// Marks the subtree as an anonymous public project view and names the project.
// User-scoped data hooks (profile, organization, system settings) read this to
// skip fetching authenticated endpoints that a public viewer neither has access
// to nor needs; data widgets read the id to ask the backend for the public seat
// of an otherwise authenticated process.
//
// A context (rather than the route path) is used deliberately: public projects
// can be served from custom domains, so URL-based detection is unreliable. The
// value is set synchronously in the render tree, so it is correct on the first
// render — unlike the Redux `mapMode`, which is dispatched in a post-mount effect.
const PublicProjectContext = createContext<{ projectId: string } | null>(null);

export const PublicProjectProvider = ({
  projectId,
  children,
}: {
  projectId: string;
  children: ReactNode;
}) => <PublicProjectContext.Provider value={{ projectId }}>{children}</PublicProjectContext.Provider>;

export const useIsPublicProject = (): boolean => useContext(PublicProjectContext) !== null;

/** The public project's id inside a public view; null in the editor and everywhere else. */
export const usePublicProjectId = (): string | null => useContext(PublicProjectContext)?.projectId ?? null;
