"use client";

import { type ReactNode, createContext, useContext } from "react";

// Marks the subtree as an anonymous public project view. User-scoped data
// hooks (profile, organization, system settings) read this to skip fetching
// authenticated endpoints that a public viewer neither has access to nor needs.
//
// A context (rather than the route path) is used deliberately: public projects
// can be served from custom domains, so URL-based detection is unreliable. The
// value is set synchronously in the render tree, so it is correct on the first
// render — unlike the Redux `mapMode`, which is dispatched in a post-mount effect.
const PublicProjectContext = createContext(false);

export const PublicProjectProvider = ({ children }: { children: ReactNode }) => (
  <PublicProjectContext.Provider value={true}>{children}</PublicProjectContext.Provider>
);

export const useIsPublicProject = (): boolean => useContext(PublicProjectContext);
