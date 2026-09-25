import type { TestingLibraryMatchers } from "@testing-library/jest-dom/matchers";

// jest-dom's own Vitest typings extend `Assertion<T>`, which since Vitest 5 is
// `Assertion<R, T>` built from `Matchers<R, T>`; interfaces with different type
// parameters don't merge, so its matchers disappear from `expect()`. Custom
// matchers belong on `Matchers`, with the exact type parameters Vitest declares.
declare module "vitest" {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  interface Matchers<R extends void | Promise<void> = void | Promise<void>, T = unknown>
    extends TestingLibraryMatchers<unknown, R> {}
}
