import type { StoryFn, Meta } from "@storybook/react";

import { createPageStory } from "../login/createPageStory";

const pageId = "logout-confirm.ftl";

const { PageStory } = createPageStory({ pageId });

export default {
  title: "Pages/Auth/Logout Confirm",
  component: PageStory,
} as Meta<typeof PageStory>;

export const Default: StoryFn<typeof PageStory> = () => <PageStory />;
