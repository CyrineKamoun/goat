"use client";

import { Box, Button, Container, Paper, Skeleton, Tab, Tabs, Typography } from "@mui/material";
import { useRouter } from "next/navigation";
import { use, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { ICON_NAME, Icon } from "@p4b/ui/components/Icon";

import { useBundle, useBundleDependencies, useBundleLayers } from "@/lib/api/bundles";

import { CustomTabPanel, a11yProps } from "@/components/common/CustomTabPanel";
import BundleLayers from "@/components/dashboard/bundle/BundleLayers";
import BundleSummary from "@/components/dashboard/bundle/BundleSummary";

export default function BundleDetailPage(props: { params: Promise<{ bundleId: string }> }) {
  const { bundleId } = use(props.params);

  const router = useRouter();
  const { t } = useTranslation("common");
  const { bundle, isLoading } = useBundle(bundleId);
  const { members, isLoading: areMembersLoading } = useBundleLayers(bundleId);
  const { dependencies } = useBundleDependencies(bundleId);
  const [value, setValue] = useState(0);

  const handleChange = (_event: React.SyntheticEvent, newValue: number) => {
    setValue(newValue);
  };

  const tabItems = useMemo(
    () => [
      { label: t("summary"), value: "summary" },
      { label: t("layers"), value: "layers" },
    ],
    [t]
  );

  return (
    <Container sx={{ py: 10, px: 10 }} maxWidth="xl">
      <Box
        sx={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          mb: 8,
        }}>
        <Button
          variant="text"
          startIcon={<Icon iconName={ICON_NAME.CHEVRON_LEFT} style={{ fontSize: 12 }} />}
          sx={{
            borderRadius: 0,
          }}
          onClick={() => router.back()}>
          <Typography variant="body2" color="inherit">
            {t("back")}
          </Typography>
        </Button>
      </Box>
      {isLoading && <Skeleton variant="rectangular" width="100%" height={600} />}
      {!isLoading && bundle && (
        <Box>
          <Paper elevation={3} sx={{ p: 4 }}>
            <Typography variant="h6" fontWeight="bold">
              {bundle.name}
            </Typography>
            <Box sx={{ width: "100%", mt: 8 }}>
              <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
                <Tabs value={value} scrollButtons onChange={handleChange}>
                  {tabItems.map((item) => (
                    <Tab key={item.value} label={item.label} {...a11yProps(item.value)} />
                  ))}
                </Tabs>
              </Box>
              {tabItems.map((item) => (
                <CustomTabPanel
                  key={item.value}
                  value={value}
                  index={tabItems.findIndex((tab) => tab.value === item.value)}>
                  {item.value === "summary" && <BundleSummary bundle={bundle} dependencies={dependencies} />}
                  {item.value === "layers" && (
                    <BundleLayers members={members} isLoading={areMembersLoading} />
                  )}
                </CustomTabPanel>
              ))}
            </Box>
          </Paper>
        </Box>
      )}
    </Container>
  );
}
