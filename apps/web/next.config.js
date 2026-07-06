const withBundleAnalyzer = require("@next/bundle-analyzer")({
  enabled: process.env.ANALYZE === "true",
});
const { withSentryConfig } = require("@sentry/nextjs");

const nextConfig = {
  output: "standalone",
  env: {
    // Client-bundle vars derived from their single source of truth. An
    // explicitly set NEXT_PUBLIC_* value always wins — Docker builds set them
    // to placeholders that entrypoint.sh substitutes at container start.
    NEXT_PUBLIC_AUTH: process.env.NEXT_PUBLIC_AUTH ?? process.env.AUTH ?? "",
    NEXT_PUBLIC_APP_URL: process.env.NEXT_PUBLIC_APP_URL ?? process.env.NEXTAUTH_URL ?? "",
    NEXT_PUBLIC_KEYCLOAK_ISSUER:
      process.env.NEXT_PUBLIC_KEYCLOAK_ISSUER ??
      (process.env.KEYCLOAK_SERVER_URL && process.env.REALM_NAME
        ? `${process.env.KEYCLOAK_SERVER_URL}/realms/${process.env.REALM_NAME}`
        : ""),
    NEXT_PUBLIC_KEYCLOAK_CLIENT_ID:
      process.env.NEXT_PUBLIC_KEYCLOAK_CLIENT_ID ?? process.env.KEYCLOAK_CLIENT_ID ?? "",
    NEXT_PUBLIC_APP_ENVIRONMENT:
      process.env.NEXT_PUBLIC_APP_ENVIRONMENT ?? process.env.ENVIRONMENT ?? "",
  },
  reactStrictMode: true,
  transpilePackages: ["@p4b/ui", "@p4b/tsconfig"],
  modularizeImports: {
    "@mui/icons-material": {
      transform: "@mui/icons-material/{{member}}",
    },
  },
  images: {
    domains: ["assets.plan4better.de", "source.unsplash.com"],
  },
  webpack: (config) => {
    config.module.exprContextCritical = false; // Todo: Added to suppress warnings from cog-protocol (Find a better solution)
    return config;
  },
};

const sentryConfig = {
  // For all available options, see:
  // https://github.com/getsentry/sentry-webpack-plugin#options
  org: "plan4better",
  project: "goat-frontend",
  // Suppresses source map uploading logs during build
  silent: true,
  // Upload a larger set of source maps for prettier stack traces (increases build time)
  widenClientFileUpload: true,
  // Routes browser requests to Sentry through a Next.js rewrite to circumvent ad-blockers (increases server load)
  tunnelRoute: "/api/monitoring",
  // Hides source maps from generated client bundles
  hideSourceMaps: true,
  // Automatically tree-shake Sentry logger statements to reduce bundle size
  disableLogger: true,
};

const finalConfig = process.env.NEXT_PUBLIC_SENTRY_DSN
  ? withSentryConfig(nextConfig, sentryConfig)
  : nextConfig;

module.exports = withBundleAnalyzer(finalConfig);
