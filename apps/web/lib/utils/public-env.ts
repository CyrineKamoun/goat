/**
 * Value of a NEXT_PUBLIC_* var inlined at build time, or undefined when there
 * is no usable value (unset at build, or still the APP_NEXT_PUBLIC_* Docker
 * placeholder that entrypoint.sh substitutes at container start).
 */
export const publicEnv = (value: string | undefined): string | undefined =>
  value && !value.startsWith("APP_NEXT_PUBLIC_") ? value : undefined;
