"use client";

import { useEffect, useState } from "react";

/** A value that only updates `delayMs` after the input stops changing — the
 * hero search's 200 ms debounce, so a fast typist does not fire a request
 * per keystroke. */
export const useDebouncedValue = <T>(value: T, delayMs: number): T => {
  const [debounced, setDebounced] = useState(value);

  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delayMs);
    return () => clearTimeout(timer);
  }, [value, delayMs]);

  return debounced;
};
