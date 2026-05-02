/// <reference types="vite/client" />
import type { TestConvex } from "convex-test";
import { AsyncResource } from "node:async_hooks";
import { setTimeout as nodeSetTimeout } from "node:timers";
import type { SchemaDefinition } from "convex/server";
import schema from "./component/schema.js";
const modules = import.meta.glob("./component/**/*.ts");

const CONVEX_TEST_TIMER_PATCH = Symbol.for("quick-convex.convex-test-timer-patch");

function isVitestRuntime() {
  return (
    "process" in globalThis &&
    typeof globalThis.process === "object" &&
    globalThis.process !== null &&
    "env" in globalThis.process &&
    typeof globalThis.process.env === "object" &&
    globalThis.process.env !== null &&
    "VITEST" in globalThis.process.env
  );
}

function installConvexTestTimerPatch() {
  if (!isVitestRuntime()) {
    return;
  }

  const setTimeoutWithoutConvexTestTransaction = new AsyncResource(
    "quick-convex-test-timer",
  );
  const nativeSetTimeout = globalThis.setTimeout ?? nodeSetTimeout;
  if ((nativeSetTimeout as any)[CONVEX_TEST_TIMER_PATCH]) {
    return;
  }

  const patchedSetTimeout = ((handler: TimerHandler, timeout?: number, ...args: any[]) => {
    if (typeof handler !== "function") {
      return nativeSetTimeout(handler, timeout, ...args);
    }

    const stack = new Error().stack ?? "";
    if (!/node_modules[/\\]convex-test/.test(stack)) {
      return nativeSetTimeout(handler, timeout, ...args);
    }

    const callback = handler as (...callbackArgs: any[]) => void;

    // convex-test can create scheduled-function timers inside a transaction
    // AsyncLocalStorage scope. Running the callback from a clean async resource
    // prevents scheduled component functions from inheriting stale transaction state.
    return nativeSetTimeout(
      () =>
        setTimeoutWithoutConvexTestTransaction.runInAsyncScope(
          callback,
          undefined,
          ...args,
        ),
      timeout,
    );
  }) as typeof globalThis.setTimeout;
  (patchedSetTimeout as any)[CONVEX_TEST_TIMER_PATCH] = true;
  globalThis.setTimeout = patchedSetTimeout;
}

/**
 * Register the component with the test convex instance.
 * @param t - The test convex instance, e.g. from calling `convexTest`.
 * @param name - The name of the component, as registered in convex.config.ts.
 */
export function register<Schema extends SchemaDefinition<any, boolean>>(
  t: TestConvex<Schema>,
  name: string = "quickConvex",
) {
  installConvexTestTimerPatch();
  t.registerComponent(name, schema, modules);
}
export default { register, schema, modules };
