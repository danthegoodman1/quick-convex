/* eslint-disable */
/**
 * Generated `ComponentApi` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type { FunctionReference } from "convex/server";

/**
 * A utility for referencing a Convex component's exposed API.
 *
 * Useful when expecting a parameter like `components.myComponent`.
 * Usage:
 * ```ts
 * async function myFunction(ctx: QueryCtx, component: ComponentApi) {
 *   return ctx.runQuery(component.someFile.someQuery, { ...args });
 * }
 * ```
 */
export type ComponentApi<Name extends string | undefined = string | undefined> =
  {
    lib: {
      enqueue: FunctionReference<
        "mutation",
        "internal",
        {
          config?: {
            defaultLeaseDurationMs?: number;
            defaultOrderBy?: "vesting" | "fifo";
            defaultRetryBehavior?: {
              base: number;
              initialBackoffMs: number;
              maxAttempts: number;
            };
            maxConcurrentManagers?: number;
            minInactiveBeforeDeleteMs?: number;
            pointerBatchSize?: number;
            retryByDefault?: boolean;
            scannerBackoffMaxMs?: number;
            scannerBackoffMinMs?: number;
            scannerLeaseDurationMs?: number;
          };
          handler: string;
          handlerType?: "action" | "mutation";
          onCompleteContext?: any;
          onCompleteHandler?: string;
          payload: any;
          queueId: string;
          retry?: boolean;
          retryBehavior?: {
            base: number;
            initialBackoffMs: number;
            maxAttempts: number;
          };
          runAfter?: number;
          runAt?: number;
        },
        string,
        Name
      >;
      enqueueBatch: FunctionReference<
        "mutation",
        "internal",
        {
          config?: {
            defaultLeaseDurationMs?: number;
            defaultOrderBy?: "vesting" | "fifo";
            defaultRetryBehavior?: {
              base: number;
              initialBackoffMs: number;
              maxAttempts: number;
            };
            maxConcurrentManagers?: number;
            minInactiveBeforeDeleteMs?: number;
            pointerBatchSize?: number;
            retryByDefault?: boolean;
            scannerBackoffMaxMs?: number;
            scannerBackoffMinMs?: number;
            scannerLeaseDurationMs?: number;
          };
          items: Array<{
            handler: string;
            handlerType?: "action" | "mutation";
            onCompleteContext?: any;
            onCompleteHandler?: string;
            payload: any;
            queueId: string;
            retry?: boolean;
            retryBehavior?: {
              base: number;
              initialBackoffMs: number;
              maxAttempts: number;
            };
            runAfter?: number;
            runAt?: number;
          }>;
        },
        Array<string>,
        Name
      >;
      getQueueStats: FunctionReference<
        "query",
        "internal",
        { queueId: string },
        { itemCount: number; leasedCount: number; pendingCount: number },
        Name
      >;
    };
  };
