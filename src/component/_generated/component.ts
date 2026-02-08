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
            maxConcurrentManagers?: number;
            maxRetries?: number;
            minInactiveBeforeDeleteMs?: number;
            pointerBatchSize?: number;
            scannerBackoffMaxMs?: number;
            scannerBackoffMinMs?: number;
            scannerLeaseDurationMs?: number;
          };
          handler: string;
          handlerType?: "action" | "mutation";
          payload: any;
          queueId: string;
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
            maxConcurrentManagers?: number;
            maxRetries?: number;
            minInactiveBeforeDeleteMs?: number;
            pointerBatchSize?: number;
            scannerBackoffMaxMs?: number;
            scannerBackoffMinMs?: number;
            scannerLeaseDurationMs?: number;
          };
          items: Array<{
            handler: string;
            handlerType?: "action" | "mutation";
            payload: any;
            queueId: string;
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
        {
          deadLetterCount: number;
          itemCount: number;
          leasedCount: number;
          pendingCount: number;
        },
        Name
      >;
      listDeadLetters: FunctionReference<
        "query",
        "internal",
        { limit?: number; queueId?: string },
        Array<{
          _creationTime: number;
          _id: string;
          errorCount: number;
          handler: string;
          handlerType?: "action" | "mutation";
          lastError?: string;
          movedAt: number;
          payload: any;
          queueId: string;
        }>,
        Name
      >;
      replayDeadLetter: FunctionReference<
        "mutation",
        "internal",
        { deadLetterId: string; runAfter?: number; runAt?: number },
        null | string,
        Name
      >;
    };
  };
