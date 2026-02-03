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
            defaultPriority?: number;
            maxConcurrentManagers?: number;
            maxRetries?: number;
            minInactiveBeforeDeleteMs?: number;
            pointerBatchSize?: number;
            scannerBackoffMaxMs?: number;
            scannerBackoffMinMs?: number;
            scannerLeaseDurationMs?: number;
          };
          delayMs?: number;
          handler: string;
          payload: any;
          priority?: number;
          queueId: string;
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
            defaultPriority?: number;
            maxConcurrentManagers?: number;
            maxRetries?: number;
            minInactiveBeforeDeleteMs?: number;
            pointerBatchSize?: number;
            scannerBackoffMaxMs?: number;
            scannerBackoffMinMs?: number;
            scannerLeaseDurationMs?: number;
          };
          items: Array<{
            delayMs?: number;
            handler: string;
            payload: any;
            priority?: number;
            queueId: string;
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
        { deadLetterId: string; delayMs?: number; priority?: number },
        null | string,
        Name
      >;
    };
  };
