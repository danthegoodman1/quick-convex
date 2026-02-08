/// <reference types="vite/client" />

import {
  anyApi,
  createFunctionHandle,
  makeFunctionReference,
  type ApiFromModules,
} from "convex/server";
import { v } from "convex/values";
import { describe, expect, test } from "vitest";
import { api, internal } from "./_generated/api.js";
import { action, mutation } from "./_generated/server.js";
import { initConvexTest } from "./setup.test.js";

export const workerAction = action({
  args: {
    payload: v.object({ value: v.number() }),
    queueId: v.string(),
  },
  returns: v.null(),
  handler: async () => null,
});

export const workerMutation = mutation({
  args: {
    payload: v.object({ value: v.number() }),
    queueId: v.string(),
  },
  returns: v.null(),
  handler: async () => null,
});

const workerActionRef = makeFunctionReference<
  "action",
  { payload: { value: number }; queueId: string }
>("lib.test:workerAction");

const workerMutationRef = makeFunctionReference<
  "mutation",
  { payload: { value: number }; queueId: string }
>("lib.test:workerMutation");

export const createWorkerHandle = mutation({
  args: {
    type: v.union(v.literal("action"), v.literal("mutation")),
  },
  returns: v.string(),
  handler: async (_ctx, args) => {
    if (args.type === "mutation") {
      return await createFunctionHandle(workerMutationRef);
    }
    return await createFunctionHandle(workerActionRef);
  },
});

const testApi = (
  anyApi as unknown as ApiFromModules<{
    "lib.test": {
      createWorkerHandle: typeof createWorkerHandle;
    };
  }>
)["lib.test"];

describe("component runtime", () => {
  test("enqueue defaults handlerType to action for legacy callers", async () => {
    const t = initConvexTest();

    const itemId = await t.mutation(api.lib.enqueue, {
      queueId: "queue-legacy",
      payload: { value: 1 },
      handler: "legacy-handler",
    });

    const row = await t.run(async (ctx) => await ctx.db.get(itemId));
    expect(row).not.toBeNull();
    expect(row?.handlerType).toBe("action");
  });

  test("runWorker dispatches action and completes item", async () => {
    const t = initConvexTest();

    const handle = await t.mutation(testApi.createWorkerHandle, { type: "action" });
    const itemId = await t.run(async (ctx) => {
      return await ctx.db.insert("queueItems", {
        queueId: "queue-run-action",
        payload: { value: 1 },
        handler: handle,
        handlerType: "action",
        vestingTime: 0,
        leaseId: "lease-action",
        errorCount: 0,
      });
    });

    await t.action(internal.scanner.runWorker, {
      itemId,
      leaseId: "lease-action",
      handler: handle,
      handlerType: "action",
      payload: { value: 1 },
      queueId: "queue-run-action",
    });

    const row = await t.run(async (ctx) => await ctx.db.get(itemId));
    expect(row).toBeNull();
  });

  test("runWorker dispatches mutation and completes item", async () => {
    const t = initConvexTest();

    const handle = await t.mutation(testApi.createWorkerHandle, {
      type: "mutation",
    });
    const itemId = await t.run(async (ctx) => {
      return await ctx.db.insert("queueItems", {
        queueId: "queue-run-mutation",
        payload: { value: 2 },
        handler: handle,
        handlerType: "mutation",
        vestingTime: 0,
        leaseId: "lease-mutation",
        errorCount: 0,
      });
    });

    await t.action(internal.scanner.runWorker, {
      itemId,
      leaseId: "lease-mutation",
      handler: handle,
      handlerType: "mutation",
      payload: { value: 2 },
      queueId: "queue-run-mutation",
    });

    const row = await t.run(async (ctx) => await ctx.db.get(itemId));
    expect(row).toBeNull();
  });

  test("replayDeadLetter preserves handlerType", async () => {
    const t = initConvexTest();

    const deadLetterId = await t.run(async (ctx) => {
      return await ctx.db.insert("deadLetterItems", {
        queueId: "queue-dlq",
        payload: { value: 1 },
        handler: "mutation-handler",
        handlerType: "mutation",
        errorCount: 11,
        movedAt: 1000,
      });
    });

    const itemId = await t.mutation(api.lib.replayDeadLetter, { deadLetterId });
    expect(itemId).not.toBeNull();

    const row = await t.run(async (ctx) => await ctx.db.get(itemId!));
    expect(row).not.toBeNull();
    expect(row?.handlerType).toBe("mutation");
  });
});
