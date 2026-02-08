/// <reference types="vite/client" />

import {
  anyApi,
  createFunctionHandle,
  makeFunctionReference,
  type ApiFromModules,
} from "convex/server";
import { v } from "convex/values";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { internal } from "./_generated/api.js";
import { action, mutation } from "./_generated/server.js";
import { initConvexTest } from "./setup.test.js";

export const markProbeExecuted = mutation({
  args: {
    probeId: v.id("queueItems"),
    via: v.union(v.literal("action"), v.literal("mutation")),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await ctx.db.patch(args.probeId, {
      payload: { executedBy: args.via },
    });
    return null;
  },
});

const markProbeExecutedRef = makeFunctionReference<
  "mutation",
  {
    probeId: string;
    via: "action" | "mutation";
  }
>("lib.test:markProbeExecuted");

export const workerActionExec = action({
  args: {
    payload: v.object({ probeId: v.id("queueItems") }),
    queueId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await ctx.runMutation(markProbeExecutedRef, {
      probeId: args.payload.probeId,
      via: "action",
    });
    return null;
  },
});

export const workerMutationExec = mutation({
  args: {
    payload: v.object({ probeId: v.id("queueItems") }),
    queueId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await ctx.runMutation(markProbeExecutedRef, {
      probeId: args.payload.probeId,
      via: "mutation",
    });
    return null;
  },
});

export const workerAlwaysFailAction = action({
  args: {
    payload: v.any(),
    queueId: v.string(),
  },
  returns: v.null(),
  handler: async () => {
    throw new Error("intentional failure");
  },
});

const workerActionExecRef = makeFunctionReference<
  "action",
  { payload: { probeId: string }; queueId: string }
>("lib.test:workerActionExec");

const workerMutationExecRef = makeFunctionReference<
  "mutation",
  { payload: { probeId: string }; queueId: string }
>("lib.test:workerMutationExec");

const workerAlwaysFailActionRef = makeFunctionReference<
  "action",
  { payload: unknown; queueId: string }
>("lib.test:workerAlwaysFailAction");

export const createWorkerHandle = mutation({
  args: {
    type: v.union(
      v.literal("actionExec"),
      v.literal("mutationExec"),
      v.literal("alwaysFailAction"),
    ),
  },
  returns: v.string(),
  handler: async (_ctx, args) => {
    if (args.type === "actionExec") {
      return await createFunctionHandle(workerActionExecRef);
    }
    if (args.type === "mutationExec") {
      return await createFunctionHandle(workerMutationExecRef);
    }
    return await createFunctionHandle(workerAlwaysFailActionRef);
  },
});

const testApi = (
  anyApi as unknown as ApiFromModules<{
    "lib.test": {
      createWorkerHandle: typeof createWorkerHandle;
    };
  }>
)["lib.test"];

describe("component runtime execution", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
  });

  test("runWorker executes action handlers", async () => {
    const t = initConvexTest();
    const probeId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "probe",
        payload: { executedBy: null },
        handler: "probe",
        vestingTime: 0,
        errorCount: 0,
      }),
    );

    const handle = await t.mutation(testApi.createWorkerHandle, {
      type: "actionExec",
    });
    const itemId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "queue-action",
        payload: { probeId },
        handler: handle,
        handlerType: "action",
        vestingTime: 0,
        leaseId: "lease-action",
        errorCount: 0,
      }),
    );

    await t.action(internal.scanner.runWorker, {
      itemId,
      leaseId: "lease-action",
      handler: handle,
      handlerType: "action",
      payload: { probeId },
      queueId: "queue-action",
    });

    const [probe, processed] = await t.run(async (ctx) => {
      return Promise.all([ctx.db.get(probeId), ctx.db.get(itemId)]);
    });
    expect(probe?.payload).toEqual({ executedBy: "action" });
    expect(processed).toBeNull();
  });

  test("runWorker executes mutation handlers", async () => {
    const t = initConvexTest();
    const probeId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "probe",
        payload: { executedBy: null },
        handler: "probe",
        vestingTime: 0,
        errorCount: 0,
      }),
    );

    const handle = await t.mutation(testApi.createWorkerHandle, {
      type: "mutationExec",
    });
    const itemId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "queue-mutation",
        payload: { probeId },
        handler: handle,
        handlerType: "mutation",
        vestingTime: 0,
        leaseId: "lease-mutation",
        errorCount: 0,
      }),
    );

    await t.action(internal.scanner.runWorker, {
      itemId,
      leaseId: "lease-mutation",
      handler: handle,
      handlerType: "mutation",
      payload: { probeId },
      queueId: "queue-mutation",
    });

    const [probe, processed] = await t.run(async (ctx) => {
      return Promise.all([ctx.db.get(probeId), ctx.db.get(itemId)]);
    });
    expect(probe?.payload).toEqual({ executedBy: "mutation" });
    expect(processed).toBeNull();
  });

  test("vesting order allows later ready item after head fails and is delayed", async () => {
    const t = initConvexTest();
    const queueId = "queue-vesting";
    const probeId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "probe",
        payload: { executedBy: null },
        handler: "probe",
        vestingTime: 0,
        errorCount: 0,
      }),
    );

    const [failHandle, mutationHandle] = await Promise.all([
      t.mutation(testApi.createWorkerHandle, { type: "alwaysFailAction" }),
      t.mutation(testApi.createWorkerHandle, { type: "mutationExec" }),
    ]);

    const [firstId, secondId] = await t.run(async (ctx) => {
      const a = await ctx.db.insert("queueItems", {
        queueId,
        payload: { tag: "first" },
        handler: failHandle,
        handlerType: "action",
        vestingTime: 0,
        errorCount: 0,
      });
      const b = await ctx.db.insert("queueItems", {
        queueId,
        payload: { probeId },
        handler: mutationHandle,
        handlerType: "mutation",
        vestingTime: 0,
        errorCount: 0,
      });
      return [a, b];
    });

    const firstLease = await t.mutation(internal.lib.dequeue, {
      queueId,
      limit: 1,
      orderBy: "vesting",
    });
    expect(firstLease).toHaveLength(1);
    expect(firstLease[0].item._id).toBe(firstId);

    await t.action(internal.scanner.runWorker, {
      itemId: firstLease[0].item._id,
      leaseId: firstLease[0].leaseId,
      handler: firstLease[0].item.handler,
      handlerType: "action",
      payload: firstLease[0].item.payload,
      queueId,
    });

    const secondLease = await t.mutation(internal.lib.dequeue, {
      queueId,
      limit: 1,
      orderBy: "vesting",
    });
    expect(secondLease).toHaveLength(1);
    expect(secondLease[0].item._id).toBe(secondId);

    await t.action(internal.scanner.runWorker, {
      itemId: secondLease[0].item._id,
      leaseId: secondLease[0].leaseId,
      handler: secondLease[0].item.handler,
      handlerType: "mutation",
      payload: secondLease[0].item.payload,
      queueId,
    });

    const probe = await t.run(async (ctx) => ctx.db.get(probeId));
    expect(probe?.payload).toEqual({ executedBy: "mutation" });
  });

  test("fifo order blocks later ready items behind delayed head", async () => {
    const t = initConvexTest();
    const queueId = "queue-fifo";

    const failHandle = await t.mutation(testApi.createWorkerHandle, {
      type: "alwaysFailAction",
    });

    const [firstId] = await t.run(async (ctx) => {
      const a = await ctx.db.insert("queueItems", {
        queueId,
        payload: { tag: "first" },
        handler: failHandle,
        handlerType: "action",
        vestingTime: 0,
        errorCount: 0,
      });
      await ctx.db.insert("queueItems", {
        queueId,
        payload: { tag: "second" },
        handler: failHandle,
        handlerType: "action",
        vestingTime: 0,
        errorCount: 0,
      });
      return [a];
    });

    const firstLease = await t.mutation(internal.lib.dequeue, {
      queueId,
      limit: 1,
      orderBy: "fifo",
    });
    expect(firstLease).toHaveLength(1);
    expect(firstLease[0].item._id).toBe(firstId);

    await t.action(internal.scanner.runWorker, {
      itemId: firstLease[0].item._id,
      leaseId: firstLease[0].leaseId,
      handler: firstLease[0].item.handler,
      handlerType: "action",
      payload: firstLease[0].item.payload,
      queueId,
    });

    const secondLease = await t.mutation(internal.lib.dequeue, {
      queueId,
      limit: 1,
      orderBy: "fifo",
    });
    expect(secondLease).toHaveLength(0);
  });

  test("finalizePointer uses FIFO head for next vesting", async () => {
    const t = initConvexTest();
    const queueId = "queue-fifo-pointer";
    const now = Date.now();
    const delayedHeadVesting = now + 60_000;

    const pointerId = await t.run(async (ctx) => {
      await ctx.db.insert("queueItems", {
        queueId,
        payload: { tag: "head-delayed" },
        handler: "h1",
        handlerType: "action",
        vestingTime: delayedHeadVesting,
        errorCount: 0,
      });
      await ctx.db.insert("queueItems", {
        queueId,
        payload: { tag: "later-ready" },
        handler: "h2",
        handlerType: "action",
        vestingTime: now - 1_000,
        errorCount: 0,
      });

      return await ctx.db.insert("queuePointers", {
        queueId,
        vestingTime: now,
        leaseId: "pointer-lease",
        leaseExpiry: now + 30_000,
        lastActiveTime: now,
      });
    });

    await t.mutation(internal.scanner.finalizePointer, {
      pointerId,
      pointerLeaseId: "pointer-lease",
      isEmpty: true,
      orderBy: "fifo",
    });

    const pointer = await t.run(async (ctx) => ctx.db.get(pointerId));
    expect(pointer).not.toBeNull();
    expect(pointer?.vestingTime).toBe(delayedHeadVesting);
  });
});
