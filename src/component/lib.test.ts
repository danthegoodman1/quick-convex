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

const completionStatusValidator = v.union(
  v.literal("success"),
  v.literal("failure"),
  v.literal("cancelled"),
);
const onCompleteAttemptsByWorkId = new Map<string, number>();

const markProbeExecutedRef = makeFunctionReference<
  "mutation",
  {
    probeId: string;
    via: "action" | "mutation";
  }
>("lib.test:markProbeExecuted");

export const recordOnComplete = mutation({
  args: {
    workId: v.string(),
    context: v.optional(v.object({ probeId: v.id("queueItems") })),
    status: completionStatusValidator,
    result: v.any(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    if (args.context?.probeId) {
      await ctx.db.patch(args.context.probeId, {
        payload: {
          completion: {
            workId: args.workId,
            status: args.status,
            result: args.result,
          },
        },
      });
    }
    return null;
  },
});

export const onCompleteTimeoutTwice = mutation({
  args: {
    workId: v.string(),
    context: v.optional(v.object({ probeId: v.id("queueItems") })),
    status: completionStatusValidator,
    result: v.any(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const attempts = (onCompleteAttemptsByWorkId.get(args.workId) ?? 0) + 1;
    onCompleteAttemptsByWorkId.set(args.workId, attempts);
    if (attempts <= 2) {
      throw new Error("timed out while running onComplete");
    }
    if (args.context?.probeId) {
      await ctx.db.patch(args.context.probeId, {
        payload: { attempts },
      });
    }
    return null;
  },
});

export const onCompleteAlwaysFails = mutation({
  args: {
    workId: v.string(),
    context: v.optional(v.object({ probeId: v.id("queueItems") })),
    status: completionStatusValidator,
    result: v.any(),
  },
  returns: v.null(),
  handler: async () => {
    throw new Error("onComplete failed");
  },
});

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

const recordOnCompleteRef = makeFunctionReference<
  "mutation",
  {
    workId: string;
    context?: { probeId: string };
    status: "success" | "failure" | "cancelled";
    result: any;
  }
>("lib.test:recordOnComplete");

const onCompleteTimeoutTwiceRef = makeFunctionReference<
  "mutation",
  {
    workId: string;
    context?: { probeId: string };
    status: "success" | "failure" | "cancelled";
    result: any;
  }
>("lib.test:onCompleteTimeoutTwice");

const onCompleteAlwaysFailsRef = makeFunctionReference<
  "mutation",
  {
    workId: string;
    context?: { probeId: string };
    status: "success" | "failure" | "cancelled";
    result: any;
  }
>("lib.test:onCompleteAlwaysFails");

export const createWorkerHandle = mutation({
  args: {
    type: v.union(
      v.literal("actionExec"),
      v.literal("mutationExec"),
      v.literal("alwaysFailAction"),
      v.literal("recordOnComplete"),
      v.literal("onCompleteTimeoutTwice"),
      v.literal("onCompleteAlwaysFails"),
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
    if (args.type === "recordOnComplete") {
      return await createFunctionHandle(recordOnCompleteRef);
    }
    if (args.type === "onCompleteTimeoutTwice") {
      return await createFunctionHandle(onCompleteTimeoutTwiceRef);
    }
    if (args.type === "onCompleteAlwaysFails") {
      return await createFunctionHandle(onCompleteAlwaysFailsRef);
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
    onCompleteAttemptsByWorkId.clear();
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

  test("onComplete runs after successful action worker", async () => {
    const t = initConvexTest();
    const completionProbeId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "probe-completion",
        payload: { completion: null },
        handler: "probe",
        vestingTime: 0,
        errorCount: 0,
      }),
    );

    const [actionHandle, onCompleteHandle] = await Promise.all([
      t.mutation(testApi.createWorkerHandle, { type: "actionExec" }),
      t.mutation(testApi.createWorkerHandle, { type: "recordOnComplete" }),
    ]);

    const itemId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "queue-action-on-complete",
        payload: { probeId: completionProbeId },
        handler: actionHandle,
        handlerType: "action",
        onCompleteHandler: onCompleteHandle,
        onCompleteContext: { probeId: completionProbeId },
        phase: "run",
        vestingTime: 0,
        leaseId: "lease-action",
        errorCount: 0,
      }),
    );

    await t.action(internal.scanner.runWorkerAction, {
      itemId,
      leaseId: "lease-action",
      handler: actionHandle,
      handlerType: "action",
      payload: { probeId: completionProbeId },
      queueId: "queue-action-on-complete",
    });

    const [probe, item] = await t.run(async (ctx) =>
      Promise.all([ctx.db.get(completionProbeId), ctx.db.get(itemId)]),
    );
    expect((probe?.payload as any)?.completion?.status).toBe("success");
    expect(item).toBeNull();
  });

  test("onComplete runs after successful mutation worker", async () => {
    const t = initConvexTest();
    const completionProbeId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "probe-completion",
        payload: { completion: null },
        handler: "probe",
        vestingTime: 0,
        errorCount: 0,
      }),
    );

    const [mutationHandle, onCompleteHandle] = await Promise.all([
      t.mutation(testApi.createWorkerHandle, { type: "mutationExec" }),
      t.mutation(testApi.createWorkerHandle, { type: "recordOnComplete" }),
    ]);

    const itemId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "queue-mutation-on-complete",
        payload: { probeId: completionProbeId },
        handler: mutationHandle,
        handlerType: "mutation",
        onCompleteHandler: onCompleteHandle,
        onCompleteContext: { probeId: completionProbeId },
        phase: "run",
        vestingTime: 0,
        leaseId: "lease-mutation",
        errorCount: 0,
      }),
    );

    await t.mutation(internal.scanner.runWorkerMutation, {
      itemId,
      leaseId: "lease-mutation",
      handler: mutationHandle,
      payload: { probeId: completionProbeId },
      queueId: "queue-mutation-on-complete",
    });

    const [probe, item] = await t.run(async (ctx) =>
      Promise.all([ctx.db.get(completionProbeId), ctx.db.get(itemId)]),
    );
    expect((probe?.payload as any)?.completion?.status).toBe("success");
    expect(item).toBeNull();
  });

  test("worker retry happens before terminal failure onComplete", async () => {
    const t = initConvexTest();
    const queueId = "queue-retry-on-complete";
    const completionProbeId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "probe-completion",
        payload: { completion: null },
        handler: "probe",
        vestingTime: 0,
        errorCount: 0,
      }),
    );

    const [failHandle, onCompleteHandle] = await Promise.all([
      t.mutation(testApi.createWorkerHandle, { type: "alwaysFailAction" }),
      t.mutation(testApi.createWorkerHandle, { type: "recordOnComplete" }),
    ]);

    const itemId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId,
        payload: { any: "value" },
        handler: failHandle,
        handlerType: "action",
        onCompleteHandler: onCompleteHandle,
        onCompleteContext: { probeId: completionProbeId },
        phase: "run",
        retryEnabled: true,
        retryBehavior: {
          maxAttempts: 2,
          initialBackoffMs: 100,
          base: 2,
        },
        vestingTime: 0,
        errorCount: 0,
      }),
    );

    const firstLease = await t.mutation(internal.lib.dequeue, {
      queueId,
      limit: 1,
      orderBy: "vesting",
    });
    expect(firstLease).toHaveLength(1);

    await t.action(internal.scanner.runWorkerAction, {
      itemId,
      leaseId: firstLease[0].leaseId,
      handler: failHandle,
      handlerType: "action",
      payload: { any: "value" },
      queueId,
    });

    const [afterFirstAttemptItem, completionAfterFirstAttempt] = await t.run(
      async (ctx) =>
        Promise.all([ctx.db.get(itemId), ctx.db.get(completionProbeId)]),
    );
    expect(afterFirstAttemptItem).not.toBeNull();
    expect(afterFirstAttemptItem?.errorCount).toBe(1);
    expect((completionAfterFirstAttempt?.payload as any)?.completion).toBeNull();

    await t.run(async (ctx) =>
      ctx.db.patch(itemId, {
        vestingTime: 0,
      }),
    );

    const secondLease = await t.mutation(internal.lib.dequeue, {
      queueId,
      limit: 1,
      orderBy: "vesting",
    });
    expect(secondLease).toHaveLength(1);
    expect(secondLease[0].item._id).toBe(itemId);

    await t.action(internal.scanner.runWorkerAction, {
      itemId,
      leaseId: secondLease[0].leaseId,
      handler: failHandle,
      handlerType: "action",
      payload: { any: "value" },
      queueId,
    });

    const [afterSecondAttemptItem, completionAfterSecondAttempt] = await t.run(
      async (ctx) =>
        Promise.all([ctx.db.get(itemId), ctx.db.get(completionProbeId)]),
    );
    expect(afterSecondAttemptItem).toBeNull();
    expect((completionAfterSecondAttempt?.payload as any)?.completion?.status).toBe(
      "failure",
    );
  });

  test("recovery phase skips worker execution and runs onComplete", async () => {
    const t = initConvexTest();
    const workerProbeId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "probe-worker",
        payload: { executedBy: null },
        handler: "probe",
        vestingTime: 0,
        errorCount: 0,
      }),
    );
    const completionProbeId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "probe-completion",
        payload: { completion: null },
        handler: "probe",
        vestingTime: 0,
        errorCount: 0,
      }),
    );

    const [actionHandle, onCompleteHandle] = await Promise.all([
      t.mutation(testApi.createWorkerHandle, { type: "actionExec" }),
      t.mutation(testApi.createWorkerHandle, { type: "recordOnComplete" }),
    ]);

    const itemId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "queue-recovery",
        payload: { probeId: workerProbeId },
        handler: actionHandle,
        handlerType: "action",
        onCompleteHandler: onCompleteHandle,
        onCompleteContext: { probeId: completionProbeId },
        phase: "onComplete",
        completionStatus: "success",
        completionResult: null,
        vestingTime: 0,
        leaseId: "lease-recovery",
        errorCount: 0,
      }),
    );

    await t.action(internal.scanner.runWorkerAction, {
      itemId,
      leaseId: "lease-recovery",
      handler: actionHandle,
      handlerType: "action",
      payload: { probeId: workerProbeId },
      queueId: "queue-recovery",
    });

    const [workerProbe, completionProbe, item] = await t.run(async (ctx) =>
      Promise.all([
        ctx.db.get(workerProbeId),
        ctx.db.get(completionProbeId),
        ctx.db.get(itemId),
      ]),
    );
    expect(workerProbe?.payload).toEqual({ executedBy: null });
    expect((completionProbe?.payload as any)?.completion?.status).toBe("success");
    expect(item).toBeNull();
  });

  test("onComplete timeout retries twice before succeeding", async () => {
    const t = initConvexTest();
    const completionProbeId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "probe-timeout",
        payload: { attempts: 0 },
        handler: "probe",
        vestingTime: 0,
        errorCount: 0,
      }),
    );
    const onCompleteHandle = await t.mutation(testApi.createWorkerHandle, {
      type: "onCompleteTimeoutTwice",
    });

    const itemId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "queue-timeout",
        payload: { ignored: true },
        handler: "noop",
        handlerType: "mutation",
        onCompleteHandler: onCompleteHandle,
        onCompleteContext: { probeId: completionProbeId },
        phase: "onComplete",
        completionStatus: "success",
        completionResult: null,
        vestingTime: 0,
        leaseId: "lease-1",
        errorCount: 0,
      }),
    );

    await t.mutation(internal.scanner.runWorkerMutation, {
      itemId,
      leaseId: "lease-1",
      handler: "noop",
      payload: { ignored: true },
      queueId: "queue-timeout",
    });
    await t.run(async (ctx) =>
      ctx.db.patch(itemId, { leaseId: "lease-2", vestingTime: 0 }),
    );
    await t.mutation(internal.scanner.runWorkerMutation, {
      itemId,
      leaseId: "lease-2",
      handler: "noop",
      payload: { ignored: true },
      queueId: "queue-timeout",
    });
    await t.run(async (ctx) =>
      ctx.db.patch(itemId, { leaseId: "lease-3", vestingTime: 0 }),
    );
    await t.mutation(internal.scanner.runWorkerMutation, {
      itemId,
      leaseId: "lease-3",
      handler: "noop",
      payload: { ignored: true },
      queueId: "queue-timeout",
    });

    const [probe, item] = await t.run(async (ctx) =>
      Promise.all([ctx.db.get(completionProbeId), ctx.db.get(itemId)]),
    );
    expect((probe?.payload as any)?.attempts).toBe(3);
    expect(item).toBeNull();
  });

  test("terminal onComplete failure deletes item", async () => {
    const t = initConvexTest();
    const onCompleteHandle = await t.mutation(testApi.createWorkerHandle, {
      type: "onCompleteAlwaysFails",
    });
    const itemId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "queue-on-complete-fail",
        payload: { ignored: true },
        handler: "noop",
        handlerType: "mutation",
        onCompleteHandler: onCompleteHandle,
        phase: "onComplete",
        completionStatus: "success",
        completionResult: null,
        vestingTime: 0,
        leaseId: "lease-fail",
        errorCount: 0,
      }),
    );

    await t.mutation(internal.scanner.runWorkerMutation, {
      itemId,
      leaseId: "lease-fail",
      handler: "noop",
      payload: { ignored: true },
      queueId: "queue-on-complete-fail",
    });

    const item = await t.run(async (ctx) => ctx.db.get(itemId));
    expect(item).toBeNull();
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
        retryEnabled: true,
        retryBehavior: {
          maxAttempts: 2,
          initialBackoffMs: 60_000,
          base: 2,
        },
        vestingTime: 0,
        errorCount: 0,
      });
      await ctx.db.insert("queueItems", {
        queueId,
        payload: { tag: "second" },
        handler: failHandle,
        handlerType: "action",
        retryEnabled: true,
        retryBehavior: {
          maxAttempts: 2,
          initialBackoffMs: 60_000,
          base: 2,
        },
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

  test("finalizePointer parks empty queues to avoid pointer hot-loop", async () => {
    const t = initConvexTest();
    const queueId = "queue-empty-pointer";
    const now = Date.now();
    const minInactiveBeforeDeleteMs = 12_345;

    const pointerId = await t.run(async (ctx) => {
      await ctx.db.insert("config", { minInactiveBeforeDeleteMs });
      return await ctx.db.insert("queuePointers", {
        queueId,
        vestingTime: now,
        leaseId: "pointer-lease",
        leaseExpiry: now + 30_000,
        lastActiveTime: now - 5_000,
      });
    });

    await t.mutation(internal.scanner.finalizePointer, {
      pointerId,
      pointerLeaseId: "pointer-lease",
      isEmpty: true,
      orderBy: "vesting",
    });

    const pointer = await t.run(async (ctx) => ctx.db.get(pointerId));
    expect(pointer).not.toBeNull();
    expect(pointer?.vestingTime).toBeGreaterThanOrEqual(
      now + minInactiveBeforeDeleteMs,
    );
    expect(pointer?.leaseId).toBeUndefined();
    expect(pointer?.leaseExpiry).toBeUndefined();
  });
});
