/// <reference types="vite/client" />

import {
  anyApi,
  createFunctionHandle,
  makeFunctionReference,
  type ApiFromModules,
} from "convex/server";
import { v } from "convex/values";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { api, internal } from "./_generated/api.js";
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
const executionsByQueueId = new Map<string, "action" | "mutation">();

const markProbeExecutedRef = makeFunctionReference<
  "mutation",
  {
    probeId: string;
    via: "action" | "mutation";
  }
>("lib.test:markProbeExecuted");

export const markQueueExecuted = mutation({
  args: {
    queueId: v.string(),
    via: v.union(v.literal("action"), v.literal("mutation")),
  },
  returns: v.null(),
  handler: async (_ctx, args) => {
    executionsByQueueId.set(args.queueId, args.via);
    return null;
  },
});

const markQueueExecutedRef = makeFunctionReference<
  "mutation",
  {
    queueId: string;
    via: "action" | "mutation";
  }
>("lib.test:markQueueExecuted");

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

export const workerMutationExecNoProbe = mutation({
  args: {
    payload: v.any(),
    queueId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await ctx.runMutation(markQueueExecutedRef, {
      queueId: args.queueId,
      via: "mutation",
    });
    return null;
  },
});

export const workerActionExecNoProbe = action({
  args: {
    payload: v.any(),
    queueId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await ctx.runMutation(markQueueExecutedRef, {
      queueId: args.queueId,
      via: "action",
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

const workerMutationExecNoProbeRef = makeFunctionReference<
  "mutation",
  { payload: unknown; queueId: string }
>("lib.test:workerMutationExecNoProbe");

const workerActionExecNoProbeRef = makeFunctionReference<
  "action",
  { payload: unknown; queueId: string }
>("lib.test:workerActionExecNoProbe");

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

const dequeueRef = makeFunctionReference<
  "mutation",
  {
    queueId: string;
    limit?: number;
    orderBy?: "vesting" | "fifo";
  },
  Array<{ leaseId: string }>
>("lib:dequeue");

const finalizePointerRef = makeFunctionReference<
  "mutation",
  {
    pointerId: string;
    pointerLeaseId: string;
    isEmpty: boolean;
    orderBy: "vesting" | "fifo";
  },
  null
>("scanner:finalizePointer");

export const createWorkerHandle = mutation({
  args: {
    type: v.union(
      v.literal("actionExec"),
      v.literal("actionExecNoProbe"),
      v.literal("mutationExec"),
      v.literal("mutationExecNoProbe"),
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
    if (args.type === "actionExecNoProbe") {
      return await createFunctionHandle(workerActionExecNoProbeRef);
    }
    if (args.type === "mutationExec") {
      return await createFunctionHandle(workerMutationExecRef);
    }
    if (args.type === "mutationExecNoProbe") {
      return await createFunctionHandle(workerMutationExecNoProbeRef);
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

export const insertThenDequeue = mutation({
  args: {
    queueId: v.string(),
  },
  returns: v.number(),
  handler: async (ctx, args) => {
    await ctx.db.insert("queueItems", {
      queueId: args.queueId,
      priority: 0,
      payload: { insertedInCaller: true },
      handler: "noop",
      handlerType: "action",
      vestingTime: Date.now() - 1_000,
      errorCount: 0,
    });

    const leased = await ctx.runMutation(dequeueRef, {
      queueId: args.queueId,
      limit: 1,
      orderBy: "vesting",
    });

    return leased.length;
  },
});

export const insertThenFinalizePointer = mutation({
  args: {
    pointerId: v.id("queuePointers"),
    pointerLeaseId: v.string(),
    queueId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await ctx.db.insert("queueItems", {
      queueId: args.queueId,
      priority: 15,
      payload: { insertedInCaller: true },
      handler: "noop",
      handlerType: "action",
      vestingTime: Date.now() - 1_000,
      errorCount: 0,
    });

    await ctx.runMutation(finalizePointerRef, {
      pointerId: args.pointerId,
      pointerLeaseId: args.pointerLeaseId,
      isEmpty: false,
      orderBy: "vesting",
    });

    return null;
  },
});

const testApi = (
  anyApi as unknown as ApiFromModules<{
    "lib.test": {
      createWorkerHandle: typeof createWorkerHandle;
      insertThenDequeue: typeof insertThenDequeue;
      insertThenFinalizePointer: typeof insertThenFinalizePointer;
    };
  }>
)["lib.test"];

describe("component runtime execution", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    onCompleteAttemptsByWorkId.clear();
    executionsByQueueId.clear();
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
        priority: 0,
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
        priority: 0,
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
        priority: 0,
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
        priority: 0,
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
        priority: 0,
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
        priority: 0,
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
        priority: 0,
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
        priority: 0,
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
        priority: 0,
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
        priority: 0,
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
        priority: 0,
        payload: { executedBy: null },
        handler: "probe",
        vestingTime: 0,
        errorCount: 0,
      }),
    );
    const completionProbeId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "probe-completion",
        priority: 0,
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
        priority: 0,
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
        priority: 0,
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
        priority: 0,
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
        priority: 0,
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
        priority: 0,
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
        priority: 0,
        payload: { tag: "first" },
        handler: failHandle,
        handlerType: "action",
        vestingTime: 0,
        errorCount: 0,
      });
      const b = await ctx.db.insert("queueItems", {
        queueId,
        priority: 0,
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

  test("vesting order prioritizes higher-priority items before lower-priority items", async () => {
    const t = initConvexTest();
    const queueId = "queue-priority-order";

    const ids = await t.run(async (ctx) => {
      const low = await ctx.db.insert("queueItems", {
        queueId,
        priority: 1,
        payload: { tag: "low" },
        handler: "h-low",
        handlerType: "action",
        vestingTime: 0,
        errorCount: 0,
      });
      const highLater = await ctx.db.insert("queueItems", {
        queueId,
        priority: 5,
        payload: { tag: "high-later" },
        handler: "h-high-later",
        handlerType: "action",
        vestingTime: 10,
        errorCount: 0,
      });
      const highEarlier = await ctx.db.insert("queueItems", {
        queueId,
        priority: 5,
        payload: { tag: "high-earlier" },
        handler: "h-high-earlier",
        handlerType: "action",
        vestingTime: 0,
        errorCount: 0,
      });

      return { low, highEarlier, highLater };
    });

    const leased = await t.mutation(internal.lib.dequeue, {
      queueId,
      limit: 3,
      orderBy: "vesting",
    });

    expect(leased.map((entry) => entry.item._id)).toEqual([
      ids.highEarlier,
      ids.highLater,
      ids.low,
    ]);
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
        priority: 0,
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
        priority: 0,
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

  test("enqueue rejects priorities outside the supported range", async () => {
    const t = initConvexTest();

    await expect(
      t.mutation(api.lib.enqueue, {
        queueId: "queue-invalid-priority",
        priority: 16,
        payload: { any: "value" },
        handler: "noop",
      }),
    ).rejects.toThrow("priority must be an integer between 0 and 15");
  });

  test("dequeue confirms when snapshot misses a caller transaction write", async () => {
    const t = initConvexTest();
    const queueId = "queue-snapshot-confirm-dequeue";

    const leasedCount = await t.mutation(testApi.insertThenDequeue, {
      queueId,
    });

    const items = await t.run(async (ctx) =>
      ctx.db
        .query("queueItems")
        .withIndex("by_queue_priority_and_vesting_time", (q) => q.eq("queueId", queueId))
        .collect(),
    );

    // Snapshot queries intentionally don't observe the caller mutation's pending
    // insert, so dequeue must do a dependency-bearing confirmation before
    // returning empty.
    expect(leasedCount).toBe(1);
    expect(items).toHaveLength(1);
    expect(items[0].leaseId).toBeDefined();
  });

  test("claimAvailablePointers is bounded by available manager slots", async () => {
    const t = initConvexTest();
    const now = Date.now();
    const scannerLeaseId = "scanner-lease";

    await t.run(async (ctx) => {
      await ctx.db.insert("config", {
        managerSlots: 2,
        workersPerManager: 1,
        pointerBatchSize: 10,
      });
      await ctx.db.insert("managerSlots", {
        slotNumber: 0,
        leaseId: "busy-slot",
        leaseExpiry: now + 30_000,
        queueId: "busy",
      });
      await ctx.db.insert("queuePointers", {
        queueId: "queue-slot-a",
        priority: 0,
        vestingTime: now - 1_000,
        lastActiveTime: now,
      });
      await ctx.db.insert("queuePointers", {
        queueId: "queue-slot-b",
        priority: 0,
        vestingTime: now - 1_000,
        lastActiveTime: now,
      });
      await ctx.db.insert("scannerState", {
        leaseId: scannerLeaseId,
        leaseExpiry: now + 5_000,
        lastRunAt: now,
      });
    });

    const result = await t.mutation(internal.scanner.claimAvailablePointers, {
      leaseId: scannerLeaseId,
    });

    expect(result.valid).toBe(true);
    expect(result.workersPerManager).toBe(1);
    expect(result.pointers).toHaveLength(1);
    expect(result.pointers[0].slotNumber).toBe(1);

    const slots = await t.run(async (ctx) =>
      ctx.db.query("managerSlots").withIndex("by_slot_number").collect(),
    );
    expect(slots.map((slot) => slot.slotNumber)).toEqual([0, 1]);
  });

  test("claimAvailablePointers prefers higher-priority due pointers in vesting mode", async () => {
    const t = initConvexTest();
    const now = Date.now();
    const scannerLeaseId = "scanner-priority-lease";

    await t.run(async (ctx) => {
      await ctx.db.insert("config", {
        defaultOrderBy: "vesting",
        managerSlots: 1,
        workersPerManager: 1,
        pointerBatchSize: 10,
      });
      await ctx.db.insert("queuePointers", {
        queueId: "queue-low-priority",
        priority: 1,
        vestingTime: now - 1_000,
        lastActiveTime: now,
      });
      await ctx.db.insert("queuePointers", {
        queueId: "queue-high-priority",
        priority: 12,
        vestingTime: now - 1_000,
        lastActiveTime: now,
      });
      await ctx.db.insert("scannerState", {
        leaseId: scannerLeaseId,
        leaseExpiry: now + 5_000,
        lastRunAt: now,
      });
    });

    const result = await t.mutation(internal.scanner.claimAvailablePointers, {
      leaseId: scannerLeaseId,
    });

    expect(result.valid).toBe(true);
    expect(result.pointers).toHaveLength(1);
    expect(result.pointers[0].queueId).toBe("queue-high-priority");
  });

  test("claimScannerLease inspection is read-only on scannerState", async () => {
    const t = initConvexTest();
    const now = Date.now();
    const scannerLeaseId = "scanner-inspect-lease";

    await t.run(async (ctx) => {
      await ctx.db.insert("config", {
        managerSlots: 2,
        workersPerManager: 1,
      });
      await ctx.db.insert("queuePointers", {
        queueId: "queue-inspect",
        priority: 0,
        vestingTime: now - 1_000,
        lastActiveTime: now,
      });
      await ctx.db.insert("scannerState", {
        leaseId: scannerLeaseId,
        leaseExpiry: now + 5_000,
        lastRunAt: now,
      });
    });

    const inspection = await t.query(internal.scanner.claimScannerLease, {
      leaseId: scannerLeaseId,
    });

    const scannerState = await t.run(async (ctx) =>
      ctx.db.query("scannerState").first(),
    );

    expect(inspection.valid).toBe(true);
    expect(inspection.hasDuePointers).toBe(true);
    expect(inspection.availableSlotCount).toBe(2);
    expect(scannerState?.leaseId).toBe(scannerLeaseId);
    expect(scannerState?.leaseExpiry).toBe(now + 5_000);
    expect(scannerState?.lastRunAt).toBe(now);
  });

  test("tryClaimManagerSlot claims and releases slot ownership", async () => {
    const t = initConvexTest();
    const now = Date.now();
    const pointerLeaseId = "pointer-lease";

    const pointerId = await t.run(async (ctx) => {
      await ctx.db.insert("config", { managerSlots: 1 });
      await ctx.db.insert("managerSlots", { slotNumber: 0 });
      return await ctx.db.insert("queuePointers", {
        queueId: "queue-slot-claim",
        priority: 0,
        vestingTime: now,
        leaseId: pointerLeaseId,
        leaseExpiry: now + 600_000,
        lastActiveTime: now,
      });
    });

    const claim = await t.mutation(internal.scanner.tryClaimManagerSlot, {
      slotNumber: 0,
      pointerId,
      pointerLeaseId,
      queueId: "queue-slot-claim",
    });
    expect(claim.claimed).toBe(true);
    expect(claim.slotLeaseId).toBeDefined();

    const secondClaim = await t.mutation(internal.scanner.tryClaimManagerSlot, {
      slotNumber: 0,
      pointerId,
      pointerLeaseId,
      queueId: "queue-slot-claim",
    });
    expect(secondClaim.claimed).toBe(false);

    const outOfRangeClaim = await t.mutation(internal.scanner.tryClaimManagerSlot, {
      slotNumber: 1,
      pointerId,
      pointerLeaseId,
      queueId: "queue-slot-claim",
    });
    expect(outOfRangeClaim.claimed).toBe(false);

    const wrongRelease = await t.mutation(internal.scanner.releaseManagerSlot, {
      slotNumber: 0,
      slotLeaseId: "wrong-lease",
    });
    expect(wrongRelease).toBe(false);

    const released = await t.mutation(internal.scanner.releaseManagerSlot, {
      slotNumber: 0,
      slotLeaseId: claim.slotLeaseId!,
    });
    expect(released).toBe(true);

    const slot = await t.run(async (ctx) =>
      ctx.db
        .query("managerSlots")
        .withIndex("by_slot_number", (q) => q.eq("slotNumber", 0))
        .unique(),
    );
    expect(slot?.leaseId).toBeUndefined();
    expect(slot?.leaseExpiry).toBeUndefined();
    expect(slot?.pointerId).toBeUndefined();
    expect(slot?.queueId).toBeUndefined();
  });

  test("runManager executes workers directly and releases manager slot", async () => {
    const t = initConvexTest();
    const now = Date.now();
    const queueId = "queue-manager-direct";
    const pointerLeaseId = "pointer-lease-manager";

    const probeId = await t.run(async (ctx) =>
      ctx.db.insert("queueItems", {
        queueId: "probe",
        priority: 0,
        payload: { executedBy: null },
        handler: "probe",
        vestingTime: 0,
        errorCount: 0,
      }),
    );

    const mutationHandle = await t.mutation(testApi.createWorkerHandle, {
      type: "mutationExec",
    });

    const { pointerId, itemId } = await t.run(async (ctx) => {
      await ctx.db.insert("config", {
        managerSlots: 1,
        workersPerManager: 1,
      });
      await ctx.db.insert("managerSlots", { slotNumber: 0 });

      const pointerId = await ctx.db.insert("queuePointers", {
        queueId,
        priority: 0,
        vestingTime: now,
        leaseId: pointerLeaseId,
        leaseExpiry: now + 600_000,
        lastActiveTime: now,
      });
      const itemId = await ctx.db.insert("queueItems", {
        queueId,
        priority: 0,
        payload: { probeId },
        handler: mutationHandle,
        handlerType: "mutation",
        vestingTime: now - 1_000,
        errorCount: 0,
      });
      return { pointerId, itemId };
    });

    await t.action(internal.scanner.runManager, {
      pointerId,
      queueId,
      pointerLeaseId,
      orderBy: "vesting",
      workersPerManager: 1,
      slotNumber: 0,
    });

    const [probe, item, pointer, slot] = await t.run(async (ctx) =>
      Promise.all([
        ctx.db.get(probeId),
        ctx.db.get(itemId),
        ctx.db.get(pointerId),
        ctx.db
          .query("managerSlots")
          .withIndex("by_slot_number", (q) => q.eq("slotNumber", 0))
          .unique(),
      ]),
    );

    expect(probe?.payload).toEqual({ executedBy: "mutation" });
    expect(item).toBeNull();
    expect(pointer?.leaseId).toBeUndefined();
    expect(pointer?.leaseExpiry).toBeUndefined();
    expect(slot?.leaseId).toBeUndefined();
    expect(slot?.leaseExpiry).toBeUndefined();
  });

  test("scanner does not leave a follow-up wake scheduled after draining the only ready queue", async () => {
    const t = initConvexTest();
    const queueId = "queue-single-dispatch";

    const handle = await t.mutation(testApi.createWorkerHandle, {
      type: "mutationExecNoProbe",
    });

    await t.mutation(api.lib.enqueue, {
      queueId,
      payload: { ignored: true },
      handler: handle,
      handlerType: "mutation",
    });

    // Drive the immediate wake -> scan -> manager chain, but intentionally do
    // not advance the scanner backoff timer. If Quick rescheduled the scanner
    // unnecessarily, `scheduledFunctionId` will still be set here.
    for (let i = 0; i < 3; i++) {
      vi.advanceTimersByTime(0);
      await t.finishInProgressScheduledFunctions();
    }

    const [items, scannerState, slot] = await t.run(async (ctx) =>
      Promise.all([
        ctx.db
          .query("queueItems")
          .withIndex("by_queue_priority_and_vesting_time", (q) => q.eq("queueId", queueId))
          .collect(),
        ctx.db.query("scannerState").first(),
        ctx.db
          .query("managerSlots")
          .withIndex("by_slot_number", (q) => q.eq("slotNumber", 0))
          .unique(),
      ]),
    );

    expect(executionsByQueueId.get(queueId)).toBe("mutation");
    expect(items).toHaveLength(0);
    expect(scannerState?.scheduledFunctionId).toBeUndefined();
    expect(slot?.leaseId).toBeUndefined();
    expect(slot?.leaseExpiry).toBeUndefined();
  });

  test("scheduled scanner drain runs action-backed queue workers", async () => {
    const t = initConvexTest();
    const queueId = "queue-action-scheduled-drain";

    const handle = await t.mutation(testApi.createWorkerHandle, {
      type: "actionExecNoProbe",
    });

    await t.mutation(api.lib.enqueue, {
      queueId,
      payload: { ignored: true },
      handler: handle,
      handlerType: "action",
    });

    // Exercise the user-facing queue path: enqueue wakes the scanner, the
    // scanner schedules a manager, and the manager runs the action worker.
    for (let i = 0; i < 4; i++) {
      vi.advanceTimersByTime(0);
      await t.finishInProgressScheduledFunctions();
    }

    const [items, scannerState] = await t.run(async (ctx) =>
      Promise.all([
        ctx.db
          .query("queueItems")
          .withIndex("by_queue_priority_and_vesting_time", (q) => q.eq("queueId", queueId))
          .collect(),
        ctx.db.query("scannerState").first(),
      ]),
    );

    expect(executionsByQueueId.get(queueId)).toBe("action");
    expect(items).toHaveLength(0);
    expect(scannerState?.scheduledFunctionId).toBeUndefined();
  });

  test("enqueue recovers stale leased pointer and manager slot so ready work runs promptly", async () => {
    const t = initConvexTest();
    const now = Date.now();
    const queueId = "queue-stale-leased-pointer";
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    try {
      const handle = await t.mutation(testApi.createWorkerHandle, {
        type: "mutationExecNoProbe",
      });

      const { pointerId, slotNumber } = await t.run(async (ctx) => {
        await ctx.db.insert("config", {
          defaultOrderBy: "vesting",
          managerSlots: 4,
          workersPerManager: 1,
          scannerBackoffMaxMs: 5_000,
        });
        const pointerId = await ctx.db.insert("queuePointers", {
          queueId,
          priority: 1,
          vestingTime: now + 600_000,
          leaseId: "stale-pointer-lease",
          leaseExpiry: now + 600_000,
          lastActiveTime: now - 6_000,
        });
        await ctx.db.insert("managerSlots", {
          slotNumber: 1,
          leaseId: "stale-manager-slot",
          leaseExpiry: now + 600_000,
          pointerId,
          queueId,
        });
        return { pointerId, slotNumber: 1 };
      });

      await t.mutation(api.lib.enqueue, {
        queueId,
        priority: 1,
        payload: { ignored: true },
        handler: handle,
        handlerType: "mutation",
      });

      for (let i = 0; i < 4; i++) {
        vi.advanceTimersByTime(0);
        await t.finishInProgressScheduledFunctions();
      }

      const [items, pointer, slot] = await t.run(async (ctx) =>
        Promise.all([
          ctx.db
            .query("queueItems")
            .withIndex("by_queue_priority_and_vesting_time", (q) => q.eq("queueId", queueId))
            .collect(),
          ctx.db.get(pointerId),
          ctx.db
            .query("managerSlots")
            .withIndex("by_slot_number", (q) => q.eq("slotNumber", slotNumber))
            .unique(),
        ]),
      );

      expect(executionsByQueueId.get(queueId)).toBe("mutation");
      expect(items).toHaveLength(0);
      expect(pointer?.leaseId).toBeUndefined();
      expect(pointer?.leaseExpiry).toBeUndefined();
      expect(slot?.leaseId).toBeUndefined();
      expect(slot?.leaseExpiry).toBeUndefined();
      expect(warnSpy).toHaveBeenCalledWith(
        "[quick] recovering stale leased pointer",
        expect.objectContaining({
          queueId,
          activeManagerSlots: [slotNumber],
        }),
      );
    } finally {
      warnSpy.mockRestore();
    }
  });

  test("enqueue does not recover a healthy leased pointer while work is still in flight", async () => {
    const t = initConvexTest();
    const now = Date.now();
    const queueId = "queue-healthy-leased-pointer";
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => {});

    try {
      const pointerId = await t.run(async (ctx) => {
        await ctx.db.insert("config", {
          defaultOrderBy: "vesting",
          managerSlots: 4,
          workersPerManager: 1,
          scannerBackoffMaxMs: 5_000,
        });
        const pointerId = await ctx.db.insert("queuePointers", {
          queueId,
          priority: 1,
          vestingTime: now + 600_000,
          leaseId: "healthy-pointer-lease",
          leaseExpiry: now + 600_000,
          lastActiveTime: now,
        });
        await ctx.db.insert("managerSlots", {
          slotNumber: 2,
          leaseId: "healthy-manager-slot",
          leaseExpiry: now + 600_000,
          pointerId,
          queueId,
        });
        await ctx.db.insert("queueItems", {
          queueId,
          priority: 1,
          payload: { inFlight: true },
          handler: "noop",
          handlerType: "action",
          vestingTime: now + 30_000,
          leaseId: "healthy-item-lease",
          leaseExpiry: now + 30_000,
          errorCount: 0,
        });
        return pointerId;
      });

      await t.mutation(api.lib.enqueue, {
        queueId,
        priority: 1,
        payload: { ignored: true },
        handler: "noop",
        handlerType: "action",
      });

      const [pointer, slot] = await t.run(async (ctx) =>
        Promise.all([
          ctx.db.get(pointerId),
          ctx.db
            .query("managerSlots")
            .withIndex("by_slot_number", (q) => q.eq("slotNumber", 2))
            .unique(),
        ]),
      );

      expect(pointer?.leaseId).toBe("healthy-pointer-lease");
      expect(pointer?.leaseExpiry).toBe(now + 600_000);
      expect(slot?.leaseId).toBe("healthy-manager-slot");
      expect(slot?.leaseExpiry).toBe(now + 600_000);
      expect(infoSpy).toHaveBeenCalledWith(
        "[quick] enqueue skipped pointer promotion because queue still has leased work",
        expect.objectContaining({ queueId }),
      );
    } finally {
      infoSpy.mockRestore();
    }
  });

  test("parkScanner logs when the scanner becomes fully parked", async () => {
    const t = initConvexTest();
    const now = Date.now();
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => {});

    try {
      await t.run(async (ctx) => {
        await ctx.db.insert("scannerState", {
          leaseId: "scanner-lease-fully-parked",
          leaseExpiry: now + 30_000,
          lastRunAt: now,
        });
      });

      await t.mutation(internal.scanner.parkScanner, {
        leaseId: "scanner-lease-fully-parked",
      });

      const scannerState = await t.run(async (ctx) =>
        ctx.db.query("scannerState").first(),
      );
      expect(scannerState?.leaseId).toBeUndefined();
      expect(scannerState?.leaseExpiry).toBeUndefined();
      expect(scannerState?.scheduledFunctionId).toBeUndefined();
      expect(infoSpy).toHaveBeenCalledWith("[quick] scanner fully parked");
    } finally {
      infoSpy.mockRestore();
    }
  });

  test("enqueue wake logs when it wakes a parked scanner and cancels the stale parked wake", async () => {
    const t = initConvexTest();
    const now = Date.now();
    const infoSpy = vi.spyOn(console, "info").mockImplementation(() => {});

    try {
      const parkedWakeId = await t.run(async (ctx) => {
        const scheduledFunctionId = await ctx.scheduler.runAfter(
          60_000,
          internal.scanner.watchdogRecoverScanner,
          {},
        );
        await ctx.db.insert("scannerState", {
          scheduledFunctionId,
          lastRunAt: now,
        });
        return scheduledFunctionId;
      });

      const woke = await t.mutation(internal.scanner.tryWakeScanner, {
        reason: "enqueue",
      });

      const [scannerState, parkedWake] = await t.run(async (ctx) =>
        Promise.all([
          ctx.db.query("scannerState").first(),
          ctx.db.system.get("_scheduled_functions", parkedWakeId),
        ]),
      );

      expect(woke).toBe(true);
      expect(scannerState?.leaseId).toBeDefined();
      expect(scannerState?.scheduledFunctionId).toBeDefined();
      expect(scannerState?.scheduledFunctionId).not.toBe(parkedWakeId);
      expect(parkedWake?.state.kind).toBe("canceled");
      expect(infoSpy).toHaveBeenCalledWith("[quick] enqueue woke parked scanner");
    } finally {
      infoSpy.mockRestore();
    }
  });

  test("runScanner parks a leased future pointer without holding the scanner lease", async () => {
    const t = initConvexTest();
    const now = Date.now();
    const scannerLeaseId = "scanner-lease-future-pointer";
    const futureVestingTime = now + 20_000;

    await t.run(async (ctx) => {
      await ctx.db.insert("scannerState", {
        leaseId: scannerLeaseId,
        leaseExpiry: now + 30_000,
        lastRunAt: now,
      });
      await ctx.db.insert("queuePointers", {
        queueId: "queue-future-pointer",
        priority: 0,
        vestingTime: futureVestingTime,
        leaseId: "pointer-lease",
        leaseExpiry: futureVestingTime,
        lastActiveTime: now,
      });
    });

    await t.mutation(internal.scanner.runScanner, {
      leaseId: scannerLeaseId,
    });

    const scannerState = await t.run(async (ctx) =>
      ctx.db.query("scannerState").first(),
    );
    expect(scannerState?.leaseId).toBeUndefined();
    expect(scannerState?.leaseExpiry).toBeUndefined();
    expect(scannerState?.scheduledFunctionId).toBeDefined();

    const woke = await t.mutation(internal.scanner.tryWakeScanner, {});
    expect(woke).toBe(true);
  });

  test("runScanner slow-backs off without holding the scanner lease when all slots are busy", async () => {
    const t = initConvexTest();
    const now = Date.now();
    const scannerLeaseId = "scanner-lease-busy-slots";

    await t.run(async (ctx) => {
      await ctx.db.insert("config", {
        managerSlots: 1,
        scannerBackoffMaxMs: 12_345,
      });
      await ctx.db.insert("scannerState", {
        leaseId: scannerLeaseId,
        leaseExpiry: now + 30_000,
        lastRunAt: now,
      });
      await ctx.db.insert("managerSlots", {
        slotNumber: 0,
        leaseId: "busy-slot",
        leaseExpiry: now + 60_000,
      });
      await ctx.db.insert("queuePointers", {
        queueId: "queue-due-busy-slot",
        priority: 0,
        vestingTime: now - 1_000,
        lastActiveTime: now,
      });
    });

    await t.mutation(internal.scanner.runScanner, {
      leaseId: scannerLeaseId,
    });

    const scannerState = await t.run(async (ctx) =>
      ctx.db.query("scannerState").first(),
    );
    expect(scannerState?.leaseId).toBeUndefined();
    expect(scannerState?.leaseExpiry).toBeUndefined();
    expect(scannerState?.scheduledFunctionId).toBeDefined();

    const woke = await t.mutation(internal.scanner.tryWakeScanner, {});
    expect(woke).toBe(true);
  });

  test("runScanner claims pointer and manager slot in the same mutation", async () => {
    const t = initConvexTest();
    const now = Date.now();
    const scannerLeaseId = "scanner-lease-transactional-claim";

    const pointerId = await t.run(async (ctx) => {
      await ctx.db.insert("config", {
        managerSlots: 1,
        workersPerManager: 1,
      });
      await ctx.db.insert("scannerState", {
        leaseId: scannerLeaseId,
        leaseExpiry: now + 30_000,
        lastRunAt: now,
      });
      return await ctx.db.insert("queuePointers", {
        queueId: "queue-transactional-scanner",
        priority: 0,
        vestingTime: now - 1_000,
        lastActiveTime: now,
      });
    });

    await t.mutation(internal.scanner.runScanner, {
      leaseId: scannerLeaseId,
    });

    const [pointer, slot, scannerState] = await t.run(async (ctx) =>
      Promise.all([
        ctx.db.get(pointerId),
        ctx.db
          .query("managerSlots")
          .withIndex("by_slot_number", (q) => q.eq("slotNumber", 0))
          .unique(),
        ctx.db.query("scannerState").first(),
      ]),
    );

    // The scanner mutation should commit the pointer lease and slot ownership
    // together, avoiding the old action query -> mutation decision gap.
    expect(pointer?.leaseId).toBeDefined();
    expect(slot?.leaseId).toBeDefined();
    expect(slot?.pointerId).toBe(pointerId);
    expect(slot?.queueId).toBe("queue-transactional-scanner");
    expect(scannerState?.leaseId).not.toBe(scannerLeaseId);
    expect(scannerState?.scheduledFunctionId).toBeDefined();
  });

  test("runManager finalizes pointer when manager slot claim fails", async () => {
    const t = initConvexTest();
    const now = Date.now();
    const queueId = "queue-manager-claim-fail";
    const pointerLeaseId = "pointer-lease-fail";

    const pointerId = await t.run(async (ctx) => {
      await ctx.db.insert("config", { managerSlots: 1, workersPerManager: 1 });
      await ctx.db.insert("managerSlots", {
        slotNumber: 0,
        leaseId: "already-owned",
        leaseExpiry: now + 120_000,
      });
      return await ctx.db.insert("queuePointers", {
        queueId,
        priority: 0,
        vestingTime: now,
        leaseId: pointerLeaseId,
        leaseExpiry: now + 600_000,
        lastActiveTime: now,
      });
    });

    await t.action(internal.scanner.runManager, {
      pointerId,
      queueId,
      pointerLeaseId,
      orderBy: "vesting",
      workersPerManager: 1,
      slotNumber: 0,
    });

    const pointer = await t.run(async (ctx) => ctx.db.get(pointerId));
    expect(pointer?.leaseId).toBeUndefined();
    expect(pointer?.leaseExpiry).toBeUndefined();
  });

  test("finalizePointer uses FIFO head for next vesting", async () => {
    const t = initConvexTest();
    const queueId = "queue-fifo-pointer";
    const now = Date.now();
    const delayedHeadVesting = now + 60_000;

    const pointerId = await t.run(async (ctx) => {
      await ctx.db.insert("queueItems", {
        queueId,
        priority: 0,
        payload: { tag: "head-delayed" },
        handler: "h1",
        handlerType: "action",
        vestingTime: delayedHeadVesting,
        errorCount: 0,
      });
      await ctx.db.insert("queueItems", {
        queueId,
        priority: 0,
        payload: { tag: "later-ready" },
        handler: "h2",
        handlerType: "action",
        vestingTime: now - 1_000,
        errorCount: 0,
      });

      return await ctx.db.insert("queuePointers", {
        queueId,
        priority: 0,
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

  test("finalizePointer confirms vesting state before releasing leased pointer", async () => {
    const t = initConvexTest();
    const now = Date.now();
    const queueId = "queue-finalize-confirms-vesting";
    const pointerLeaseId = "pointer-lease-confirm-vesting";

    const pointerId = await t.run(async (ctx) => {
      await ctx.db.insert("queueItems", {
        queueId,
        priority: 1,
        payload: { tag: "snapshot-visible-low-priority" },
        handler: "low",
        handlerType: "action",
        vestingTime: now - 1_000,
        errorCount: 0,
      });
      return await ctx.db.insert("queuePointers", {
        queueId,
        priority: 1,
        vestingTime: now,
        leaseId: pointerLeaseId,
        leaseExpiry: now + 600_000,
        lastActiveTime: now,
      });
    });

    await t.mutation(testApi.insertThenFinalizePointer, {
      pointerId,
      pointerLeaseId,
      queueId,
    });

    const pointer = await t.run(async (ctx) => ctx.db.get(pointerId));

    // Snapshot finalization doesn't observe the caller's pending high-priority
    // insert, so vesting mode must confirm before publishing the next pointer.
    expect(pointer?.priority).toBe(15);
    expect(pointer?.leaseId).toBeUndefined();
    expect(pointer?.leaseExpiry).toBeUndefined();
  });

  test("finalizePointer keeps pointer priority when the same tier still has ready work", async () => {
    const t = initConvexTest();
    const queueId = "queue-priority-pointer-stays-high";
    const now = Date.now();

    const pointerId = await t.run(async (ctx) => {
      const processedHighId = await ctx.db.insert("queueItems", {
        queueId,
        priority: 15,
        payload: { tag: "processed-high" },
        handler: "h-processed",
        handlerType: "action",
        vestingTime: now - 2_000,
        errorCount: 0,
      });
      await ctx.db.insert("queueItems", {
        queueId,
        priority: 15,
        payload: { tag: "remaining-high" },
        handler: "h-remaining",
        handlerType: "action",
        vestingTime: now - 1_000,
        errorCount: 0,
      });
      await ctx.db.insert("queueItems", {
        queueId,
        priority: 3,
        payload: { tag: "remaining-low" },
        handler: "h-low",
        handlerType: "action",
        vestingTime: now - 500,
        errorCount: 0,
      });
      const pointerId = await ctx.db.insert("queuePointers", {
        queueId,
        priority: 15,
        vestingTime: now - 2_000,
        leaseId: "pointer-lease-high",
        leaseExpiry: now + 30_000,
        lastActiveTime: now,
      });
      await ctx.db.delete(processedHighId);
      return pointerId;
    });

    await t.mutation(internal.scanner.finalizePointer, {
      pointerId,
      pointerLeaseId: "pointer-lease-high",
      isEmpty: false,
      orderBy: "vesting",
    });

    const pointer = await t.run(async (ctx) => ctx.db.get(pointerId));
    expect(pointer).not.toBeNull();
    expect(pointer?.priority).toBe(15);
  });

  test("finalizePointer demotes pointer priority when the higher tier is exhausted", async () => {
    const t = initConvexTest();
    const queueId = "queue-priority-pointer-demotes";
    const now = Date.now();

    const pointerId = await t.run(async (ctx) => {
      const processedHighId = await ctx.db.insert("queueItems", {
        queueId,
        priority: 15,
        payload: { tag: "processed-high" },
        handler: "h-processed",
        handlerType: "action",
        vestingTime: now - 2_000,
        errorCount: 0,
      });
      await ctx.db.insert("queueItems", {
        queueId,
        priority: 4,
        payload: { tag: "remaining-low" },
        handler: "h-low",
        handlerType: "action",
        vestingTime: now - 1_000,
        errorCount: 0,
      });
      const pointerId = await ctx.db.insert("queuePointers", {
        queueId,
        priority: 15,
        vestingTime: now - 2_000,
        leaseId: "pointer-lease-demote",
        leaseExpiry: now + 30_000,
        lastActiveTime: now,
      });
      await ctx.db.delete(processedHighId);
      return pointerId;
    });

    await t.mutation(internal.scanner.finalizePointer, {
      pointerId,
      pointerLeaseId: "pointer-lease-demote",
      isEmpty: false,
      orderBy: "vesting",
    });

    const pointer = await t.run(async (ctx) => ctx.db.get(pointerId));
    expect(pointer).not.toBeNull();
    expect(pointer?.priority).toBe(4);
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
        priority: 0,
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
