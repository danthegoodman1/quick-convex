import { Quick } from "@danthegoodman/quick-convex";
import { action, mutation, query } from "./_generated/server.js";
import { components } from "./_generated/api.js";
import { makeFunctionReference } from "convex/server";
import { v } from "convex/values";

const quickVesting = new Quick(components.quickVesting, {
  defaultOrderBy: "vesting",
});
const quickFifo = new Quick(components.quickFifo, {
  defaultOrderBy: "fifo",
});

const processCommentActionRef = makeFunctionReference<
  "action",
  { payload: { text: string; targetId: string }; queueId: string }
>("example:processCommentAction");

const processCommentMutationRef = makeFunctionReference<
  "mutation",
  { payload: { text: string; targetId: string }; queueId: string }
>("example:processCommentMutation");

export const processCommentAction = action({
  args: {
    payload: v.object({
      text: v.string(),
      targetId: v.string(),
    }),
    queueId: v.string(),
  },
  returns: v.null(),
  handler: async () => null,
});

export const processCommentMutation = mutation({
  args: {
    payload: v.object({
      text: v.string(),
      targetId: v.string(),
    }),
    queueId: v.string(),
  },
  returns: v.null(),
  handler: async () => null,
});

export const enqueueCommentAction = mutation({
  args: { text: v.string(), targetId: v.string(), delayMs: v.optional(v.number()) },
  returns: v.string(),
  handler: async (ctx, args) => {
    return await quickVesting.enqueueAction(ctx, {
      queueId: args.targetId,
      fn: processCommentActionRef,
      args: { text: args.text, targetId: args.targetId },
      delayMs: args.delayMs,
    });
  },
});

export const enqueueCommentMutation = mutation({
  args: { text: v.string(), targetId: v.string(), delayMs: v.optional(v.number()) },
  returns: v.string(),
  handler: async (ctx, args) => {
    return await quickVesting.enqueueMutation(ctx, {
      queueId: args.targetId,
      fn: processCommentMutationRef,
      args: { text: args.text, targetId: args.targetId },
      delayMs: args.delayMs,
    });
  },
});

export const enqueueCommentBatchActionVesting = mutation({
  args: { targetId: v.string() },
  returns: v.array(v.string()),
  handler: async (ctx, args) => {
    return await quickVesting.enqueueBatchAction(ctx, [
      {
        queueId: args.targetId,
        fn: processCommentActionRef,
        args: { text: "first", targetId: args.targetId },
      },
      {
        queueId: args.targetId,
        fn: processCommentActionRef,
        args: { text: "second", targetId: args.targetId },
      },
    ]);
  },
});

export const enqueueCommentBatchActionFifo = mutation({
  args: { targetId: v.string() },
  returns: v.array(v.string()),
  handler: async (ctx, args) => {
    return await quickFifo.enqueueBatchAction(ctx, [
      {
        queueId: args.targetId,
        fn: processCommentActionRef,
        args: { text: "first", targetId: args.targetId },
      },
      {
        queueId: args.targetId,
        fn: processCommentActionRef,
        args: { text: "second", targetId: args.targetId },
      },
    ]);
  },
});

export const queueStats = query({
  args: {
    targetId: v.string(),
    mode: v.optional(v.union(v.literal("vesting"), v.literal("fifo"))),
  },
  handler: async (ctx, args) => {
    const component =
      args.mode === "fifo" ? components.quickFifo : components.quickVesting;

    return await ctx.runQuery(component.lib.getQueueStats, {
      queueId: args.targetId,
    });
  },
});
