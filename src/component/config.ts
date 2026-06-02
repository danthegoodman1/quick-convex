import { makeFunctionReference } from "convex/server"
import { v } from "convex/values"
import { mutation, query } from "./_generated/server.js"
import {
  resolvedConfigValidator,
  resolveAndMaybeUpdateConfig,
  resolveConfig,
  retryBehaviorValidator,
} from "./lib.js"

const tryWakeScannerRef = makeFunctionReference<
  "mutation",
  {
    reason?: "enqueue" | "pointerReady"
  },
  boolean
>("scanner:tryWakeScanner")

const configUpdateArgs = {
  scannerLeaseDurationMs: v.optional(v.number()),
  scannerBackoffMinMs: v.optional(v.number()),
  scannerBackoffMaxMs: v.optional(v.number()),
  pointerBatchSize: v.optional(v.number()),
  managerSlots: v.optional(v.number()),
  workersPerManager: v.optional(v.number()),
  defaultOrderBy: v.optional(v.union(v.literal("vesting"), v.literal("fifo"))),
  defaultLeaseDurationMs: v.optional(v.number()),
  minInactiveBeforeDeleteMs: v.optional(v.number()),
  retryByDefault: v.optional(v.boolean()),
  defaultRetryBehavior: v.optional(retryBehaviorValidator),
}

export const update = mutation({
  args: configUpdateArgs,
  returns: resolvedConfigValidator,
  handler: async (ctx, args) => {
    const previous = await resolveConfig(ctx)
    const config = await resolveAndMaybeUpdateConfig(ctx, args)

    if (config.managerSlots > 0 && previous.managerSlots === 0) {
      await ctx.runMutation(tryWakeScannerRef, {})
    }

    return config
  },
})

export const kick = mutation({
  args: {},
  returns: v.boolean(),
  handler: async (ctx) =>
    await ctx.runMutation(tryWakeScannerRef, {}),
})

export const get = query({
  args: {},
  returns: resolvedConfigValidator,
  handler: async (ctx) => await resolveConfig(ctx),
})
