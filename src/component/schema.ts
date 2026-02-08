import { defineSchema, defineTable } from "convex/server"
import { v } from "convex/values"

export default defineSchema({
  queueItems: defineTable({
    queueId: v.string(),
    payload: v.any(),
    handler: v.string(),
    handlerType: v.optional(v.union(v.literal("action"), v.literal("mutation"))),
    onCompleteHandler: v.optional(v.string()),
    onCompleteContext: v.optional(v.any()),
    phase: v.optional(v.union(v.literal("run"), v.literal("onComplete"))),
    completionStatus: v.optional(
      v.union(v.literal("success"), v.literal("failure"), v.literal("cancelled"))
    ),
    completionResult: v.optional(v.any()),
    onCompleteTimeoutRetries: v.optional(v.number()),
    retryEnabled: v.optional(v.boolean()),
    retryBehavior: v.optional(
      v.object({
        maxAttempts: v.number(),
        initialBackoffMs: v.number(),
        base: v.number(),
      })
    ),
    vestingTime: v.number(),
    leaseId: v.optional(v.string()),
    leaseExpiry: v.optional(v.number()),
    errorCount: v.number(),
  })
    .index("by_queue_and_vesting_time", ["queueId", "vestingTime"])
    .index("by_queue_fifo", ["queueId"]),

  queuePointers: defineTable({
    queueId: v.string(),
    vestingTime: v.number(),
    leaseId: v.optional(v.string()),
    leaseExpiry: v.optional(v.number()),
    lastActiveTime: v.number(),
  })
    .index("by_vesting", ["vestingTime"])
    .index("by_queue", ["queueId"]),

  scannerState: defineTable({
    leaseId: v.optional(v.string()),
    leaseExpiry: v.optional(v.number()),
    lastRunAt: v.number(),
    scheduledFunctionId: v.optional(v.string()),
  }),

  config: defineTable({
    scannerLeaseDurationMs: v.optional(v.number()),
    scannerBackoffMinMs: v.optional(v.number()),
    scannerBackoffMaxMs: v.optional(v.number()),
    pointerBatchSize: v.optional(v.number()),
    maxConcurrentManagers: v.optional(v.number()),
    managerBatchSize: v.optional(v.number()),
    defaultOrderBy: v.optional(v.union(v.literal("vesting"), v.literal("fifo"))),
    defaultLeaseDurationMs: v.optional(v.number()),
    minInactiveBeforeDeleteMs: v.optional(v.number()),
    retryByDefault: v.optional(v.boolean()),
    defaultRetryBehavior: v.optional(
      v.object({
        maxAttempts: v.number(),
        initialBackoffMs: v.number(),
        base: v.number(),
      })
    ),
  }),
})
