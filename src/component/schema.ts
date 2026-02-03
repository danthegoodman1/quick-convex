import { defineSchema, defineTable } from "convex/server"
import { v } from "convex/values"

export default defineSchema({
  queueItems: defineTable({
    queueId: v.string(),
    payload: v.any(),
    handler: v.string(),
    priority: v.number(),
    vestingTime: v.number(),
    leaseId: v.optional(v.string()),
    leaseExpiry: v.optional(v.number()),
    errorCount: v.number(),
  })
    .index("by_queue_priority_vesting", ["queueId", "priority", "vestingTime"])
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

  deadLetterItems: defineTable({
    queueId: v.string(),
    payload: v.any(),
    handler: v.string(),
    errorCount: v.number(),
    lastError: v.optional(v.string()),
    movedAt: v.number(),
  }).index("by_queue", ["queueId"]),

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
    defaultPriority: v.optional(v.number()),
    defaultLeaseDurationMs: v.optional(v.number()),
    minInactiveBeforeDeleteMs: v.optional(v.number()),
    maxRetries: v.optional(v.number()),
  }),
})
