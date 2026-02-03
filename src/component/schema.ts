import { defineSchema, defineTable } from "convex/server"
import { v } from "convex/values"

export default defineSchema({
  queueItems: defineTable({
    queueId: v.string(),
    payload: v.any(),
    itemType: v.string(),
    handler: v.string(),
    priority: v.number(),
    vestingTime: v.number(),
    leaseId: v.optional(v.string()),
    leaseExpiry: v.optional(v.number()),
    errorCount: v.number(),
  })
    .index("by_queue_priority_vesting", ["queueId", "priority", "vestingTime"])
    .index("by_queue_fifo", ["queueId", "priority"]),

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
    itemType: v.string(),
    handler: v.string(),
    errorCount: v.number(),
    lastError: v.optional(v.string()),
    movedAt: v.number(),
  })
    .index("by_queue", ["queueId"])
    .index("by_type", ["itemType"]),

  scannerState: defineTable({
    partition: v.string(),
    leaseId: v.optional(v.string()),
    leaseExpiry: v.optional(v.number()),
    lastRunAt: v.number(),
    scheduledFunctionId: v.optional(v.string()),
  }).index("by_partition", ["partition"]),
})
