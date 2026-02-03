import { v } from "convex/values"
import { v7 as uuid } from "uuid"
import {
  internalMutation,
  internalQuery,
  mutation,
  query,
  type QueryCtx,
} from "./_generated/server.js"
import { internal } from "./_generated/api.js"
import schema from "./schema.js"

const DEFAULT_PRIORITY = 0
const DEFAULT_LEASE_DURATION_MS = 30_000
const MIN_INACTIVE_BEFORE_DELETE_MS = 60_000
const MAX_RETRIES = 10

const queueItemValidator = schema.tables.queueItems.validator.extend({
  _id: v.id("queueItems"),
  _creationTime: v.number(),
})

const queuePointerValidator = schema.tables.queuePointers.validator.extend({
  _id: v.id("queuePointers"),
  _creationTime: v.number(),
})

export const configValidator = schema.tables.config.validator

export type Config = typeof configValidator.type

export type ResolvedConfig = {
  scannerLeaseDurationMs: number
  scannerBackoffMinMs: number
  scannerBackoffMaxMs: number
  pointerBatchSize: number
  maxConcurrentManagers: number
  defaultPriority: number
  defaultLeaseDurationMs: number
  minInactiveBeforeDeleteMs: number
  maxRetries: number
}

const CONFIG_DEFAULTS: ResolvedConfig = {
  scannerLeaseDurationMs: 10_000,
  scannerBackoffMinMs: 100,
  scannerBackoffMaxMs: 5_000,
  pointerBatchSize: 50,
  maxConcurrentManagers: 10,
  defaultPriority: DEFAULT_PRIORITY,
  defaultLeaseDurationMs: DEFAULT_LEASE_DURATION_MS,
  minInactiveBeforeDeleteMs: MIN_INACTIVE_BEFORE_DELETE_MS,
  maxRetries: MAX_RETRIES,
}

export async function resolveConfig(ctx: QueryCtx): Promise<ResolvedConfig> {
  const config = await ctx.db.query("config").first()
  if (!config) {
    return CONFIG_DEFAULTS
  }
  return {
    scannerLeaseDurationMs: config.scannerLeaseDurationMs ?? CONFIG_DEFAULTS.scannerLeaseDurationMs,
    scannerBackoffMinMs: config.scannerBackoffMinMs ?? CONFIG_DEFAULTS.scannerBackoffMinMs,
    scannerBackoffMaxMs: config.scannerBackoffMaxMs ?? CONFIG_DEFAULTS.scannerBackoffMaxMs,
    pointerBatchSize: config.pointerBatchSize ?? CONFIG_DEFAULTS.pointerBatchSize,
    maxConcurrentManagers: config.maxConcurrentManagers ?? CONFIG_DEFAULTS.maxConcurrentManagers,
    defaultPriority: config.defaultPriority ?? CONFIG_DEFAULTS.defaultPriority,
    defaultLeaseDurationMs: config.defaultLeaseDurationMs ?? CONFIG_DEFAULTS.defaultLeaseDurationMs,
    minInactiveBeforeDeleteMs: config.minInactiveBeforeDeleteMs ?? CONFIG_DEFAULTS.minInactiveBeforeDeleteMs,
    maxRetries: config.maxRetries ?? CONFIG_DEFAULTS.maxRetries,
  }
}

export const getConfig = internalQuery({
  args: {},
  returns: v.object({
    scannerLeaseDurationMs: v.number(),
    scannerBackoffMinMs: v.number(),
    scannerBackoffMaxMs: v.number(),
    pointerBatchSize: v.number(),
    maxConcurrentManagers: v.number(),
    defaultPriority: v.number(),
    defaultLeaseDurationMs: v.number(),
    minInactiveBeforeDeleteMs: v.number(),
    maxRetries: v.number(),
  }),
  handler: async (ctx) => resolveConfig(ctx),
})

export const upsertConfig = internalMutation({
  args: {
    config: configValidator,
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const definedFields: Partial<Config> = {}
    for (const [key, value] of Object.entries(args.config)) {
      if (value !== undefined) {
        definedFields[key as keyof Config] = value
      }
    }

    if (Object.keys(definedFields).length === 0) {
      return null
    }

    const existing = await ctx.db.query("config").first()
    if (existing) {
      await ctx.db.patch(existing._id, definedFields)
    } else {
      await ctx.db.insert("config", definedFields)
    }

    return null
  },
})

export const updatePointerVesting = internalMutation({
  args: {
    queueId: v.string(),
    vestingTime: v.number(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const now = Date.now()

    const pointer = await ctx.db
      .query("queuePointers")
      .withIndex("by_queue", (q) => q.eq("queueId", args.queueId))
      .unique()

    if (pointer && args.vestingTime < pointer.vestingTime) {
      await ctx.db.patch(pointer._id, {
        vestingTime: args.vestingTime,
        lastActiveTime: now,
      })
    }

    return null
  },
})

export const enqueue = mutation({
  args: {
    queueId: v.string(),
    payload: v.any(),
    handler: v.string(),
    priority: v.optional(v.number()),
    delayMs: v.optional(v.number()),
    config: v.optional(configValidator),
  },
  returns: v.id("queueItems"),
  handler: async (ctx, args) => {
    if (args.config) {
      const definedFields: Partial<Config> = {}
      for (const [key, value] of Object.entries(args.config)) {
        if (value !== undefined) {
          definedFields[key as keyof Config] = value
        }
      }

      if (Object.keys(definedFields).length > 0) {
        const existing = await ctx.db.query("config").first()
        if (existing) {
          await ctx.db.patch(existing._id, definedFields)
        } else {
          await ctx.db.insert("config", definedFields)
        }
      }
    }

    const config = await resolveConfig(ctx)
    const now = Date.now()
    const vestingTime = now + (args.delayMs ?? 0)

    const itemId = await ctx.db.insert("queueItems", {
      queueId: args.queueId,
      payload: args.payload,
      handler: args.handler,
      priority: args.priority ?? config.defaultPriority,
      vestingTime,
      errorCount: 0,
    })

    const existingPointer = await ctx.db
      .query("queuePointers")
      .withIndex("by_queue", (q) => q.eq("queueId", args.queueId))
      .unique()

    if (existingPointer) {
      await ctx.scheduler.runAfter(0, internal.lib.updatePointerVesting, {
        queueId: args.queueId,
        vestingTime,
      })
    } else {
      await ctx.db.insert("queuePointers", {
        queueId: args.queueId,
        vestingTime,
        lastActiveTime: now,
      })
    }

    if (vestingTime <= now) {
      await ctx.scheduler.runAfter(0, internal.scanner.tryWakeScanner, {})
    }

    return itemId
  },
})

export const enqueueBatch = mutation({
  args: {
    items: v.array(
      v.object({
        queueId: v.string(),
        payload: v.any(),
        handler: v.string(),
        priority: v.optional(v.number()),
        delayMs: v.optional(v.number()),
      })
    ),
    config: v.optional(configValidator),
  },
  returns: v.array(v.id("queueItems")),
  handler: async (ctx, args) => {
    if (args.config) {
      const definedFields: Partial<Config> = {}
      for (const [key, value] of Object.entries(args.config)) {
        if (value !== undefined) {
          definedFields[key as keyof Config] = value
        }
      }

      if (Object.keys(definedFields).length > 0) {
        const existing = await ctx.db.query("config").first()
        if (existing) {
          await ctx.db.patch(existing._id, definedFields)
        } else {
          await ctx.db.insert("config", definedFields)
        }
      }
    }

    const config = await resolveConfig(ctx)
    const now = Date.now()
    const itemIds: Array<typeof schema.tables.queueItems.validator.type & { _id: string }> = []
    const pointerUpdates = new Map<
      string,
      { vestingTime: number; lastActiveTime: number }
    >()

    for (const item of args.items) {
      const vestingTime = now + (item.delayMs ?? 0)

      const itemId = await ctx.db.insert("queueItems", {
        queueId: item.queueId,
        payload: item.payload,
        handler: item.handler,
        priority: item.priority ?? config.defaultPriority,
        vestingTime,
        errorCount: 0,
      })

      itemIds.push(itemId as any)

      const existing = pointerUpdates.get(item.queueId)
      if (!existing || vestingTime < existing.vestingTime) {
        pointerUpdates.set(item.queueId, { vestingTime, lastActiveTime: now })
      }
    }

    for (const [queueId, update] of pointerUpdates) {
      const existingPointer = await ctx.db
        .query("queuePointers")
        .withIndex("by_queue", (q) => q.eq("queueId", queueId))
        .unique()

      if (existingPointer) {
        await ctx.scheduler.runAfter(0, internal.lib.updatePointerVesting, {
          queueId,
          vestingTime: update.vestingTime,
        })
      } else {
        await ctx.db.insert("queuePointers", {
          queueId,
          vestingTime: update.vestingTime,
          lastActiveTime: update.lastActiveTime,
        })
      }
    }

    const hasImmediateWork = Array.from(pointerUpdates.values()).some(
      (update) => update.vestingTime <= now
    )
    if (hasImmediateWork) {
      await ctx.scheduler.runAfter(0, internal.scanner.tryWakeScanner, {})
    }

    return itemIds as any
  },
})

export const peekPointers = internalQuery({
  args: {
    limit: v.optional(v.number()),
  },
  returns: v.array(queuePointerValidator),
  handler: async (ctx, args) => {
    const now = Date.now()
    const limit = args.limit ?? 100

    const pointers = await ctx.db
      .query("queuePointers")
      .withIndex("by_vesting", (q) => q.lte("vestingTime", now))
      .take(limit)

    return pointers.filter((p) => !p.leaseExpiry || p.leaseExpiry <= now)
  },
})

export const obtainPointerLease = internalMutation({
  args: {
    pointerId: v.id("queuePointers"),
    leaseDurationMs: v.optional(v.number()),
  },
  returns: v.union(v.null(), v.string()),
  handler: async (ctx, args) => {
    const now = Date.now()
    const pointer = await ctx.db.get(args.pointerId)

    if (!pointer) {
      return null
    }

    if (pointer.leaseExpiry && pointer.leaseExpiry > now) {
      return null
    }

    const config = await resolveConfig(ctx)
    const leaseId = uuid()
    const leaseExpiry = now + (args.leaseDurationMs ?? config.defaultLeaseDurationMs)

    await ctx.db.patch(args.pointerId, {
      leaseId,
      leaseExpiry,
      vestingTime: leaseExpiry,
    })

    return leaseId
  },
})

export const peekItems = internalQuery({
  args: {
    queueId: v.string(),
    limit: v.optional(v.number()),
    orderBy: v.optional(v.union(v.literal("priority"), v.literal("fifo"))),
  },
  returns: v.array(queueItemValidator),
  handler: async (ctx, args) => {
    const now = Date.now()
    const limit = args.limit ?? 10
    const orderBy = args.orderBy ?? "priority"

    const indexName =
      orderBy === "fifo" ? "by_queue_fifo" : "by_queue_priority_vesting"

    const items = await ctx.db
      .query("queueItems")
      .withIndex(indexName, (q) => q.eq("queueId", args.queueId))
      .take(limit * 2)

    return items
      .filter((item) => {
        if (item.vestingTime > now) return false
        if (item.leaseExpiry && item.leaseExpiry > now) return false
        return true
      })
      .slice(0, limit)
  },
})

export const obtainItemLease = internalMutation({
  args: {
    itemId: v.id("queueItems"),
    leaseDurationMs: v.optional(v.number()),
  },
  returns: v.union(v.null(), v.string()),
  handler: async (ctx, args) => {
    const now = Date.now()
    const item = await ctx.db.get(args.itemId)

    if (!item) {
      return null
    }

    if (item.leaseExpiry && item.leaseExpiry > now) {
      return null
    }

    const config = await resolveConfig(ctx)
    const leaseId = uuid()
    const leaseExpiry = now + (args.leaseDurationMs ?? config.defaultLeaseDurationMs)

    await ctx.db.patch(args.itemId, {
      leaseId,
      leaseExpiry,
      vestingTime: leaseExpiry,
    })

    return leaseId
  },
})

export const dequeue = internalMutation({
  args: {
    queueId: v.string(),
    limit: v.optional(v.number()),
    leaseDurationMs: v.optional(v.number()),
    orderBy: v.optional(v.union(v.literal("priority"), v.literal("fifo"))),
  },
  returns: v.array(
    v.object({
      item: queueItemValidator,
      leaseId: v.string(),
    })
  ),
  handler: async (ctx, args) => {
    const config = await resolveConfig(ctx)
    const now = Date.now()
    const limit = args.limit ?? 10
    const leaseDurationMs = args.leaseDurationMs ?? config.defaultLeaseDurationMs
    const orderBy = args.orderBy ?? "priority"

    const indexName =
      orderBy === "fifo" ? "by_queue_fifo" : "by_queue_priority_vesting"

    const items = await ctx.db
      .query("queueItems")
      .withIndex(indexName, (q) => q.eq("queueId", args.queueId))
      .take(limit * 2)

    const availableItems = items.filter((item) => {
      if (item.vestingTime > now) return false
      if (item.leaseExpiry && item.leaseExpiry > now) return false
      return true
    })

    const result: Array<{
      item: typeof queueItemValidator.type
      leaseId: string
    }> = []

    for (const item of availableItems.slice(0, limit)) {
      const leaseId = uuid()
      const leaseExpiry = now + leaseDurationMs

      await ctx.db.patch(item._id, {
        leaseId,
        leaseExpiry,
        vestingTime: leaseExpiry,
      })

      result.push({
        item: { ...item, leaseId, leaseExpiry, vestingTime: leaseExpiry },
        leaseId,
      })
    }

    return result
  },
})

export const complete = internalMutation({
  args: {
    itemId: v.id("queueItems"),
    leaseId: v.optional(v.string()),
  },
  returns: v.boolean(),
  handler: async (ctx, args) => {
    const item = await ctx.db.get(args.itemId)

    if (!item) {
      return false
    }

    if (args.leaseId && item.leaseId !== args.leaseId) {
      return false
    }

    await ctx.db.delete(args.itemId)

    const remainingItems = await ctx.db
      .query("queueItems")
      .withIndex("by_queue_priority_vesting", (q) =>
        q.eq("queueId", item.queueId)
      )
      .first()

    if (!remainingItems) {
      const pointer = await ctx.db
        .query("queuePointers")
        .withIndex("by_queue", (q) => q.eq("queueId", item.queueId))
        .unique()

      if (pointer) {
        await ctx.db.patch(pointer._id, {
          leaseId: undefined,
          leaseExpiry: undefined,
        })
      }
    }

    return true
  },
})

export const requeue = internalMutation({
  args: {
    itemId: v.id("queueItems"),
    leaseId: v.optional(v.string()),
    delayMs: v.optional(v.number()),
    error: v.optional(v.string()),
  },
  returns: v.boolean(),
  handler: async (ctx, args) => {
    const config = await resolveConfig(ctx)
    const now = Date.now()
    const item = await ctx.db.get(args.itemId)

    if (!item) {
      return false
    }

    if (args.leaseId && item.leaseId !== args.leaseId) {
      return false
    }

    const newErrorCount = item.errorCount + 1

    if (newErrorCount > config.maxRetries) {
      await ctx.db.insert("deadLetterItems", {
        queueId: item.queueId,
        payload: item.payload,
        handler: item.handler,
        errorCount: newErrorCount,
        lastError: args.error,
        movedAt: now,
      })
      await ctx.db.delete(args.itemId)
      return true
    }

    const backoffMs = args.delayMs ?? Math.min(1000 * Math.pow(2, newErrorCount), 300_000)
    const vestingTime = now + backoffMs

    await ctx.db.patch(args.itemId, {
      vestingTime,
      errorCount: newErrorCount,
      leaseId: undefined,
      leaseExpiry: undefined,
    })

    const pointer = await ctx.db
      .query("queuePointers")
      .withIndex("by_queue", (q) => q.eq("queueId", item.queueId))
      .unique()

    if (pointer && vestingTime < pointer.vestingTime) {
      await ctx.db.patch(pointer._id, {
        vestingTime,
        lastActiveTime: now,
      })
    }

    return true
  },
})

export const releasePointerLease = internalMutation({
  args: {
    pointerId: v.id("queuePointers"),
    leaseId: v.string(),
    nextVestingTime: v.optional(v.number()),
  },
  returns: v.boolean(),
  handler: async (ctx, args) => {
    const now = Date.now()
    const pointer = await ctx.db.get(args.pointerId)

    if (!pointer) {
      return false
    }

    if (pointer.leaseId !== args.leaseId) {
      return false
    }

    await ctx.db.patch(args.pointerId, {
      leaseId: undefined,
      leaseExpiry: undefined,
      vestingTime: args.nextVestingTime ?? now,
      lastActiveTime: now,
    })

    return true
  },
})

export const garbageCollectPointers = internalMutation({
  args: {
    limit: v.optional(v.number()),
  },
  returns: v.number(),
  handler: async (ctx, args) => {
    const config = await resolveConfig(ctx)
    const now = Date.now()
    const limit = args.limit ?? 1000

    const pointers = await ctx.db
      .query("queuePointers")
      .withIndex("by_vesting", (q) => q.lte("vestingTime", now))
      .take(limit)

    let deleted = 0

    for (const pointer of pointers) {
      if (pointer.leaseExpiry && pointer.leaseExpiry > now) {
        continue
      }

      if (now - pointer.lastActiveTime < config.minInactiveBeforeDeleteMs) {
        continue
      }

      const hasItems = await ctx.db
        .query("queueItems")
        .withIndex("by_queue_priority_vesting", (q) =>
          q.eq("queueId", pointer.queueId)
        )
        .first()

      if (!hasItems) {
        await ctx.db.delete(pointer._id)
        deleted++
      }
    }

    return deleted
  },
})

export const getQueueStats = query({
  args: {
    queueId: v.string(),
  },
  returns: v.object({
    itemCount: v.number(),
    pendingCount: v.number(),
    leasedCount: v.number(),
    deadLetterCount: v.number(),
  }),
  handler: async (ctx, args) => {
    const now = Date.now()

    const items = await ctx.db
      .query("queueItems")
      .withIndex("by_queue_priority_vesting", (q) =>
        q.eq("queueId", args.queueId)
      )
      .collect()

    const deadLetters = await ctx.db
      .query("deadLetterItems")
      .withIndex("by_queue", (q) => q.eq("queueId", args.queueId))
      .collect()

    const pendingCount = items.filter(
      (item) =>
        item.vestingTime <= now && (!item.leaseExpiry || item.leaseExpiry <= now)
    ).length

    const leasedCount = items.filter(
      (item) => item.leaseExpiry && item.leaseExpiry > now
    ).length

    return {
      itemCount: items.length,
      pendingCount,
      leasedCount,
      deadLetterCount: deadLetters.length,
    }
  },
})

export const listDeadLetters = query({
  args: {
    queueId: v.optional(v.string()),
    limit: v.optional(v.number()),
  },
  returns: v.array(
    schema.tables.deadLetterItems.validator.extend({
      _id: v.id("deadLetterItems"),
      _creationTime: v.number(),
    })
  ),
  handler: async (ctx, args) => {
    const limit = args.limit ?? 100
    const { queueId } = args

    if (queueId) {
      return await ctx.db
        .query("deadLetterItems")
        .withIndex("by_queue", (q) => q.eq("queueId", queueId))
        .take(limit)
    }

    return await ctx.db.query("deadLetterItems").take(limit)
  },
})

export const replayDeadLetter = mutation({
  args: {
    deadLetterId: v.id("deadLetterItems"),
    priority: v.optional(v.number()),
    delayMs: v.optional(v.number()),
  },
  returns: v.union(v.null(), v.id("queueItems")),
  handler: async (ctx, args) => {
    const config = await resolveConfig(ctx)
    const deadLetter = await ctx.db.get(args.deadLetterId)

    if (!deadLetter) {
      return null
    }

    const now = Date.now()
    const vestingTime = now + (args.delayMs ?? 0)

    const itemId = await ctx.db.insert("queueItems", {
      queueId: deadLetter.queueId,
      payload: deadLetter.payload,
      handler: deadLetter.handler,
      priority: args.priority ?? config.defaultPriority,
      vestingTime,
      errorCount: 0,
    })

    const existingPointer = await ctx.db
      .query("queuePointers")
      .withIndex("by_queue", (q) => q.eq("queueId", deadLetter.queueId))
      .unique()

    if (existingPointer) {
      if (vestingTime < existingPointer.vestingTime) {
        await ctx.db.patch(existingPointer._id, {
          vestingTime,
          lastActiveTime: now,
        })
      } else {
        await ctx.db.patch(existingPointer._id, {
          lastActiveTime: now,
        })
      }
    } else {
      await ctx.db.insert("queuePointers", {
        queueId: deadLetter.queueId,
        vestingTime,
        lastActiveTime: now,
      })
    }

    await ctx.db.delete(args.deadLetterId)

    return itemId
  },
})
