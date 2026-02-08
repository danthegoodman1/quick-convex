import { v } from "convex/values"
import { v7 as uuid } from "uuid"
import {
  internalMutation,
  internalQuery,
  mutation,
  query,
  type QueryCtx,
  type MutationCtx,
} from "./_generated/server.js"
import { internal } from "./_generated/api.js"
import schema from "./schema.js"

export type QueueOrder = "vesting" | "fifo"
export type HandlerType = "action" | "mutation"
export type CompletionStatus = "success" | "failure" | "cancelled"

const DEFAULT_LEASE_DURATION_MS = 30_000
const MIN_INACTIVE_BEFORE_DELETE_MS = 60_000
const DEFAULT_ORDER_BY: QueueOrder = "vesting"
const DEFAULT_RETRY_BY_DEFAULT = false
const DEFAULT_ON_COMPLETE_TIMEOUT_RETRIES = 2

export const retryBehaviorValidator = v.object({
  maxAttempts: v.number(),
  initialBackoffMs: v.number(),
  base: v.number(),
})
export type RetryBehavior = typeof retryBehaviorValidator.type

const DEFAULT_RETRY_BEHAVIOR: RetryBehavior = {
  maxAttempts: 5,
  initialBackoffMs: 250,
  base: 2,
}

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

const resolvedConfigValidator = v.object({
  scannerLeaseDurationMs: v.number(),
  scannerBackoffMinMs: v.number(),
  scannerBackoffMaxMs: v.number(),
  pointerBatchSize: v.number(),
  maxConcurrentManagers: v.number(),
  defaultOrderBy: v.union(v.literal("vesting"), v.literal("fifo")),
  defaultLeaseDurationMs: v.number(),
  minInactiveBeforeDeleteMs: v.number(),
  retryByDefault: v.boolean(),
  defaultRetryBehavior: retryBehaviorValidator,
})

export type ResolvedConfig = {
  scannerLeaseDurationMs: number
  scannerBackoffMinMs: number
  scannerBackoffMaxMs: number
  pointerBatchSize: number
  maxConcurrentManagers: number
  defaultOrderBy: QueueOrder
  defaultLeaseDurationMs: number
  minInactiveBeforeDeleteMs: number
  retryByDefault: boolean
  defaultRetryBehavior: RetryBehavior
}

const CONFIG_DEFAULTS: ResolvedConfig = {
  scannerLeaseDurationMs: 10_000,
  scannerBackoffMinMs: 100,
  scannerBackoffMaxMs: 5_000,
  pointerBatchSize: 50,
  maxConcurrentManagers: 10,
  defaultOrderBy: DEFAULT_ORDER_BY,
  defaultLeaseDurationMs: DEFAULT_LEASE_DURATION_MS,
  minInactiveBeforeDeleteMs: MIN_INACTIVE_BEFORE_DELETE_MS,
  retryByDefault: DEFAULT_RETRY_BY_DEFAULT,
  defaultRetryBehavior: DEFAULT_RETRY_BEHAVIOR,
}

function applyConfigDefaults(partial: Partial<Config> | null | undefined): ResolvedConfig {
  if (!partial) return CONFIG_DEFAULTS
  return {
    scannerLeaseDurationMs: partial.scannerLeaseDurationMs ?? CONFIG_DEFAULTS.scannerLeaseDurationMs,
    scannerBackoffMinMs: partial.scannerBackoffMinMs ?? CONFIG_DEFAULTS.scannerBackoffMinMs,
    scannerBackoffMaxMs: partial.scannerBackoffMaxMs ?? CONFIG_DEFAULTS.scannerBackoffMaxMs,
    pointerBatchSize: partial.pointerBatchSize ?? CONFIG_DEFAULTS.pointerBatchSize,
    maxConcurrentManagers: partial.maxConcurrentManagers ?? CONFIG_DEFAULTS.maxConcurrentManagers,
    defaultOrderBy: partial.defaultOrderBy ?? CONFIG_DEFAULTS.defaultOrderBy,
    defaultLeaseDurationMs: partial.defaultLeaseDurationMs ?? CONFIG_DEFAULTS.defaultLeaseDurationMs,
    minInactiveBeforeDeleteMs: partial.minInactiveBeforeDeleteMs ?? CONFIG_DEFAULTS.minInactiveBeforeDeleteMs,
    retryByDefault: partial.retryByDefault ?? CONFIG_DEFAULTS.retryByDefault,
    defaultRetryBehavior: partial.defaultRetryBehavior ?? CONFIG_DEFAULTS.defaultRetryBehavior,
  }
}

export async function resolveConfig(ctx: QueryCtx): Promise<ResolvedConfig> {
  const config = await ctx.db.query("config").first()
  return applyConfigDefaults(config)
}

function resolveVestingTime(
  now: number,
  args: {
    runAfter?: number
    runAt?: number
  }
): number {
  if (args.runAfter !== undefined && args.runAt !== undefined) {
    throw new Error("Specify only one of runAfter or runAt")
  }

  if (args.runAt !== undefined) {
    return args.runAt
  }

  if (args.runAfter !== undefined) {
    return now + args.runAfter
  }

  return now
}

export function resolveItemRetryPolicy(
  config: ResolvedConfig,
  args: {
    retry?: boolean
    retryBehavior?: RetryBehavior
  }
): {
  retryEnabled: boolean
  retryBehavior?: RetryBehavior
} {
  const retryEnabled =
    args.retry ?? (args.retryBehavior ? true : config.retryByDefault)
  if (!retryEnabled) {
    return { retryEnabled: false }
  }
  return {
    retryEnabled: true,
    retryBehavior: args.retryBehavior ?? config.defaultRetryBehavior,
  }
}

export function getRetryDelayMs(
  retryBehavior: RetryBehavior,
  attemptNumber: number
): number {
  const exponent = Math.max(0, attemptNumber - 1)
  return retryBehavior.initialBackoffMs * Math.pow(retryBehavior.base, exponent)
}

export function isTimeoutError(error: unknown): boolean {
  const messages: string[] = []

  if (typeof error === "string") {
    messages.push(error)
  }

  if (error && typeof error === "object") {
    const candidate = error as {
      message?: unknown
      cause?: unknown
      data?: unknown
    }

    if (typeof candidate.message === "string") {
      messages.push(candidate.message)
    }
    if (typeof candidate.cause === "string") {
      messages.push(candidate.cause)
    }
    if (candidate.data && typeof candidate.data === "object") {
      const data = candidate.data as { message?: unknown }
      if (typeof data.message === "string") {
        messages.push(data.message)
      }
    } else if (typeof candidate.data === "string") {
      messages.push(candidate.data)
    }
  }

  messages.push(String(error))
  return messages.some((message) => /timed out|timeout/i.test(message))
}

export const getResolvedConfig = internalQuery({
  args: {},
  returns: resolvedConfigValidator,
  handler: async (ctx) => resolveConfig(ctx),
})

async function resolveAndMaybeUpdateConfig(
  ctx: MutationCtx,
  updates: Config | undefined
): Promise<ResolvedConfig> {
  const existing = await ctx.db.query("config").first()

  if (!updates) {
    return applyConfigDefaults(existing)
  }

  const sanitizedUpdates = Object.fromEntries(
    Object.entries(updates).filter(([, value]) => value !== undefined)
  ) as Partial<Config>

  const fieldsToUpdate = Object.fromEntries(
    (Object.entries(sanitizedUpdates) as Array<[keyof Config, Config[keyof Config]]>).filter(
      ([key, value]) => existing?.[key] !== value
    )
  ) as Partial<Config>

  if (Object.keys(fieldsToUpdate).length > 0) {
    if (existing) {
      await ctx.db.patch(existing._id, fieldsToUpdate)
    } else {
      await ctx.db.insert("config", fieldsToUpdate)
    }
  }

  const merged = existing ? { ...existing, ...fieldsToUpdate } : fieldsToUpdate
  return applyConfigDefaults(merged)
}

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
      await ctx.scheduler.runAfter(0, internal.scanner.tryWakeScanner, {})
    }

    return null
  },
})

export const enqueue = mutation({
  args: {
    queueId: v.string(),
    payload: v.any(),
    handler: v.string(),
    handlerType: v.optional(v.union(v.literal("action"), v.literal("mutation"))),
    onCompleteHandler: v.optional(v.string()),
    onCompleteContext: v.optional(v.any()),
    retry: v.optional(v.boolean()),
    retryBehavior: v.optional(retryBehaviorValidator),
    runAfter: v.optional(v.number()),
    runAt: v.optional(v.number()),
    config: v.optional(configValidator),
  },
  returns: v.id("queueItems"),
  handler: async (ctx, args) => {
    if (args.config) {
      await resolveAndMaybeUpdateConfig(ctx, args.config)
    }
    const resolvedConfig = await resolveConfig(ctx)
    const now = Date.now()
    const vestingTime = resolveVestingTime(now, args)
    const retryPolicy = resolveItemRetryPolicy(resolvedConfig, {
      retry: args.retry,
      retryBehavior: args.retryBehavior,
    })

    const itemId = await ctx.db.insert("queueItems", {
      queueId: args.queueId,
      payload: args.payload,
      handler: args.handler,
      handlerType: args.handlerType ?? "action",
      onCompleteHandler: args.onCompleteHandler,
      onCompleteContext: args.onCompleteContext,
      phase: "run",
      retryEnabled: retryPolicy.retryEnabled,
      retryBehavior: retryPolicy.retryBehavior,
      vestingTime,
      errorCount: 0,
    })

    const existingPointer = await ctx.db
      .query("queuePointers")
      .withIndex("by_queue", (q) => q.eq("queueId", args.queueId))
      .unique()

    if (existingPointer) {
      if (vestingTime < existingPointer.vestingTime) {
        await ctx.scheduler.runAfter(0, internal.lib.updatePointerVesting, {
          queueId: args.queueId,
          vestingTime,
        })
      }
    } else {
      await ctx.db.insert("queuePointers", {
        queueId: args.queueId,
        vestingTime,
        lastActiveTime: now,
      })
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
        handlerType: v.optional(v.union(v.literal("action"), v.literal("mutation"))),
        onCompleteHandler: v.optional(v.string()),
        onCompleteContext: v.optional(v.any()),
        retry: v.optional(v.boolean()),
        retryBehavior: v.optional(retryBehaviorValidator),
        runAfter: v.optional(v.number()),
        runAt: v.optional(v.number()),
      })
    ),
    config: v.optional(configValidator),
  },
  returns: v.array(v.id("queueItems")),
  handler: async (ctx, args) => {
    if (args.config) {
      await resolveAndMaybeUpdateConfig(ctx, args.config)
    }
    const resolvedConfig = await resolveConfig(ctx)
    const now = Date.now()
    const itemIds: Array<typeof schema.tables.queueItems.validator.type & { _id: string }> = []
    const pointerUpdates = new Map<
      string,
      { vestingTime: number; lastActiveTime: number }
    >()

    for (const item of args.items) {
      const vestingTime = resolveVestingTime(now, item)
      const retryPolicy = resolveItemRetryPolicy(resolvedConfig, {
        retry: item.retry,
        retryBehavior: item.retryBehavior,
      })

      const itemId = await ctx.db.insert("queueItems", {
        queueId: item.queueId,
        payload: item.payload,
        handler: item.handler,
        handlerType: item.handlerType ?? "action",
        onCompleteHandler: item.onCompleteHandler,
        onCompleteContext: item.onCompleteContext,
        phase: "run",
        retryEnabled: retryPolicy.retryEnabled,
        retryBehavior: retryPolicy.retryBehavior,
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
        if (update.vestingTime < existingPointer.vestingTime) {
          await ctx.scheduler.runAfter(0, internal.lib.updatePointerVesting, {
            queueId,
            vestingTime: update.vestingTime,
          })
        }
      } else {
        await ctx.db.insert("queuePointers", {
          queueId,
          vestingTime: update.vestingTime,
          lastActiveTime: update.lastActiveTime,
        })
        await ctx.scheduler.runAfter(0, internal.scanner.tryWakeScanner, {})
      }
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

async function collectAvailableItems(
  db: QueryCtx["db"],
  args: {
    queueId: string
    limit: number
    orderBy: QueueOrder
    now: number
  }
): Promise<Array<typeof queueItemValidator.type>> {
  const query =
    args.orderBy === "fifo"
      ? db
          .query("queueItems")
          .withIndex("by_queue_fifo", (q) => q.eq("queueId", args.queueId))
      : db
          .query("queueItems")
          .withIndex("by_queue_and_vesting_time", (q) =>
            q.eq("queueId", args.queueId)
          )

  const availableItems: Array<typeof queueItemValidator.type> = []
  const maxScan = 1000
  let scanned = 0

  for await (const item of query) {
    scanned++

    if (scanned > maxScan) {
      break
    }

    if (item.vestingTime > args.now) {
      // For vesting order, all later rows are also in the future.
      // For FIFO, strict head-of-line semantics block on the first future row.
      break
    }

    if (item.leaseExpiry && item.leaseExpiry > args.now) {
      if (args.orderBy === "fifo") {
        // Strict FIFO: do not skip a leased head item.
        break
      }
      continue
    }

    availableItems.push(item)

    if (availableItems.length >= args.limit) {
      break
    }
  }

  return availableItems
}

export const peekItems = internalQuery({
  args: {
    queueId: v.string(),
    limit: v.optional(v.number()),
    orderBy: v.optional(v.union(v.literal("vesting"), v.literal("fifo"))),
  },
  returns: v.array(queueItemValidator),
  handler: async (ctx, args) => {
    const config = await resolveConfig(ctx)
    const now = Date.now()
    const limit = args.limit ?? 100
    const orderBy = (args.orderBy ?? config.defaultOrderBy) as QueueOrder

    return await collectAvailableItems(ctx.db, {
      queueId: args.queueId,
      limit,
      orderBy,
      now,
    })
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

export const extendItemLease = internalMutation({
  args: {
    itemId: v.id("queueItems"),
    leaseId: v.string(),
    leaseDurationMs: v.optional(v.number()),
  },
  returns: v.boolean(),
  handler: async (ctx, args) => {
    const now = Date.now()
    const item = await ctx.db.get(args.itemId)

    if (!item) {
      return false
    }

    if (item.leaseId !== args.leaseId) {
      return false
    }

    const config = await resolveConfig(ctx)
    const leaseExpiry = now + (args.leaseDurationMs ?? config.defaultLeaseDurationMs)

    await ctx.db.patch(args.itemId, {
      leaseExpiry,
      vestingTime: leaseExpiry,
    })

    return true
  },
})

export const dequeue = internalMutation({
  args: {
    queueId: v.string(),
    limit: v.optional(v.number()),
    leaseDurationMs: v.optional(v.number()),
    orderBy: v.optional(v.union(v.literal("vesting"), v.literal("fifo"))),
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
    const orderBy = (args.orderBy ?? config.defaultOrderBy) as QueueOrder

    const availableItems = await collectAvailableItems(ctx.db, {
      queueId: args.queueId,
      limit,
      orderBy,
      now,
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

async function schedulePointerWakeIfEarlier(
  ctx: MutationCtx,
  queueId: string,
  vestingTime: number
) {
  const pointer = await ctx.db
    .query("queuePointers")
    .withIndex("by_queue", (q) => q.eq("queueId", queueId))
    .unique()
  if (pointer && vestingTime < pointer.vestingTime) {
    await ctx.scheduler.runAfter(0, internal.lib.updatePointerVesting, {
      queueId,
      vestingTime,
    })
  }
}

export const prepareActionWorkerExecution = internalMutation({
  args: {
    itemId: v.id("queueItems"),
    leaseId: v.string(),
  },
  returns: v.object({
    valid: v.boolean(),
    phase: v.union(v.literal("run"), v.literal("onComplete")),
  }),
  handler: async (ctx, args) => {
    const item = await ctx.db.get(args.itemId)
    if (!item || item.leaseId !== args.leaseId) {
      return {
        valid: false,
        phase: "run" as const,
      }
    }
    return {
      valid: true,
      phase: (item.phase ?? "run") as "run" | "onComplete",
    }
  },
})

async function runOnCompleteForItem(
  ctx: MutationCtx,
  item: typeof queueItemValidator.type,
  _leaseId: string
): Promise<{
  done: boolean
  retriedOnComplete: boolean
}> {
  if (!item.onCompleteHandler) {
    await ctx.db.delete(item._id)
    return { done: true, retriedOnComplete: false }
  }

  try {
    const fnHandle = item.onCompleteHandler as any
    await ctx.runMutation(fnHandle, {
      workId: item._id,
      context: item.onCompleteContext,
      status: item.completionStatus ?? "failure",
      result: item.completionResult,
    })
    await ctx.db.delete(item._id)
    return { done: true, retriedOnComplete: false }
  } catch (error) {
    const timeoutRetries = item.onCompleteTimeoutRetries ?? 0
    if (
      isTimeoutError(error) &&
      timeoutRetries < DEFAULT_ON_COMPLETE_TIMEOUT_RETRIES
    ) {
      const now = Date.now()
      const retryVestingTime = now + 100
      await ctx.db.patch(item._id, {
        onCompleteTimeoutRetries: timeoutRetries + 1,
        vestingTime: retryVestingTime,
        leaseId: undefined,
        leaseExpiry: undefined,
      })
      await schedulePointerWakeIfEarlier(ctx, item.queueId, retryVestingTime)
      return { done: false, retriedOnComplete: true }
    }

    console.error("[quick] onComplete terminal failure", error)
    await ctx.db.delete(item._id)
    return { done: true, retriedOnComplete: false }
  }
}

export const finalizeActionWorker = internalMutation({
  args: {
    itemId: v.id("queueItems"),
    leaseId: v.string(),
    status: v.optional(
      v.union(v.literal("success"), v.literal("failure"), v.literal("cancelled"))
    ),
    result: v.optional(v.any()),
  },
  returns: v.object({
    done: v.boolean(),
    retriedWork: v.boolean(),
    retriedOnComplete: v.boolean(),
  }),
  handler: async (ctx, args) => {
    const item = await ctx.db.get(args.itemId)
    if (!item || item.leaseId !== args.leaseId) {
      return {
        done: false,
        retriedWork: false,
        retriedOnComplete: false,
      }
    }

    if ((item.phase ?? "run") === "onComplete") {
      const result = await runOnCompleteForItem(ctx, item as any, args.leaseId)
      return {
        done: result.done,
        retriedWork: false,
        retriedOnComplete: result.retriedOnComplete,
      }
    }

    if (!args.status) {
      return {
        done: false,
        retriedWork: false,
        retriedOnComplete: false,
      }
    }

    if (args.status === "failure") {
      const retryEnabled = item.retryEnabled ?? false
      const retryBehavior = item.retryBehavior ?? DEFAULT_RETRY_BEHAVIOR
      const nextErrorCount = item.errorCount + 1
      const maxAttempts = Math.max(1, retryBehavior.maxAttempts)

      if (retryEnabled && nextErrorCount < maxAttempts) {
        const backoffMs = getRetryDelayMs(retryBehavior, nextErrorCount)
        const retryVestingTime = Date.now() + backoffMs
        await ctx.db.patch(item._id, {
          errorCount: nextErrorCount,
          vestingTime: retryVestingTime,
          leaseId: undefined,
          leaseExpiry: undefined,
        })
        await schedulePointerWakeIfEarlier(ctx, item.queueId, retryVestingTime)
        return {
          done: false,
          retriedWork: true,
          retriedOnComplete: false,
        }
      }
    }

    await ctx.db.patch(item._id, {
      phase: "onComplete",
      completionStatus: args.status,
      completionResult: args.result,
    })

    const refreshed = await ctx.db.get(item._id)
    if (!refreshed || refreshed.leaseId !== args.leaseId) {
      return {
        done: false,
        retriedWork: false,
        retriedOnComplete: false,
      }
    }

    const onCompleteResult = await runOnCompleteForItem(ctx, refreshed as any, args.leaseId)
    return {
      done: onCompleteResult.done,
      retriedWork: false,
      retriedOnComplete: onCompleteResult.retriedOnComplete,
    }
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
    const limit = args.limit ?? 100

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
      .withIndex("by_queue_and_vesting_time", (q) =>
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
  }),
  handler: async (ctx, args) => {
    const now = Date.now()

    const items = await ctx.db
      .query("queueItems")
      .withIndex("by_queue_and_vesting_time", (q) =>
        q.eq("queueId", args.queueId)
      )
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
    }
  },
})
