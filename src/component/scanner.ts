import { v } from "convex/values"
import { v7 as uuid } from "uuid"
import type { FunctionHandle } from "convex/server"
import {
  internalAction,
  internalMutation,
} from "./_generated/server.js"
import { internal } from "./_generated/api.js"
import type { Id } from "./_generated/dataModel.js"

const SCANNER_LEASE_DURATION_MS = 10_000
const SCANNER_BACKOFF_MIN_MS = 100
const SCANNER_BACKOFF_MAX_MS = 5_000
const POINTER_BATCH_SIZE = 50
const MAX_CONCURRENT_MANAGERS = 10

export const tryWakeScanner = internalMutation({
  args: {},
  returns: v.boolean(),
  handler: async (ctx) => {
    const now = Date.now()

    const state = await ctx.db.query("scannerState").first()

    if (state) {
      if (state.leaseExpiry && state.leaseExpiry > now) {
        return false
      }

      const leaseId = uuid()
      await ctx.db.patch(state._id, {
        leaseId,
        leaseExpiry: now + SCANNER_LEASE_DURATION_MS,
        lastRunAt: now,
      })

      const scheduledId = await ctx.scheduler.runAfter(
        0,
        internal.scanner.runScanner,
        { leaseId }
      )

      await ctx.db.patch(state._id, {
        scheduledFunctionId: scheduledId,
      })

      return true
    }

    const leaseId = uuid()
    const stateId = await ctx.db.insert("scannerState", {
      leaseId,
      leaseExpiry: now + SCANNER_LEASE_DURATION_MS,
      lastRunAt: now,
    })

    const scheduledId = await ctx.scheduler.runAfter(
      0,
      internal.scanner.runScanner,
      { leaseId }
    )

    await ctx.db.patch(stateId, {
      scheduledFunctionId: scheduledId,
    })

    return true
  },
})

export const claimScannerLease = internalMutation({
  args: {
    leaseId: v.string(),
  },
  returns: v.object({
    valid: v.boolean(),
    pointers: v.array(
      v.object({
        pointerId: v.id("queuePointers"),
        queueId: v.string(),
        pointerLeaseId: v.string(),
      })
    ),
  }),
  handler: async (ctx, args) => {
    const now = Date.now()

    const state = await ctx.db.query("scannerState").first()

    if (!state || state.leaseId !== args.leaseId) {
      return { valid: false, pointers: [] }
    }

    if (state.leaseExpiry && state.leaseExpiry < now) {
      return { valid: false, pointers: [] }
    }

    await ctx.db.patch(state._id, {
      leaseExpiry: now + SCANNER_LEASE_DURATION_MS,
      lastRunAt: now,
    })

    const pointers = await ctx.db
      .query("queuePointers")
      .withIndex("by_vesting", (q) => q.lte("vestingTime", now))
      .take(POINTER_BATCH_SIZE)

    const availablePointers = pointers.filter(
      (p) => !p.leaseExpiry || p.leaseExpiry <= now
    )

    const claimedPointers: Array<{
      pointerId: typeof availablePointers[0]["_id"]
      queueId: string
      pointerLeaseId: string
    }> = []

    for (const pointer of availablePointers.slice(0, MAX_CONCURRENT_MANAGERS)) {
      const pointerLeaseId = uuid()
      const pointerLeaseExpiry = now + SCANNER_LEASE_DURATION_MS

      await ctx.db.patch(pointer._id, {
        leaseId: pointerLeaseId,
        leaseExpiry: pointerLeaseExpiry,
        vestingTime: pointerLeaseExpiry,
      })

      claimedPointers.push({
        pointerId: pointer._id,
        queueId: pointer.queueId,
        pointerLeaseId,
      })
    }

    return { valid: true, pointers: claimedPointers }
  },
})

export const runScanner = internalAction({
  args: {
    leaseId: v.string(),
  },
  handler: async (ctx, args) => {
    const result = await ctx.runMutation(internal.scanner.claimScannerLease, {
      leaseId: args.leaseId,
    })

    if (!result.valid) {
      return
    }

    if (result.pointers.length === 0) {
      await ctx.runMutation(internal.scanner.rescheduleScanner, {
        leaseId: args.leaseId,
        hasWork: false,
      })
      return
    }

    const managerPromises = result.pointers.map(
      (pointer: {
        pointerId: Id<"queuePointers">
        queueId: string
        pointerLeaseId: string
      }) =>
        ctx.runAction(internal.scanner.runManager, {
          pointerId: pointer.pointerId,
          queueId: pointer.queueId,
          pointerLeaseId: pointer.pointerLeaseId,
        })
    )

    await Promise.all(managerPromises)

    await ctx.runMutation(internal.scanner.rescheduleScanner, {
      leaseId: args.leaseId,
      hasWork: true,
    })
  },
})

export const rescheduleScanner = internalMutation({
  args: {
    leaseId: v.string(),
    hasWork: v.boolean(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const now = Date.now()

    const state = await ctx.db.query("scannerState").first()

    if (!state || state.leaseId !== args.leaseId) {
      return null
    }

    const newLeaseId = uuid()
    const delayMs = args.hasWork ? SCANNER_BACKOFF_MIN_MS : SCANNER_BACKOFF_MAX_MS

    await ctx.db.patch(state._id, {
      leaseId: newLeaseId,
      leaseExpiry: now + SCANNER_LEASE_DURATION_MS,
      lastRunAt: now,
    })

    const scheduledId = await ctx.scheduler.runAfter(
      delayMs,
      internal.scanner.runScanner,
      { leaseId: newLeaseId }
    )

    await ctx.db.patch(state._id, {
      scheduledFunctionId: scheduledId,
    })

    return null
  },
})

export const runManager = internalAction({
  args: {
    pointerId: v.id("queuePointers"),
    queueId: v.string(),
    pointerLeaseId: v.string(),
  },
  handler: async (ctx, args) => {
    const items = await ctx.runMutation(internal.lib.dequeue, {
      queueId: args.queueId,
      limit: 10,
    })

    if (items.length === 0) {
      await ctx.runMutation(internal.scanner.finalizePointer, {
        pointerId: args.pointerId,
        pointerLeaseId: args.pointerLeaseId,
        isEmpty: true,
      })
      return
    }

    const workerPromises = items.map(
      (item: {
        item: {
          _id: Id<"queueItems">
          payload: unknown
          handler: string
        }
        leaseId: string
      }) =>
        ctx.runAction(internal.scanner.runWorker, {
          itemId: item.item._id,
          leaseId: item.leaseId,
          handler: item.item.handler,
          payload: item.item.payload,
          queueId: args.queueId,
        })
    )

    await Promise.all(workerPromises)

    await ctx.runMutation(internal.scanner.finalizePointer, {
      pointerId: args.pointerId,
      pointerLeaseId: args.pointerLeaseId,
      isEmpty: false,
    })
  },
})

export const finalizePointer = internalMutation({
  args: {
    pointerId: v.id("queuePointers"),
    pointerLeaseId: v.string(),
    isEmpty: v.boolean(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const now = Date.now()
    const pointer = await ctx.db.get(args.pointerId)

    if (!pointer) {
      return null
    }

    if (pointer.leaseId !== args.pointerLeaseId) {
      return null
    }

    const nextItem = await ctx.db
      .query("queueItems")
      .withIndex("by_queue_priority_vesting", (q) =>
        q.eq("queueId", pointer.queueId)
      )
      .first()

    if (!nextItem) {
      await ctx.db.patch(args.pointerId, {
        leaseId: undefined,
        leaseExpiry: undefined,
        vestingTime: now,
      })
      return null
    }

    const nextVestingTime = Math.max(nextItem.vestingTime, now)

    await ctx.db.patch(args.pointerId, {
      leaseId: undefined,
      leaseExpiry: undefined,
      vestingTime: nextVestingTime,
      lastActiveTime: now,
    })

    return null
  },
})

export const runWorker = internalAction({
  args: {
    itemId: v.id("queueItems"),
    leaseId: v.string(),
    handler: v.string(),
    payload: v.any(),
    queueId: v.string(),
  },
  handler: async (ctx, args) => {
    try {
      const fnHandle = args.handler as FunctionHandle<
        "action",
        { payload: unknown; queueId: string }
      >
      await ctx.runAction(fnHandle, {
        payload: args.payload,
        queueId: args.queueId,
      })

      await ctx.runMutation(internal.lib.complete, {
        itemId: args.itemId,
        leaseId: args.leaseId,
      })
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error)

      await ctx.runMutation(internal.lib.requeue, {
        itemId: args.itemId,
        leaseId: args.leaseId,
        error: errorMessage,
      })
    }
  },
})

export const watchdogRecoverScanner = internalMutation({
  args: {},
  returns: v.boolean(),
  handler: async (ctx): Promise<boolean> => {
    const now = Date.now()

    const state = await ctx.db.query("scannerState").first()

    if (!state) {
      const woke: boolean = await ctx.runMutation(
        internal.scanner.tryWakeScanner,
        {}
      )
      return woke
    }

    if (state.leaseExpiry && state.leaseExpiry > now) {
      return false
    }

    const hasWork = await ctx.db
      .query("queuePointers")
      .withIndex("by_vesting", (q) => q.lte("vestingTime", now))
      .first()

    if (!hasWork) {
      return false
    }

    const leaseId = uuid()
    await ctx.db.patch(state._id, {
      leaseId,
      leaseExpiry: now + SCANNER_LEASE_DURATION_MS,
      lastRunAt: now,
    })

    const scheduledId = await ctx.scheduler.runAfter(
      0,
      internal.scanner.runScanner,
      { leaseId }
    )

    await ctx.db.patch(state._id, {
      scheduledFunctionId: scheduledId,
    })

    return true
  },
})
