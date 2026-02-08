import { v } from "convex/values"
import { v7 as uuid } from "uuid"
import type { FunctionHandle } from "convex/server"
import {
  internalAction,
  internalMutation,
  type MutationCtx,
} from "./_generated/server.js"
import { internal } from "./_generated/api.js"
import type { Id } from "./_generated/dataModel.js"
import { resolveConfig } from "./lib.js"

const POINTER_OVERSCAN_MULTIPLIER = 5

async function wakeScanner(
  ctx: MutationCtx,
  opts: {
    /**
     * Whether to check for work before waking the scanner, used during recovery
     */
    checkForWork?: boolean
  } = {}
): Promise<boolean> {
  const config = await resolveConfig(ctx)
  const now = Date.now()
  const state = await ctx.db.query("scannerState").first()

  if (state && state.leaseExpiry && state.leaseExpiry > now) {
    return false
  }

  if (opts.checkForWork) {
    const hasWork = await ctx.db
      .query("queuePointers")
      .withIndex("by_vesting", (q) => q.lte("vestingTime", now))
      .first()
    if (!hasWork) {
      return false
    }
  }

  const leaseId = uuid()
  const leaseExpiry = now + config.scannerLeaseDurationMs

  const stateId = state
    ? (await ctx.db.patch(state._id, { leaseId, leaseExpiry, lastRunAt: now }), state._id)
    : await ctx.db.insert("scannerState", { leaseId, leaseExpiry, lastRunAt: now })

  const scheduledId = await ctx.scheduler.runAfter(
    0,
    internal.scanner.runScanner,
    { leaseId }
  )

  await ctx.db.patch(stateId, { scheduledFunctionId: scheduledId })

  return true
}

export const tryWakeScanner = internalMutation({
  args: {},
  returns: v.boolean(),
  handler: async (ctx) => wakeScanner(ctx), // We don't want to wake the scanner because this is only called after work is enqueued
})

export const claimScannerLease = internalMutation({
  args: {
    leaseId: v.string(),
  },
  returns: v.object({
    valid: v.boolean(),
    hasDuePointers: v.boolean(),
    nextPointerVestingTime: v.union(v.null(), v.number()),
    pointers: v.array(
      v.object({
        pointerId: v.id("queuePointers"),
        queueId: v.string(),
        pointerLeaseId: v.string(),
      })
    ),
  }),
  handler: async (ctx, args) => {
    const config = await resolveConfig(ctx)
    const now = Date.now()

    const state = await ctx.db.query("scannerState").first()

    if (!state || state.leaseId !== args.leaseId) {
      return {
        valid: false,
        hasDuePointers: false,
        nextPointerVestingTime: null,
        pointers: [],
      }
    }

    if (state.leaseExpiry && state.leaseExpiry < now) {
      return {
        valid: false,
        hasDuePointers: false,
        nextPointerVestingTime: null,
        pointers: [],
      }
    }

    await ctx.db.patch(state._id, {
      leaseExpiry: now + config.scannerLeaseDurationMs,
      lastRunAt: now,
    })

    const duePointersQuery = () =>
      ctx.db.query("queuePointers").withIndex("by_vesting", (q) => q.lte("vestingTime", now))
    const hasDuePointers = (await duePointersQuery().first()) !== null

    const nextPointer = await ctx.db
      .query("queuePointers")
      .withIndex("by_vesting")
      .first()

    const availablePointers: Array<{
      _id: Id<"queuePointers">
      queueId: string
    }> = []
    const maxPointersToScan =
      Math.max(config.pointerBatchSize, config.maxConcurrentManagers) *
      POINTER_OVERSCAN_MULTIPLIER
    let scannedPointers = 0

    for await (const pointer of duePointersQuery()) {
      scannedPointers++

      if (pointer.leaseExpiry && pointer.leaseExpiry > now) {
        if (scannedPointers >= maxPointersToScan) {
          break
        }
        continue
      }

      availablePointers.push({
        _id: pointer._id,
        queueId: pointer.queueId,
      })

      if (availablePointers.length >= config.maxConcurrentManagers) {
        break
      }
      if (scannedPointers >= maxPointersToScan) {
        break
      }
    }

    const claimedPointers: Array<{
      pointerId: Id<"queuePointers">
      queueId: string
      pointerLeaseId: string
    }> = []

    for (const pointer of availablePointers) {
      const pointerLeaseId = uuid()
      const pointerLeaseExpiry = now + config.scannerLeaseDurationMs

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

    return {
      valid: true,
      hasDuePointers,
      nextPointerVestingTime: nextPointer?.vestingTime ?? null,
      pointers: claimedPointers,
    }
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
      if (result.hasDuePointers) {
        await ctx.runMutation(internal.scanner.rescheduleScanner, {
          leaseId: args.leaseId,
          hasWork: true,
        })
      } else {
        await ctx.runMutation(internal.scanner.parkScanner, {
          leaseId: args.leaseId,
          nextPointerVestingTime: result.nextPointerVestingTime ?? undefined,
        })
      }
      return
    }

    await Promise.all(
      result.pointers.map((pointer) =>
        ctx.scheduler.runAfter(0, internal.scanner.runManager, {
          pointerId: pointer.pointerId,
          queueId: pointer.queueId,
          pointerLeaseId: pointer.pointerLeaseId,
        })
      )
    )

    await ctx.runMutation(internal.scanner.rescheduleScanner, {
      leaseId: args.leaseId,
      hasWork: true,
    })
  },
})

export const parkScanner = internalMutation({
  args: {
    leaseId: v.string(),
    nextPointerVestingTime: v.optional(v.number()),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const now = Date.now()
    const state = await ctx.db.query("scannerState").first()

    if (!state || state.leaseId !== args.leaseId) {
      return null
    }

    if (args.nextPointerVestingTime === undefined) {
      await ctx.db.patch(state._id, {
        leaseId: undefined,
        leaseExpiry: undefined,
        scheduledFunctionId: undefined,
        lastRunAt: now,
      })
      return null
    }

    const delayMs = Math.max(0, args.nextPointerVestingTime - now)
    const scheduledId = await ctx.scheduler.runAfter(
      delayMs,
      internal.scanner.watchdogRecoverScanner,
      {}
    )

    await ctx.db.patch(state._id, {
      leaseId: undefined,
      leaseExpiry: undefined,
      scheduledFunctionId: scheduledId,
      lastRunAt: now,
    })

    return null
  },
})

export const rescheduleScanner = internalMutation({
  args: {
    leaseId: v.string(),
    hasWork: v.boolean(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const config = await resolveConfig(ctx)
    const now = Date.now()

    const state = await ctx.db.query("scannerState").first()

    if (!state || state.leaseId !== args.leaseId) {
      return null
    }

    const newLeaseId = uuid()
    const delayMs = args.hasWork ? config.scannerBackoffMinMs : config.scannerBackoffMaxMs

    await ctx.db.patch(state._id, {
      leaseId: newLeaseId,
      leaseExpiry: now + config.scannerLeaseDurationMs,
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

    await Promise.all(
      items.map((item) =>
        ctx.scheduler.runAfter(0, internal.scanner.runWorker, {
          itemId: item.item._id,
          leaseId: item.leaseId,
          handler: item.item.handler,
          handlerType: item.item.handlerType ?? "action",
          payload: item.item.payload,
          queueId: args.queueId,
        })
      )
    )

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
      .withIndex("by_queue_and_vesting_time", (q) =>
        q.eq("queueId", pointer.queueId)
      )
      .first()

    const nextVestingTime = nextItem
      ? Math.max(now, nextItem.vestingTime)
      : now

    await ctx.db.patch(args.pointerId, {
      leaseId: undefined,
      leaseExpiry: undefined,
      vestingTime: nextVestingTime,
      lastActiveTime: nextItem ? now : pointer.lastActiveTime,
    })

    return null
  },
})

export const runWorker = internalAction({
  args: {
    itemId: v.id("queueItems"),
    leaseId: v.string(),
    handler: v.string(),
    handlerType: v.union(v.literal("action"), v.literal("mutation")),
    payload: v.any(),
    queueId: v.string(),
  },
  handler: async (ctx, args) => {
    const config = await ctx.runQuery(internal.lib.getResolvedConfig, {})
    const extendIntervalMs = Math.max(
      100,
      Math.floor(config.defaultLeaseDurationMs / 2)
    )

    const sleep = (ms: number) =>
      new Promise<void>((resolve) => {
        setTimeout(resolve, ms)
      })

    let stop = false
    let leaseValid = true
    let stopResolve: (() => void) | undefined
    const stopSignal = new Promise<void>((resolve) => {
      stopResolve = resolve
    })

    const extendLeaseLoop = async () => {
      while (true) {
        await Promise.race([sleep(extendIntervalMs), stopSignal])

        if (stop) {
          return
        }

        const extended = await ctx.runMutation(internal.lib.extendItemLease, {
          itemId: args.itemId,
          leaseId: args.leaseId,
        })

        if (!extended) {
          leaseValid = false
          stop = true
          stopResolve?.()
          return
        }
      }
    }

    const extendPromise = extendLeaseLoop()
    let handlerError: unknown | null = null

    try {
      if (args.handlerType === "mutation") {
        const fnHandle = args.handler as FunctionHandle<
          "mutation",
          { payload: unknown; queueId: string }
        >
        await ctx.runMutation(fnHandle, {
          payload: args.payload,
          queueId: args.queueId,
        })
      } else {
        const fnHandle = args.handler as FunctionHandle<
          "action",
          { payload: unknown; queueId: string }
        >
        await ctx.runAction(fnHandle, {
          payload: args.payload,
          queueId: args.queueId,
        })
      }
    } catch (error) {
      handlerError = error
    }

    stop = true
    stopResolve?.()
    await extendPromise

    if (!leaseValid) {
      return
    }

    if (handlerError === null) {
      await ctx.runMutation(internal.lib.complete, {
        itemId: args.itemId,
        leaseId: args.leaseId,
      })
      return
    }

    const errorMessage =
      handlerError instanceof Error ? handlerError.message : String(handlerError)

    await ctx.runMutation(internal.lib.requeue, {
      itemId: args.itemId,
      leaseId: args.leaseId,
      error: errorMessage,
    })
  },
})

export const watchdogRecoverScanner = internalMutation({
  args: {},
  returns: v.boolean(),
  handler: async (ctx) => wakeScanner(ctx, { checkForWork: true }),
})
