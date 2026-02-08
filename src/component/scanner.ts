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
import { getRetryDelayMs, isTimeoutError, resolveConfig } from "./lib.js"

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
    orderBy: v.union(v.literal("vesting"), v.literal("fifo")),
    managerBatchSize: v.number(),
    hasDuePointers: v.boolean(),
    nextPointerLeased: v.boolean(),
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
        orderBy: config.defaultOrderBy,
        managerBatchSize: config.managerBatchSize,
        hasDuePointers: false,
        nextPointerLeased: false,
        nextPointerVestingTime: null,
        pointers: [],
      }
    }

    if (state.leaseExpiry && state.leaseExpiry < now) {
      return {
        valid: false,
        orderBy: config.defaultOrderBy,
        managerBatchSize: config.managerBatchSize,
        hasDuePointers: false,
        nextPointerLeased: false,
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
      orderBy: config.defaultOrderBy,
      managerBatchSize: config.managerBatchSize,
      hasDuePointers,
      nextPointerLeased:
        nextPointer?.leaseExpiry !== undefined && nextPointer.leaseExpiry > now,
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
      if (result.hasDuePointers || result.nextPointerLeased) {
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
          orderBy: result.orderBy,
          managerBatchSize: result.managerBatchSize,
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
    orderBy: v.union(v.literal("vesting"), v.literal("fifo")),
    managerBatchSize: v.number(),
  },
  handler: async (ctx, args) => {
    const items = await ctx.runMutation(internal.lib.dequeue, {
      queueId: args.queueId,
      limit: args.managerBatchSize,
      orderBy: args.orderBy,
    })

    if (items.length === 0) {
      await ctx.runMutation(internal.scanner.finalizePointer, {
        pointerId: args.pointerId,
        pointerLeaseId: args.pointerLeaseId,
        isEmpty: true,
        orderBy: args.orderBy,
      })
      return
    }

    await Promise.all(
      items.map((item) =>
        (item.item.handlerType ?? "action") === "mutation"
          ? ctx.scheduler.runAfter(0, internal.scanner.runWorkerMutation, {
              itemId: item.item._id,
              leaseId: item.leaseId,
              handler: item.item.handler,
              payload: item.item.payload,
              queueId: args.queueId,
            })
          : ctx.scheduler.runAfter(0, internal.scanner.runWorkerAction, {
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
      orderBy: args.orderBy,
    })
  },
})

export const finalizePointer = internalMutation({
  args: {
    pointerId: v.id("queuePointers"),
    pointerLeaseId: v.string(),
    isEmpty: v.boolean(),
    orderBy: v.union(v.literal("vesting"), v.literal("fifo")),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const now = Date.now()
    const pointer = await ctx.db.get(args.pointerId)
    let cachedConfig: Awaited<ReturnType<typeof resolveConfig>> | null = null

    if (!pointer) {
      return null
    }

    if (pointer.leaseId !== args.pointerLeaseId) {
      return null
    }

    const nextItem =
      args.orderBy === "fifo"
        ? await ctx.db
            .query("queueItems")
            .withIndex("by_queue_fifo", (q) => q.eq("queueId", pointer.queueId))
            .first()
        : await ctx.db
            .query("queueItems")
            .withIndex("by_queue_and_vesting_time", (q) =>
              q.eq("queueId", pointer.queueId)
            )
            .first()

    let nextVestingTime: number
    let nextLastActiveTime: number

    if (!nextItem) {
      if (!cachedConfig) {
        cachedConfig = await resolveConfig(ctx)
      }
      // Empty queues should park until the GC window to avoid hot-looping
      // over pointers that currently have no items.
      nextVestingTime = now + cachedConfig.minInactiveBeforeDeleteMs
      nextLastActiveTime = now
    } else if (
      args.orderBy === "fifo" &&
      nextItem.leaseExpiry !== undefined &&
      nextItem.leaseExpiry > now
    ) {
      // FIFO head is currently leased. Recheck soon so completion can unblock
      // following items without waiting for full lease expiry.
      if (!cachedConfig) {
        cachedConfig = await resolveConfig(ctx)
      }
      nextVestingTime = Math.min(
        nextItem.leaseExpiry,
        now + cachedConfig.scannerBackoffMinMs
      )
      nextLastActiveTime = now
    } else {
      nextVestingTime = Math.max(now, nextItem.vestingTime)
      nextLastActiveTime = now
    }

    await ctx.db.patch(args.pointerId, {
      leaseId: undefined,
      leaseExpiry: undefined,
      vestingTime: nextVestingTime,
      lastActiveTime: nextLastActiveTime,
    })

    if (nextVestingTime <= now) {
      await ctx.scheduler.runAfter(0, internal.scanner.tryWakeScanner, {})
    }

    return null
  },
})

export const runWorkerAction = internalAction({
  args: {
    itemId: v.id("queueItems"),
    leaseId: v.string(),
    handler: v.string(),
    handlerType: v.union(v.literal("action"), v.literal("mutation")),
    payload: v.any(),
    queueId: v.string(),
  },
  handler: async (ctx, args) => {
    const preflight = await ctx.runMutation(internal.lib.prepareActionWorkerExecution, {
      itemId: args.itemId,
      leaseId: args.leaseId,
    })

    if (!preflight.valid) {
      return
    }

    if (preflight.phase === "onComplete") {
      await ctx.runMutation(internal.lib.finalizeActionWorker, {
        itemId: args.itemId,
        leaseId: args.leaseId,
      })
      return
    }

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
    let status: "success" | "failure" = "success"
    let result: unknown = null

    try {
      const fnHandle = args.handler as FunctionHandle<
        "action",
        { payload: unknown; queueId: string }
      >
      result = await ctx.runAction(fnHandle, {
        payload: args.payload,
        queueId: args.queueId,
      })
    } catch (error) {
      status = "failure"
      result = error instanceof Error ? error.message : String(error)
    }

    stop = true
    stopResolve?.()
    await extendPromise

    if (!leaseValid) {
      return
    }

    await ctx.runMutation(internal.lib.finalizeActionWorker, {
      itemId: args.itemId,
      leaseId: args.leaseId,
      status,
      result,
    })
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

export const runWorkerMutation = internalMutation({
  args: {
    itemId: v.id("queueItems"),
    leaseId: v.string(),
    handler: v.string(),
    payload: v.any(),
    queueId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const config = await resolveConfig(ctx)
    let item = await ctx.db.get(args.itemId)
    if (!item || item.leaseId !== args.leaseId) {
      return null
    }

    if ((item.phase ?? "run") === "run") {
      let status: "success" | "failure" = "success"
      let result: unknown = null

      try {
        const fnHandle = args.handler as FunctionHandle<
          "mutation",
          { payload: unknown; queueId: string }
        >
        result = await ctx.runMutation(fnHandle, {
          payload: args.payload,
          queueId: args.queueId,
        })
      } catch (error) {
        status = "failure"
        result = error instanceof Error ? error.message : String(error)
      }

      if (status === "failure") {
        const retryEnabled = item.retryEnabled ?? false
        const retryBehavior = item.retryBehavior ?? config.defaultRetryBehavior
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
          return null
        }
      }

      await ctx.db.patch(item._id, {
        phase: "onComplete",
        completionStatus: status,
        completionResult: result,
      })

      item = await ctx.db.get(item._id)
      if (!item || item.leaseId !== args.leaseId) {
        return null
      }
    }

    if (!item.onCompleteHandler) {
      await ctx.db.delete(item._id)
      return null
    }

    try {
      const onCompleteHandle = item.onCompleteHandler as FunctionHandle<
        "mutation",
        {
          workId: string
          context?: unknown
          status: "success" | "failure" | "cancelled"
          result: unknown
        }
      >
      await ctx.runMutation(onCompleteHandle, {
        workId: item._id,
        context: item.onCompleteContext,
        status: (item.completionStatus ?? "failure") as
          | "success"
          | "failure"
          | "cancelled",
        result: item.completionResult,
      })
      await ctx.db.delete(item._id)
      return null
    } catch (error) {
      const timeoutRetries = item.onCompleteTimeoutRetries ?? 0
      if (isTimeoutError(error) && timeoutRetries < 2) {
        const retryVestingTime = Date.now() + config.scannerBackoffMinMs
        await ctx.db.patch(item._id, {
          onCompleteTimeoutRetries: timeoutRetries + 1,
          vestingTime: retryVestingTime,
          leaseId: undefined,
          leaseExpiry: undefined,
        })
        await schedulePointerWakeIfEarlier(ctx, item.queueId, retryVestingTime)
        return null
      }

      console.error("[quick] onComplete terminal failure", error)
      await ctx.db.delete(item._id)
      return null
    }
  },
})

// Backwards compatibility for internal callers still pointing at runWorker.
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
    if (args.handlerType === "mutation") {
      await ctx.runMutation(internal.scanner.runWorkerMutation, {
        itemId: args.itemId,
        leaseId: args.leaseId,
        handler: args.handler,
        payload: args.payload,
        queueId: args.queueId,
      })
      return
    }
    await ctx.runAction(internal.scanner.runWorkerAction, args)
  },
})

export const watchdogRecoverScanner = internalMutation({
  args: {},
  returns: v.boolean(),
  handler: async (ctx) => wakeScanner(ctx, { checkForWork: true }),
})
