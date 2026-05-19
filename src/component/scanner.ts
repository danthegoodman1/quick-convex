import { v } from "convex/values"
import { v7 as uuid } from "uuid"
import { makeFunctionReference } from "convex/server"
import type {
  FunctionHandle,
  FunctionReference,
  FunctionReturnType,
  OptionalRestArgs,
} from "convex/server"
import {
  internalAction,
  internalMutation,
  internalQuery,
  type MutationCtx,
} from "./_generated/server.js"
import { internal } from "./_generated/api.js"
import type { Id } from "./_generated/dataModel.js"
import {
  DEFAULT_PRIORITY,
  computeVestingPointerState,
  getRetryDelayMs,
  isTimeoutError,
  resolveConfig,
} from "./lib.js"

const POINTER_OVERSCAN_MULTIPLIER = 5
const MANAGER_SLOT_LEASE_MS = 600_000
const OCC_RETRY_MAX_ATTEMPTS = 5
const OCC_RETRY_BASE_DELAY_MS = 25
const OCC_RETRY_MAX_DELAY_MS = 250

type SnapshotMutationCtx = MutationCtx & {
  runSnapshotQuery<Query extends FunctionReference<"query", "public" | "internal">>(
    query: Query,
    ...args: OptionalRestArgs<Query>
  ): Promise<FunctionReturnType<Query>>
}

function withSnapshotQueries(ctx: MutationCtx): SnapshotMutationCtx {
  return ctx as SnapshotMutationCtx
}

type ClaimablePointerSnapshot = {
  orderBy: "vesting" | "fifo"
  workersPerManager: number
  hasDuePointers: boolean
  nextPointerLeased: boolean
  nextPointerVestingTime: number | null
  availableSlotNumbers: number[]
  pointers: Array<{
    pointerId: Id<"queuePointers">
    queueId: string
    priority: number
    vestingTime: number
  }>
}

type PointerFinalizationSnapshot = {
  hasItems: boolean
  state: {
    priority: number
    vestingTime: number
    leaseExpiry?: number
  } | null
}

const getClaimablePointerSnapshotRef = makeFunctionReference<
  "query",
  { now: number },
  ClaimablePointerSnapshot
>("scanner:getClaimablePointerSnapshot")

const getPointerFinalizationSnapshotRef = makeFunctionReference<
  "query",
  {
    queueId: string
    orderBy: "vesting" | "fifo"
    now: number
  },
  PointerFinalizationSnapshot
>("scanner:getPointerFinalizationSnapshot")

function isOptimisticConcurrencyError(error: unknown): boolean {
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
  return messages.some((message) =>
    /documents read from or written to|changed while this mutation was being run|optimistic concurrency|error#1/i.test(
      message
    )
  )
}

const sleep = (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms)
  })

async function runMutationWithOccRetry<T>(
  run: () => Promise<T>,
  opts: {
    operation: string
    context?: Record<string, unknown>
    returnNullOnExhausted?: boolean
  }
): Promise<T | null> {
  for (let attempt = 1; attempt <= OCC_RETRY_MAX_ATTEMPTS; attempt++) {
    try {
      return await run()
    } catch (error) {
      if (!isOptimisticConcurrencyError(error)) {
        throw error
      }

      if (attempt === OCC_RETRY_MAX_ATTEMPTS) {
        if (opts.returnNullOnExhausted) {
          console.error("[quick] mutation hit repeated OCC conflicts; letting another action pass win", {
            operation: opts.operation,
            attempts: attempt,
            ...opts.context,
          }, error)
          return null
        }
        throw error
      }

      console.warn("[quick] retrying mutation after OCC conflict", {
        operation: opts.operation,
        attempt,
        maxAttempts: OCC_RETRY_MAX_ATTEMPTS,
        ...opts.context,
      })
      await sleep(
        Math.min(OCC_RETRY_MAX_DELAY_MS, OCC_RETRY_BASE_DELAY_MS * attempt)
      )
    }
  }

  return null
}

async function wakeScanner(
  ctx: MutationCtx,
  opts: {
    /**
     * Whether to check for work before waking the scanner, used during recovery
     */
    checkForWork?: boolean
    reason?: "enqueue" | "pointerReady"
  } = {}
): Promise<boolean> {
  const config = await resolveConfig(ctx)
  const now = Date.now()
  const state = await ctx.db.query("scannerState").first()
  const wasParked =
    state !== null &&
    (state.leaseExpiry === undefined || state.leaseExpiry <= now)

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

  if (state?.scheduledFunctionId) {
    const scheduledFunctionId = state.scheduledFunctionId as Id<"_scheduled_functions">
    const scheduledJob = await ctx.db.system.get("_scheduled_functions", scheduledFunctionId)
    if (scheduledJob?.state.kind === "pending") {
      await ctx.scheduler.cancel(scheduledFunctionId)
    }
  }

  const stateId = state
    ? (await ctx.db.patch(state._id, { leaseId, leaseExpiry, lastRunAt: now }), state._id)
    : await ctx.db.insert("scannerState", { leaseId, leaseExpiry, lastRunAt: now })

  const scheduledId = await ctx.scheduler.runAfter(
    0,
    internal.scanner.runScanner,
    { leaseId }
  )

  await ctx.db.patch(stateId, { scheduledFunctionId: scheduledId })

  if (opts.reason === "enqueue" && wasParked) {
    console.info("[quick] enqueue woke parked scanner")
  }

  return true
}

async function clearScannerScheduleIfComponentIdle(ctx: MutationCtx) {
  const state = await ctx.db.query("scannerState").first()
  if (!state) {
    return
  }

  const remainingItem = await ctx.db.query("queueItems").first()
  if (remainingItem) {
    return
  }

  if (state.scheduledFunctionId) {
    const scheduledFunctionId = state.scheduledFunctionId as Id<"_scheduled_functions">
    const scheduledJob = await ctx.db.system.get("_scheduled_functions", scheduledFunctionId)
    if (scheduledJob?.state.kind === "pending") {
      await ctx.scheduler.cancel(scheduledFunctionId)
    }
  }

  await ctx.db.patch(state._id, {
    leaseId: undefined,
    leaseExpiry: undefined,
    scheduledFunctionId: undefined,
    lastRunAt: Date.now(),
  })
}

export const tryWakeScanner = internalMutation({
  args: {
    reason: v.optional(v.union(v.literal("enqueue"), v.literal("pointerReady"))),
  },
  returns: v.boolean(),
  handler: async (ctx, args) => wakeScanner(ctx, { reason: args.reason }),
})

export const claimScannerLease = internalQuery({
  args: {
    leaseId: v.string(),
  },
  returns: v.object({
    valid: v.boolean(),
    orderBy: v.union(v.literal("vesting"), v.literal("fifo")),
    workersPerManager: v.number(),
    hasDuePointers: v.boolean(),
    nextPointerLeased: v.boolean(),
    nextPointerVestingTime: v.union(v.null(), v.number()),
    availableSlotCount: v.number(),
  }),
  handler: async (ctx, args) => {
    const config = await resolveConfig(ctx)
    const now = Date.now()

    const state = await ctx.db.query("scannerState").first()

    if (!state || state.leaseId !== args.leaseId) {
      return {
        valid: false,
        orderBy: config.defaultOrderBy,
        workersPerManager: config.workersPerManager,
        hasDuePointers: false,
        nextPointerLeased: false,
        nextPointerVestingTime: null,
        availableSlotCount: 0,
      }
    }

    if (state.leaseExpiry && state.leaseExpiry < now) {
      return {
        valid: false,
        orderBy: config.defaultOrderBy,
        workersPerManager: config.workersPerManager,
        hasDuePointers: false,
        nextPointerLeased: false,
        nextPointerVestingTime: null,
        availableSlotCount: 0,
      }
    }

    const duePointersQuery = () =>
      ctx.db.query("queuePointers").withIndex("by_vesting", (q) => q.lte("vestingTime", now))
    const hasDuePointers = (await duePointersQuery().first()) !== null

    const nextPointer = await ctx.db
      .query("queuePointers")
      .withIndex("by_vesting")
      .first()

    let existingSlots = 0
    let availableSlotCount = 0

    for await (const slot of ctx.db
      .query("managerSlots")
      .withIndex("by_slot_number", (q) =>
        q.gte("slotNumber", 0).lt("slotNumber", config.managerSlots)
      )) {
      existingSlots++
      if (!slot.leaseExpiry || slot.leaseExpiry <= now) {
        availableSlotCount++
      }
    }

    availableSlotCount += Math.max(0, config.managerSlots - existingSlots)

    return {
      valid: true,
      orderBy: config.defaultOrderBy,
      workersPerManager: config.workersPerManager,
      hasDuePointers,
      nextPointerLeased:
        nextPointer?.leaseExpiry !== undefined && nextPointer.leaseExpiry > now,
      nextPointerVestingTime: nextPointer?.vestingTime ?? null,
      availableSlotCount,
    }
  },
})

export const getClaimablePointerSnapshot = internalQuery({
  args: {
    now: v.number(),
  },
  returns: v.object({
    orderBy: v.union(v.literal("vesting"), v.literal("fifo")),
    workersPerManager: v.number(),
    hasDuePointers: v.boolean(),
    nextPointerLeased: v.boolean(),
    nextPointerVestingTime: v.union(v.null(), v.number()),
    availableSlotNumbers: v.array(v.number()),
    pointers: v.array(
      v.object({
        pointerId: v.id("queuePointers"),
        queueId: v.string(),
        priority: v.number(),
        vestingTime: v.number(),
      })
    ),
  }),
  handler: async (ctx, args) => {
    const config = await resolveConfig(ctx)
    const duePointersQuery = () =>
      ctx.db.query("queuePointers").withIndex("by_vesting", (q) =>
        q.lte("vestingTime", args.now)
      )
    const hasDuePointers = (await duePointersQuery().first()) !== null

    const nextPointer = await ctx.db
      .query("queuePointers")
      .withIndex("by_vesting")
      .first()

    const managerSlotsByNumber = new Map<
      number,
      {
        slotNumber: number
        leaseExpiry?: number
      }
    >()

    for await (const slot of ctx.db
      .query("managerSlots")
      .withIndex("by_slot_number", (q) =>
        q.gte("slotNumber", 0).lt("slotNumber", config.managerSlots)
      )) {
      managerSlotsByNumber.set(slot.slotNumber, {
        slotNumber: slot.slotNumber,
        leaseExpiry: slot.leaseExpiry,
      })
    }

    const availableSlotNumbers: number[] = []
    for (let slotNumber = 0; slotNumber < config.managerSlots; slotNumber++) {
      const slot = managerSlotsByNumber.get(slotNumber)
      if (!slot || !slot.leaseExpiry || slot.leaseExpiry <= args.now) {
        availableSlotNumbers.push(slotNumber)
      }
    }

    const pointers: Array<{
      pointerId: Id<"queuePointers">
      queueId: string
      priority: number
      vestingTime: number
    }> = []
    const maxPointersToScan =
      Math.max(config.pointerBatchSize, config.managerSlots) *
      POINTER_OVERSCAN_MULTIPLIER
    let scannedPointers = 0

    if (availableSlotNumbers.length > 0) {
      for await (const pointer of duePointersQuery()) {
        scannedPointers++

        if (pointer.leaseExpiry && pointer.leaseExpiry > args.now) {
          if (scannedPointers >= maxPointersToScan) {
            break
          }
          continue
        }

        pointers.push({
          pointerId: pointer._id,
          queueId: pointer.queueId,
          priority: pointer.priority,
          vestingTime: pointer.vestingTime,
        })

        if (scannedPointers >= maxPointersToScan) {
          break
        }
      }
    }

    pointers.sort((a, b) => {
      if (config.defaultOrderBy === "vesting" && a.priority !== b.priority) {
        return b.priority - a.priority
      }
      return a.vestingTime - b.vestingTime
    })

    return {
      orderBy: config.defaultOrderBy,
      workersPerManager: config.workersPerManager,
      hasDuePointers,
      nextPointerLeased:
        nextPointer?.leaseExpiry !== undefined && nextPointer.leaseExpiry > args.now,
      nextPointerVestingTime: nextPointer?.vestingTime ?? null,
      availableSlotNumbers,
      pointers,
    }
  },
})

async function getScannerInspectionForConfirmation(
  ctx: MutationCtx,
  config: Awaited<ReturnType<typeof resolveConfig>>,
  now: number
) {
  const duePointer = await ctx.db
    .query("queuePointers")
    .withIndex("by_vesting", (q) => q.lte("vestingTime", now))
    .first()

  const nextPointer = await ctx.db
    .query("queuePointers")
    .withIndex("by_vesting")
    .first()

  let existingSlots = 0
  let availableSlotCount = 0

  for await (const slot of ctx.db
    .query("managerSlots")
    .withIndex("by_slot_number", (q) =>
      q.gte("slotNumber", 0).lt("slotNumber", config.managerSlots)
    )) {
    existingSlots++
    if (!slot.leaseExpiry || slot.leaseExpiry <= now) {
      availableSlotCount++
    }
  }

  availableSlotCount += Math.max(0, config.managerSlots - existingSlots)

  return {
    hasDuePointers: duePointer !== null,
    nextPointerVestingTime: nextPointer?.vestingTime ?? null,
    availableSlotCount,
  }
}

async function getOrCreateManagerSlot(
  ctx: MutationCtx,
  slotNumber: number
): Promise<{
  _id: Id<"managerSlots">
  leaseExpiry?: number
}> {
  const existing = await ctx.db
    .query("managerSlots")
    .withIndex("by_slot_number", (q) => q.eq("slotNumber", slotNumber))
    .unique()

  if (existing) {
    return {
      _id: existing._id,
      leaseExpiry: existing.leaseExpiry,
    }
  }

  const slotId = await ctx.db.insert("managerSlots", { slotNumber })
  return { _id: slotId }
}

async function parkScannerInTransaction(
  ctx: MutationCtx,
  args: {
    leaseId: string
    nextPointerVestingTime?: number
  },
  now: number
) {
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
    console.info("[quick] scanner fully parked")
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
}

async function rescheduleScannerInTransaction(
  ctx: MutationCtx,
  args: {
    leaseId: string
    hasWork: boolean
  },
  config: Awaited<ReturnType<typeof resolveConfig>>,
  now: number
) {
  const state = await ctx.db.query("scannerState").first()

  if (!state || state.leaseId !== args.leaseId) {
    return null
  }

  let scheduledId: Id<"_scheduled_functions">

  if (args.hasWork) {
    const newLeaseId = uuid()

    await ctx.db.patch(state._id, {
      leaseId: newLeaseId,
      leaseExpiry: now + config.scannerLeaseDurationMs,
      lastRunAt: now,
    })

    scheduledId = await ctx.scheduler.runAfter(
      config.scannerBackoffMinMs,
      internal.scanner.runScanner,
      { leaseId: newLeaseId }
    )
  } else {
    await ctx.db.patch(state._id, {
      leaseId: undefined,
      leaseExpiry: undefined,
      lastRunAt: now,
    })

    // Slow-path retries should not hold the scanner lease so a newly enqueued
    // item can wake the scanner immediately instead of waiting for backoff.
    scheduledId = await ctx.scheduler.runAfter(
      config.scannerBackoffMaxMs,
      internal.scanner.watchdogRecoverScanner,
      {}
    )
  }

  await ctx.db.patch(state._id, {
    scheduledFunctionId: scheduledId,
  })

  return null
}

async function claimAvailablePointersInTransaction(
  ctx: MutationCtx,
  args: {
    leaseId: string
    now?: number
    config?: Awaited<ReturnType<typeof resolveConfig>>
  }
) {
  const config = args.config ?? (await resolveConfig(ctx))
  const now = args.now ?? Date.now()
  const state = await ctx.db.query("scannerState").first()

  if (
    !state ||
    state.leaseId !== args.leaseId ||
    (state.leaseExpiry !== undefined && state.leaseExpiry < now)
  ) {
    return {
      valid: false,
      orderBy: config.defaultOrderBy,
      workersPerManager: config.workersPerManager,
      hasDuePointers: false,
      nextPointerLeased: false,
      nextPointerVestingTime: null,
      pointers: [],
    }
  }

  const snapshot = await withSnapshotQueries(ctx).runSnapshotQuery(
    getClaimablePointerSnapshotRef,
    { now }
  )

  const claimedPointers: Array<{
    pointerId: Id<"queuePointers">
    queueId: string
    pointerLeaseId: string
    slotNumber: number
    slotLeaseId: string
  }> = []

  let slotIndex = 0
  for (const candidate of snapshot.pointers) {
    if (claimedPointers.length >= config.pointerBatchSize) {
      break
    }

    const pointer = await ctx.db.get(candidate.pointerId)
    if (
      !pointer ||
      pointer.vestingTime > now ||
      (pointer.leaseExpiry !== undefined && pointer.leaseExpiry > now)
    ) {
      continue
    }

    let selectedSlot:
      | {
          slotNumber: number
          slotId: Id<"managerSlots">
        }
      | undefined

    while (slotIndex < snapshot.availableSlotNumbers.length) {
      const slotNumber = snapshot.availableSlotNumbers[slotIndex]
      slotIndex++

      if (slotNumber === undefined) {
        break
      }

      const slot = await getOrCreateManagerSlot(ctx, slotNumber)
      if (!slot.leaseExpiry || slot.leaseExpiry <= now) {
        selectedSlot = {
          slotNumber,
          slotId: slot._id,
        }
        break
      }
    }

    if (!selectedSlot) {
      break
    }

    const pointerLeaseId = uuid()
    const slotLeaseId = uuid()
    const leaseExpiry = now + MANAGER_SLOT_LEASE_MS

    await ctx.db.patch(pointer._id, {
      leaseId: pointerLeaseId,
      leaseExpiry,
      vestingTime: leaseExpiry,
      lastActiveTime: now,
    })

    await ctx.db.patch(selectedSlot.slotId, {
      leaseId: slotLeaseId,
      leaseExpiry,
      pointerId: pointer._id,
      queueId: pointer.queueId,
    })

    console.info("[quick] scanner claimed pointer", {
      queueId: pointer.queueId,
      pointerId: pointer._id,
      pointerLeaseId,
      pointerLeaseExpiry: leaseExpiry,
      slotNumber: selectedSlot.slotNumber,
    })

    claimedPointers.push({
      pointerId: pointer._id,
      queueId: pointer.queueId,
      pointerLeaseId,
      slotNumber: selectedSlot.slotNumber,
      slotLeaseId,
    })
  }

  return {
    valid: true,
    orderBy: snapshot.orderBy,
    workersPerManager: snapshot.workersPerManager,
    hasDuePointers: snapshot.hasDuePointers,
    nextPointerLeased: snapshot.nextPointerLeased,
    nextPointerVestingTime: snapshot.nextPointerVestingTime,
    pointers: claimedPointers,
  }
}

async function computePointerFinalizationState(
  db: Parameters<typeof computeVestingPointerState>[0],
  args: {
    queueId: string
    orderBy: "vesting" | "fifo"
    now: number
  }
): Promise<{
  hasItems: boolean
  state: {
    priority: number
    vestingTime: number
    leaseExpiry?: number
  } | null
}> {
  if (args.orderBy === "vesting") {
    return await computeVestingPointerState(db, {
      queueId: args.queueId,
      now: args.now,
    })
  }

  const nextItem = await db
    .query("queueItems")
    .withIndex("by_queue_fifo", (q) => q.eq("queueId", args.queueId))
    .first()

  if (!nextItem) {
    return {
      hasItems: false,
      state: null,
    }
  }

  return {
    hasItems: true,
    state: {
      priority: DEFAULT_PRIORITY,
      vestingTime: nextItem.vestingTime,
      leaseExpiry: nextItem.leaseExpiry,
    },
  }
}

function pointerStateNeedsConfirmation(
  state: Awaited<ReturnType<typeof computePointerFinalizationState>>,
  now: number
) {
  return (
    !state.hasItems ||
    !state.state ||
    state.state.vestingTime > now ||
    (state.state.leaseExpiry !== undefined && state.state.leaseExpiry > now)
  )
}

export const getPointerFinalizationSnapshot = internalQuery({
  args: {
    queueId: v.string(),
    orderBy: v.union(v.literal("vesting"), v.literal("fifo")),
    now: v.number(),
  },
  returns: v.object({
    hasItems: v.boolean(),
    state: v.union(
      v.null(),
      v.object({
        priority: v.number(),
        vestingTime: v.number(),
        leaseExpiry: v.optional(v.number()),
      })
    ),
  }),
  handler: async (ctx, args) =>
    await computePointerFinalizationState(ctx.db, {
      queueId: args.queueId,
      orderBy: args.orderBy,
      now: args.now,
    }),
})

export const claimAvailablePointers = internalMutation({
  args: {
    leaseId: v.string(),
  },
  returns: v.object({
    valid: v.boolean(),
    orderBy: v.union(v.literal("vesting"), v.literal("fifo")),
    workersPerManager: v.number(),
    hasDuePointers: v.boolean(),
    nextPointerLeased: v.boolean(),
    nextPointerVestingTime: v.union(v.null(), v.number()),
    pointers: v.array(
      v.object({
        pointerId: v.id("queuePointers"),
        queueId: v.string(),
        pointerLeaseId: v.string(),
        slotNumber: v.number(),
        slotLeaseId: v.string(),
      })
    ),
  }),
  handler: async (ctx, args) => claimAvailablePointersInTransaction(ctx, args),
})

export const runScanner = internalMutation({
  args: {
    leaseId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const config = await resolveConfig(ctx)
    const now = Date.now()
    const result = await claimAvailablePointersInTransaction(ctx, {
      leaseId: args.leaseId,
      config,
      now,
    })

    if (!result.valid) {
      return null
    }

    if (result.pointers.length === 0) {
      const confirmation = await getScannerInspectionForConfirmation(ctx, config, now)
      if (!confirmation.hasDuePointers) {
        await parkScannerInTransaction(
          ctx,
          {
            leaseId: args.leaseId,
            nextPointerVestingTime: confirmation.nextPointerVestingTime ?? undefined,
          },
          now
        )
      } else {
        await rescheduleScannerInTransaction(
          ctx,
          {
            leaseId: args.leaseId,
            hasWork: confirmation.availableSlotCount > 0,
          },
          config,
          now
        )
      }
      return null
    }

    for (const pointer of result.pointers) {
      await ctx.scheduler.runAfter(0, internal.scanner.runManager, {
        pointerId: pointer.pointerId,
        queueId: pointer.queueId,
        pointerLeaseId: pointer.pointerLeaseId,
        orderBy: result.orderBy,
        workersPerManager: result.workersPerManager,
        slotNumber: pointer.slotNumber,
        slotLeaseId: pointer.slotLeaseId,
      })
    }

    await rescheduleScannerInTransaction(
      ctx,
      {
        leaseId: args.leaseId,
        hasWork: true,
      },
      config,
      now
    )

    return null
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
    return await parkScannerInTransaction(ctx, args, now)
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
    return await rescheduleScannerInTransaction(ctx, args, config, now)
  },
})

export const tryClaimManagerSlot = internalMutation({
  args: {
    slotNumber: v.number(),
    pointerId: v.id("queuePointers"),
    pointerLeaseId: v.string(),
    queueId: v.string(),
    slotLeaseId: v.optional(v.string()),
  },
  returns: v.object({
    claimed: v.boolean(),
    slotLeaseId: v.optional(v.string()),
  }),
  handler: async (ctx, args) => {
    const config = await resolveConfig(ctx)
    if (args.slotNumber < 0 || args.slotNumber >= config.managerSlots) {
      return { claimed: false }
    }

    const now = Date.now()
    const slot = await ctx.db
      .query("managerSlots")
      .withIndex("by_slot_number", (q) => q.eq("slotNumber", args.slotNumber))
      .unique()
    if (!slot) {
      return { claimed: false }
    }

    const pointer = await ctx.db.get(args.pointerId)
    if (
      !pointer ||
      pointer.queueId !== args.queueId ||
      pointer.leaseId !== args.pointerLeaseId ||
      (pointer.leaseExpiry !== undefined && pointer.leaseExpiry <= now)
    ) {
      return { claimed: false }
    }

    if (args.slotLeaseId) {
      if (
        slot.leaseId === args.slotLeaseId &&
        slot.pointerId === args.pointerId &&
        slot.queueId === args.queueId &&
        slot.leaseExpiry !== undefined &&
        slot.leaseExpiry > now
      ) {
        return { claimed: true, slotLeaseId: args.slotLeaseId }
      }
      return { claimed: false }
    }

    if (slot.leaseExpiry && slot.leaseExpiry > now) {
      return { claimed: false }
    }

    const slotLeaseId = uuid()
    await ctx.db.patch(slot._id, {
      leaseId: slotLeaseId,
      leaseExpiry: now + MANAGER_SLOT_LEASE_MS,
      pointerId: args.pointerId,
      queueId: args.queueId,
    })

    console.info("[quick] manager slot claimed", {
      queueId: args.queueId,
      pointerId: args.pointerId,
      pointerLeaseId: args.pointerLeaseId,
      slotNumber: args.slotNumber,
      slotLeaseId,
    })

    return { claimed: true, slotLeaseId }
  },
})

export const releaseManagerSlot = internalMutation({
  args: {
    slotNumber: v.number(),
    slotLeaseId: v.string(),
  },
  returns: v.boolean(),
  handler: async (ctx, args) => {
    const slot = await ctx.db
      .query("managerSlots")
      .withIndex("by_slot_number", (q) => q.eq("slotNumber", args.slotNumber))
      .unique()
    if (!slot || slot.leaseId !== args.slotLeaseId) {
      return false
    }
    await ctx.db.patch(slot._id, {
      leaseId: undefined,
      leaseExpiry: undefined,
      pointerId: undefined,
      queueId: undefined,
    })
    return true
  },
})

export const runManager = internalAction({
  args: {
    pointerId: v.id("queuePointers"),
    queueId: v.string(),
    pointerLeaseId: v.string(),
    orderBy: v.union(v.literal("vesting"), v.literal("fifo")),
    workersPerManager: v.number(),
    slotNumber: v.number(),
    slotLeaseId: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    console.info("[quick] runManager started", {
      queueId: args.queueId,
      pointerId: args.pointerId,
      pointerLeaseId: args.pointerLeaseId,
      slotNumber: args.slotNumber,
    })

    const slotClaim = await runMutationWithOccRetry(
      () =>
        ctx.runMutation(internal.scanner.tryClaimManagerSlot, {
          slotNumber: args.slotNumber,
          pointerId: args.pointerId,
          pointerLeaseId: args.pointerLeaseId,
          queueId: args.queueId,
          slotLeaseId: args.slotLeaseId,
        }),
      {
        operation: "tryClaimManagerSlot",
        context: {
          queueId: args.queueId,
          pointerId: args.pointerId,
          pointerLeaseId: args.pointerLeaseId,
          slotNumber: args.slotNumber,
        },
      }
    )

    if (!slotClaim) {
      return
    }

    if (!slotClaim.claimed || !slotClaim.slotLeaseId) {
      await runMutationWithOccRetry(
        () =>
          ctx.runMutation(internal.scanner.finalizePointer, {
            pointerId: args.pointerId,
            pointerLeaseId: args.pointerLeaseId,
            isEmpty: false,
            orderBy: args.orderBy,
          }),
        {
          operation: "finalizePointer",
          context: {
            queueId: args.queueId,
            pointerId: args.pointerId,
            pointerLeaseId: args.pointerLeaseId,
            slotNumber: args.slotNumber,
            slotLeaseId: slotClaim.slotLeaseId,
          },
        }
      )
      return
    }

    let pointerFinalized = false
    let runError: unknown = null
    try {
      const items = await runMutationWithOccRetry(
        () =>
          ctx.runMutation(internal.lib.dequeue, {
            queueId: args.queueId,
            limit: args.workersPerManager,
            orderBy: args.orderBy,
          }),
        {
          operation: "dequeue",
          context: {
            queueId: args.queueId,
            pointerId: args.pointerId,
            pointerLeaseId: args.pointerLeaseId,
            slotNumber: args.slotNumber,
            slotLeaseId: slotClaim.slotLeaseId,
          },
        }
      )

      if (!items) {
        return
      }

      if (items.length === 0) {
        await runMutationWithOccRetry(
          () =>
            ctx.runMutation(internal.scanner.finalizePointer, {
              pointerId: args.pointerId,
              pointerLeaseId: args.pointerLeaseId,
              isEmpty: true,
              orderBy: args.orderBy,
            }),
          {
            operation: "finalizePointer",
            context: {
              queueId: args.queueId,
              pointerId: args.pointerId,
              pointerLeaseId: args.pointerLeaseId,
              slotNumber: args.slotNumber,
              slotLeaseId: slotClaim.slotLeaseId,
              isEmpty: true,
            },
          }
        )
        pointerFinalized = true
        return
      }

      await Promise.allSettled(
        items.map((item) =>
          (item.item.handlerType ?? "action") === "mutation"
            ? ctx.runMutation(internal.scanner.runWorkerMutation, {
                itemId: item.item._id,
                leaseId: item.leaseId,
                handler: item.item.handler,
                payload: item.item.payload,
                queueId: args.queueId,
              })
            : ctx.runAction(internal.scanner.runWorkerAction, {
                itemId: item.item._id,
                leaseId: item.leaseId,
                handler: item.item.handler,
                handlerType: item.item.handlerType ?? "action",
                payload: item.item.payload,
                queueId: args.queueId,
              })
        )
      )

      await runMutationWithOccRetry(
        () =>
          ctx.runMutation(internal.scanner.finalizePointer, {
            pointerId: args.pointerId,
            pointerLeaseId: args.pointerLeaseId,
            isEmpty: false,
            orderBy: args.orderBy,
          }),
        {
          operation: "finalizePointer",
          context: {
            queueId: args.queueId,
            pointerId: args.pointerId,
            pointerLeaseId: args.pointerLeaseId,
            slotNumber: args.slotNumber,
            slotLeaseId: slotClaim.slotLeaseId,
            isEmpty: false,
          },
        }
      )
      pointerFinalized = true
    } catch (error) {
      runError = error
      console.error("[quick] runManager failed before cleanup", {
        queueId: args.queueId,
        pointerId: args.pointerId,
        pointerLeaseId: args.pointerLeaseId,
        slotNumber: args.slotNumber,
        slotLeaseId: slotClaim.slotLeaseId,
      }, error)
    } finally {
      let cleanupError: unknown = null

      if (!pointerFinalized) {
        try {
          await runMutationWithOccRetry(
            () =>
              ctx.runMutation(internal.scanner.finalizePointer, {
                pointerId: args.pointerId,
                pointerLeaseId: args.pointerLeaseId,
                isEmpty: false,
                orderBy: args.orderBy,
              }),
            {
              operation: "finalizePointer",
              context: {
                queueId: args.queueId,
                pointerId: args.pointerId,
                pointerLeaseId: args.pointerLeaseId,
                slotNumber: args.slotNumber,
                slotLeaseId: slotClaim.slotLeaseId,
                isEmpty: false,
                phase: "cleanup",
              },
            }
          )
          pointerFinalized = true
        } catch (error) {
          cleanupError = error
          console.error("[quick] finalizePointer failed during runManager cleanup", {
            queueId: args.queueId,
            pointerId: args.pointerId,
            pointerLeaseId: args.pointerLeaseId,
            slotNumber: args.slotNumber,
            slotLeaseId: slotClaim.slotLeaseId,
          }, error)
        }
      }

      try {
        const released = await runMutationWithOccRetry(
          () =>
            ctx.runMutation(internal.scanner.releaseManagerSlot, {
              slotNumber: args.slotNumber,
              slotLeaseId: slotClaim.slotLeaseId,
            }),
          {
            operation: "releaseManagerSlot",
            context: {
              queueId: args.queueId,
              pointerId: args.pointerId,
              pointerLeaseId: args.pointerLeaseId,
              slotNumber: args.slotNumber,
              slotLeaseId: slotClaim.slotLeaseId,
            },
          }
        )
        if (!released) {
          console.error("[quick] releaseManagerSlot returned false during runManager cleanup", {
            queueId: args.queueId,
            pointerId: args.pointerId,
            pointerLeaseId: args.pointerLeaseId,
            slotNumber: args.slotNumber,
            slotLeaseId: slotClaim.slotLeaseId,
          })
        }
      } catch (error) {
        cleanupError ??= error
        console.error("[quick] releaseManagerSlot threw during runManager cleanup", {
          queueId: args.queueId,
          pointerId: args.pointerId,
          pointerLeaseId: args.pointerLeaseId,
          slotNumber: args.slotNumber,
          slotLeaseId: slotClaim.slotLeaseId,
        }, error)
      }

      if (pointerFinalized) {
        console.info("[quick] runManager cleaned up", {
          queueId: args.queueId,
          pointerId: args.pointerId,
          pointerLeaseId: args.pointerLeaseId,
          slotNumber: args.slotNumber,
          slotLeaseId: slotClaim.slotLeaseId,
        })
      }
      runError = cleanupError ?? runError
    }

    if (runError) {
      throw runError
    }
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

    let nextVestingTime: number
    let nextPriority = DEFAULT_PRIORITY
    let nextLastActiveTime: number

    let nextState = await withSnapshotQueries(ctx).runSnapshotQuery(
      getPointerFinalizationSnapshotRef,
      {
        queueId: pointer.queueId,
        orderBy: args.orderBy,
        now,
      }
    )

    if (args.orderBy === "vesting" || pointerStateNeedsConfirmation(nextState, now)) {
      // Enqueues skip promotion while a pointer is actively leased, so vesting
      // finalization must confirm even ready snapshot results before releasing.
      nextState = await computePointerFinalizationState(ctx.db, {
        queueId: pointer.queueId,
        orderBy: args.orderBy,
        now,
      })
    }

    if (!nextState.hasItems || !nextState.state) {
      if (!cachedConfig) {
        cachedConfig = await resolveConfig(ctx)
      }
      // Empty queues should park until the GC window to avoid hot-looping
      // over pointers that currently have no items.
      nextVestingTime = now + cachedConfig.minInactiveBeforeDeleteMs
      nextLastActiveTime = now
    } else if (
      args.orderBy === "fifo" &&
      nextState.state.leaseExpiry !== undefined &&
      nextState.state.leaseExpiry > now
    ) {
      // FIFO head is currently leased. Recheck soon so completion can unblock
      // following items without waiting for full lease expiry.
      if (!cachedConfig) {
        cachedConfig = await resolveConfig(ctx)
      }
      nextVestingTime = Math.min(
        nextState.state.leaseExpiry,
        now + cachedConfig.scannerBackoffMinMs
      )
      nextLastActiveTime = now
    } else {
      nextPriority = nextState.state.priority
      nextVestingTime = Math.max(now, nextState.state.vestingTime)
      nextLastActiveTime = now
    }

    await ctx.db.patch(args.pointerId, {
      leaseId: undefined,
      leaseExpiry: undefined,
      priority: nextPriority,
      vestingTime: nextVestingTime,
      lastActiveTime: nextLastActiveTime,
    })

    if (nextVestingTime <= now) {
      await ctx.scheduler.runAfter(0, internal.scanner.tryWakeScanner, {
        reason: "pointerReady",
      })
    } else {
      // Once the last queue item drains, there is no useful work left for the
      // scanner to poll. Clear any follow-up wake so convex-test doesn't keep
      // a stale scheduled function alive past test completion.
      await clearScannerScheduleIfComponentIdle(ctx)
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

async function schedulePointerStateUpdateIfBetter(
  ctx: MutationCtx,
  queueId: string,
  priority: number,
  vestingTime: number
) {
  await ctx.scheduler.runAfter(0, internal.lib.updatePointerState, {
    queueId,
    priority,
    vestingTime,
  })
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
          if (config.defaultOrderBy === "vesting") {
            await schedulePointerStateUpdateIfBetter(
              ctx,
              item.queueId,
              item.priority,
              retryVestingTime
            )
          }
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
        if (config.defaultOrderBy === "vesting") {
          await schedulePointerStateUpdateIfBetter(
            ctx,
            item.queueId,
            item.priority,
            retryVestingTime
          )
        }
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
