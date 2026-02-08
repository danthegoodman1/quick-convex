/// <reference types="vite/client" />

import { describe, expect, test } from "vitest"
import { initConvexTest } from "./setup.test.js"

describe("async iterator", () => {
  test("iterates over query results with break", async () => {
    const t = initConvexTest()

    const queueId = "test-queue"
    const now = 1000

    // Insert some items with varying vesting times
    await t.run(async (ctx) => {
      await ctx.db.insert("queueItems", {
        queueId,
        payload: { value: 1 },
        handler: "testHandler",
        vestingTime: 500, // before now
        errorCount: 0,
      })
      await ctx.db.insert("queueItems", {
        queueId,
        payload: { value: 2 },
        handler: "testHandler",
        vestingTime: 800, // before now
        errorCount: 0,
      })
      await ctx.db.insert("queueItems", {
        queueId,
        payload: { value: 3 },
        handler: "testHandler",
        vestingTime: 1500, // after now - should break
        errorCount: 0,
      })
    })

    // Test the async iterator pattern
    const result = await t.run(async (ctx) => {
      const query = ctx.db
        .query("queueItems")
        .withIndex("by_queue_fifo", (q) => q.eq("queueId", queueId))

      const items: Array<{ payload: unknown; vestingTime: number }> = []

      for await (const item of query) {
        if (item.vestingTime > now) {
          break
        }
        items.push({ payload: item.payload, vestingTime: item.vestingTime })
      }

      return items
    })

    expect(result).toHaveLength(2)
    expect(result[0].payload).toEqual({ value: 1 })
    expect(result[1].payload).toEqual({ value: 2 })
  })
})
