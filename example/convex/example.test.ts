import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { initConvexTest } from "./setup.test";
import { api } from "./_generated/api";

describe("example", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
  });

  test("enqueueCommentAction enqueues a job", async () => {
    const t = initConvexTest();

    const itemId = await t.mutation(api.example.enqueueCommentAction, {
      text: "hello",
      targetId: "post-1",
    });

    const stats = await t.query(api.example.queueStats, {
      targetId: "post-1",
      mode: "vesting",
    });
    expect(itemId).toBeDefined();
    expect(stats.itemCount).toBe(1);
  });

  test("enqueueCommentMutation enqueues a job", async () => {
    const t = initConvexTest();

    const itemId = await t.mutation(api.example.enqueueCommentMutation, {
      text: "hello",
      targetId: "post-2",
    });

    const stats = await t.query(api.example.queueStats, {
      targetId: "post-2",
      mode: "vesting",
    });
    expect(itemId).toBeDefined();
    expect(stats.itemCount).toBe(1);
  });

  test("enqueueCommentBatchActionFifo enqueues jobs in fifo component", async () => {
    const t = initConvexTest();

    const itemIds = await t.mutation(api.example.enqueueCommentBatchActionFifo, {
      targetId: "post-fifo",
    });

    const stats = await t.query(api.example.queueStats, {
      targetId: "post-fifo",
      mode: "fifo",
    });
    expect(itemIds).toHaveLength(2);
    expect(stats.itemCount).toBe(2);
  });
});
