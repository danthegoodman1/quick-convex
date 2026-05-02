import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { initConvexTest } from "./setup.test";
import { api } from "./_generated/api";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

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

  test("scheduled drain runs an action-backed component queue", async () => {
    vi.useRealTimers();
    const t = initConvexTest();
    const targetId = "post-action-scheduled-drain";

    await t.mutation(api.example.enqueueCommentAction, {
      text: "hello",
      targetId,
    });

    // This follows the public component path used by consumers: app mutation
    // enqueues work, scanner schedules a manager, and manager runs an action.
    for (let i = 0; i < 100; i++) {
      await t.finishInProgressScheduledFunctions();
      const stats = await t.query(api.example.queueStats, {
        targetId,
        mode: "vesting",
      });
      if (stats.itemCount === 0) {
        break;
      }
      await sleep(25);
    }

    const stats = await t.query(api.example.queueStats, {
      targetId,
      mode: "vesting",
    });

    expect(stats.itemCount).toBe(0);
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
