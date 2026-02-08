import { describe, expect, test } from "vitest";
import { initConvexTest } from "./setup.test";
import { api } from "./_generated/api";

describe("example", () => {
  test("enqueueCommentAction enqueues a job", async () => {
    const t = initConvexTest();

    const itemId = await t.mutation(api.example.enqueueCommentAction, {
      text: "hello",
      targetId: "post-1",
    });

    const stats = await t.query(api.example.queueStats, { targetId: "post-1" });
    expect(itemId).toBeDefined();
    expect(stats.itemCount).toBe(1);
  });

  test("enqueueCommentMutation enqueues a job", async () => {
    const t = initConvexTest();

    const itemId = await t.mutation(api.example.enqueueCommentMutation, {
      text: "hello",
      targetId: "post-2",
    });

    const stats = await t.query(api.example.queueStats, { targetId: "post-2" });
    expect(itemId).toBeDefined();
    expect(stats.itemCount).toBe(1);
  });
});
