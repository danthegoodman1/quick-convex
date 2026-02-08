import {
  createFunctionHandle,
  makeFunctionReference,
  type FunctionReference,
  getFunctionName,
} from "convex/server";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import type { ComponentApi } from "../component/_generated/component.js";
import { Quick } from "./index.js";

vi.mock("convex/server", async () => {
  const actual = await vi.importActual<typeof import("convex/server")>(
    "convex/server",
  );
  return {
    ...actual,
    createFunctionHandle: vi.fn(),
  };
});

const actionWorkerRef = makeFunctionReference<
  "action",
  { payload: { value: number }; queueId: string }
>("index.test:workerAction");
const actionWorkerTwoRef = makeFunctionReference<
  "action",
  { payload: { value: number }; queueId: string }
>("index.test:workerActionTwo");
const mutationWorkerRef = makeFunctionReference<
  "mutation",
  { payload: { value: number }; queueId: string }
>("index.test:workerMutation");

function makeComponentApiMock() {
  const enqueueRef = makeFunctionReference<
    "mutation",
    {
      queueId: string;
      payload: any;
      handler: string;
      handlerType?: "action" | "mutation";
      delayMs?: number;
      config?: { defaultOrderBy: "vesting" | "fifo" };
    }
  >("component.lib:enqueue");

  const enqueueBatchRef = makeFunctionReference<
    "mutation",
    {
      items: Array<{
        queueId: string;
        payload: any;
        handler: string;
        handlerType?: "action" | "mutation";
        delayMs?: number;
      }>;
      config?: { defaultOrderBy: "vesting" | "fifo" };
    }
  >("component.lib:enqueueBatch");

  return {
    lib: {
      enqueue: enqueueRef,
      enqueueBatch: enqueueBatchRef,
    },
  } as unknown as ComponentApi;
}

function makeCtxMock() {
  return {
    runMutation: vi.fn(),
  };
}

describe("Quick client", () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  afterEach(() => {
    vi.resetAllMocks();
  });

  test("enqueueAction calls enqueue with handlerType action", async () => {
    const ctx = makeCtxMock();
    const component = makeComponentApiMock();
    const quick = new Quick(component);
    const handleMock = vi.mocked(createFunctionHandle);
    handleMock.mockResolvedValueOnce("action-handle" as any);
    ctx.runMutation.mockResolvedValueOnce("item-id");

    const result = await quick.enqueueAction(ctx, {
      queueId: "queue-a",
      fn: actionWorkerRef,
      args: { value: 1 },
      delayMs: 25,
    });

    expect(result).toBe("item-id");
    expect(handleMock).toHaveBeenCalledTimes(1);
    expect(handleMock).toHaveBeenCalledWith(actionWorkerRef);
    expect(ctx.runMutation).toHaveBeenCalledTimes(1);
    expect(ctx.runMutation).toHaveBeenCalledWith(component.lib.enqueue, {
      queueId: "queue-a",
      payload: { value: 1 },
      handler: "action-handle",
      handlerType: "action",
      delayMs: 25,
    });
  });

  test("enqueueMutation calls enqueue with handlerType mutation", async () => {
    const ctx = makeCtxMock();
    const component = makeComponentApiMock();
    const quick = new Quick(component);
    const handleMock = vi.mocked(createFunctionHandle);
    handleMock.mockResolvedValueOnce("mutation-handle" as any);
    ctx.runMutation.mockResolvedValueOnce("item-id");

    const result = await quick.enqueueMutation(ctx, {
      queueId: "queue-b",
      fn: mutationWorkerRef,
      args: { value: 2 },
    });

    expect(result).toBe("item-id");
    expect(handleMock).toHaveBeenCalledTimes(1);
    expect(handleMock).toHaveBeenCalledWith(mutationWorkerRef);
    expect(ctx.runMutation).toHaveBeenCalledTimes(1);
    expect(ctx.runMutation).toHaveBeenCalledWith(component.lib.enqueue, {
      queueId: "queue-b",
      payload: { value: 2 },
      handler: "mutation-handle",
      handlerType: "mutation",
      delayMs: undefined,
    });
  });

  test("enqueueBatchAction dedupes createFunctionHandle per unique function", async () => {
    const ctx = makeCtxMock();
    const component = makeComponentApiMock();
    const quick = new Quick(component);
    const handleMock = vi.mocked(createFunctionHandle);
    handleMock.mockImplementation(
      async (fn: FunctionReference<any, any, any>) =>
        `handle:${getFunctionName(fn)}` as any,
    );
    ctx.runMutation.mockResolvedValueOnce(["id-1", "id-2", "id-3"]);

    const result = await quick.enqueueBatchAction(ctx, [
      { queueId: "queue-c", fn: actionWorkerRef, args: { value: 1 } },
      { queueId: "queue-c", fn: actionWorkerRef, args: { value: 2 } },
      { queueId: "queue-c", fn: actionWorkerTwoRef, args: { value: 3 } },
    ]);

    expect(result).toEqual(["id-1", "id-2", "id-3"]);
    expect(handleMock).toHaveBeenCalledTimes(2);
    expect(handleMock).toHaveBeenNthCalledWith(1, actionWorkerRef);
    expect(handleMock).toHaveBeenNthCalledWith(2, actionWorkerTwoRef);
    expect(ctx.runMutation).toHaveBeenCalledTimes(1);
    expect(ctx.runMutation).toHaveBeenCalledWith(component.lib.enqueueBatch, {
      items: [
        {
          queueId: "queue-c",
          payload: { value: 1 },
          handler: "handle:index.test:workerAction",
          handlerType: "action",
          delayMs: undefined,
        },
        {
          queueId: "queue-c",
          payload: { value: 2 },
          handler: "handle:index.test:workerAction",
          handlerType: "action",
          delayMs: undefined,
        },
        {
          queueId: "queue-c",
          payload: { value: 3 },
          handler: "handle:index.test:workerActionTwo",
          handlerType: "action",
          delayMs: undefined,
        },
      ],
    });
  });

  test("constructor defaultOrderBy is forwarded to enqueue and enqueueBatch", async () => {
    const ctx = makeCtxMock();
    const component = makeComponentApiMock();
    const quick = new Quick(component, { defaultOrderBy: "fifo" });
    const handleMock = vi.mocked(createFunctionHandle);
    handleMock.mockImplementation(
      async (fn: FunctionReference<any, any, any>) =>
        `handle:${getFunctionName(fn)}` as any,
    );
    ctx.runMutation.mockResolvedValueOnce("item-id");
    ctx.runMutation.mockResolvedValueOnce(["id-1"]);

    await quick.enqueueAction(ctx, {
      queueId: "queue-config",
      fn: actionWorkerRef,
      args: { value: 99 },
    });

    await quick.enqueueBatchAction(ctx, [
      { queueId: "queue-config", fn: actionWorkerRef, args: { value: 100 } },
    ]);

    expect(ctx.runMutation).toHaveBeenNthCalledWith(1, component.lib.enqueue, {
      queueId: "queue-config",
      payload: { value: 99 },
      handler: "handle:index.test:workerAction",
      handlerType: "action",
      delayMs: undefined,
      config: { defaultOrderBy: "fifo" },
    });

    expect(ctx.runMutation).toHaveBeenNthCalledWith(
      2,
      component.lib.enqueueBatch,
      {
        items: [
          {
            queueId: "queue-config",
            payload: { value: 100 },
            handler: "handle:index.test:workerAction",
            handlerType: "action",
            delayMs: undefined,
          },
        ],
        config: { defaultOrderBy: "fifo" },
      },
    );
  });
});
