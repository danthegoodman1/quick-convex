import { makeFunctionReference } from "convex/server";
import { Quick } from "./index.js";

declare const quick: Quick;
declare const ctx: { runMutation: (...args: any[]) => Promise<any> };

const actionWorker = makeFunctionReference<
  "action",
  { payload: { value: number }; queueId: string }
>("types:testAction");
const actionWorkerAlt = makeFunctionReference<
  "action",
  { payload: { name: string }; queueId: string }
>("types:testActionAlt");
const mutationWorker = makeFunctionReference<
  "mutation",
  { payload: { value: number }; queueId: string }
>("types:testMutation");
const wrongShapeWorker = makeFunctionReference<
  "action",
  { payload: { value: number }; queueId: string; extra: string }
>("types:testWrong");

void quick.enqueueAction(ctx, {
  queueId: "queue-a",
  fn: actionWorker,
  args: { value: 1 },
});

void quick.enqueueMutation(ctx, {
  queueId: "queue-a",
  fn: mutationWorker,
  args: { value: 1 },
});

void quick.enqueueBatchAction(ctx, [
  {
    queueId: "queue-a",
    fn: actionWorker,
    args: { value: 1 },
  },
  {
    queueId: "queue-a",
    fn: actionWorkerAlt,
    args: { name: "ok" },
  },
]);

// @ts-expect-error queueId is required
void quick.enqueueAction(ctx, { fn: actionWorker, args: { value: 1 } });

// @ts-expect-error wrong function type for enqueueAction
void quick.enqueueAction(ctx, { queueId: "queue-a", fn: mutationWorker, args: { value: 1 } });

// @ts-expect-error args type mismatch
void quick.enqueueMutation(ctx, { queueId: "queue-a", fn: mutationWorker, args: { value: "wrong" } });

// @ts-expect-error worker args must be exactly payload + queueId
void quick.enqueueAction(ctx, { queueId: "queue-a", fn: wrongShapeWorker, args: { value: 1 } });

void quick.enqueueBatchAction(ctx, [
  {
    queueId: "queue-a",
    fn: actionWorker,
    args: { value: 1 },
  },
  // @ts-expect-error batch item payload inference should follow each item function ref
  {
    queueId: "queue-a",
    fn: actionWorkerAlt,
    args: { value: 2 },
  },
]);

// @ts-expect-error mutation workers are not allowed in action batch
void quick.enqueueBatchAction(ctx, [{ queueId: "queue-a", fn: mutationWorker, args: { value: 1 } }]);
