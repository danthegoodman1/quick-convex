# QuiCK Convex

A [QuiCK](https://www.foundationdb.org/files/QuiCK.pdf)-style queue implementation as a Convex component.

## Installation

```ts
// convex/convex.config.ts
import { defineApp } from "convex/server";
import quickConvex from "@danthegoodman/quick-convex/convex.config.js";

const app = defineApp();
app.use(quickConvex, { name: "quickVesting" });
app.use(quickConvex, { name: "quickFifo" });

export default app;
```

## Quick Class API

Use the class API for enqueueing work:

```ts
import { Quick } from "@danthegoodman/quick-convex";
import { components, api } from "./_generated/api";

const quickVesting = new Quick(components.quickVesting, {
  defaultOrderBy: "vesting",
});

const quickFifo = new Quick(components.quickFifo, {
  defaultOrderBy: "fifo",
});
```

### Worker function contract

Workers must accept this argument shape:

```ts
{
  payload: TPayload;
  queueId: string;
}
```

This applies to both action and mutation workers.

### Enqueue action worker

```ts
export const enqueueEmail = mutation({
  args: { userId: v.string() },
  handler: async (ctx, args) => {
    return await quickVesting.enqueueAction(ctx, {
      queueId: args.userId,
      fn: api.jobs.sendEmailWorker,
      args: { userId: args.userId },
      delayMs: 5_000,
    });
  },
});
```

### Enqueue mutation worker

```ts
export const enqueueMutationWorker = mutation({
  args: { userId: v.string() },
  handler: async (ctx, args) => {
    return await quickVesting.enqueueMutation(ctx, {
      queueId: args.userId,
      fn: api.jobs.processUserMutationWorker,
      args: { userId: args.userId },
    });
  },
});
```

### Batch enqueue (multi-function)

`enqueueBatchAction` and `enqueueBatchMutation` accept per-item function refs and dedupe handle creation per unique function in the batch.

```ts
export const enqueueBatch = mutation({
  args: { queueId: v.string() },
  handler: async (ctx, args) => {
    return await quickFifo.enqueueBatchAction(ctx, [
      {
        queueId: args.queueId,
        fn: api.jobs.workerA,
        args: { value: 1 },
      },
      {
        queueId: args.queueId,
        fn: api.jobs.workerA,
        args: { value: 2 },
      },
      {
        queueId: args.queueId,
        fn: api.jobs.workerB,
        args: { value: 3 },
      },
    ]);
  },
});
```

## Queue behavior

- Supports `"vesting"` and `"fifo"` order modes.
- Uses pointer-based scanning and leasing for concurrent processing.
- Includes cron-based recovery and pointer garbage collection.

## Example

See `/Users/dangoodman/code/quick-convex/example/convex/example.ts` for end-to-end usage with `Quick`.
