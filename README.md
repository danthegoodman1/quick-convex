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
  retryByDefault: true,
  defaultRetryBehavior: {
    maxAttempts: 5,
    initialBackoffMs: 250,
    base: 2,
  },
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
      runAfter: 5_000,
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

### Guaranteed onComplete callback

`onComplete` is always a mutation handle and runs for both action and mutation workers.
Quick persists completion state (`phase: "onComplete"`) and resumes there after crashes, so completion handlers are retried safely.

```ts
import { vOnCompleteArgs } from "@danthegoodman/quick-convex";

export const onEmailComplete = mutation({
  args: vOnCompleteArgs(v.object({ userId: v.string() })),
  handler: async (_ctx, args) => {
    // args: { workId, context, status, result }
    return null;
  },
});

export const enqueueEmail = mutation({
  args: { userId: v.string() },
  handler: async (ctx, args) => {
    return await quickVesting.enqueueAction(ctx, {
      queueId: args.userId,
      fn: api.jobs.sendEmailWorker,
      args: { userId: args.userId },
      onComplete: {
        fn: api.jobs.onEmailComplete,
        context: { userId: args.userId },
      },
    });
  },
});
```

`vOnCompleteArgs()` without a context validator uses `context: any`.

### Retry configuration

- Set class defaults in `new Quick(component, { retryByDefault, defaultRetryBehavior })`.
- Override per item with `retry`:
- `retry: false` disables retries for that item.
- `retry: true` uses class/default retry behavior.
- `retry: { maxAttempts, initialBackoffMs, base }` sets per-item behavior.

## Queue behavior

- Supports `"vesting"` and `"fifo"` order modes.
- Uses pointer-based scanning and leasing for concurrent processing.
- Includes cron-based recovery and pointer garbage collection.

### Choosing an ordering mode

- Use `"vesting"` when throughput is the priority. Ready items can run as soon as they are due, so delayed/retried items do not block newer ready work in the same queue.
- Use `"fifo"` when strict per-`queueId` ordering is required. This enforces head-of-line semantics for that ordering domain.
- In `"fifo"` mode, a delayed/retrying head item stalls the rest of that same `queueId` until it is ready again.

In practice, FIFO queues are often a cleaner and more performant alternative to creating many `maxParallelism: 1` workpools (one per ordering domain). With Quick FIFO, use `queueId` as the domain key (for example `userId`, `accountId`, or `aggregateId`), and each domain stays ordered while different domains can still process in parallel.

## Compare to Convex Workpools

Quick is heavily inspired by Convex Workpools and the QuiCK paper. Workpools are excellent and production-proven, and this component builds on many of the same ideas.

### Workpools strengths

- Slightly lighter weight runtime model.
- Officially maintained by the Convex team.
- Operationally simpler in many common setups.
- Production-proven at scale in Convex.

### Workpools edge case to be aware of

- In some bursty scale-up patterns (idle `0` to many scheduled items), contention can appear around work claiming.
- Today, Workpools do not retry onComplete timeout failures.

### Quick strengths

- Multiple ordering modes, especially strict per-domain FIFO via `queueId`.
- FIFO is much easier to model than emulating ordering via many `maxParallelism: 1` workpools.
- Faster and lower contention ramp from idle to heavy load.
- OnComplete timeout failures are retried up to 2 times.
- QuiCK model proven at scale at Apple (but not this implementation!)

### Tradeoff to keep in mind

- Quick is a bit heavier per unit work when load is low or not amortized (more queue-management actions/mutations around each job).

## Example

See `/Users/dangoodman/code/quick-convex/example/convex/example.ts` for end-to-end usage with `Quick`.
