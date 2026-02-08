import {
  createFunctionHandle,
  getFunctionName,
  type FunctionArgs,
  type FunctionReference,
  type GenericActionCtx,
  type GenericDataModel,
} from "convex/server";
import type { ComponentApi } from "../component/_generated/component.js";

type WorkerHandlerType = "action" | "mutation";
export type QuickOrder = "vesting" | "fifo";

type WorkerArgShape<TPayload> = {
  payload: TPayload;
  queueId: string;
};

type WorkerRef<
  TType extends WorkerHandlerType,
  TPayload = unknown,
> = FunctionReference<TType, "public" | "internal", WorkerArgShape<TPayload>>;

type BatchInputItem<TRef extends FunctionReference<any, any, any>> = {
  queueId: string;
  fn: TRef;
  args: unknown;
  runAfter?: number;
  runAt?: number;
};

type NormalizedBatchItem<TItem> = TItem extends BatchInputItem<
  infer TRef extends FunctionReference<any, any, any>
>
  ? {
      queueId: string;
      fn: TRef;
      args: WorkerPayload<TRef>;
      runAfter?: number;
      runAt?: number;
    }
  : never;

export type ActionWorkerRef<TPayload = unknown> = WorkerRef<"action", TPayload>;
export type MutationWorkerRef<TPayload = unknown> = WorkerRef<
  "mutation",
  TPayload
>;

export type WorkerPayload<
  Fn extends FunctionReference<any, any, any>,
> = FunctionArgs<Fn> extends WorkerArgShape<infer TPayload>
  ? Exclude<keyof FunctionArgs<Fn>, keyof WorkerArgShape<TPayload>> extends never
    ? TPayload
    : never
  : never;

export type QuickCtx = Pick<
  GenericActionCtx<GenericDataModel>,
  "runMutation"
>;

export type QuickOptions = {
  defaultOrderBy?: QuickOrder;
};

export type EnqueueActionRequest<
  Fn extends ActionWorkerRef<any> = ActionWorkerRef<any>,
> = {
  queueId: string;
  fn: Fn;
  args: WorkerPayload<Fn>;
  runAfter?: number;
  runAt?: number;
};

export type EnqueueMutationRequest<
  Fn extends MutationWorkerRef<any> = MutationWorkerRef<any>,
> = {
  queueId: string;
  fn: Fn;
  args: WorkerPayload<Fn>;
  runAfter?: number;
  runAt?: number;
};

export type EnqueueBatchActionItem<
  Fn extends ActionWorkerRef<any> = ActionWorkerRef<any>,
> = EnqueueActionRequest<Fn>;

export type EnqueueBatchMutationItem<
  Fn extends MutationWorkerRef<any> = MutationWorkerRef<any>,
> = EnqueueMutationRequest<Fn>;

export class Quick {
  private readonly config?: { defaultOrderBy: QuickOrder };

  constructor(
    private readonly component: ComponentApi,
    options?: QuickOptions,
  ) {
    this.config = options?.defaultOrderBy
      ? { defaultOrderBy: options.defaultOrderBy }
      : undefined;
  }

  async enqueueAction<Fn extends ActionWorkerRef<any>>(
    ctx: QuickCtx,
    request: EnqueueActionRequest<Fn>,
  ) {
    return await this.enqueueWithType(ctx, request, "action");
  }

  async enqueueMutation<Fn extends MutationWorkerRef<any>>(
    ctx: QuickCtx,
    request: EnqueueMutationRequest<Fn>,
  ) {
    return await this.enqueueWithType(ctx, request, "mutation");
  }

  async enqueueBatchAction<
    Items extends ReadonlyArray<BatchInputItem<ActionWorkerRef<any>>>,
  >(
    ctx: QuickCtx,
    items: Items & { [K in keyof Items]: NormalizedBatchItem<Items[K]> },
  ) {
    return await this.enqueueBatchWithType(ctx, items, "action");
  }

  async enqueueBatchMutation<
    Items extends ReadonlyArray<BatchInputItem<MutationWorkerRef<any>>>,
  >(
    ctx: QuickCtx,
    items: Items & { [K in keyof Items]: NormalizedBatchItem<Items[K]> },
  ) {
    return await this.enqueueBatchWithType(ctx, items, "mutation");
  }

  private async enqueueWithType<
    Fn extends FunctionReference<WorkerHandlerType, "public" | "internal", any>,
  >(
    ctx: QuickCtx,
    request: {
      queueId: string;
      fn: Fn;
      args: WorkerPayload<Fn>;
      runAfter?: number;
      runAt?: number;
    },
    handlerType: WorkerHandlerType,
  ) {
    const handle = await createFunctionHandle(request.fn);
    return await ctx.runMutation(this.component.lib.enqueue, {
      queueId: request.queueId,
      payload: request.args,
      handler: handle,
      handlerType,
      runAfter: request.runAfter,
      runAt: request.runAt,
      ...(this.config ? { config: this.config } : {}),
    });
  }

  private async enqueueBatchWithType<
    Fn extends FunctionReference<WorkerHandlerType, "public" | "internal", any>,
  >(
    ctx: QuickCtx,
    items: ReadonlyArray<{
      queueId: string;
      fn: Fn;
      args: WorkerPayload<Fn>;
      runAfter?: number;
      runAt?: number;
    }>,
    handlerType: WorkerHandlerType,
  ) {
    const handleByFunction = new Map<string, string>();
    const mappedItems = [];

    for (const item of items) {
      const functionName = getFunctionName(item.fn);
      let handle = handleByFunction.get(functionName);
      if (!handle) {
        handle = await createFunctionHandle(item.fn);
        handleByFunction.set(functionName, handle);
      }
      mappedItems.push({
        queueId: item.queueId,
        payload: item.args,
        handler: handle,
        handlerType,
        runAfter: item.runAfter,
        runAt: item.runAt,
      });
    }

    return await ctx.runMutation(this.component.lib.enqueueBatch, {
      items: mappedItems,
      ...(this.config ? { config: this.config } : {}),
    });
  }
}
