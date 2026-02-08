import {
  createFunctionHandle,
  getFunctionName,
  type FunctionArgs,
  type FunctionReference,
  type GenericActionCtx,
  type GenericDataModel,
} from "convex/server";
import {
  v,
  type VAny,
  type Validator,
} from "convex/values";
import type { ComponentApi } from "../component/_generated/component.js";

type WorkerHandlerType = "action" | "mutation";
export type QuickOrder = "vesting" | "fifo";

export type RetryBehavior = {
  maxAttempts: number;
  initialBackoffMs: number;
  base: number;
};

export type RetryOption = boolean | RetryBehavior;

export type OnCompleteStatus = "success" | "failure" | "cancelled";

export type OnCompleteArgs<TContext = unknown> = {
  workId: string;
  context?: TContext;
  status: OnCompleteStatus;
  result: any;
};

type WorkerArgShape<TPayload> = {
  payload: TPayload;
  queueId: string;
};

type WorkerRef<
  TType extends WorkerHandlerType,
  TPayload = unknown,
> = FunctionReference<TType, "public" | "internal", WorkerArgShape<TPayload>>;

export type ActionWorkerRef<TPayload = unknown> = WorkerRef<"action", TPayload>;
export type MutationWorkerRef<TPayload = unknown> = WorkerRef<
  "mutation",
  TPayload
>;

export type OnCompleteMutationRef<TContext = unknown> = FunctionReference<
  "mutation",
  "public" | "internal",
  OnCompleteArgs<TContext>
>;

export type WorkerPayload<
  Fn extends FunctionReference<any, any, any>,
> = FunctionArgs<Fn> extends WorkerArgShape<infer TPayload>
  ? Exclude<keyof FunctionArgs<Fn>, keyof WorkerArgShape<TPayload>> extends never
    ? TPayload
    : never
  : never;

export type OnCompleteContext<
  Fn extends FunctionReference<any, any, any>,
> = FunctionArgs<Fn> extends OnCompleteArgs<infer TContext>
  ? Exclude<keyof FunctionArgs<Fn>, keyof OnCompleteArgs<TContext>> extends never
    ? TContext
    : never
  : never;

export type QuickCtx = Pick<
  GenericActionCtx<GenericDataModel>,
  "runMutation"
>;

export type QuickOptions = {
  defaultOrderBy?: QuickOrder;
  retryByDefault?: boolean;
  defaultRetryBehavior?: RetryBehavior;
  managerBatchSize?: number;
};

type OnCompleteRequest<
  Fn extends OnCompleteMutationRef<any> = OnCompleteMutationRef<any>,
> = {
  fn: Fn;
  context?: OnCompleteContext<Fn>;
};

export type EnqueueActionRequest<
  Fn extends ActionWorkerRef<any> = ActionWorkerRef<any>,
  OnCompleteFn extends OnCompleteMutationRef<any> = OnCompleteMutationRef<any>,
> = {
  queueId: string;
  fn: Fn;
  args: WorkerPayload<Fn>;
  runAfter?: number;
  runAt?: number;
  retry?: RetryOption;
  onComplete?: OnCompleteRequest<OnCompleteFn>;
};

export type EnqueueMutationRequest<
  Fn extends MutationWorkerRef<any> = MutationWorkerRef<any>,
  OnCompleteFn extends OnCompleteMutationRef<any> = OnCompleteMutationRef<any>,
> = {
  queueId: string;
  fn: Fn;
  args: WorkerPayload<Fn>;
  runAfter?: number;
  runAt?: number;
  retry?: RetryOption;
  onComplete?: OnCompleteRequest<OnCompleteFn>;
};

export type EnqueueBatchActionItem<
  Fn extends ActionWorkerRef<any> = ActionWorkerRef<any>,
  OnCompleteFn extends OnCompleteMutationRef<any> = OnCompleteMutationRef<any>,
> = EnqueueActionRequest<Fn, OnCompleteFn>;

export type EnqueueBatchMutationItem<
  Fn extends MutationWorkerRef<any> = MutationWorkerRef<any>,
  OnCompleteFn extends OnCompleteMutationRef<any> = OnCompleteMutationRef<any>,
> = EnqueueMutationRequest<Fn, OnCompleteFn>;

type BatchInputItem<TRef extends FunctionReference<any, any, any>> = {
  queueId: string;
  fn: TRef;
  args: unknown;
  runAfter?: number;
  runAt?: number;
  retry?: RetryOption;
  onComplete?: OnCompleteRequest<OnCompleteMutationRef<any>>;
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
      retry?: RetryOption;
      onComplete?: OnCompleteRequest<OnCompleteMutationRef<any>>;
    }
  : never;

function normalizeRetryOption(
  retry: RetryOption | undefined,
): { retry?: boolean; retryBehavior?: RetryBehavior } {
  if (retry === undefined) {
    return {};
  }
  if (typeof retry === "boolean") {
    return { retry };
  }
  return {
    retry: true,
    retryBehavior: retry,
  };
}

export const vOnComplete = vOnCompleteArgs(v.any());

export function vOnCompleteArgs<
  V extends Validator<any, "required", any> = VAny,
>(context?: V) {
  const contextValidator = context ? v.optional(context) : v.optional(v.any());

  return v.object({
    workId: v.string(),
    context: contextValidator,
    status: v.union(
      v.literal("success"),
      v.literal("failure"),
      v.literal("cancelled"),
    ),
    result: v.any(),
  });
}

export class Quick {
  private readonly config?: {
    defaultOrderBy?: QuickOrder;
    retryByDefault?: boolean;
    defaultRetryBehavior?: RetryBehavior;
    managerBatchSize?: number;
  };

  constructor(
    private readonly component: ComponentApi,
    options?: QuickOptions,
  ) {
    if (!options) {
      this.config = undefined;
      return;
    }

    const config: {
      defaultOrderBy?: QuickOrder;
      retryByDefault?: boolean;
      defaultRetryBehavior?: RetryBehavior;
      managerBatchSize?: number;
    } = {};

    if (options.defaultOrderBy !== undefined) {
      config.defaultOrderBy = options.defaultOrderBy;
    }
    if (options.retryByDefault !== undefined) {
      config.retryByDefault = options.retryByDefault;
    }
    if (options.defaultRetryBehavior !== undefined) {
      config.defaultRetryBehavior = options.defaultRetryBehavior;
    }
    if (options.managerBatchSize !== undefined) {
      config.managerBatchSize = options.managerBatchSize;
    }

    this.config = Object.keys(config).length > 0 ? config : undefined;
  }

  async enqueueAction<
    Fn extends ActionWorkerRef<any>,
    OnCompleteFn extends OnCompleteMutationRef<any> = OnCompleteMutationRef<any>,
  >(
    ctx: QuickCtx,
    request: EnqueueActionRequest<Fn, OnCompleteFn>,
  ) {
    return await this.enqueueWithType(ctx, request, "action");
  }

  async enqueueMutation<
    Fn extends MutationWorkerRef<any>,
    OnCompleteFn extends OnCompleteMutationRef<any> = OnCompleteMutationRef<any>,
  >(
    ctx: QuickCtx,
    request: EnqueueMutationRequest<Fn, OnCompleteFn>,
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
      retry?: RetryOption;
      onComplete?: OnCompleteRequest<OnCompleteMutationRef<any>>;
    },
    handlerType: WorkerHandlerType,
  ) {
    const handle = await createFunctionHandle(request.fn);
    const onCompleteHandle = request.onComplete
      ? await createFunctionHandle(request.onComplete.fn)
      : undefined;
    const retry = normalizeRetryOption(request.retry);

    return await ctx.runMutation(this.component.lib.enqueue, {
      queueId: request.queueId,
      payload: request.args,
      handler: handle,
      handlerType,
      runAfter: request.runAfter,
      runAt: request.runAt,
      onCompleteHandler: onCompleteHandle,
      onCompleteContext: request.onComplete?.context,
      ...retry,
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
      retry?: RetryOption;
      onComplete?: OnCompleteRequest<OnCompleteMutationRef<any>>;
    }>,
    handlerType: WorkerHandlerType,
  ) {
    const handleByFunction = new Map<string, string>();
    const onCompleteHandleByFunction = new Map<string, string>();
    const mappedItems = [];

    for (const item of items) {
      const functionName = getFunctionName(item.fn);
      let handle = handleByFunction.get(functionName);
      if (!handle) {
        handle = await createFunctionHandle(item.fn);
        handleByFunction.set(functionName, handle);
      }

      let onCompleteHandle: string | undefined;
      if (item.onComplete) {
        const onCompleteName = getFunctionName(item.onComplete.fn);
        onCompleteHandle = onCompleteHandleByFunction.get(onCompleteName);
        if (!onCompleteHandle) {
          onCompleteHandle = await createFunctionHandle(item.onComplete.fn);
          onCompleteHandleByFunction.set(onCompleteName, onCompleteHandle);
        }
      }

      const retry = normalizeRetryOption(item.retry);
      mappedItems.push({
        queueId: item.queueId,
        payload: item.args,
        handler: handle,
        handlerType,
        runAfter: item.runAfter,
        runAt: item.runAt,
        onCompleteHandler: onCompleteHandle,
        onCompleteContext: item.onComplete?.context,
        ...retry,
      });
    }

    return await ctx.runMutation(this.component.lib.enqueueBatch, {
      items: mappedItems,
      ...(this.config ? { config: this.config } : {}),
    });
  }
}
