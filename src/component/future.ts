import {
  getFunctionAddress,
  type FunctionReference,
  type FunctionReturnType,
  type OptionalRestArgs,
} from "convex/server"
import { convexToJson, jsonToConvex } from "convex/values"

declare const Convex: {
  asyncSyscall: (op: string, jsonArgs: string) => Promise<string>
}

/**
 * Run a query without adding its reads to the caller mutation's read set.
 *
 * Snapshot queries do not observe pending writes from the caller transaction.
 * If missing those writes or concurrent commits could affect correctness, pair
 * the snapshot with a dependency-bearing confirmation read before returning
 * empty, parking, or publishing queue state.
 */
export async function runSnapshotQuery<
  Query extends FunctionReference<"query", "public" | "internal">,
>(query: Query, ...args: OptionalRestArgs<Query>): Promise<FunctionReturnType<Query>> {
  const queryArgs = (args[0] ?? {}) as Record<string, unknown>
  const syscallArgs = {
    udfType: "snapshotQuery",
    args: convexToJson(queryArgs as never),
    ...getFunctionAddress(query),
  }
  const resultStr = await Convex.asyncSyscall(
    "1.0/runUdf",
    JSON.stringify(syscallArgs)
  )
  return jsonToConvex(JSON.parse(resultStr)) as FunctionReturnType<Query>
}
