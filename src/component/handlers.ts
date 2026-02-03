import { v } from "convex/values"
import { internalAction } from "./_generated/server.js"

export const processItem = internalAction({
  args: {
    itemId: v.id("queueItems"),
    payload: v.any(),
    itemType: v.string(),
    queueId: v.string(),
  },
  handler: async (ctx, args) => {
    // Default handler - users should override this in their app
    // by re-exporting from their convex folder or using component configuration
    console.log(
      `Processing item ${args.itemId} of type ${args.itemType} in queue ${args.queueId}`,
      args.payload
    )
  },
})
