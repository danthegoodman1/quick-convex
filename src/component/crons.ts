import { cronJobs } from "convex/server"
import { internal } from "./_generated/api.js"

const crons = cronJobs()

crons.interval(
  "garbage collect empty queue pointers",
  { minutes: 1 },
  internal.lib.garbageCollectPointers,
  {}
)

crons.interval(
  "watchdog recover scanner",
  { minutes: 1 },
  internal.scanner.watchdogRecoverScanner,
  {}
)

export default crons
