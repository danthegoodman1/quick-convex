# QuiCK Convex Audit Findings

Date: 2026-02-08

## Scope

Audit compared the intended behavior in `README.md` with the current Convex component implementation in `src/component`, plus related client/example surfaces.

## Correctness Findings

1. Delayed jobs can be processed late (up to watchdog interval).
   - Current behavior only wakes scanner immediately for jobs where `vestingTime <= now`.
   - Delayed-only workloads rely on cron recovery.
   - References:
     - `src/component/lib.ts:188`
     - `src/component/lib.ts:259`
     - `src/component/crons.ts:13`

2. FIFO ordering guarantee from README is not enforced by scanner default path.
   - README says per-queue insertion ordering is supported.
   - Scanner manager calls `dequeue` without `orderBy`, which defaults to vesting order.
   - References:
     - `README.md:7`
     - `src/component/scanner.ts:217`
     - `src/component/lib.ts:473`

3. FIFO scan can skip ahead to later records.
   - In collection loop, unvested rows cause `continue` instead of `break`.
   - This allows out-of-order candidate selection under FIFO mode.
   - Reference:
     - `src/component/lib.ts:353`

4. Generated component API types are stale and still expose removed priority fields.
   - Generated type includes `priority` / `defaultPriority`.
   - Runtime schema has no such fields.
   - References:
     - `src/component/_generated/component.ts:33`
     - `src/component/_generated/component.ts:45`
     - `src/component/schema.ts:5`

5. Public docs and helper client still target template comment API, not queue API.
   - README and example call `lib.add`/`lib.list`/`lib.translate` which are not in this component.
   - References:
     - `README.md:115`
     - `src/client/index.ts:76`
     - `example/convex/example.ts:14`

## Performance Findings

1. Scanner can poll continuously when there is no work.
   - Reschedules itself repeatedly with backoff even when no pointers are ready.
   - References:
     - `src/component/scanner.ts:146`
     - `src/component/scanner.ts:188`

2. Dequeue scans more rows than necessary in vesting mode.
   - Query is ordered by vesting, but loop continues scanning after first future item.
   - References:
     - `src/component/lib.ts:338`
     - `src/component/lib.ts:353`
     - `src/component/lib.ts:343`

3. Queue stats do full-table collection.
   - `getQueueStats` collects all queue items and dead letters, then filters in memory.
   - References:
     - `src/component/lib.ts:699`
     - `src/component/lib.ts:706`

4. Pointer claim batch may underutilize available capacity.
   - Scanner takes a fixed due-pointer batch, then filters out leased pointers.
   - If early rows are leased, later due rows in index order are not considered until next cycle.
   - References:
     - `src/component/scanner.ts:100`
     - `src/component/scanner.ts:102`

## Recommendation: Delayed Job Processing

Preferred fix:

1. Add an internal scheduler path that tracks and schedules a single earliest delayed wake (`wakeAtMs`) in scanner state.
2. Call this from enqueue, enqueueBatch, requeue, and replayDeadLetter when `vestingTime > now`.
3. Use token/version checks to ignore stale scheduled wake actions.
4. Keep the 1-minute watchdog cron as crash recovery only.

Minimal stopgap:

- Wake scanner on all enqueue operations (immediate and delayed). This improves correctness but can increase background polling overhead.
