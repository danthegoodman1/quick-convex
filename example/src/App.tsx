import "./App.css";
import { useMutation, useQuery } from "convex/react";
import { api } from "../convex/_generated/api";
import { useCallback, useEffect, useState } from "react";

function App() {
  const [queueId, setQueueId] = useState("blog-post-1");
  const [text, setText] = useState("Process this comment");
  const [randomizeQueueId, setRandomizeQueueId] = useState(false);
  const [statsQueueId, setStatsQueueId] = useState("blog-post-1");

  const enqueueAction = useMutation(api.example.enqueueCommentAction);
  const enqueueMutation = useMutation(api.example.enqueueCommentMutation);
  const enqueueBatchVesting = useMutation(api.example.enqueueCommentBatchActionVesting);
  const enqueueBatchFifo = useMutation(api.example.enqueueCommentBatchActionFifo);
  const vestingStats = useQuery(api.example.queueStats, {
    targetId: statsQueueId,
    mode: "vesting",
  });
  const fifoStats = useQuery(api.example.queueStats, {
    targetId: statsQueueId,
    mode: "fifo",
  });

  const resolveTargetId = useCallback(() => {
    if (!randomizeQueueId) {
      return queueId;
    }
    const suffix = Math.random().toString(36).slice(2, 8);
    return `${queueId}-${suffix}`;
  }, [queueId, randomizeQueueId]);

  const handleEnqueueAction = useCallback(() => {
    const targetId = resolveTargetId();
    setStatsQueueId(targetId);
    void enqueueAction({
      text,
      targetId,
    });
  }, [enqueueAction, resolveTargetId, text]);

  const handleEnqueueMutation = useCallback(() => {
    const targetId = resolveTargetId();
    setStatsQueueId(targetId);
    void enqueueMutation({
      text,
      targetId,
    });
  }, [enqueueMutation, resolveTargetId, text]);

  const handleEnqueueBatchVesting = useCallback(() => {
    const targetId = resolveTargetId();
    setStatsQueueId(targetId);
    void enqueueBatchVesting({ targetId });
  }, [enqueueBatchVesting, resolveTargetId]);

  const handleEnqueueBatchFifo = useCallback(() => {
    const targetId = resolveTargetId();
    setStatsQueueId(targetId);
    void enqueueBatchFifo({ targetId });
  }, [enqueueBatchFifo, resolveTargetId]);

  useEffect(() => {
    if (!randomizeQueueId) {
      setStatsQueueId(queueId);
    }
  }, [queueId, randomizeQueueId]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (
        target &&
        (target.tagName === "INPUT" ||
          target.tagName === "TEXTAREA" ||
          target.tagName === "SELECT" ||
          target.isContentEditable)
      ) {
        return;
      }

      const key = event.key.toLowerCase();
      if (key === "a") {
        event.preventDefault();
        handleEnqueueAction();
      } else if (key === "m") {
        event.preventDefault();
        handleEnqueueMutation();
      } else if (key === "v") {
        event.preventDefault();
        handleEnqueueBatchVesting();
      } else if (key === "f") {
        event.preventDefault();
        handleEnqueueBatchFifo();
      }
    };

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [
    handleEnqueueAction,
    handleEnqueueMutation,
    handleEnqueueBatchVesting,
    handleEnqueueBatchFifo,
  ]);

  return (
    <>
      <h1>QuiCK Convex Example</h1>
      <div className="card">
        <p>Enqueue work with the Quick class-backed API.</p>
        <p>Hotkeys (hold to repeat): A action, M mutation, V batch vesting, F batch FIFO.</p>

        <div style={{ marginBottom: "0.75rem" }}>
          <label>
            Queue ID:
            <input
              style={{ marginLeft: "0.5rem" }}
              value={queueId}
              onChange={(e) => setQueueId(e.target.value)}
            />
          </label>
        </div>

        <div style={{ marginBottom: "1rem" }}>
          <label>
            Payload text:
            <input
              style={{ marginLeft: "0.5rem", width: "20rem" }}
              value={text}
              onChange={(e) => setText(e.target.value)}
            />
          </label>
        </div>

        <div style={{ marginBottom: "1rem" }}>
          <label>
            <input
              type="checkbox"
              checked={randomizeQueueId}
              onChange={(e) => setRandomizeQueueId(e.target.checked)}
            />
            <span style={{ marginLeft: "0.5rem" }}>
              Randomize queue ID per enqueue (uses Queue ID as prefix)
            </span>
          </label>
        </div>

        <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
          <button
            onClick={handleEnqueueAction}
          >
            Enqueue Action Worker (A)
          </button>
          <button onClick={handleEnqueueMutation}>
            Enqueue Mutation Worker (M)
          </button>
          <button onClick={handleEnqueueBatchVesting}>
            Enqueue Batch (Vesting) (V)
          </button>
          <button onClick={handleEnqueueBatchFifo}>
            Enqueue Batch (FIFO) (F)
          </button>
        </div>

        <div
          style={{
            marginTop: "1rem",
            display: "grid",
            gap: "0.75rem",
            gridTemplateColumns: "repeat(auto-fit, minmax(18rem, 1fr))",
          }}
        >
          <div style={{ gridColumn: "1 / -1", textAlign: "left" }}>
            <strong>Stats Queue ID:</strong> <code>{statsQueueId}</code>
          </div>
          <div>
            <strong>Vesting Stats</strong>
            <pre style={{ marginTop: "0.5rem", textAlign: "left" }}>
              {JSON.stringify(vestingStats ?? null, null, 2)}
            </pre>
          </div>
          <div>
            <strong>FIFO Stats</strong>
            <pre style={{ marginTop: "0.5rem", textAlign: "left" }}>
              {JSON.stringify(fifoStats ?? null, null, 2)}
            </pre>
          </div>
        </div>
      </div>
    </>
  );
}

export default App;
