import "./App.css";
import { useMutation, useQuery } from "convex/react";
import { api } from "../convex/_generated/api";
import { useState } from "react";

function App() {
  const [queueId, setQueueId] = useState("blog-post-1");
  const [text, setText] = useState("Process this comment");

  const enqueueAction = useMutation(api.example.enqueueCommentAction);
  const enqueueMutation = useMutation(api.example.enqueueCommentMutation);
  const enqueueBatch = useMutation(api.example.enqueueCommentBatchAction);
  const stats = useQuery(api.example.queueStats, { targetId: queueId });

  return (
    <>
      <h1>QuiCK Convex Example</h1>
      <div className="card">
        <p>Enqueue work with the Quick class-backed API.</p>

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

        <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
          <button
            onClick={() =>
              enqueueAction({
                text,
                targetId: queueId,
              })
            }
          >
            Enqueue Action Worker
          </button>
          <button
            onClick={() =>
              enqueueMutation({
                text,
                targetId: queueId,
              })
            }
          >
            Enqueue Mutation Worker
          </button>
          <button
            onClick={() =>
              enqueueBatch({
                targetId: queueId,
                orderBy: "vesting",
              })
            }
          >
            Enqueue Batch (Vesting)
          </button>
          <button
            onClick={() =>
              enqueueBatch({
                targetId: queueId,
                orderBy: "fifo",
              })
            }
          >
            Enqueue Batch (FIFO)
          </button>
          <button onClick={() => enqueueBatch({ targetId: queueId })}>
            Enqueue Batch (Current Mode)
          </button>
        </div>

        <pre style={{ marginTop: "1rem", textAlign: "left" }}>
          {JSON.stringify(stats ?? null, null, 2)}
        </pre>
      </div>
    </>
  );
}

export default App;
