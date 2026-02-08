import { defineApp } from "convex/server";
import quickConvex from "@danthegoodman/quick-convex/convex.config.js";

const app = defineApp();
app.use(quickConvex, { name: "quickVesting" });
app.use(quickConvex, { name: "quickFifo" });

export default app;
