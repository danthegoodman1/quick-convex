import { defineApp } from "convex/server";
import quickConvex from "@danthegoodman/quick-convex/convex.config.js";

const app = defineApp();
app.use(quickConvex);

export default app;
