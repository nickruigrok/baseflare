import { v } from "baseflare/values";
import { mutation } from "../functions/mutation";
import { query } from "../functions/query";
import { defineRules } from "../permissions/define-rules";
import { defineSchema } from "../schema/define-schema";
import { defineTable } from "../schema/define-table";
import { createWorker } from "./create-worker";
import { buildBaseflareManifest } from "./manifest";

// biome-ignore lint/performance/noBarrelFile: Workers test entry must export Durable Object classes.
export { RealtimeConnectionDO } from "./realtime/connection-do";
export { RealtimeSubscriptionDO } from "./realtime/subscription-do";

const schema = defineSchema({
  todos: defineTable({
    completed: v.boolean().default(false),
    ownerToken: v.string(),
    text: v.string(),
  }).index("by_owner", ["ownerToken"]),
});

const rules = defineRules({
  todos: {
    insert: () => true,
    read: () => true,
  },
});

const listTodos = query({
  args: { ownerToken: v.string() },
  handler(ctx, args) {
    return ctx.db
      .query("todos")
      .filter({ ownerToken: args.ownerToken })
      .order("text", "asc")
      .collect();
  },
});

const createTodo = mutation({
  args: { ownerToken: v.string(), text: v.string() },
  async handler(ctx, args) {
    return await ctx.db.insert("todos", {
      ownerToken: args.ownerToken,
      text: args.text,
    });
  },
});

export default createWorker(
  buildBaseflareManifest({
    mutations: [
      { definition: createTodo, exportName: "create", modulePath: "todos" },
    ],
    queries: [
      { definition: listTodos, exportName: "list", modulePath: "todos" },
    ],
    rules,
    schema,
  })
);
