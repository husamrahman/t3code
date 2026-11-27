// Completes the outbox result in orchestration tests that do not run the resource worker.
import { CommandId, type ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type * as Orchestrator from "../Orchestrator.ts";

export const reportSettlementFixture = Effect.fn("SettlementFixtures.report")(function* (
  orchestrator: Orchestrator.OrchestratorV2["Service"],
  threadId: ThreadId,
) {
  const { thread } = yield* orchestrator.getThreadProjection(threadId);
  const cleanup = thread.settlementCleanup;
  if (cleanup?.state.type !== "pending") throw new Error("Expected pending settlement cleanup.");
  return yield* orchestrator.dispatch({
    type: "thread.settlement-cleanup.report",
    commandId: CommandId.make(`server:settlement-cleanup:${cleanup.attemptId}`),
    threadId,
    intentId: cleanup.intentId,
    attemptId: cleanup.attemptId,
    blocker: null,
  });
});
