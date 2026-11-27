// Gives resource managers a projection-backed admission check inside their own locks.
import { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ProjectionStore from "./ProjectionStore.ts";

export class SettlementResourceAdmission extends Context.Reference<{
  readonly permitsStart: (threadId: string) => Effect.Effect<boolean>;
}>("t3/orchestration-v2/SettlementResourceAdmission", {
  defaultValue: () => ({ permitsStart: () => Effect.succeed(true) }),
}) {}

export const layer = Layer.effect(
  SettlementResourceAdmission,
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const permitsStart = Effect.fn("SettlementResourceAdmission.permitsStart")(
      function* (threadId: string) {
        const shell = yield* projections.getThreadShell(ThreadId.make(threadId));
        const state = shell?.settlementCleanup?.state.type;
        return (
          state !== "pending" &&
          state !== "blocked" &&
          !(state === "complete" && shell?.settledOverride === "settled")
        );
      },
      Effect.orElseSucceed(() => false),
    );
    return { permitsStart };
  }),
).pipe(Layer.provide(ProjectionStore.layer));
