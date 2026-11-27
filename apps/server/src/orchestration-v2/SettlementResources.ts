// Stops thread-owned resources and verifies that a disposable worktree can be removed.
import { type OrchestrationV2AppThread, type ThreadId } from "@t3tools/contracts";
import { resolveProjectScripts, settleProjectScript } from "@t3tools/shared/projectScripts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as PreviewManager from "../preview/Manager.ts";
import * as PortScanner from "../preview/PortScanner.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import { makeIsManagedWorktreePath } from "../worktreesDirectory.ts";
import { withWorkspaceLease } from "../workspace/workspaceLease.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";

type Cleanup = NonNullable<OrchestrationV2AppThread["settlementCleanup"]>;
type Blocker = Omit<Extract<Cleanup["state"], { type: "blocked" }>, "type">;
type Resource = Blocker["remaining"][number];

class SettlementResourceError extends Schema.TaggedError<SettlementResourceError>()(
  "SettlementResourceError",
  {
    reason: Schema.String,
    remaining: Schema.Array(
      Schema.Struct({
        kind: Schema.Literals(["provider", "terminal", "preview", "process", "worktree"]),
        id: Schema.String,
      }),
    ),
  },
) {}

export class SettlementResources extends Context.Reference<{
  readonly cleanup: (threadId: ThreadId) => Effect.Effect<void, SettlementResourceError>;
}>("t3/orchestration-v2/SettlementResources", {
  defaultValue: () => ({
    cleanup: (threadId) =>
      Effect.fail(
        new SettlementResourceError({
          reason: "Settlement resource cleanup is unavailable.",
          remaining: [{ kind: "terminal", id: threadId }],
        }),
      ),
  }),
}) {}

export const layer = Layer.effect(
  SettlementResources,
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const projects = yield* ProjectStore.ProjectStoreV2;
    const providers = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const settings = yield* ServerSettings.ServerSettingsService;
    const terminals = yield* TerminalManager.TerminalManager;
    const previews = yield* PreviewManager.PreviewManager;
    const ports = yield* PortScanner.PortDiscovery;
    const git = yield* GitVcsDriver.GitVcsDriver;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const isManagedWorktree = yield* makeIsManagedWorktreePath;

    const removeWorktree = Effect.fn("SettlementResources.removeWorktree")(function* (
      thread: OrchestrationV2AppThread,
      worktreePath: string,
    ) {
      const exists = yield* fs.exists(worktreePath);
      if (!(yield* isManagedWorktree(worktreePath))) {
        if (!exists) return;
        const project = yield* projects.get(thread.projectId);
        if (Option.isSome(project) && path.resolve(project.value.workspaceRoot) === worktreePath)
          return;
        return yield* Effect.fail(
          "Worktree is outside T3's managed directories. Move or remove it safely before retrying.",
        );
      }

      const project = yield* projects.get(thread.projectId);
      if (Option.isNone(project))
        return yield* Effect.fail("The worktree's project no longer exists.");
      const cwd = project.value.workspaceRoot;
      const registered = (yield* git.listWorktreePaths(cwd)).some(
        (registeredPath) => path.resolve(registeredPath) === worktreePath,
      );
      if (!exists) {
        if (registered)
          return yield* Effect.fail(
            "The Git worktree is still registered but its directory is missing.",
          );
        return;
      }
      const realWorktree = yield* fs.realPath(worktreePath);
      if (
        realWorktree !==
          path.join(yield* fs.realPath(path.dirname(worktreePath)), path.basename(worktreePath)) ||
        !registered ||
        (yield* fs.stat(path.join(worktreePath, ".git"))).type !== "File"
      )
        return yield* Effect.fail("The directory is not a linked worktree of this project.");

      for (const candidate of yield* projects.list()) {
        const root = yield* fs.realPath(candidate.workspaceRoot).pipe(Effect.option);
        if (
          Option.isSome(root) &&
          (root.value === realWorktree || root.value.startsWith(realWorktree + path.sep))
        )
          return;
      }
      const snapshot = yield* projections.getShellSnapshot({ worktreePath });
      const others = [...snapshot.threads, ...snapshot.archivedThreads].filter(
        (candidate) => candidate.id !== thread.id,
      );
      if (others.some((candidate) => candidate.id === thread.lineage.parentThreadId)) return;
      if (
        others.some((candidate) => candidate.lineage.rootThreadId !== thread.lineage.rootThreadId)
      )
        return;
      const workspaceUsers = others.filter(
        (candidate) => candidate.archivedAt === null && candidate.settledOverride !== "settled",
      );
      if (workspaceUsers.length > 0) {
        return yield* Effect.fail(
          `Worktree is shared by ${workspaceUsers.map(({ id }) => id).join(", ")}.`,
        );
      }
      const liveTerminals = yield* terminals.listForSettlement?.() ?? Effect.succeed([]);
      for (const terminal of liveTerminals) {
        if (terminal.status !== "running" && terminal.status !== "starting") continue;
        const realCwd = yield* fs.realPath(terminal.cwd).pipe(Effect.option);
        if (
          Option.isSome(realCwd) &&
          (realCwd.value === realWorktree || realCwd.value.startsWith(realWorktree + path.sep))
        ) {
          return yield* Effect.fail(`Terminal ${terminal.terminalId} still uses the worktree.`);
        }
      }
      const processDirs = (yield* fs.readDirectory("/proc")).filter((entry) => /^\d+$/.test(entry));
      const worktreeUser = yield* Effect.findFirst(processDirs, (pid) =>
        fs.realPath(`/proc/${pid}/cwd`).pipe(
          Effect.map((cwd) => cwd === realWorktree || cwd.startsWith(realWorktree + path.sep)),
          Effect.orElseSucceed(() => false),
        ),
      );
      if (Option.isSome(worktreeUser)) {
        return yield* Effect.fail(`Process ${worktreeUser.value} still uses the worktree.`);
      }

      const readGit = Effect.fn("SettlementResources.inspectWorktree")(function* (
        args: ReadonlyArray<string>,
        maxOutputBytes = 64 * 1024,
      ) {
        const result = yield* git.execute({
          operation: "SettlementResources.inspectWorktree",
          cwd: worktreePath,
          args,
          maxOutputBytes,
        });
        if (result.stdoutTruncated)
          return yield* Effect.fail("Git output exceeds the safety inspection limit.");
        return result.stdout;
      });
      const head = yield* readGit(["symbolic-ref", "-q", "HEAD"]).pipe(
        Effect.mapError(() => "Worktree HEAD is detached or unreadable."),
      );
      if (thread.branch === null || head.trim() !== `refs/heads/${thread.branch}`)
        return yield* Effect.fail("Worktree branch differs from the thread's recorded branch.");
      if ((yield* git.readBranchBase(cwd, thread.branch)) === null)
        return yield* Effect.fail("Git has no T3 worktree creation metadata for this branch.");
      const tracked = yield* readGit(["ls-files", "-v", "-z"], 4 * 1024 * 1024);
      if (tracked.split("\0").some((entry) => /^[a-zS] /.test(entry))) {
        return yield* Effect.fail(
          "Git marks files assume-unchanged or skip-worktree. Clear these flags and inspect the files before retrying.",
        );
      }
      const refs = yield* readGit([
        "for-each-ref",
        "--contains=HEAD",
        "--format=%(refname)",
        "refs/heads",
        "refs/remotes",
      ]);
      if (
        !refs
          .trim()
          .split("\n")
          .some((ref) => ref !== "" && ref !== head.trim())
      )
        return yield* Effect.fail(
          "Worktree branch has commits absent from every other local branch and remote-tracking branch.",
        );
      const status = yield* readGit([
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.ignoreStat=false",
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
        "--ignored=matching",
        "--ignore-submodules=none",
      ]);
      if (status !== "")
        return yield* Effect.fail(
          "Worktree contains staged, unstaged, untracked, or ignored files.",
        );
      yield* git.removeWorktree({ cwd, path: worktreePath, force: false });
      if (
        (yield* fs.exists(worktreePath)) ||
        (yield* git.listWorktreePaths(cwd)).includes(worktreePath)
      )
        return yield* Effect.fail("Git did not remove the worktree and its registration.");
    });

    const cleanup = Effect.fn("SettlementResources.cleanup")(function* (threadId: ThreadId) {
      const terminal: Resource = { kind: "terminal", id: threadId };
      const preview: Resource = { kind: "preview", id: threadId };
      const blocked = (cause: unknown, remaining: ReadonlyArray<Resource>) =>
        new SettlementResourceError({
          reason: cause instanceof Error ? cause.message : String(cause),
          remaining,
        });
      const { thread, providerSessions } = yield* projections
        .getThreadProviderContext(threadId)
        .pipe(Effect.mapError((cause) => blocked(cause, [terminal, preview])));
      const worktreePath = thread.worktreePath === null ? null : path.resolve(thread.worktreePath);
      const worktree: ReadonlyArray<Resource> =
        worktreePath === null ? [] : [{ kind: "worktree", id: worktreePath }];
      if (worktreePath !== null) {
        const project = yield* projects
          .get(thread.projectId)
          .pipe(Effect.mapError((cause) => blocked(cause, [terminal, preview, ...worktree])));
        if (Option.isSome(project)) {
          const currentSettings = yield* settings.getSettings.pipe(
            Effect.mapError((cause) => blocked(cause, [terminal, preview, ...worktree])),
          );
          if (
            settleProjectScript(
              resolveProjectScripts(currentSettings, {
                id: project.value.projectId,
                scripts: project.value.scripts,
              }),
            )
          ) {
            return yield* blocked(
              "This project has a Run on settle action. Run it manually if needed, disable its settle trigger, then retry.",
              [terminal, preview, ...worktree],
            );
          }
        }
      }
      for (const [index, session] of providerSessions.entries()) {
        yield* providers
          .detach({ providerSessionId: session.id, threadId, revokeMcpCredential: true })
          .pipe(
            Effect.mapError((cause) =>
              blocked(cause, [
                ...providerSessions
                  .slice(index)
                  .map(({ id }) => ({ kind: "provider" as const, id })),
                terminal,
                preview,
                ...worktree,
              ]),
            ),
          );
      }
      yield* terminals.close({ threadId, verifyExit: true }).pipe(
        Effect.mapError((cause) =>
          blocked(cause, [
            {
              kind: "terminal",
              id: "terminalId" in cause ? (cause.terminalId ?? threadId) : threadId,
            },
            preview,
            ...worktree,
          ]),
        ),
      );
      yield* ports
        .stopThreadProcesses(threadId)
        .pipe(
          Effect.mapError((cause) =>
            blocked(cause, [{ kind: "process", id: threadId }, preview, ...worktree]),
          ),
        );
      yield* previews
        .close({ threadId })
        .pipe(Effect.mapError((cause) => blocked(cause, [preview, ...worktree])));
      if (worktreePath === null) return;
      return yield* withWorkspaceLease(worktreePath, removeWorktree(thread, worktreePath)).pipe(
        Effect.mapError((cause) => blocked(cause, worktree)),
      );
    });
    return { cleanup };
  }),
);
