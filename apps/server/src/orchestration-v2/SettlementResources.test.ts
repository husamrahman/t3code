// Exercises settlement resource cleanup with registered Git worktrees on Linux.
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  IsoDateTime,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";

import * as ServerConfig from "../config.ts";
import * as PreviewManager from "../preview/Manager.ts";
import * as PortScanner from "../preview/PortScanner.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as SettlementResources from "./SettlementResources.ts";

const platform = Layer.mergeAll(NodeServices.layer, NodeCrypto.layer);

it.live.skipIf(HostProcess.Platform.defaultValue() !== "linux")(
  "preserves ignored files and safely replays cleanup after Git removed the checkout",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const testRoot = path.join(process.cwd(), ".t3");
      yield* fs.makeDirectory(testRoot, { recursive: true });
      const baseDir = yield* fs.makeTempDirectoryScoped({
        directory: testRoot,
        prefix: "settlement-test-",
      });
      const configs = ServerConfig.layerTest(process.cwd(), baseDir);
      const gitLayer = GitVcsDriver.layer.pipe(
        Layer.provideMerge(configs),
        Layer.provideMerge(platform),
      );
      yield* Effect.gen(function* () {
        const git = yield* GitVcsDriver.GitVcsDriver;
        const { worktreesDir } = yield* ServerConfig.ServerConfig;
        const repo = path.join(baseDir, "repo");
        const worktree = path.join(worktreesDir, "repo", "topic");
        const alias = path.join(worktreesDir, "repo", "alias");
        const ignored = path.join(worktree, "secret.env");
        yield* fs.makeDirectory(repo);
        yield* fs.makeDirectory(path.dirname(worktree), { recursive: true });
        const run = (cwd: string, args: ReadonlyArray<string>) =>
          git.execute({ operation: "settlement fixture", cwd, args });
        yield* run(repo, ["init", "-b", "main"]);
        const ignore = path.join(repo, ".gitignore");
        yield* fs.writeFileString(ignore, "*.env\n");
        yield* run(repo, ["add", ".gitignore"]);
        yield* run(repo, [
          "-c",
          "user.name=Test",
          "-c",
          "user.email=test@example.invalid",
          "commit",
          "-m",
          "Ignore local configuration",
        ]);
        yield* run(repo, ["worktree", "add", "-b", "topic", worktree, "main"]);
        yield* run(repo, ["config", "branch.topic.t3-merge-base", "main"]);
        yield* fs.writeFileString(ignored, "fixture data\n");
        yield* fs.symlink(worktree, alias);
        let selectedWorktree = worktree;
        const now = yield* DateTime.now;
        const at = IsoDateTime.make(DateTime.formatIso(now));
        const threadId = ThreadId.make("settlement-test");
        const projectId = ProjectId.make("settlement-project");
        const instanceId = ProviderInstanceId.make("codex");
        const intentId = CommandId.make("settlement-intent");
        const thread = {
          id: threadId,
          projectId,
          title: "Worktree cleanup",
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          deletedAt: null,
          lastVisitedAt: null,
          createdBy: "user",
          creationSource: "web",
          providerInstanceId: instanceId,
          modelSelection: { instanceId, model: "gpt-5.4" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "topic",
          worktreePath: worktree,
          activeProviderThreadId: null,
          forkedFrom: null,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          settledAt: null,
          settledOverride: null,
          settlementCleanup: { intentId, attemptId: intentId, state: { type: "pending" } },
        } satisfies OrchestrationV2AppThread;
        const project: ProjectStore.ProjectRow = {
          projectId,
          title: "Fixture",
          workspaceRoot: repo,
          defaultModelSelection: null,
          defaultThreadEnvMode: null,
          autoPull: false,
          faviconPath: null,
          projectIcon: null,
          scripts: [],
          pinnedAt: null,
          archivedAt: null,
          deletedAt: null,
          createdAt: at,
          updatedAt: at,
        };
        const layer = SettlementResources.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.mock(ProjectionStore.ProjectionStoreV2)({
                getThreadProviderContext: () =>
                  Effect.succeed({
                    thread: { ...thread, worktreePath: selectedWorktree },
                    providerSessions: [],
                    providerThreads: [],
                  }),
                getShellSnapshot: () =>
                  Effect.succeed({
                    schemaVersion: 2,
                    snapshotSequence: 0,
                    threads: [],
                    archivedThreads: [],
                  }),
              }),
              Layer.mock(ProjectStore.ProjectStoreV2)({
                get: () => Effect.succeedSome(project),
                list: () => Effect.succeed([project]),
              }),
              Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
              Layer.mock(PortScanner.PortDiscovery)({
                stopThreadProcesses: () => Effect.void,
                trackAgent: () => () => {},
              }),
              Layer.mock(TerminalManager.TerminalManager)({
                close: () => Effect.void,
                listForSettlement: () => Effect.succeed([]),
              }),
              Layer.mock(PreviewManager.PreviewManager)({ close: () => Effect.void }),
              ServerSettings.layerTest(),
            ),
          ),
        );
        yield* Effect.gen(function* () {
          const resources = yield* SettlementResources.SettlementResources;
          const blocker = yield* resources.cleanup(threadId).pipe(Effect.flip);
          expect(blocker.reason).toMatch(/ignored files/);
          expect(blocker.remaining).toEqual([{ kind: "worktree", id: worktree }]);
          expect(yield* fs.readFileString(ignored)).toBe("fixture data\n");
          yield* fs.remove(ignored);
          selectedWorktree = alias;
          expect((yield* resources.cleanup(threadId).pipe(Effect.flip)).reason).toMatch(
            /not a linked worktree/,
          );
          expect(yield* fs.exists(worktree)).toBe(true);
          selectedWorktree = worktree;
          yield* resources.cleanup(threadId);
          expect(yield* fs.exists(worktree)).toBe(false);
          expect(yield* git.listWorktreePaths(repo)).not.toContain(worktree);
          expect((yield* run(repo, ["branch", "--list", "topic"])).stdout).toContain("topic");
          yield* resources.cleanup(threadId);
          expect(yield* fs.exists(repo)).toBe(true);
        }).pipe(Effect.provide(layer));
      }).pipe(Effect.provide(gitLayer));
    }).pipe(Effect.scoped, Effect.provide(platform)),
);
