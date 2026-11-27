# Thread settlement cleanup

The Linux server writes the cleanup intent, attempt, and outbox entry in the settle command's
transaction. The worker finishes or records a blocker. Only the successful result commits
`thread.settled`. Restart resumes the same attempt. Cleanup can encounter a worktree already
removed by that attempt. T3 retains the branch and the recorded path so it can recreate the
checkout if the user resumes work.

The worker detaches provider sessions through T3's session manager, closes the thread's
terminals and previews, and verifies that tagged child processes have stopped. It signals a
process group only when its members belong to the same thread. It keeps project checkouts
and worktrees used by independent threads. A side chat keeps its parent's checkout. An owner
can retry once dependent threads finish. Linked Git worktrees require T3 creation metadata
and another local or remote-tracking ref that contains HEAD. Tracked edits, untracked files,
ignored files, detached HEADs, and uncertain ownership block removal.

On this fork, a configured **Run on settle** action blocks cleanup. Run the action manually if
needed and disable its settle trigger before retrying. A server restart cannot verify whether
an interrupted arbitrary script already ran.

## Agent OS

Read `settlementCleanup` from the thread shell or from `payload_json` in
`orchestration_v2_projection_threads`, keyed by `thread_id`. The field is absent or null when
there is no recorded attempt. The object contains `intentId`, `attemptId`, and `state`.
`state.type` is `pending`, `blocked`, or `complete`. A blocked state also has a readable
`reason` and `remaining`, a list of `{ kind, id }` entries for provider sessions, terminals,
previews, processes, or worktrees. A group request can observe an intent that was already
started through another entry point.

An initial `thread.settle` may specify `acknowledgeCleanup: true` on
`orchestration.dispatchCommand` to return a receipt before cleanup completes. While a cleanup
is pending or blocked, ordinary settlement commands observe that same intent. Submit a
deliberate retry with a fresh command ID and the last blocked attempt:

```json
{
  "type": "thread.settle",
  "commandId": "new-command-id",
  "threadId": "thread-id",
  "retryCleanup": {
    "intentId": "original-intent-id",
    "attemptId": "observed-blocked-attempt-id"
  }
}
```

The receipt acknowledges the retry. Resend the same command ID after a transport timeout.
T3 rejects a new retry command whose attempt is stale. Re-read the projection before acting on
a stale response. Close a recovery item when the matching intent has `state.type: "complete"`
and the projection confirms settlement. A missing thread or timed-out read leaves the cleanup
unverified.

## Release and rollback

Build the Linux bundle and executable with the pinned Node version. Keep the binary and its
adjacent `node_modules`, client assets and runtime files together. Verify the release candidate
on a loopback port with an explicit disposable `--base-dir`. Test the Agent OS command and
reconciler against it before changing the active service. Record the current package path and
take a stopped-service SQLite backup before cutover.

To roll back, stop new settle requests and drain in-flight cleanup before stopping the candidate.
Switch the service back to the saved v0.1.7 package and verify it starts. The older worker cannot
decode a pending cleanup outbox entry. If a rollback cannot wait for that queue to drain, preserve
the candidate database and use the pre-cutover backup for recovery. Workspace removals are
filesystem changes. The preserved branch refs allow Git to recreate those clean worktrees.
