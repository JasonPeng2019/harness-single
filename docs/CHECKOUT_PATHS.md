# Optional worker checkout scope

`harness-config.json` may include `checkout_paths`, a nonempty JSON list of
literal directory names relative to the repository root. Omit the key to keep
the normal full checkout. For example:

```json
{
  "root_workspace": "<absolute operator workspace>",
  "checkout_paths": ["src", "docs/operator guides"]
}
```

Use `/` between path components on every platform. Entries cannot be absolute,
contain traversal, use Git wildcard or pathspec syntax, or repeat another
entry. Each directory must exist in the task card's assigned base commit;
bootstrap refuses before creating its branch or worktree if it does not.

This setting is part of the epoch configuration identity. Change it only when
preparing a fresh epoch. Sparse checkout changes which files are materialized;
it does not remove files from Git history or restrict what Git can access.

Git cone mode includes files at the repository root and files in ancestor
directories needed to reach each selected directory. It also includes the
contents beneath each selected directory. The worker's linked worktree is
created without checkout, then sparse rules are installed in that worktree
before its first checkout, so excluded trees are never first materialized and
deleted afterward. Source and sibling worktree settings are not changed.

Before creating a worker branch, bootstrap checks the assigned base commit for
tracked inputs it will preserve or use during materialization. This includes
tracked files under `.agent-workspace/` and the selected provider's tracked
configuration or hook file (`.codex/config.toml` and `.codex/hooks.json` for
Codex, `.claude/settings.json` for Claude Code, or `.qwen/settings.json` for
Qwen Code). If cone mode would exclude one, bootstrap refuses and names the
missing path. Add its containing directory to `checkout_paths` or omit the
setting for a full checkout; bootstrap does not widen the requested scope.

The installed Git must support linked-worktree-specific configuration and
`git sparse-checkout set --cone --no-sparse-index`. Bootstrap checks the actual
Git commands and reports their failure; no full-checkout fallback is attempted.
