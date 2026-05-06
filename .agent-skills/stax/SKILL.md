---
name: stax
description: Use when working with stacked Git branches or PRs using stax, including creating, inserting, submitting, restacking, merging, repairing stacks, resolving conflicts, checking CI, and using stax lanes/worktrees for isolated agent work.
---

# Stax

## Provenance

- Upstream source: copied from an internal Stax skill.
- Local status: copied into dagster-slurm with frontmatter normalized to the repo-supported `name` and `description` keys only.

Use `stax` for stacked branches: small branches layered on their parent branch, usually with one PR per branch. Prefer `stax` commands over raw `git rebase`, branch deletion, or PR retargeting because Stax maintains stack metadata.

## Core Commands

```bash
stax status            # stack tree; aliases: s, ls
stax ll                # stack tree with PR URLs/details
stax log               # stack tree with commits and PR info
stax ci --stack        # CI for the current stack

stax create <name>     # create a branch stacked on current
stax create <name> -am "message"
stax create <name> --from <branch>
stax create <name> --insert
stax create <name> --below
stax modify -a         # amend all changes into current branch commit; alias: m
stax rename <name>
stax split

stax submit            # submit current stack; alias: ss
stax submit --draft
stax submit --rerequest-review
stax branch submit     # submit only current branch
stax upstack submit    # submit current branch and descendants
stax downstack submit  # submit ancestors and current branch

stax sync --restack    # update trunk, clean merged branches, restack
stax refresh           # sync, restack, then submit
stax restack --all
stax cascade           # restack bottom-up, then submit

stax merge --when-ready
stax merge --all --when-ready

stax continue          # after resolving conflicts
stax abort
stax validate
stax fix --dry-run
stax fix --yes
```

## Branch Placement

Use normal `create` when new work belongs above the current branch:

```bash
stax create api-validation -am "Add API validation"
```

Use `--insert` when the new branch should sit between the current branch and its children. This keeps the current branch where it is and reparents descendants onto the new branch.

```bash
stax create shared-types --insert -am "Add shared type definitions"
```

Use `--below` when the new branch should sit below the current branch. This creates the new branch from the current branch's parent, then reparents the current branch and its descendants onto the new branch. Use it for prerequisites discovered after a branch already exists.

```bash
stax create schema-base --below -am "Add schema base"
```

`--below` can be combined with `-m` or `-am` to commit on the new lower branch. It does not combine with `--insert` or `--from`.

## Common Flows

Start a stack:

```bash
stax sync --restack
stax create backend-contract -am "Add backend contract"
stax create ui-integration -am "Wire UI to backend contract"
stax submit
```

Add a prerequisite under existing work:

```bash
stax checkout ui-integration
stax create backend-contract --below -am "Add backend contract"
stax restack --all
stax submit
```

Handle review feedback:

```bash
stax checkout <branch>
# edit files
stax modify -a
stax submit --rerequest-review
```

Catch up after trunk or a parent branch moved:

```bash
stax sync --restack
stax status
stax submit
```

Resolve restack conflicts:

```bash
stax restack --all
# resolve conflicts
git add -A
stax continue
stax status
```

Merge after checks pass:

```bash
stax ci --stack
stax merge --all --when-ready
```

Repair stack metadata:

```bash
stax validate
stax fix --dry-run
stax fix --yes
```

## Agent Lanes

Use lanes when running isolated agent work in parallel. Each lane has its own worktree and branch.

```bash
stax lane fix-ci --agent codex "Fix CI failure"
stax lane docs-pass --agent claude "Update docs"
stax wt ll
stax lane fix-ci
stax wt rs
stax wt rm fix-ci --delete-branch
```

Use `stax wt ll` to inspect lane/worktree state, `stax lane <name>` to re-enter a lane, and `stax wt rs` after trunk moves to restack all Stax-managed worktrees.

## Operating Rules

- Inspect with `stax status` or `stax ll` before changing stack shape.
- Keep one logical change per branch when practical.
- Prefer `stax modify -a` for updating an existing branch.
- Prefer `stax sync --restack` or `stax refresh` after upstream changes.
- Use `stax create --below` for newly discovered prerequisites under current work.
- Use `stax validate` and `stax fix --dry-run` before deep stack surgery.
- Do not run setup, skill-install, release, or maintainer-only commands unless the user explicitly asks.
