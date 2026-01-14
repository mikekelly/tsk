# tsk

Minimal task tracker for AI coding agents. Fast, minimal task tracking with plain markdown files.

This is an opinionated fork of [dots](https://github.com/joelreymont/dots).

- ability to insert tasks inbetween other tasks
- ability to order/reorder tasks
- removed task priority
- removed slugs from task ids
- renamed `on` and `off` to `start` and `complete`

## What is tsk?

A CLI task tracker with **zero dependencies** — tasks are plain markdown files with YAML frontmatter in `.tsk/`. No database, no server, no configuration. Copy the folder between machines, commit to git, edit with any tool. Parent-child relationships map to folders. Each task has an ID, title, status, and optional dependencies.

## Quick Start

```bash
# Initialize in current directory
tsk init

# Add tasks
tsk "Build auth system"
# Output: a1b2c3d4

# Add subtask
tsk add "Design schema" -P a1b2c3d4
# Output: e5f6a7b8

# Add subtask blocked by other task
tsk add "Implement endpoints" -P a1b2c3d4 -a e5f6a7b8
# Output: c9d0e1f2 

# Add task before/after another
tsk add "Plan out endpoints" --before c9d0e1f2 
# Output: aad9e1e4 

tsk tree
# [a1b2c3d4] o Build auth system
#   +- [e5f6a7b8] o Design schema
#   +- [aad9e1e4] o Plan out endpoints
#   +- [c9d0e1f2] o Implement endpoints

# Start working
tsk start e5f6a7b8

# Complete task
tsk complete e5f6a7b8 -r "Schema finalized"
```

## Installation

### Homebrew

```bash
brew install mikekelly/acp/tsk
```

### From source (requires Zig 0.15+)

```bash
git clone https://github.com/mikekelly/tsk.git
cd tsk
zig build -Doptimize=ReleaseSmall
cp zig-out/bin/tsk ~/.local/bin/
```

### Verify installation

```bash
tsk --version
# Output: tsk 0.6.3
```

## Command Reference

### Initialize

```bash
tsk init
```
Creates `.tsk/` directory. Runs `git add .tsk` if in a git repository. Safe to run if already exists.

### Add Task

```bash
tsk add "title" [-d "description"] [-P PARENT_ID] [-a BLOCKER_ID] [--after ID | --before ID] [--json]
tsk "title"  # shorthand for: tsk add "title"
```

Options:
- `-d "text"`: Long description (markdown body of the file)
- `-P ID`: Parent task ID (creates folder hierarchy)
- `-a ID`: Blocked by task ID (dependency - new task waits for this one)
- `--after ID`: Position after sibling task (inherits parent from target)
- `--before ID`: Position before sibling task (inherits parent from target)
- `--json`: Output created task as JSON

Note: `--after`/`--before` cannot be combined with `-P` (parent is inferred from the target task).

Examples:
```bash
tsk add "Design API"
# Output: 1a2b3c4d

tsk add "Implement API" -a 1a2b3c4d -d "REST endpoints for user management"
# Output: 3c4d5e6f (blocked until 1a2b3c4d is done)

tsk add "Write tests" --after 3c4d5e6f
# Output: 5e6f7a8b (positioned after 3c4d5e6f, same parent)
```

### Start Task

```bash
tsk start <id> [id2 ...]
```
Marks task(s) as `active`. Use when you begin working on tasks. Supports short ID prefixes.

### Unstart Task

```bash
tsk unstart <id> [id2 ...]
```
Sets task(s) back to `open` status. Use when you want to stop working on a task without completing it.

### Complete Task

```bash
tsk complete <id> [id2 ...] [-r "reason"]
```
Marks task(s) as `done` and archives them. Optional reason applies to all. Root tasks are moved to `.tsk/archive/`. Child tasks wait for parent to close before moving.

### Show Task Details

```bash
tsk show <id>
```

Outputs the raw task file (YAML frontmatter + markdown description):
```markdown
---
title: Design API
status: open
created-at: 2024-12-24T10:30:00Z
peer-index: 0
---

REST endpoints for user management
```

### Remove Task

```bash
tsk rm <id> [id2 ...]
```
Permanently deletes task file(s). If removing a parent, children are also deleted.

### Show Hierarchy

```bash
tsk tree [id]
```

Without arguments: shows all open root tasks and their children.
With `id`: shows that specific task's tree (including closed children).

Output:
```
[a1b2c3d4] o Build auth system
  +- [e5f6a7b8] o Design schema
  +- [c9d0e1f2] o Implement endpoints (blocked)
  +- [d3e4f5a6] o Write tests (blocked)
```

### Show Ready Tasks

```bash
tsk ready [--json]
```
Lists tasks that are `open` and have no blocking dependencies (or blocker is `done`).

### Fix Orphans

```bash
tsk fix
```
Promotes orphaned children to root and removes missing parent folders.

### Search Tasks

```bash
tsk find "query"
```
Case-insensitive search across title, description, close-reason, created-at, and closed-at. Shows open tasks first, then archived.

### Purge Archive

```bash
tsk purge
```
Permanently deletes all archived (completed) tasks from `.tsk/archive/`.

## Storage Format

Tasks are stored as markdown files with YAML frontmatter in `.tsk/`:

```
.tsk/
  a1b2c3d4.md                 # Root task (no children)
  e5f6a7b8/                   # Parent with children
    e5f6a7b8.md               # Parent task file
    c9d0e1f2.md               # Child task
  archive/                    # Closed tasks
    d3e4f5a6.md               # Archived root task
    f7a8b9c0/                 # Archived tree
      f7a8b9c0.md
      a1b2c3d4.md
```

### File Format

```markdown
---
title: Fix the bug
status: open
assignee: joel
created-at: 2024-12-24T10:30:00Z
blocks:
  - a3f2b1c8
---

Description as markdown body here.
```

### ID Format

IDs are 8-character random hex strings: `a3f2b1c8`

Commands accept short prefixes:

```bash
tsk start a3f2b1    # Matches a3f2b1c8
tsk show a3f     # Error if ambiguous (multiple matches)
```

### Status Flow

```
open -> active -> done (archived)
```

- `open`: Task created, not started
- `active`: Currently being worked on
- `done`: Completed, moved to archive

### Dependencies

- `parent (-P)`: Creates folder hierarchy. Parent folder contains child files.
- `blocks (-a)`: Stored in frontmatter. Task blocked until all blockers are `done`.

### Archive Behavior

When a task is marked done:
- **Root tasks**: Immediately moved to `.tsk/archive/`
- **Child tasks**: Stay in parent folder until parent is closed
- **Parent tasks**: Only archive when ALL children are closed (moves entire folder)

## License

MIT
