---
title: Add file locking to hookSync
status: closed
priority: 0
issue-type: task
created-at: "\"\\\"\\\\\\\"2026-01-10T06:29:57.130349+02:00\\\\\\\"\\\"\""
closed-at: "\"2026-01-10T06:31:13.068378+02:00\""
close-reason: added flock-based locking to hookSync
---

TOCTOU race: loadMapping->process->saveMappingAtomic has no locking. Concurrent syncs clobber each other.
