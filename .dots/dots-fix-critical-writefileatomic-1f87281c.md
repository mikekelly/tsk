---
title: "Fix CRITICAL: writeFileAtomic race"
status: closed
priority: 0
issue-type: task
created-at: "\"\\\"\\\\\\\"2026-01-10T06:54:16.010949+02:00\\\\\\\"\\\"\""
closed-at: "\"2026-01-10T06:56:37.382709+02:00\""
close-reason: unique random tmp suffix prevents concurrent write conflicts
---

storage.zig:44-58 - no locking, concurrent writes corrupt
