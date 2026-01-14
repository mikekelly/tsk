---
title: "Fix HIGH: ParseResult.deinit leak"
status: closed
priority: 1
issue-type: task
created-at: "\"\\\"\\\\\\\"2026-01-10T06:54:28.826726+02:00\\\\\\\"\\\"\""
closed-at: "\"2026-01-10T07:01:44.070335+02:00\""
close-reason: manual ownership tracking is correct - blocks transferred to Issue on success, freed on error
---

storage.zig:753-755
