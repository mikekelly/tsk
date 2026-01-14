---
title: "Fix HIGH: resolveIdOrFatal memory leak"
status: closed
priority: 1
issue-type: task
created-at: "\"\\\"\\\\\\\"2026-01-10T06:54:16.409280+02:00\\\\\\\"\\\"\""
closed-at: "\"2026-01-10T06:57:55.385096+02:00\""
close-reason: cleanup resolved IDs before fatal exit
---

main.zig:144 - fatal doesn't unwind errdefer
