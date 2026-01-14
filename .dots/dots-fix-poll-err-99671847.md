---
title: Fix poll error masking in hookSync
status: closed
priority: 1
issue-type: task
created-at: "\"\\\"\\\\\\\"2026-01-10T06:31:40.100714+02:00\\\\\\\"\\\"\""
closed-at: "\"2026-01-10T06:32:33.252740+02:00\""
close-reason: propagate poll errors via try
---

main.zig:713 - poll errors silently return success. Should propagate or log.
