---
title: "Fix CRITICAL: migration error masking"
status: closed
priority: 0
issue-type: task
created-at: "\"\\\"\\\\\\\"2026-01-10T06:54:15.802742+02:00\\\\\\\"\\\"\""
closed-at: "\"2026-01-10T06:56:00.700412+02:00\""
close-reason: added HydrateResult with skip counts, stderr warnings
---

main.zig:996,1007 - else=>continue swallows errors
