const std = @import("std");
const fs = std.fs;
const Allocator = std.mem.Allocator;
const storage_mod = @import("storage.zig");
const build_options = @import("build_options");

const libc = @cImport({
    @cInclude("time.h");
});

const Storage = storage_mod.Storage;
const Issue = storage_mod.Issue;
const Status = storage_mod.Status;

const TSK_DIR = storage_mod.TSK_DIR;
const max_jsonl_line_bytes = 1024 * 1024;

// Command dispatch table
const Handler = *const fn (Allocator, []const []const u8) anyerror!void;
const Command = struct { names: []const []const u8, handler: Handler };

const commands = [_]Command{
    .{ .names = &.{ "add", "create" }, .handler = cmdAdd },
    .{ .names = &.{ "ls", "list" }, .handler = cmdList },
    .{ .names = &.{ "start", "it" }, .handler = cmdStart },
    .{ .names = &.{ "complete", "done" }, .handler = cmdComplete },
    .{ .names = &.{"unstart"}, .handler = cmdUnstart },
    .{ .names = &.{ "rm", "delete" }, .handler = cmdRm },
    .{ .names = &.{"show"}, .handler = cmdShow },
    .{ .names = &.{"ready"}, .handler = cmdReady },
    .{ .names = &.{"tree"}, .handler = cmdTree },
    .{ .names = &.{"fix"}, .handler = cmdFix },
    .{ .names = &.{"find"}, .handler = cmdFind },
    .{ .names = &.{"update"}, .handler = cmdUpdate },
    .{ .names = &.{"close"}, .handler = cmdClose },
    .{ .names = &.{"purge"}, .handler = cmdPurge },
    .{ .names = &.{"init"}, .handler = cmdInitWrapper },
    .{ .names = &.{ "help", "--help", "-h" }, .handler = cmdHelp },
    .{ .names = &.{ "--version", "-v" }, .handler = cmdVersion },
};

fn findCommand(name: []const u8) ?Handler {
    inline for (commands) |cmd| {
        inline for (cmd.names) |n| {
            if (std.mem.eql(u8, name, n)) return cmd.handler;
        }
    }
    return null;
}

pub fn main() void {
    if (run()) |_| {} else |err| handleError(err);
}

fn run() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() == .leak) @panic("memory leak detected");
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        try cmdReady(allocator, &.{"--json"});
    } else {
        const cmd = args[1];
        if (findCommand(cmd)) |handler| {
            try handler(allocator, args[2..]);
        } else if (std.mem.eql(u8, cmd, "hook")) {
            fatal("Unknown command: hook\n", .{});
        } else {
            // Quick add: tsk "title"
            try cmdAdd(allocator, args[1..]);
        }
    }

    if (stdout_writer) |*writer| {
        try writer.interface.flush();
    }
    if (stderr_writer) |*writer| {
        try writer.interface.flush();
    }
}

fn cmdInitWrapper(allocator: Allocator, args: []const []const u8) !void {
    return cmdInit(allocator, args);
}

fn cmdHelp(_: Allocator, _: []const []const u8) !void {
    return stdout().writeAll(USAGE);
}

fn cmdVersion(_: Allocator, _: []const []const u8) !void {
    return stdout().print("tsk {s} ({s})\n", .{ build_options.version, build_options.git_hash });
}

fn openStorage(allocator: Allocator) !Storage {
    return Storage.open(allocator);
}

// I/O helpers
var stdout_buffer: [4096]u8 = undefined;
var stdout_writer: ?fs.File.Writer = null;
var stderr_buffer: [4096]u8 = undefined;
var stderr_writer: ?fs.File.Writer = null;

fn stdout() *std.Io.Writer {
    if (stdout_writer == null) {
        stdout_writer = fs.File.stdout().writer(&stdout_buffer);
    }
    return &stdout_writer.?.interface;
}

fn stderr() *std.Io.Writer {
    if (stderr_writer == null) {
        stderr_writer = fs.File.stderr().writer(&stderr_buffer);
    }
    return &stderr_writer.?.interface;
}

fn fatal(comptime fmt: []const u8, args: anytype) noreturn {
    const w = stderr();
    w.print(fmt, args) catch {};
    w.flush() catch {};
    std.process.exit(1);
}

fn handleError(err: anyerror) noreturn {
    switch (err) {
        error.OutOfMemory => fatal("Out of memory\n", .{}),
        error.FileNotFound => fatal("Missing issue file or directory in .tsk\n", .{}),
        error.AccessDenied => fatal("Permission denied\n", .{}),
        error.NotDir => fatal("Expected a directory but found a file\n", .{}),
        error.InvalidFrontmatter => fatal("Invalid issue frontmatter\n", .{}),
        error.InvalidStatus => fatal("Invalid issue status\n", .{}),
        error.InvalidId => fatal("Invalid issue id\n", .{}),
        error.DependencyNotFound => fatal("Dependency not found\n", .{}),
        error.DependencyCycle => fatal("Dependency would create a cycle\n", .{}),
        error.IssueAlreadyExists => fatal("Issue already exists\n", .{}),
        error.ChildrenNotClosed => fatal("Cannot close: children are not all closed\n", .{}),
        error.IssueNotFound => fatal("Issue not found\n", .{}),
        error.AmbiguousId => fatal("Ambiguous issue id\n", .{}),
        error.InvalidJsonl => fatal("Invalid JSONL file\n", .{}),
        error.JsonlLineTooLong => fatal("JSONL line too long\n", .{}),
        error.InvalidTimestamp => fatal("Invalid system time\n", .{}),
        error.TimestampOverflow => fatal("System time out of range\n", .{}),
        error.LocaltimeFailed => fatal("Failed to read local time\n", .{}),
        error.IoError => fatal("I/O error\n", .{}),
        else => fatal("Unexpected internal error (code: {s})\n", .{@errorName(err)}),
    }
}

// ID resolution helper - resolves short ID or exits with error
fn resolveIdOrFatal(storage: *storage_mod.Storage, id: []const u8) []const u8 {
    return storage.resolveId(id) catch |err| switch (err) {
        error.IssueNotFound => fatal("Issue not found: {s}\n", .{id}),
        error.AmbiguousId => fatal("Ambiguous ID: {s}\n", .{id}),
        else => fatal("Error resolving ID: {s}\n", .{id}),
    };
}

fn resolveIdActiveOrFatal(storage: *storage_mod.Storage, id: []const u8) []const u8 {
    return storage.resolveIdActive(id) catch |err| switch (err) {
        error.IssueNotFound => fatal("Issue not found: {s}\n", .{id}),
        error.AmbiguousId => fatal("Ambiguous ID: {s}\n", .{id}),
        else => fatal("Error resolving ID: {s}\n", .{id}),
    };
}

// Status parsing helper
fn parseStatusArg(status_str: []const u8) Status {
    return Status.parse(status_str) orelse fatal("Invalid status: {s}\n", .{status_str});
}

// Arg parsing helper
fn getArg(args: []const []const u8, i: *usize, flag: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, args[i.*], flag) and i.* + 1 < args.len) {
        i.* += 1;
        return args[i.*];
    }
    return null;
}

fn hasFlag(args: []const []const u8, flag: []const u8) bool {
    for (args) |arg| {
        if (std.mem.eql(u8, arg, flag)) return true;
    }
    return false;
}

const USAGE =
    \\tsk - Task tracker
    \\
    \\Usage: tsk [command] [options]
    \\
    \\Commands:
    \\  tsk "title"                  Quick add a task
    \\  tsk add "title" [options]    Add a task (-d desc, -P parent, -a after)
    \\  tsk ls [--status S] [--json] List tasks
    \\  tsk start <id>               Start working on a task
    \\  tsk complete <id> [-r reason] Complete a task
    \\  tsk unstart <id>             Set task back to open
    \\  tsk rm <id>                  Remove a task
    \\  tsk show <id>                Show task details
    \\  tsk ready [--json]           Show unblocked tasks
    \\  tsk tree [id]                Show hierarchy (with id: includes closed children)
    \\  tsk fix                      Repair missing parents
    \\  tsk find "query"             Search all tasks (open first, then archived)
    \\  tsk purge                    Delete archived tasks
    \\  tsk init                     Initialize .tsk directory
    \\
    \\Examples:
    \\  tsk "Fix the bug"
    \\  tsk add "Design API" -d "REST endpoints"
    \\  tsk add "Implement" -P tsk-1 -a tsk-2
    \\  tsk start tsk-3
    \\  tsk complete tsk-3 -r "shipped"
    \\
;

fn gitAddTsk(allocator: Allocator) !void {
    // Add .tsk to git if in a git repo
    fs.cwd().access(".git", .{}) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };

    // Run git add .tsk
    var child = std.process.Child.init(&.{ "git", "add", TSK_DIR }, allocator);
    const term = try child.spawnAndWait();
    switch (term) {
        .Exited => |code| {
            if (code != 0) {
                try stderr().print("Warning: git add failed with exit code {d}\n", .{code});
            }
        },
        .Signal => |sig| try stderr().print("Warning: git add killed by signal {d}\n", .{sig}),
        else => try stderr().writeAll("Warning: git add terminated abnormally\n"),
    }
}

fn cmdInit(allocator: Allocator, args: []const []const u8) !void {
    var storage = try openStorage(allocator);
    defer storage.close();

    // Handle --from-jsonl flag for migration
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (getArg(args, &i, "--from-jsonl")) |jsonl_path| {
            const result = try hydrateFromJsonl(allocator, &storage, jsonl_path);
            if (result.imported > 0) try stdout().print("Imported {d} issues from {s}\n", .{ result.imported, jsonl_path });
            if (result.skipped > 0) try stderr().print("Warning: skipped {d} issues due to errors\n", .{result.skipped});
            if (result.dep_skipped > 0) try stderr().print("Warning: skipped {d} dependencies due to errors\n", .{result.dep_skipped});
        }
    }

    try gitAddTsk(allocator);
}

fn cmdAdd(allocator: Allocator, args: []const []const u8) !void {
    if (args.len == 0) fatal("Usage: tsk add <title> [options]\n", .{});

    var title: []const u8 = "";
    var description: []const u8 = "";
    var parent: ?[]const u8 = null;
    var after: ?[]const u8 = null; // -a: creates blocks dependency
    var position_after: ?[]const u8 = null; // --after: positioning
    var position_before: ?[]const u8 = null; // --before: positioning

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (getArg(args, &i, "-d")) |v| {
            description = v;
        } else if (getArg(args, &i, "-P")) |v| {
            parent = v;
        } else if (getArg(args, &i, "-a")) |v| {
            after = v;
        } else if (getArg(args, &i, "--after")) |v| {
            position_after = v;
        } else if (getArg(args, &i, "--before")) |v| {
            position_before = v;
        } else if (title.len == 0 and args[i].len > 0 and args[i][0] != '-') {
            title = args[i];
        }
    }

    if (title.len == 0) fatal("Error: title required\n", .{});
    if (parent != null and after != null and std.mem.eql(u8, parent.?, after.?)) {
        fatal("Error: parent and after cannot be the same issue\n", .{});
    }
    if (position_after != null and position_before != null) {
        fatal("Error: cannot use both --after and --before\n", .{});
    }
    if (parent != null and (position_after != null or position_before != null)) {
        fatal("Error: cannot use -P with --after/--before (parent is inferred from position target)\n", .{});
    }

    var storage = try openStorage(allocator);
    defer storage.close();

    const id = try storage_mod.generateId(allocator);
    defer allocator.free(id);

    var ts_buf: [40]u8 = undefined;
    const now = try formatTimestamp(&ts_buf);

    // Handle after dependency (blocks)
    var blocks: []const []const u8 = &.{};
    var blocks_buf: [1][]const u8 = undefined;
    var resolved_after: ?[]const u8 = null;
    if (after) |after_id| {
        // Resolve short ID if needed
        resolved_after = storage.resolveId(after_id) catch |err| switch (err) {
            error.IssueNotFound => fatal("After issue not found: {s}\n", .{after_id}),
            error.AmbiguousId => fatal("Ambiguous ID: {s}\n", .{after_id}),
            else => return err,
        };
        blocks_buf[0] = resolved_after.?;
        blocks = &blocks_buf;
    }
    defer if (resolved_after) |r| allocator.free(r);

    // Resolve positioning targets
    var resolved_position_after: ?[]const u8 = null;
    var resolved_position_before: ?[]const u8 = null;
    if (position_after) |pos_id| {
        resolved_position_after = storage.resolveId(pos_id) catch |err| switch (err) {
            error.IssueNotFound => fatal("Position target not found: {s}\n", .{pos_id}),
            error.AmbiguousId => fatal("Ambiguous ID: {s}\n", .{pos_id}),
            else => return err,
        };
    }
    defer if (resolved_position_after) |p| allocator.free(p);

    if (position_before) |pos_id| {
        resolved_position_before = storage.resolveId(pos_id) catch |err| switch (err) {
            error.IssueNotFound => fatal("Position target not found: {s}\n", .{pos_id}),
            error.AmbiguousId => fatal("Ambiguous ID: {s}\n", .{pos_id}),
            else => return err,
        };
    }
    defer if (resolved_position_before) |p| allocator.free(p);

    // If positioning is specified but no parent, infer parent from position target
    var resolved_parent: ?[]const u8 = null;
    var inferred_parent: ?[]const u8 = null;
    if (parent) |parent_id| {
        resolved_parent = storage.resolveId(parent_id) catch |err| switch (err) {
            error.IssueNotFound => fatal("Parent issue not found: {s}\n", .{parent_id}),
            error.AmbiguousId => fatal("Ambiguous ID: {s}\n", .{parent_id}),
            else => return err,
        };
    } else if (resolved_position_after != null or resolved_position_before != null) {
        // Infer parent from position target
        const target_id = resolved_position_after orelse resolved_position_before.?;
        const target_issue = storage.getIssue(target_id) catch |err| switch (err) {
            error.IssueNotFound => fatal("Position target not found\n", .{}),
            else => return err,
        } orelse fatal("Position target not found\n", .{});
        defer target_issue.deinit(allocator);

        if (target_issue.parent) |p| {
            inferred_parent = try allocator.dupe(u8, p);
            resolved_parent = inferred_parent;
        }
    }
    defer if (resolved_parent != null and inferred_parent == null) allocator.free(resolved_parent.?);
    defer if (inferred_parent) |p| allocator.free(p);

    // Calculate peer_index based on positioning
    const peer_index = storage.calculatePeerIndex(
        resolved_parent,
        resolved_position_after,
        resolved_position_before,
    ) catch |err| switch (err) {
        error.IssueNotFound => fatal("Position target not found among siblings\n", .{}),
        else => return err,
    };

    const issue = Issue{
        .id = id,
        .title = title,
        .description = description,
        .status = .open,
        .assignee = null,
        .created_at = now,
        .closed_at = null,
        .close_reason = null,
        .blocks = blocks,
        .peer_index = peer_index,
    };

    storage.createIssue(issue, resolved_parent) catch |err| switch (err) {
        error.DependencyNotFound => fatal("Parent or after issue not found\n", .{}),
        error.DependencyCycle => fatal("Dependency would create a cycle\n", .{}),
        else => return err,
    };

    const w = stdout();
    if (hasFlag(args, "--json")) {
        try writeIssueJson(issue, w);
        try w.writeByte('\n');
    } else {
        try w.print("{s}\n", .{id});
    }
}

fn cmdList(allocator: Allocator, args: []const []const u8) !void {
    var filter_status: ?Status = null;
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (getArg(args, &i, "--status")) |v| filter_status = parseStatusArg(v);
    }

    var storage = try openStorage(allocator);
    defer storage.close();

    const issues = try storage.listIssues(filter_status);
    defer storage_mod.freeIssues(allocator, issues);

    try writeIssueList(issues, filter_status == null, hasFlag(args, "--json"));
}

fn cmdReady(allocator: Allocator, args: []const []const u8) !void {
    var storage = try openStorage(allocator);
    defer storage.close();

    const issues = try storage.getReadyIssues();
    defer storage_mod.freeIssues(allocator, issues);

    try writeIssueList(issues, false, hasFlag(args, "--json"));
}

fn writeIssueList(issues: []const Issue, skip_done: bool, use_json: bool) !void {
    const w = stdout();
    if (use_json) {
        try w.writeByte('[');
        var first = true;
        for (issues) |issue| {
            if (skip_done and issue.status == .closed) continue;
            if (!first) try w.writeByte(',');
            first = false;
            try writeIssueJson(issue, w);
        }
        try w.writeAll("]\n");
    } else {
        for (issues) |issue| {
            if (skip_done and issue.status == .closed) continue;
            try w.print("[{s}] {c} {s}\n", .{ issue.id, issue.status.char(), issue.title });
        }
    }
}

fn cmdStart(allocator: Allocator, args: []const []const u8) !void {
    if (args.len == 0) fatal("Usage: tsk start <id> [id2 ...]\n", .{});

    var storage = try openStorage(allocator);
    defer storage.close();

    const results = try storage.resolveIds(args);
    defer storage_mod.freeResolveResults(allocator, results);

    for (results, 0..) |result, i| {
        switch (result) {
            .ok => |id| try storage.updateStatus(id, .active, null, null),
            .not_found => fatal("Issue not found: {s}\n", .{args[i]}),
            .ambiguous => fatal("Ambiguous ID: {s}\n", .{args[i]}),
        }
    }
}

fn cmdUnstart(allocator: Allocator, args: []const []const u8) !void {
    if (args.len == 0) fatal("Usage: tsk unstart <id> [id2 ...]\n", .{});

    var storage = try openStorage(allocator);
    defer storage.close();

    const results = try storage.resolveIds(args);
    defer storage_mod.freeResolveResults(allocator, results);

    for (results, 0..) |result, i| {
        switch (result) {
            .ok => |id| try storage.updateStatus(id, .open, null, null),
            .not_found => fatal("Issue not found: {s}\n", .{args[i]}),
            .ambiguous => fatal("Ambiguous ID: {s}\n", .{args[i]}),
        }
    }
}

fn cmdComplete(allocator: Allocator, args: []const []const u8) !void {
    if (args.len == 0) fatal("Usage: tsk complete <id> [id2 ...] [-r reason]\n", .{});

    var reason: ?[]const u8 = null;
    var ids: std.ArrayList([]const u8) = .{};
    defer ids.deinit(allocator);

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (getArg(args, &i, "-r")) |v| {
            reason = v;
        } else {
            try ids.append(allocator, args[i]);
        }
    }

    if (ids.items.len == 0) fatal("Usage: tsk complete <id> [id2 ...] [-r reason]\n", .{});

    var storage = try openStorage(allocator);
    defer storage.close();

    const results = try storage.resolveIds(ids.items);
    defer storage_mod.freeResolveResults(allocator, results);

    var ts_buf: [40]u8 = undefined;
    const now = try formatTimestamp(&ts_buf);

    for (results, 0..) |result, idx| {
        switch (result) {
            .ok => |id| storage.updateStatus(id, .closed, now, reason) catch |err| switch (err) {
                error.ChildrenNotClosed => fatal("Cannot close {s}: children are not all closed\n", .{id}),
                else => return err,
            },
            .not_found => fatal("Issue not found: {s}\n", .{ids.items[idx]}),
            .ambiguous => fatal("Ambiguous ID: {s}\n", .{ids.items[idx]}),
        }
    }
}

fn cmdRm(allocator: Allocator, args: []const []const u8) !void {
    if (args.len == 0) fatal("Usage: tsk rm <id> [id2 ...]\n", .{});

    var storage = try openStorage(allocator);
    defer storage.close();

    const results = try storage.resolveIds(args);
    defer storage_mod.freeResolveResults(allocator, results);

    for (results, 0..) |result, i| {
        switch (result) {
            .ok => |id| try storage.deleteIssue(id),
            .not_found => fatal("Issue not found: {s}\n", .{args[i]}),
            .ambiguous => fatal("Ambiguous ID: {s}\n", .{args[i]}),
        }
    }
}

fn cmdShow(allocator: Allocator, args: []const []const u8) !void {
    if (args.len == 0) fatal("Usage: tsk show <id>\n", .{});

    var storage = try openStorage(allocator);
    defer storage.close();

    const resolved = resolveIdOrFatal(&storage, args[0]);
    defer allocator.free(resolved);

    const content = try storage.getIssueRaw(resolved) orelse fatal("Issue not found: {s}\n", .{args[0]});
    defer allocator.free(content);

    try stdout().writeAll(content);
}

fn cmdTree(allocator: Allocator, args: []const []const u8) !void {
    if (hasFlag(args, "--help") or hasFlag(args, "-h")) {
        const w = stdout();
        try w.writeAll(
            \\Usage: tsk tree [id]
            \\
            \\Show task hierarchy.
            \\
            \\Without arguments: shows all open root tasks and their children.
            \\With id: shows that specific task's tree (including closed children).
            \\
            \\Examples:
            \\  tsk tree                    Show all open root tasks
            \\  tsk tree my-project         Show specific task and its children
            \\
        );
        return;
    }
    if (args.len > 1) fatal("Usage: tsk tree [id]\n", .{});

    var storage = try openStorage(allocator);
    defer storage.close();

    const all_issues = try storage.listIssues(null);
    defer storage_mod.freeIssues(allocator, all_issues);

    var status_by_id = try storage.buildStatusMap(all_issues);
    defer status_by_id.deinit();

    const w = stdout();
    if (args.len == 1) {
        const resolved = resolveIdActiveOrFatal(&storage, args[0]);
        defer allocator.free(resolved);

        const root = try storage.getIssue(resolved) orelse fatal("Issue not found: {s}\n", .{args[0]});
        defer root.deinit(allocator);

        try w.print("[{s}] {s} {s}\n", .{ root.id, root.status.symbol(), root.title });

        const children = try storage.getChildrenWithStatusMap(root.id, &status_by_id);
        defer storage_mod.freeChildIssues(allocator, children);

        for (children) |child| {
            const blocked_msg: []const u8 = if (child.blocked) " (blocked)" else "";
            try w.print(
                "  └─ [{s}] {s} {s}{s}\n",
                .{ child.issue.id, child.issue.status.symbol(), child.issue.title, blocked_msg },
            );
        }
        return;
    }

    const roots = try storage.getRootIssues();
    defer storage_mod.freeIssues(allocator, roots);

    for (roots) |root| {
        try w.print("[{s}] {s} {s}\n", .{ root.id, root.status.symbol(), root.title });

        const children = try storage.getChildrenWithStatusMap(root.id, &status_by_id);
        defer storage_mod.freeChildIssues(allocator, children);

        for (children) |child| {
            const blocked_msg: []const u8 = if (child.blocked) " (blocked)" else "";
            try w.print(
                "  └─ [{s}] {s} {s}{s}\n",
                .{ child.issue.id, child.issue.status.symbol(), child.issue.title, blocked_msg },
            );
        }
    }
}

fn cmdFix(allocator: Allocator, _: []const []const u8) !void {
    var storage = try openStorage(allocator);
    defer storage.close();

    const result = try storage.fixOrphans();

    const w = stdout();
    if (result.folders == 0) {
        try w.writeAll("No fixes needed\n");
        return;
    }
    try w.print("Fixed {d} orphan parent(s), moved {d} file(s)\n", .{ result.folders, result.files });
}

fn cmdFind(allocator: Allocator, args: []const []const u8) !void {
    if (args.len == 0 or hasFlag(args, "--help") or hasFlag(args, "-h")) {
        const w = stdout();
        try w.writeAll(
            \\Usage: tsk find <query>
            \\
            \\Search all tasks (open first, then archived).
            \\
            \\Searches: title, description, close-reason, created-at, closed-at
            \\
            \\Examples:
            \\  tsk find "auth"      Search for tasks mentioning auth
            \\  tsk find "2026-01"   Find tasks from January 2026
            \\
        );
        return;
    }

    var storage = try openStorage(allocator);
    defer storage.close();

    const issues = try storage.searchIssues(args[0]);
    defer storage_mod.freeIssues(allocator, issues);

    const w = stdout();
    for (issues) |issue| {
        if (issue.status != .closed) {
            try w.print("[{s}] {c} {s}\n", .{ issue.id, issue.status.char(), issue.title });
        }
    }
    for (issues) |issue| {
        if (issue.status == .closed) {
            try w.print("[{s}] {c} {s}\n", .{ issue.id, issue.status.char(), issue.title });
        }
    }
}

fn cmdUpdate(allocator: Allocator, args: []const []const u8) !void {
    if (args.len == 0) fatal("Usage: tsk update <id> [--status S]\n", .{});

    var new_status: ?Status = null;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (getArg(args, &i, "--status")) |v| new_status = parseStatusArg(v);
    }

    const status = new_status orelse fatal("--status required\n", .{});

    var storage = try openStorage(allocator);
    defer storage.close();

    const resolved = resolveIdOrFatal(&storage, args[0]);
    defer allocator.free(resolved);

    var ts_buf: [40]u8 = undefined;
    const closed_at: ?[]const u8 = if (status == .closed) try formatTimestamp(&ts_buf) else null;

    storage.updateStatus(resolved, status, closed_at, null) catch |err| switch (err) {
        error.ChildrenNotClosed => fatal("Cannot close: children are not all closed\n", .{}),
        else => return err,
    };
}

fn cmdClose(allocator: Allocator, args: []const []const u8) !void {
    if (args.len == 0) fatal("Usage: tsk close <id> [--reason R]\n", .{});

    var reason: ?[]const u8 = null;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (getArg(args, &i, "--reason")) |v| reason = v;
    }

    var storage = try openStorage(allocator);
    defer storage.close();

    const resolved = resolveIdOrFatal(&storage, args[0]);
    defer allocator.free(resolved);

    var ts_buf: [40]u8 = undefined;
    const now = try formatTimestamp(&ts_buf);

    storage.updateStatus(resolved, .closed, now, reason) catch |err| switch (err) {
        error.ChildrenNotClosed => fatal("Cannot close: children are not all closed\n", .{}),
        else => return err,
    };
}

fn cmdPurge(allocator: Allocator, _: []const []const u8) !void {
    var storage = try openStorage(allocator);
    defer storage.close();

    try storage.purgeArchive();
    try stdout().writeAll("Archive purged\n");
}

fn formatTimestamp(buf: []u8) ![]const u8 {
    const nanos = std.time.nanoTimestamp();
    if (nanos < 0) return error.InvalidTimestamp;
    const epoch_nanos: u128 = @intCast(nanos);
    const epoch_secs: libc.time_t = std.math.cast(libc.time_t, epoch_nanos / 1_000_000_000) orelse return error.TimestampOverflow;
    const micros: u64 = @intCast((epoch_nanos % 1_000_000_000) / 1000);

    var tm: libc.struct_tm = undefined;
    if (libc.localtime_r(&epoch_secs, &tm) == null) {
        return error.LocaltimeFailed;
    }

    const year: u64 = @intCast(tm.tm_year + 1900);
    const month: u64 = @intCast(tm.tm_mon + 1);
    const day: u64 = @intCast(tm.tm_mday);
    const hours: u64 = @intCast(tm.tm_hour);
    const mins: u64 = @intCast(tm.tm_min);
    const secs: u64 = @intCast(tm.tm_sec);

    const tz_offset_secs: i64 = tm.tm_gmtoff;
    const tz_sign: u8 = if (tz_offset_secs >= 0) '+' else '-';
    const tz_abs: u64 = @abs(tz_offset_secs);
    const tz_hours_abs: u64 = tz_abs / 3600;
    const tz_mins: u64 = (tz_abs % 3600) / 60;

    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}{c}{d:0>2}:{d:0>2}", .{
        year, month, day, hours, mins, secs, micros, tz_sign, tz_hours_abs, tz_mins,
    });
}

const JsonIssue = struct {
    id: []const u8,
    title: []const u8,
    description: ?[]const u8 = null,
    status: []const u8,
    created_at: []const u8,
    closed_at: ?[]const u8 = null,
    close_reason: ?[]const u8 = null,
    peer_index: f64,
};

fn writeIssueJson(issue: Issue, w: *std.Io.Writer) !void {
    const json_issue = JsonIssue{
        .id = issue.id,
        .title = issue.title,
        .description = if (issue.description.len > 0) issue.description else null,
        .status = issue.status.display(),
        .created_at = issue.created_at,
        .closed_at = issue.closed_at,
        .close_reason = issue.close_reason,
        .peer_index = issue.peer_index,
    };
    try std.json.Stringify.value(json_issue, .{}, w);
}

// JSONL hydration for migration
const JsonlDependency = struct {
    depends_on_id: []const u8,
    type: ?[]const u8 = null,
};

const JsonlIssue = struct {
    id: []const u8,
    title: []const u8,
    description: ?[]const u8 = null,
    status: []const u8,
    issue_type: ?[]const u8 = null, // ignored, kept for backward compatibility
    assignee: ?[]const u8 = null,
    created_at: []const u8,
    updated_at: ?[]const u8 = null,
    closed_at: ?[]const u8 = null,
    close_reason: ?[]const u8 = null,
    dependencies: ?[]const JsonlDependency = null,
};

const HydrateResult = struct {
    imported: usize,
    skipped: usize,
    dep_skipped: usize,
};

fn hydrateFromJsonl(allocator: Allocator, storage: *Storage, jsonl_path: []const u8) !HydrateResult {
    const file = fs.cwd().openFile(jsonl_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return .{ .imported = 0, .skipped = 0, .dep_skipped = 0 },
        else => return err,
    };
    defer file.close();

    var count: usize = 0;
    var skipped: usize = 0;
    var dep_skipped: usize = 0;
    const read_buf = try allocator.alloc(u8, max_jsonl_line_bytes);
    defer allocator.free(read_buf);
    var file_reader = fs.File.Reader.init(file, read_buf);
    const reader = &file_reader.interface;

    while (true) {
        const line = reader.takeDelimiter('\n') catch |err| switch (err) {
            error.StreamTooLong => return error.JsonlLineTooLong,
            error.ReadFailed => break,
        } orelse break;

        if (line.len == 0) continue;

        const parsed = std.json.parseFromSlice(JsonlIssue, allocator, line, .{
            .ignore_unknown_fields = true,
        }) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidJsonl,
        };
        defer parsed.deinit();

        const obj = parsed.value;

        // Normalize status
        const status = Status.parse(obj.status) orelse blk: {
            if (std.mem.eql(u8, obj.status, "in_progress")) break :blk Status.active;
            if (std.mem.eql(u8, obj.status, "done")) break :blk Status.closed;
            break :blk Status.open;
        };

        const issue = Issue{
            .id = obj.id,
            .title = obj.title,
            .description = obj.description orelse "",
            .status = status,
            .assignee = obj.assignee,
            .created_at = obj.created_at,
            .closed_at = obj.closed_at,
            .close_reason = obj.close_reason,
            .blocks = &.{},
        };

        // Determine parent from dependencies
        var parent_id: ?[]const u8 = null;
        if (obj.dependencies) |deps| {
            for (deps) |dep| {
                const dep_type = dep.type orelse "blocks";
                if (std.mem.eql(u8, dep_type, "parent-child")) {
                    parent_id = dep.depends_on_id;
                    break;
                }
            }
        }

        storage.createIssue(issue, parent_id) catch |err| switch (err) {
            error.IssueAlreadyExists => continue, // Duplicate in JSONL, skip
            error.OutOfMemory => return error.OutOfMemory,
            else => {
                skipped += 1;
                continue;
            },
        };

        // Add block dependencies
        if (obj.dependencies) |deps| {
            for (deps) |dep| {
                const dep_type = dep.type orelse "blocks";
                if (std.mem.eql(u8, dep_type, "blocks")) {
                    storage.addDependency(obj.id, dep.depends_on_id, "blocks") catch |err| switch (err) {
                        error.OutOfMemory => return error.OutOfMemory,
                        else => {
                            dep_skipped += 1;
                        },
                    };
                }
            }
        }

        count += 1;
    }

    // Second pass: archive all closed issues (after all imports, so parent-child relationships are complete)
    const all_issues = try storage.listIssues(null);
    defer storage_mod.freeIssues(allocator, all_issues);
    for (all_issues) |iss| {
        if (iss.status == .closed) {
            storage.archiveIssue(iss.id) catch |err| switch (err) {
                // ChildrenNotClosed is expected if parent closed but children aren't
                error.ChildrenNotClosed => {},
                // IssueNotFound can happen if already archived by parent move
                error.IssueNotFound => {},
                else => return err,
            };
        }
    }

    return .{ .imported = count, .skipped = skipped, .dep_skipped = dep_skipped };
}
