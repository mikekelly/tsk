const std = @import("std");
const fs = std.fs;
const Allocator = std.mem.Allocator;

pub const TSK_DIR = ".tsk";
const ARCHIVE_DIR = ".tsk/archive";

// Buffer size constants
const MAX_PATH_LEN = 512; // Maximum path length for file operations
const MAX_ID_LEN = 128; // Maximum ID length (validated in validateId)
const MAX_ISSUE_FILE_SIZE = 1024 * 1024; // 1MB max issue file

// Errors
pub const StorageError = error{
    IssueNotFound,
    IssueAlreadyExists,
    AmbiguousId,
    DependencyNotFound,
    DependencyCycle,
    DependencyConflict,
    ChildrenNotClosed,
    InvalidFrontmatter,
    InvalidStatus,
    InvalidId,
    IoError,
};

/// Validates that an ID is safe for use in paths and YAML
pub fn validateId(id: []const u8) StorageError!void {
    if (id.len == 0) return StorageError.InvalidId;
    if (id.len > MAX_ID_LEN) return StorageError.InvalidId;
    // Reject path traversal attempts
    if (std.mem.indexOf(u8, id, "/") != null) return StorageError.InvalidId;
    if (std.mem.indexOf(u8, id, "\\") != null) return StorageError.InvalidId;
    if (std.mem.indexOf(u8, id, "..") != null) return StorageError.InvalidId;
    if (std.mem.eql(u8, id, ".")) return StorageError.InvalidId;
    // Reject control characters and YAML-sensitive characters
    for (id) |c| {
        if (c < 0x20 or c == 0x7F) return StorageError.InvalidId;
        if (c == '#' or c == ':' or c == '\'' or c == '"') return StorageError.InvalidId;
    }
}

/// Write content to file atomically (write to unique .tmp, sync, rename)
/// Uses random suffix to prevent concurrent write conflicts
fn writeFileAtomic(dir: fs.Dir, path: []const u8, content: []const u8) !void {
    // Generate unique tmp filename with random suffix
    var rand_buf: [4]u8 = undefined;
    std.crypto.random.bytes(&rand_buf);
    const hex = std.fmt.bytesToHex(rand_buf, .lower);

    var tmp_path_buf: [MAX_PATH_LEN + 16]u8 = undefined; // +16 for ".XXXXXXXX.tmp"
    const tmp_path = std.fmt.bufPrint(&tmp_path_buf, "{s}.{s}.tmp", .{ path, hex }) catch return StorageError.IoError;

    const tmp_file = try dir.createFile(tmp_path, .{});
    defer tmp_file.close();
    errdefer dir.deleteFile(tmp_path) catch {};
    try tmp_file.writeAll(content);
    try tmp_file.sync();

    try dir.rename(tmp_path, path);
}

// Status enum with comptime string map
pub const Status = enum {
    open,
    active,
    closed,

    const map = std.StaticStringMap(Status).initComptime(.{
        .{ "open", .open },
        .{ "active", .active },
        .{ "closed", .closed },
        .{ "done", .closed }, // alias
    });

    pub fn parse(s: []const u8) ?Status {
        return map.get(s);
    }

    pub fn toString(self: Status) []const u8 {
        return switch (self) {
            .open => "open",
            .active => "active",
            .closed => "closed",
        };
    }

    pub fn display(self: Status) []const u8 {
        return switch (self) {
            .open => "open",
            .active => "active",
            .closed => "done",
        };
    }

    pub fn char(self: Status) u8 {
        return switch (self) {
            .open => 'o',
            .active => '>',
            .closed => 'x',
        };
    }

    pub fn symbol(self: Status) []const u8 {
        return switch (self) {
            .open => "○",
            .active => ">",
            .closed => "✓",
        };
    }
};

pub const Issue = struct {
    id: []const u8,
    title: []const u8,
    description: []const u8,
    status: Status,
    assignee: ?[]const u8,
    created_at: []const u8,
    closed_at: ?[]const u8,
    close_reason: ?[]const u8,
    blocks: []const []const u8,
    peer_index: f64 = 0.0,
    // Computed from path, not stored in frontmatter
    parent: ?[]const u8 = null,

    /// Compare issues by peer_index (ascending), then created_at (ascending)
    pub fn order(_: void, a: Issue, b: Issue) bool {
        // Sort by peer_index first
        if (a.peer_index != b.peer_index) return a.peer_index < b.peer_index;
        // Fall back to created_at
        return std.mem.order(u8, a.created_at, b.created_at) == .lt;
    }

    /// Create a copy with updated status fields (borrows all strings)
    pub fn withStatus(self: Issue, status: Status, closed_at: ?[]const u8, close_reason: ?[]const u8) Issue {
        return .{
            .id = self.id,
            .title = self.title,
            .description = self.description,
            .status = status,
            .assignee = self.assignee,
            .created_at = self.created_at,
            .closed_at = closed_at,
            .close_reason = close_reason,
            .blocks = self.blocks,
            .peer_index = self.peer_index,
            .parent = self.parent,
        };
    }

    /// Create a copy with updated blocks (borrows all strings)
    pub fn withBlocks(self: Issue, blocks: []const []const u8) Issue {
        return .{
            .id = self.id,
            .title = self.title,
            .description = self.description,
            .status = self.status,
            .assignee = self.assignee,
            .created_at = self.created_at,
            .closed_at = self.closed_at,
            .close_reason = self.close_reason,
            .blocks = blocks,
            .peer_index = self.peer_index,
            .parent = self.parent,
        };
    }

    /// Create a deep copy of this issue with all strings duplicated
    pub fn clone(self: Issue, allocator: Allocator) !Issue {
        const id = try allocator.dupe(u8, self.id);
        errdefer allocator.free(id);

        const title = try allocator.dupe(u8, self.title);
        errdefer allocator.free(title);

        const description = try allocator.dupe(u8, self.description);
        errdefer allocator.free(description);

        const assignee = if (self.assignee) |a| try allocator.dupe(u8, a) else null;
        errdefer if (assignee) |a| allocator.free(a);

        const created_at = try allocator.dupe(u8, self.created_at);
        errdefer allocator.free(created_at);

        const closed_at = if (self.closed_at) |c| try allocator.dupe(u8, c) else null;
        errdefer if (closed_at) |c| allocator.free(c);

        const close_reason = if (self.close_reason) |r| try allocator.dupe(u8, r) else null;
        errdefer if (close_reason) |r| allocator.free(r);

        var blocks: std.ArrayList([]const u8) = .{};
        errdefer {
            for (blocks.items) |b| allocator.free(b);
            blocks.deinit(allocator);
        }
        for (self.blocks) |b| {
            const duped = try allocator.dupe(u8, b);
            try blocks.append(allocator, duped);
        }

        const parent = if (self.parent) |p| try allocator.dupe(u8, p) else null;
        errdefer if (parent) |p| allocator.free(p);

        return Issue{
            .id = id,
            .title = title,
            .description = description,
            .status = self.status,
            .assignee = assignee,
            .created_at = created_at,
            .closed_at = closed_at,
            .close_reason = close_reason,
            .blocks = try blocks.toOwnedSlice(allocator),
            .peer_index = self.peer_index,
            .parent = parent,
        };
    }

    pub fn deinit(self: *const Issue, allocator: Allocator) void {
        allocator.free(self.id);
        allocator.free(self.title);
        allocator.free(self.description);
        if (self.assignee) |s| allocator.free(s);
        allocator.free(self.created_at);
        if (self.closed_at) |s| allocator.free(s);
        if (self.close_reason) |s| allocator.free(s);
        for (self.blocks) |b| allocator.free(b);
        allocator.free(self.blocks);
        if (self.parent) |p| allocator.free(p);
    }
};

const StatusMap = std.StringHashMap(Status);

pub const ResolveResult = union(enum) {
    ok: []const u8,
    not_found,
    ambiguous,
};

pub fn freeResolveResults(allocator: Allocator, results: []ResolveResult) void {
    for (results) |result| {
        switch (result) {
            .ok => |id| allocator.free(id),
            .not_found, .ambiguous => {},
        }
    }
    allocator.free(results);
}

const ResolveState = struct {
    prefix: []const u8,
    match: ?[]const u8 = null,
    ambig: bool = false,

    fn add(self: *ResolveState, allocator: Allocator, id: []const u8) !void {
        if (self.ambig) return;
        if (self.match) |m| {
            allocator.free(m);
            self.match = null;
            self.ambig = true;
            return;
        }
        self.match = try allocator.dupe(u8, id);
    }

    fn deinit(self: *ResolveState, allocator: Allocator) void {
        if (self.match) |m| allocator.free(m);
    }
};

pub fn freeIssues(allocator: Allocator, issues: []const Issue) void {
    for (issues) |*issue| {
        issue.deinit(allocator);
    }
    allocator.free(issues);
}

pub const ChildIssue = struct {
    issue: Issue,
    blocked: bool,

    /// Compare child issues by their inner issue order
    pub fn order(_: void, a: ChildIssue, b: ChildIssue) bool {
        return Issue.order({}, a.issue, b.issue);
    }

    pub fn deinit(self: *const ChildIssue, allocator: Allocator) void {
        self.issue.deinit(allocator);
    }
};

pub fn freeChildIssues(allocator: Allocator, issues: []const ChildIssue) void {
    for (issues) |*issue| {
        issue.deinit(allocator);
    }
    allocator.free(issues);
}

pub const FixResult = struct {
    folders: usize,
    files: usize,
};

pub fn freeOrphanParents(allocator: Allocator, orphans: []const []const u8) void {
    for (orphans) |name| {
        allocator.free(name);
    }
    allocator.free(orphans);
}

// YAML Frontmatter parsing
const Frontmatter = struct {
    title: []const u8 = "",
    status: Status = .open,
    assignee: ?[]const u8 = null,
    created_at: []const u8 = "",
    closed_at: ?[]const u8 = null,
    close_reason: ?[]const u8 = null,
    blocks: []const []const u8 = &.{},
    peer_index: f64 = 0.0,
};

const ParseResult = struct {
    frontmatter: Frontmatter,
    description: []const u8,
    // Track allocated strings for cleanup
    allocated_blocks: [][]const u8,
    allocated_title: ?[]const u8 = null,
    allocated_created_at: ?[]const u8 = null,
    allocated_closed_at: ?[]const u8 = null,
    allocated_close_reason: ?[]const u8 = null,
    allocated_assignee: ?[]const u8 = null,

    pub fn deinit(self: *const ParseResult, allocator: Allocator) void {
        if (self.allocated_title) |t| allocator.free(t);
        if (self.allocated_created_at) |t| allocator.free(t);
        if (self.allocated_closed_at) |t| allocator.free(t);
        if (self.allocated_close_reason) |t| allocator.free(t);
        if (self.allocated_assignee) |t| allocator.free(t);
        for (self.allocated_blocks) |b| allocator.free(b);
        allocator.free(self.allocated_blocks);
    }
};

// Frontmatter field enum and map (file-scope for efficiency)
const FrontmatterField = enum {
    title,
    status,
    assignee,
    created_at,
    closed_at,
    close_reason,
    blocks,
    peer_index,
};

const frontmatter_field_map = std.StaticStringMap(FrontmatterField).initComptime(.{
    .{ "title", .title },
    .{ "status", .status },
    .{ "assignee", .assignee },
    .{ "created-at", .created_at },
    .{ "closed-at", .closed_at },
    .{ "close-reason", .close_reason },
    .{ "blocks", .blocks },
    .{ "peer-index", .peer_index },
});

/// Result of parsing a YAML value - clearly indicates ownership
const YamlValue = union(enum) {
    borrowed: []const u8, // Points to input, caller must NOT free
    owned: []const u8, // Allocated, caller MUST free

    fn slice(self: YamlValue) []const u8 {
        return switch (self) {
            .borrowed => |s| s,
            .owned => |s| s,
        };
    }

    fn getOwned(self: YamlValue) ?[]const u8 {
        return switch (self) {
            .borrowed => null,
            .owned => |s| s,
        };
    }
};

/// Parse a YAML value, handling quoted strings with escape sequences
fn parseYamlValue(allocator: Allocator, value: []const u8) !YamlValue {
    if (value.len < 2 or value[0] != '"' or value[value.len - 1] != '"') {
        // Unquoted value, use as-is (caller should not free)
        return .{ .borrowed = value };
    }
    // Quoted value - unescape it
    var result: std.ArrayList(u8) = .{};
    errdefer result.deinit(allocator);

    var i: usize = 1; // Skip opening quote
    while (i < value.len - 1) { // Stop before closing quote
        if (value[i] == '\\' and i + 1 < value.len - 1) {
            const next = value[i + 1];
            switch (next) {
                'n' => try result.append(allocator, '\n'),
                'r' => try result.append(allocator, '\r'),
                't' => try result.append(allocator, '\t'),
                '"' => try result.append(allocator, '"'),
                '\\' => try result.append(allocator, '\\'),
                else => {
                    try result.append(allocator, '\\');
                    try result.append(allocator, next);
                },
            }
            i += 2; // Skip backslash and escaped char
        } else {
            try result.append(allocator, value[i]);
            i += 1;
        }
    }
    return .{ .owned = try result.toOwnedSlice(allocator) };
}

fn parseFrontmatter(allocator: Allocator, content: []const u8) !ParseResult {
    // Find YAML delimiters
    if (!std.mem.startsWith(u8, content, "---\n")) {
        return StorageError.InvalidFrontmatter;
    }

    const end_marker = std.mem.indexOf(u8, content[4..], "\n---");
    if (end_marker == null) {
        return StorageError.InvalidFrontmatter;
    }

    const yaml_content = content[4 .. 4 + end_marker.?];
    const description_start = 4 + end_marker.? + 4; // skip "\n---"
    const description = if (description_start < content.len)
        std.mem.trim(u8, content[description_start..], "\n\r\t ")
    else
        "";

    var fm = Frontmatter{};
    var blocks_list: std.ArrayList([]const u8) = .{};
    var allocated_title: ?[]const u8 = null;
    var allocated_created_at: ?[]const u8 = null;
    var allocated_closed_at: ?[]const u8 = null;
    var allocated_close_reason: ?[]const u8 = null;
    var allocated_assignee: ?[]const u8 = null;
    errdefer {
        if (allocated_title) |t| allocator.free(t);
        if (allocated_created_at) |t| allocator.free(t);
        if (allocated_closed_at) |t| allocator.free(t);
        if (allocated_close_reason) |t| allocator.free(t);
        if (allocated_assignee) |t| allocator.free(t);
        for (blocks_list.items) |b| allocator.free(b);
        blocks_list.deinit(allocator);
    }

    var in_blocks = false;
    var lines = std.mem.splitScalar(u8, yaml_content, '\n');

    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, "\r\t ");

        // Handle blocks array items
        if (in_blocks) {
            if (std.mem.startsWith(u8, trimmed, "- ")) {
                const block_id = std.mem.trim(u8, trimmed[2..], " ");
                // Validate block ID to prevent path traversal attacks
                validateId(block_id) catch return StorageError.InvalidFrontmatter;
                const duped = try allocator.dupe(u8, block_id);
                try blocks_list.append(allocator, duped);
                continue;
            } else if (trimmed.len > 0 and !std.mem.startsWith(u8, trimmed, " ")) {
                in_blocks = false;
            } else {
                continue;
            }
        }

        // Parse key: value
        const colon_idx = std.mem.indexOf(u8, trimmed, ":") orelse continue;
        const key = trimmed[0..colon_idx];
        const value = std.mem.trim(u8, trimmed[colon_idx + 1 ..], " ");

        const field = frontmatter_field_map.get(key) orelse continue;

        switch (field) {
            .title => {
                const parsed = try parseYamlValue(allocator, value);
                fm.title = parsed.slice();
                allocated_title = parsed.getOwned();
            },
            .status => fm.status = Status.parse(value) orelse return StorageError.InvalidStatus,
            .assignee => {
                if (value.len > 0) {
                    const parsed = try parseYamlValue(allocator, value);
                    fm.assignee = parsed.slice();
                    allocated_assignee = parsed.getOwned();
                }
            },
            .created_at => {
                const parsed = try parseYamlValue(allocator, value);
                fm.created_at = parsed.slice();
                allocated_created_at = parsed.getOwned();
            },
            .closed_at => {
                if (value.len > 0) {
                    const parsed = try parseYamlValue(allocator, value);
                    fm.closed_at = parsed.slice();
                    allocated_closed_at = parsed.getOwned();
                }
            },
            .close_reason => {
                if (value.len > 0) {
                    const parsed = try parseYamlValue(allocator, value);
                    fm.close_reason = parsed.slice();
                    allocated_close_reason = parsed.getOwned();
                }
            },
            .blocks => in_blocks = true,
            .peer_index => fm.peer_index = std.fmt.parseFloat(f64, value) catch return StorageError.InvalidFrontmatter,
        }
    }

    const allocated_blocks = try blocks_list.toOwnedSlice(allocator);
    fm.blocks = allocated_blocks;

    // Validate required fields
    if (fm.title.len == 0 or fm.created_at.len == 0) {
        // Clean up allocations on validation failure
        for (allocated_blocks) |b| allocator.free(b);
        allocator.free(allocated_blocks);
        if (allocated_title) |t| {
            allocator.free(t);
            allocated_title = null; // Prevent errdefer double-free
        }
        if (allocated_created_at) |t| {
            allocator.free(t);
            allocated_created_at = null;
        }
        if (allocated_closed_at) |t| {
            allocator.free(t);
            allocated_closed_at = null;
        }
        if (allocated_close_reason) |t| {
            allocator.free(t);
            allocated_close_reason = null;
        }
        if (allocated_assignee) |t| {
            allocator.free(t);
            allocated_assignee = null;
        }
        return StorageError.InvalidFrontmatter;
    }

    return ParseResult{
        .frontmatter = fm,
        .description = description,
        .allocated_blocks = allocated_blocks,
        .allocated_title = allocated_title,
        .allocated_created_at = allocated_created_at,
        .allocated_closed_at = allocated_closed_at,
        .allocated_close_reason = allocated_close_reason,
        .allocated_assignee = allocated_assignee,
    };
}

/// Returns true if string needs YAML quoting
fn needsYamlQuoting(s: []const u8) bool {
    if (s.len == 0) return true;
    // Check for characters that need quoting
    for (s) |c| {
        if (c == '\n' or c == '\r' or c == ':' or c == '#' or c == '"' or c == '\'' or c == '\\') return true;
    }
    // Leading/trailing whitespace
    if (s[0] == ' ' or s[0] == '\t' or s[s.len - 1] == ' ' or s[s.len - 1] == '\t') return true;
    return false;
}

/// Write a YAML-safe string value, quoting and escaping as needed
fn writeYamlValue(buf: *std.ArrayList(u8), allocator: Allocator, value: []const u8) !void {
    if (!needsYamlQuoting(value)) {
        try buf.appendSlice(allocator, value);
        return;
    }
    // Use double quotes and escape special characters
    try buf.append(allocator, '"');
    for (value) |c| {
        switch (c) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => try buf.append(allocator, c),
        }
    }
    try buf.append(allocator, '"');
}

fn serializeFrontmatter(allocator: Allocator, issue: Issue) ![]u8 {
    var buf: std.ArrayList(u8) = .{};
    errdefer buf.deinit(allocator);

    try buf.appendSlice(allocator, "---\n");
    try buf.appendSlice(allocator, "title: ");
    try writeYamlValue(&buf, allocator, issue.title);
    try buf.appendSlice(allocator, "\nstatus: ");
    try buf.appendSlice(allocator, issue.status.toString());

    if (issue.assignee) |assignee| {
        try buf.appendSlice(allocator, "\nassignee: ");
        try writeYamlValue(&buf, allocator, assignee);
    }

    try buf.appendSlice(allocator, "\ncreated-at: ");
    try writeYamlValue(&buf, allocator, issue.created_at);

    if (issue.closed_at) |closed_at| {
        try buf.appendSlice(allocator, "\nclosed-at: ");
        try writeYamlValue(&buf, allocator, closed_at);
    }

    if (issue.close_reason) |reason| {
        try buf.appendSlice(allocator, "\nclose-reason: ");
        try writeYamlValue(&buf, allocator, reason);
    }

    if (issue.blocks.len > 0) {
        try buf.appendSlice(allocator, "\nblocks:");
        for (issue.blocks) |block_id| {
            try buf.appendSlice(allocator, "\n  - ");
            try buf.appendSlice(allocator, block_id);
        }
    }

    try buf.appendSlice(allocator, "\npeer-index: ");
    var peer_index_buf: [32]u8 = undefined;
    const peer_index_str = std.fmt.bufPrint(&peer_index_buf, "{d}", .{issue.peer_index}) catch return error.OutOfMemory;
    try buf.appendSlice(allocator, peer_index_str);

    try buf.appendSlice(allocator, "\n---\n");

    if (issue.description.len > 0) {
        try buf.appendSlice(allocator, "\n");
        try buf.appendSlice(allocator, issue.description);
        try buf.appendSlice(allocator, "\n");
    }

    return buf.toOwnedSlice(allocator);
}

// ID generation - 8 hex chars
pub fn generateId(allocator: Allocator) ![]u8 {
    var rand_bytes: [4]u8 = undefined;
    std.crypto.random.bytes(&rand_bytes);
    const hex = std.fmt.bytesToHex(rand_bytes, .lower);
    return allocator.dupe(u8, &hex);
}


pub const Storage = struct {
    allocator: Allocator,
    tsk_dir: fs.Dir,

    const Self = @This();

    pub fn open(allocator: Allocator) !Self {
        // Create .tsk directory if needed
        fs.cwd().makeDir(TSK_DIR) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        // Create archive directory if needed
        fs.cwd().makeDir(ARCHIVE_DIR) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const tsk_dir = try fs.cwd().openDir(TSK_DIR, .{ .iterate = true });

        return Self{
            .allocator = allocator,
            .tsk_dir = tsk_dir,
        };
    }

    pub fn close(self: *Self) void {
        self.tsk_dir.close();
    }

    // Resolve a short ID prefix to full ID
    pub fn resolveId(self: *Self, prefix: []const u8) ![]const u8 {
        var states = [_]ResolveState{.{ .prefix = prefix }};
        errdefer states[0].deinit(self.allocator);

        try self.scanResolve(self.tsk_dir, states[0..], null);

        const archive_dir = self.tsk_dir.openDir("archive", .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (archive_dir) |*dir| {
            var d = dir.*;
            defer d.close();
            try self.scanResolve(d, states[0..], null);
        }

        if (states[0].ambig) return StorageError.AmbiguousId;
        if (states[0].match == null) return StorageError.IssueNotFound;

        return states[0].match.?;
    }

    pub fn resolveIdActive(self: *Self, prefix: []const u8) ![]const u8 {
        var states = [_]ResolveState{.{ .prefix = prefix }};
        errdefer states[0].deinit(self.allocator);

        try self.scanResolve(self.tsk_dir, states[0..], null);

        if (states[0].ambig) return StorageError.AmbiguousId;
        if (states[0].match == null) return StorageError.IssueNotFound;

        return states[0].match.?;
    }

    pub fn resolveIds(self: *Self, prefixes: []const []const u8) ![]ResolveResult {
        var states = try self.allocator.alloc(ResolveState, prefixes.len);
        errdefer {
            for (states) |*state| state.deinit(self.allocator);
            self.allocator.free(states);
        }
        for (prefixes, 0..) |prefix, i| {
            states[i] = .{ .prefix = prefix };
        }

        try self.scanResolve(self.tsk_dir, states, null);

        const archive_dir = self.tsk_dir.openDir("archive", .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (archive_dir) |*dir| {
            var d = dir.*;
            defer d.close();
            try self.scanResolve(d, states, null);
        }

        const results = try self.allocator.alloc(ResolveResult, prefixes.len);
        errdefer freeResolveResults(self.allocator, results);

        for (states, 0..) |*state, i| {
            if (state.ambig) {
                results[i] = .ambiguous;
            } else if (state.match) |m| {
                results[i] = .{ .ok = m };
                state.match = null;
            } else {
                results[i] = .not_found;
            }
        }

        for (states) |*state| state.deinit(self.allocator);
        self.allocator.free(states);

        return results;
    }

    fn addResolve(self: *Self, states: []ResolveState, id: []const u8) !void {
        for (states) |*state| {
            if (state.ambig) continue;
            if (std.mem.startsWith(u8, id, state.prefix)) {
                try state.add(self.allocator, id);
            }
        }
    }

    fn scanResolve(self: *Self, dir: fs.Dir, states: []ResolveState, parent_folder: ?[]const u8) !void {
        var iter = dir.iterate();
        while (try iter.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".md")) {
                const id = entry.name[0 .. entry.name.len - 3];
                // Skip if this file matches parent folder name (already counted)
                if (parent_folder) |pf| {
                    if (std.mem.eql(u8, id, pf)) continue;
                }
                try self.addResolve(states, id);
            } else if (entry.kind == .directory and !std.mem.eql(u8, entry.name, "archive")) {
                // Check folder name as potential ID
                try self.addResolve(states, entry.name);
                // Recurse into folder, passing folder name to skip self-reference
                var subdir = try dir.openDir(entry.name, .{ .iterate = true });
                defer subdir.close();
                try self.scanResolve(subdir, states, entry.name);
            }
        }
    }

    pub fn issueExists(self: *Self, id: []const u8) !bool {
        const path = self.findIssuePath(id) catch |err| switch (err) {
            StorageError.IssueNotFound => return false,
            else => return err,
        };
        self.allocator.free(path);
        return true;
    }

    fn findIssuePath(self: *Self, id: []const u8) ![]const u8 {
        // Try direct file: .tsk/{id}.md
        var path_buf: [MAX_PATH_LEN]u8 = undefined;
        const direct_path = std.fmt.bufPrint(&path_buf, "{s}.md", .{id}) catch return StorageError.IoError;

        if (self.tsk_dir.statFile(direct_path)) |_| {
            return self.allocator.dupe(u8, direct_path);
        } else |_| {}

        // Try folder: .tsk/{id}/{id}.md
        const folder_path = std.fmt.bufPrint(&path_buf, "{s}/{s}.md", .{ id, id }) catch return StorageError.IoError;
        if (self.tsk_dir.statFile(folder_path)) |_| {
            return self.allocator.dupe(u8, folder_path);
        } else |_| {}

        // Try in archive: .tsk/archive/{id}.md
        const archive_path = std.fmt.bufPrint(&path_buf, "archive/{s}.md", .{id}) catch return StorageError.IoError;
        if (self.tsk_dir.statFile(archive_path)) |_| {
            return self.allocator.dupe(u8, archive_path);
        } else |_| {}

        // Try archive folder: .tsk/archive/{id}/{id}.md
        const archive_folder_path = std.fmt.bufPrint(&path_buf, "archive/{s}/{s}.md", .{ id, id }) catch return StorageError.IoError;
        if (self.tsk_dir.statFile(archive_folder_path)) |_| {
            return self.allocator.dupe(u8, archive_folder_path);
        } else |_| {}

        // Search recursively in all subdirectories
        return try self.searchForIssue(self.tsk_dir, id) orelse StorageError.IssueNotFound;
    }

    const MAX_SEARCH_DEPTH = 10;

    fn searchForIssue(self: *Self, dir: fs.Dir, id: []const u8) !?[]const u8 {
        return self.searchForIssueWithDepth(dir, id, 0);
    }

    fn searchForIssueWithDepth(self: *Self, dir: fs.Dir, id: []const u8, depth: usize) !?[]const u8 {
        if (depth >= MAX_SEARCH_DEPTH) return null; // Prevent infinite recursion

        var iter = dir.iterate();
        while (try iter.next()) |entry| {
            // Skip symlinks to prevent infinite loops
            if (entry.kind == .sym_link) continue;
            if (entry.kind == .directory and !std.mem.eql(u8, entry.name, "archive")) {
                var subdir = dir.openDir(entry.name, .{ .iterate = true }) catch |err| switch (err) {
                    error.FileNotFound, error.AccessDenied => continue, // Skip inaccessible dirs
                    else => return err,
                };
                defer subdir.close();

                // Check for {id}.md in this directory
                var path_buf: [MAX_PATH_LEN]u8 = undefined;
                const filename = std.fmt.bufPrint(&path_buf, "{s}.md", .{id}) catch return StorageError.IoError;
                if (subdir.statFile(filename)) |_| {
                    const full_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ entry.name, filename });
                    return full_path;
                } else |_| {}

                // Recurse with depth limit
                if (try self.searchForIssueWithDepth(subdir, id, depth + 1)) |path| {
                    const full_path = std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ entry.name, path }) catch |err| {
                        self.allocator.free(path);
                        return err;
                    };
                    self.allocator.free(path);
                    return full_path;
                }
            }
        }
        return null;
    }

    pub fn getIssue(self: *Self, id: []const u8) !?Issue {
        // Validate ID to prevent path traversal attacks
        try validateId(id);

        const path = self.findIssuePath(id) catch |err| switch (err) {
            StorageError.IssueNotFound => return null,
            else => return err,
        };
        defer self.allocator.free(path);

        return try self.readIssueFromPath(path, id);
    }

    /// Returns raw file contents for an issue (frontmatter + description)
    pub fn getIssueRaw(self: *Self, id: []const u8) !?[]const u8 {
        try validateId(id);

        const path = self.findIssuePath(id) catch |err| switch (err) {
            StorageError.IssueNotFound => return null,
            else => return err,
        };
        defer self.allocator.free(path);

        const file = try self.tsk_dir.openFile(path, .{});
        defer file.close();

        return try file.readToEndAlloc(self.allocator, MAX_ISSUE_FILE_SIZE);
    }

    fn readIssueFromPath(self: *Self, path: []const u8, id: []const u8) !Issue {
        const file = try self.tsk_dir.openFile(path, .{});
        defer file.close();

        const content = try file.readToEndAlloc(self.allocator, MAX_ISSUE_FILE_SIZE);
        defer self.allocator.free(content);

        const parsed = try parseFrontmatter(self.allocator, content);
        // Free allocated strings after duping
        defer if (parsed.allocated_title) |t| self.allocator.free(t);
        defer if (parsed.allocated_created_at) |t| self.allocator.free(t);
        defer if (parsed.allocated_closed_at) |t| self.allocator.free(t);
        defer if (parsed.allocated_close_reason) |t| self.allocator.free(t);
        defer if (parsed.allocated_assignee) |t| self.allocator.free(t);
        // Free allocated_blocks on error (transferred to Issue on success)
        var blocks_transferred = false;
        errdefer if (!blocks_transferred) {
            for (parsed.allocated_blocks) |b| self.allocator.free(b);
            self.allocator.free(parsed.allocated_blocks);
        };

        // Determine parent from path
        const parent = try self.extractParentFromPath(path);
        errdefer if (parent) |p| self.allocator.free(p);

        const issue_id = try self.allocator.dupe(u8, id);
        errdefer self.allocator.free(issue_id);

        const title = try self.allocator.dupe(u8, parsed.frontmatter.title);
        errdefer self.allocator.free(title);

        const description = try self.allocator.dupe(u8, parsed.description);
        errdefer self.allocator.free(description);

        const assignee = if (parsed.frontmatter.assignee) |a| try self.allocator.dupe(u8, a) else null;
        errdefer if (assignee) |a| self.allocator.free(a);

        const created_at = try self.allocator.dupe(u8, parsed.frontmatter.created_at);
        errdefer self.allocator.free(created_at);

        const closed_at = if (parsed.frontmatter.closed_at) |c| try self.allocator.dupe(u8, c) else null;
        errdefer if (closed_at) |c| self.allocator.free(c);

        const close_reason = if (parsed.frontmatter.close_reason) |r| try self.allocator.dupe(u8, r) else null;
        errdefer if (close_reason) |r| self.allocator.free(r);

        // Mark blocks as transferred (will be owned by Issue)
        blocks_transferred = true;
        return Issue{
            .id = issue_id,
            .title = title,
            .description = description,
            .status = parsed.frontmatter.status,
            .assignee = assignee,
            .created_at = created_at,
            .closed_at = closed_at,
            .close_reason = close_reason,
            .blocks = parsed.allocated_blocks,
            .peer_index = parsed.frontmatter.peer_index,
            .parent = parent,
        };
    }

    fn extractParentFromPath(self: *Self, path: []const u8) !?[]const u8 {
        // Path like "parent_id/child_id.md" means parent_id is the parent
        // Path like "child_id.md" means no parent
        const slash_idx = std.mem.indexOf(u8, path, "/");
        if (slash_idx) |idx| {
            const potential_parent = path[0..idx];
            // Skip "archive" directory
            if (std.mem.eql(u8, potential_parent, "archive")) {
                // Check for archive/parent/child pattern
                const rest = path[idx + 1 ..];
                const next_slash = std.mem.indexOf(u8, rest, "/");
                if (next_slash) |next_idx| {
                    return try self.allocator.dupe(u8, rest[0..next_idx]);
                }
                return null;
            }
            // Check if this is parent/parent.md (self) or parent/child.md
            const filename = std.fs.path.basename(path);
            const file_id = filename[0 .. filename.len - 3]; // strip .md
            if (!std.mem.eql(u8, file_id, potential_parent)) {
                return try self.allocator.dupe(u8, potential_parent);
            }
        }
        return null;
    }

    pub fn createIssue(self: *Self, issue: Issue, parent_id: ?[]const u8) !void {
        // Validate IDs to prevent path traversal
        try validateId(issue.id);
        if (parent_id) |pid| try validateId(pid);
        for (issue.blocks) |b| try validateId(b);

        // Prevent overwriting existing issues
        // Note: TOCTOU race exists here - concurrent creates may both pass this check.
        // The atomic write ensures no corruption, but last writer wins.
        if (try self.issueExists(issue.id)) {
            return StorageError.IssueAlreadyExists;
        }

        const content = try serializeFrontmatter(self.allocator, issue);
        defer self.allocator.free(content);

        var path_buf: [MAX_PATH_LEN]u8 = undefined;
        const path = if (parent_id) |pid| blk: {
            try self.ensureParentFolder(pid);
            break :blk std.fmt.bufPrint(&path_buf, "{s}/{s}.md", .{ pid, issue.id }) catch return StorageError.IoError;
        } else blk: {
            // Check if a folder with this ID exists (child created before parent)
            const stat = self.tsk_dir.statFile(issue.id) catch |err| switch (err) {
                error.FileNotFound => break :blk std.fmt.bufPrint(&path_buf, "{s}.md", .{issue.id}) catch return StorageError.IoError,
                else => return err,
            };
            if (stat.kind == .directory) {
                // Folder exists - write to {id}/{id}.md
                break :blk std.fmt.bufPrint(&path_buf, "{s}/{s}.md", .{ issue.id, issue.id }) catch return StorageError.IoError;
            }
            break :blk std.fmt.bufPrint(&path_buf, "{s}.md", .{issue.id}) catch return StorageError.IoError;
        };

        try writeFileAtomic(self.tsk_dir, path, content);
    }

    fn ensureParentFolder(self: *Self, parent_id: []const u8) !void {
        var path_buf: [MAX_PATH_LEN]u8 = undefined;

        // Check if parent is already a folder
        self.tsk_dir.makeDir(parent_id) catch |err| switch (err) {
            error.PathAlreadyExists => {
                // Folder exists - check if parent.md exists in root and move it
                const old_path = std.fmt.bufPrint(&path_buf, "{s}.md", .{parent_id}) catch return StorageError.IoError;

                var new_path_buf: [MAX_PATH_LEN]u8 = undefined;
                const new_path = std.fmt.bufPrint(&new_path_buf, "{s}/{s}.md", .{ parent_id, parent_id }) catch return StorageError.IoError;

                self.tsk_dir.rename(old_path, new_path) catch |err2| switch (err2) {
                    error.FileNotFound => {}, // Parent file not in root, already correct
                    else => return err2,
                };
                return;
            },
            else => return err,
        };

        // Folder created - need to move parent.md into it
        const old_path = std.fmt.bufPrint(&path_buf, "{s}.md", .{parent_id}) catch return StorageError.IoError;

        var new_path_buf: [MAX_PATH_LEN]u8 = undefined;
        const new_path = std.fmt.bufPrint(&new_path_buf, "{s}/{s}.md", .{ parent_id, parent_id }) catch return StorageError.IoError;

        self.tsk_dir.rename(old_path, new_path) catch |err| switch (err) {
            error.FileNotFound => {}, // Parent file doesn't exist yet, that's fine
            else => return err,
        };
    }

    pub fn updateStatus(self: *Self, id: []const u8, status: Status, closed_at: ?[]const u8, close_reason: ?[]const u8) !void {
        const path = try self.findIssuePath(id);
        defer self.allocator.free(path);

        var issue = try self.readIssueFromPath(path, id);
        defer issue.deinit(self.allocator);

        // If closing, check that all children are closed first
        if (status == .closed) {
            const children = try self.getChildIssues(id);
            defer freeIssues(self.allocator, children);
            for (children) |child| {
                if (child.status != .closed) {
                    return StorageError.ChildrenNotClosed;
                }
            }
        }

        // When not closing, clear closed_at; when closing, use provided or keep existing
        const effective_closed_at: ?[]const u8 = if (status == .closed)
            (closed_at orelse issue.closed_at)
        else
            null;

        const effective_close_reason: ?[]const u8 = if (status == .closed)
            (close_reason orelse issue.close_reason)
        else
            null;

        const content = try serializeFrontmatter(self.allocator, issue.withStatus(status, effective_closed_at, effective_close_reason));
        defer self.allocator.free(content);

        try writeFileAtomic(self.tsk_dir, path, content);

        // Handle archiving if closed
        if (status == .closed) {
            try self.maybeArchive(id, path);
        }
    }

    /// Archive an issue by ID (for migration of already-closed issues)
    pub fn archiveIssue(self: *Self, id: []const u8) !void {
        const path = self.findIssuePath(id) catch |err| switch (err) {
            StorageError.IssueNotFound => return StorageError.IssueNotFound,
            else => return err,
        };
        defer self.allocator.free(path);
        try self.maybeArchive(id, path);
    }

    fn maybeArchive(self: *Self, id: []const u8, path: []const u8) !void {
        // Don't archive if already in archive
        if (std.mem.startsWith(u8, path, "archive/")) return;

        // Check if this is a child (has parent in path)
        const parent = try self.extractParentFromPath(path);
        if (parent) |p| {
            self.allocator.free(p);
            // Child issue - don't move, parent will move the whole folder
            return;
        }

        // Check if this is a parent with children
        const is_folder = std.mem.indexOf(u8, path, "/") != null;
        if (is_folder) {
            // Check all children are closed
            const folder_name = path[0..std.mem.indexOf(u8, path, "/").?];
            var folder = try self.tsk_dir.openDir(folder_name, .{ .iterate = true });
            defer folder.close();

            var iter = folder.iterate();
            while (try iter.next()) |entry| {
                if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".md")) {
                    const child_id = entry.name[0 .. entry.name.len - 3];
                    if (std.mem.eql(u8, child_id, id)) continue; // Skip self

                    const child_issue = try self.getIssue(child_id) orelse continue;
                    defer child_issue.deinit(self.allocator);

                    if (child_issue.status != .closed) {
                        return StorageError.ChildrenNotClosed;
                    }
                }
            }

            // All children closed, move entire folder
            var archive_path_buf: [MAX_PATH_LEN]u8 = undefined;
            const archive_path = std.fmt.bufPrint(&archive_path_buf, "archive/{s}", .{folder_name}) catch return StorageError.IoError;
            try self.tsk_dir.rename(folder_name, archive_path);
        } else {
            // Simple file, move to archive
            var archive_path_buf: [MAX_PATH_LEN]u8 = undefined;
            const archive_path = std.fmt.bufPrint(&archive_path_buf, "archive/{s}", .{path}) catch return StorageError.IoError;
            try self.tsk_dir.rename(path, archive_path);
        }
    }

    pub fn deleteIssue(self: *Self, id: []const u8) !void {
        const path = try self.findIssuePath(id);
        defer self.allocator.free(path);

        // Clean up dangling dependency references before deleting
        try self.removeDependencyReferences(id);

        // Determine the effective path (skip archive/ prefix if present)
        const effective_path = if (std.mem.startsWith(u8, path, "archive/"))
            path["archive/".len..]
        else
            path;

        // Check if it's a folder (has children)
        if (std.mem.indexOf(u8, effective_path, "/")) |slash_idx| {
            const folder_name = effective_path[0..slash_idx];
            const filename = std.fs.path.basename(effective_path);
            const file_id = filename[0 .. filename.len - 3];

            // If deleting parent, delete entire folder
            if (std.mem.eql(u8, file_id, folder_name)) {
                // Clean up dependency references for all children before deleting
                // Use full path for archived folders
                const full_folder = if (std.mem.startsWith(u8, path, "archive/"))
                    path[0 .. "archive/".len + slash_idx]
                else
                    folder_name;
                try self.removeChildDependencyReferences(full_folder);
                try self.tsk_dir.deleteTree(full_folder);
                return;
            }
        }

        // Simple file deletion
        try self.tsk_dir.deleteFile(path);
    }

    /// Remove dependency references for all children in a folder
    fn removeChildDependencyReferences(self: *Self, folder_name: []const u8) !void {
        var folder = self.tsk_dir.openDir(folder_name, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer folder.close();

        var it = folder.iterate();
        while (try it.next()) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, entry.name, ".md")) continue;
            const child_id = entry.name[0 .. entry.name.len - 3];
            // Skip parent file (same name as folder)
            if (std.mem.eql(u8, child_id, folder_name)) continue;
            try self.removeDependencyReferences(child_id);
        }
    }

    /// Rename an issue to a new ID, updating all dependency references
    pub fn renameIssue(self: *Self, old_id: []const u8, new_id: []const u8) !void {
        try validateId(old_id);
        try validateId(new_id);

        if (std.mem.eql(u8, old_id, new_id)) return; // No-op if same

        // Check new ID doesn't already exist
        if (try self.issueExists(new_id)) {
            return StorageError.IssueAlreadyExists;
        }

        // Get the issue
        const issue = try self.getIssue(old_id) orelse return StorageError.IssueNotFound;
        defer issue.deinit(self.allocator);

        // Find current path
        const old_path = try self.findIssuePath(old_id);
        defer self.allocator.free(old_path);

        // Check if it's a parent (has folder)
        const is_parent = std.mem.indexOf(u8, old_path, "/") != null and blk: {
            const folder_name = old_path[0..std.mem.indexOf(u8, old_path, "/").?];
            break :blk std.mem.eql(u8, folder_name, old_id);
        };

        // Create new issue with updated ID
        const new_issue = Issue{
            .id = new_id,
            .title = issue.title,
            .description = issue.description,
            .status = issue.status,
            .assignee = issue.assignee,
            .created_at = issue.created_at,
            .closed_at = issue.closed_at,
            .close_reason = issue.close_reason,
            .blocks = issue.blocks,
            .peer_index = issue.peer_index,
            .parent = issue.parent,
        };

        const content = try serializeFrontmatter(self.allocator, new_issue);
        defer self.allocator.free(content);

        if (is_parent) {
            // Parent issue: rename folder and file inside
            var new_path_buf: [MAX_PATH_LEN]u8 = undefined;
            const new_path = std.fmt.bufPrint(&new_path_buf, "{s}/{s}.md", .{ new_id, new_id }) catch return StorageError.IoError;

            // Rename folder first
            try self.tsk_dir.rename(old_id, new_id);

            // Write new content to new path (old file was renamed with folder)
            var old_file_in_new_folder_buf: [MAX_PATH_LEN]u8 = undefined;
            const old_file_in_new_folder = std.fmt.bufPrint(&old_file_in_new_folder_buf, "{s}/{s}.md", .{ new_id, old_id }) catch return StorageError.IoError;

            // Write new file before deleting old
            try writeFileAtomic(self.tsk_dir, new_path, content);
            self.tsk_dir.deleteFile(old_file_in_new_folder) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
        } else {
            // Simple file or child: just rename
            var new_path_buf: [MAX_PATH_LEN]u8 = undefined;
            const new_path = if (issue.parent) |parent| blk: {
                break :blk std.fmt.bufPrint(&new_path_buf, "{s}/{s}.md", .{ parent, new_id }) catch return StorageError.IoError;
            } else blk: {
                break :blk std.fmt.bufPrint(&new_path_buf, "{s}.md", .{new_id}) catch return StorageError.IoError;
            };

            // Write new file first, then delete old
            try writeFileAtomic(self.tsk_dir, new_path, content);
            self.tsk_dir.deleteFile(old_path) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
        }

        // Update all dependency references
        try self.updateDependencyReferences(old_id, new_id);
    }

    /// Update all references from old_id to new_id in other issues' blocks arrays
    fn updateDependencyReferences(self: *Self, old_id: []const u8, new_id: []const u8) !void {
        const issues = try self.listAllIssuesIncludingArchived();
        defer freeIssues(self.allocator, issues);

        for (issues) |issue| {
            // Check if this issue references the old ID
            var has_reference = false;
            for (issue.blocks) |b| {
                if (std.mem.eql(u8, b, old_id)) {
                    has_reference = true;
                    break;
                }
            }

            if (!has_reference) continue;

            // Build new blocks with updated reference
            var new_blocks: std.ArrayList([]const u8) = .{};
            errdefer {
                for (new_blocks.items) |b| self.allocator.free(b);
                new_blocks.deinit(self.allocator);
            }

            for (issue.blocks) |b| {
                const replacement = if (std.mem.eql(u8, b, old_id)) new_id else b;
                const duped = try self.allocator.dupe(u8, replacement);
                try new_blocks.append(self.allocator, duped);
            }

            const blocks_slice = try new_blocks.toOwnedSlice(self.allocator);
            defer {
                for (blocks_slice) |b| self.allocator.free(b);
                self.allocator.free(blocks_slice);
            }

            const path = try self.findIssuePath(issue.id);
            defer self.allocator.free(path);

            const updated_content = try serializeFrontmatter(self.allocator, issue.withBlocks(blocks_slice));
            defer self.allocator.free(updated_content);

            try writeFileAtomic(self.tsk_dir, path, updated_content);
        }
    }

    /// Remove all references to the given ID from other issues' blocks arrays
    /// Optimized: uses already-loaded issues instead of re-reading from disk
    fn removeDependencyReferences(self: *Self, deleted_id: []const u8) !void {
        // Get all issues (including archived)
        const issues = try self.listAllIssuesIncludingArchived();
        defer freeIssues(self.allocator, issues);

        for (issues) |issue| {
            // Check if this issue references the deleted ID
            var has_reference = false;
            for (issue.blocks) |b| {
                if (std.mem.eql(u8, b, deleted_id)) {
                    has_reference = true;
                    break;
                }
            }

            if (!has_reference) continue;

            // Build new blocks without the removed dependency (using already-loaded issue)
            var new_blocks: std.ArrayList([]const u8) = .{};
            errdefer {
                for (new_blocks.items) |b| self.allocator.free(b);
                new_blocks.deinit(self.allocator);
            }

            for (issue.blocks) |b| {
                if (!std.mem.eql(u8, b, deleted_id)) {
                    const duped = try self.allocator.dupe(u8, b);
                    try new_blocks.append(self.allocator, duped);
                }
            }

            const blocks_slice = try new_blocks.toOwnedSlice(self.allocator);
            defer {
                for (blocks_slice) |b| self.allocator.free(b);
                self.allocator.free(blocks_slice);
            }

            // Find path and write directly (no re-read needed)
            const path = try self.findIssuePath(issue.id);
            defer self.allocator.free(path);

            const content = try serializeFrontmatter(self.allocator, issue.withBlocks(blocks_slice));
            defer self.allocator.free(content);

            try writeFileAtomic(self.tsk_dir, path, content);
        }
    }

    pub fn listAllIssuesIncludingArchived(self: *Self) ![]Issue {
        var issues: std.ArrayList(Issue) = .{};
        errdefer {
            for (issues.items) |*iss| iss.deinit(self.allocator);
            issues.deinit(self.allocator);
        }

        // Collect from main tsk dir
        try self.collectIssuesFromDir(self.tsk_dir, "", null, &issues);

        // Also collect from archive
        if (self.tsk_dir.openDir("archive", .{ .iterate = true })) |archive_dir| {
            var ad = archive_dir;
            defer ad.close();
            try self.collectIssuesFromDir(ad, "archive", null, &issues);
        } else |err| switch (err) {
            error.FileNotFound => {}, // Archive doesn't exist yet, that's fine
            else => return err,
        }

        return issues.toOwnedSlice(self.allocator);
    }

    pub fn listIssues(self: *Self, status_filter: ?Status) ![]Issue {
        var issues: std.ArrayList(Issue) = .{};
        errdefer {
            for (issues.items) |*iss| iss.deinit(self.allocator);
            issues.deinit(self.allocator);
        }

        try self.collectIssuesFromDir(self.tsk_dir, "", status_filter, &issues);

        // Sort by priority, then created_at
        std.mem.sort(Issue, issues.items, {}, Issue.order);

        return issues.toOwnedSlice(self.allocator);
    }

    fn collectIssuesFromDir(self: *Self, dir: fs.Dir, prefix: []const u8, status_filter: ?Status, issues: *std.ArrayList(Issue)) !void {
        var iter = dir.iterate();
        while (try iter.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".md")) {
                const id = entry.name[0 .. entry.name.len - 3];
                var path_buf: [MAX_PATH_LEN]u8 = undefined;
                const path = if (prefix.len > 0)
                    std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ prefix, entry.name }) catch return StorageError.IoError
                else
                    entry.name;

                // Only skip expected parsing errors; propagate IO/allocation errors
                const issue = self.readIssueFromPath(path, id) catch |err| switch (err) {
                    StorageError.InvalidFrontmatter, StorageError.InvalidStatus => continue, // Malformed file, skip
                    else => return err, // IO/allocation errors must propagate
                };

                if (status_filter) |filter| {
                    if (issue.status != filter) {
                        issue.deinit(self.allocator);
                        continue;
                    }
                }

                issues.append(self.allocator, issue) catch |err| {
                    issue.deinit(self.allocator);
                    return err;
                };
            } else if (entry.kind == .directory and !std.mem.eql(u8, entry.name, "archive")) {
                var subdir = try dir.openDir(entry.name, .{ .iterate = true });
                defer subdir.close();

                var sub_prefix_buf: [MAX_PATH_LEN]u8 = undefined;
                const sub_prefix = if (prefix.len > 0)
                    std.fmt.bufPrint(&sub_prefix_buf, "{s}/{s}", .{ prefix, entry.name }) catch return StorageError.IoError
                else
                    entry.name;

                try self.collectIssuesFromDir(subdir, sub_prefix, status_filter, issues);
            }
        }
    }

    pub fn buildStatusMap(self: *Self, issues: []const Issue) !StatusMap {
        // Caller must keep issue IDs alive while the map is used.
        var status_by_id = StatusMap.init(self.allocator);
        if (issues.len <= std.math.maxInt(u32)) {
            try status_by_id.ensureTotalCapacity(@intCast(issues.len));
        }
        for (issues) |issue| {
            try status_by_id.put(issue.id, issue.status);
        }
        return status_by_id;
    }

    pub fn getReadyIssues(self: *Self) ![]Issue {
        const all_issues = try self.listIssues(null);
        defer self.allocator.free(all_issues);

        var ready: std.ArrayList(Issue) = .{};
        errdefer {
            for (ready.items) |*iss| iss.deinit(self.allocator);
            ready.deinit(self.allocator);
        }

        const keep = self.allocator.alloc(bool, all_issues.len) catch |err| {
            for (all_issues) |*issue| issue.deinit(self.allocator);
            return err;
        };
        defer self.allocator.free(keep);
        @memset(keep, false);
        errdefer {
            for (all_issues, 0..) |*issue, i| {
                if (!keep[i]) issue.deinit(self.allocator);
            }
        }

        var status_by_id = try self.buildStatusMap(all_issues);
        defer status_by_id.deinit();

        ready.ensureTotalCapacity(self.allocator, all_issues.len) catch |err| {
            return err;
        };

        for (all_issues, 0..) |issue, i| {
            if (issue.status != .open) continue;
            if (isBlockedByStatusMap(issue.blocks, &status_by_id)) continue;

            keep[i] = true;
            ready.appendAssumeCapacity(issue);
        }

        for (all_issues, 0..) |*issue, i| {
            if (!keep[i]) issue.deinit(self.allocator);
        }

        return ready.toOwnedSlice(self.allocator);
    }

    fn isBlockedByStatusMap(blocks: []const []const u8, status_by_id: *const StatusMap) bool {
        for (blocks) |blocker_id| {
            const status = status_by_id.get(blocker_id) orelse continue;
            if (status == .open or status == .active) return true;
        }
        return false;
    }

    fn appendOrphanChildren(self: *Self, folder_name: []const u8, issues: *std.ArrayList(Issue)) !void {
        var folder = self.tsk_dir.openDir(folder_name, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => return,
            else => return err,
        };
        defer folder.close();

        var iter = folder.iterate();
        while (try iter.next()) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".md")) continue;
            const id = entry.name[0 .. entry.name.len - 3];
            if (std.mem.eql(u8, id, folder_name)) continue;

            var path_buf: [MAX_PATH_LEN]u8 = undefined;
            const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ folder_name, entry.name }) catch return StorageError.IoError;
            const issue = self.readIssueFromPath(path, id) catch |err| switch (err) {
                StorageError.InvalidFrontmatter, StorageError.InvalidStatus => continue,
                error.FileNotFound => continue,
                else => return err,
            };

            if (issue.status != .closed) {
                try issues.append(self.allocator, issue);
            } else {
                issue.deinit(self.allocator);
            }
        }
    }

    fn scanRoots(
        self: *Self,
        issues: ?*std.ArrayList(Issue),
        orphans: ?*std.ArrayList([]const u8),
    ) !void {
        if (issues == null and orphans == null) return;

        // Only collect from root level of .tsk (not archive, not subdirs for children)
        var iter = self.tsk_dir.iterate();
        while (try iter.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".md")) {
                if (issues) |list| {
                    const id = entry.name[0 .. entry.name.len - 3];
                    const issue = self.readIssueFromPath(entry.name, id) catch |err| switch (err) {
                        StorageError.InvalidFrontmatter, StorageError.InvalidStatus => continue,
                        else => return err,
                    };

                    if (issue.status != .closed) {
                        try list.append(self.allocator, issue);
                    } else {
                        issue.deinit(self.allocator);
                    }
                }
            } else if (entry.kind == .directory and !std.mem.eql(u8, entry.name, "archive")) {
                // Folder = parent issue
                var path_buf: [MAX_PATH_LEN]u8 = undefined;
                const path = std.fmt.bufPrint(&path_buf, "{s}/{s}.md", .{ entry.name, entry.name }) catch return StorageError.IoError;
                const issue = self.readIssueFromPath(path, entry.name) catch |err| switch (err) {
                    StorageError.InvalidFrontmatter, StorageError.InvalidStatus, error.FileNotFound => {
                        if (orphans) |list| {
                            const name = try self.allocator.dupe(u8, entry.name);
                            try list.append(self.allocator, name);
                        }
                        if (issues) |list| {
                            try self.appendOrphanChildren(entry.name, list);
                        }
                        continue;
                    },
                    else => return err,
                };

                if (issues) |list| {
                    if (issue.status != .closed) {
                        try list.append(self.allocator, issue);
                    } else {
                        issue.deinit(self.allocator);
                    }
                } else {
                    issue.deinit(self.allocator);
                }
            }
        }
    }

    pub fn getRootIssues(self: *Self) ![]Issue {
        var issues: std.ArrayList(Issue) = .{};
        errdefer {
            for (issues.items) |*iss| iss.deinit(self.allocator);
            issues.deinit(self.allocator);
        }

        try self.scanRoots(&issues, null);

        // Sort by priority, then created_at
        std.mem.sort(Issue, issues.items, {}, Issue.order);

        return issues.toOwnedSlice(self.allocator);
    }

    pub fn listOrphanParents(self: *Self) ![]const []const u8 {
        var orphans: std.ArrayList([]const u8) = .{};
        errdefer {
            for (orphans.items) |name| self.allocator.free(name);
            orphans.deinit(self.allocator);
        }

        try self.scanRoots(null, &orphans);

        return orphans.toOwnedSlice(self.allocator);
    }

    pub fn fixOrphans(self: *Self) !FixResult {
        const orphans = try self.listOrphanParents();
        defer freeOrphanParents(self.allocator, orphans);

        var folders: usize = 0;
        var files: usize = 0;
        for (orphans) |name| {
            const moved = try self.promoteOrphanFolder(name);
            folders += 1;
            files += moved;
        }

        return FixResult{
            .folders = folders,
            .files = files,
        };
    }

    fn promoteOrphanFolder(self: *Self, folder_name: []const u8) !usize {
        var folder = self.tsk_dir.openDir(folder_name, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => return 0,
            else => return err,
        };
        defer folder.close();

        var names: std.ArrayList([]const u8) = .{};
        errdefer {
            for (names.items) |name| self.allocator.free(name);
            names.deinit(self.allocator);
        }

        var iter = folder.iterate();
        while (try iter.next()) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".md")) continue;

            const name = try self.allocator.dupe(u8, entry.name);
            try names.append(self.allocator, name);
        }

        for (names.items) |name| {
            if (self.tsk_dir.statFile(name)) |_| {
                return StorageError.IssueAlreadyExists;
            } else |_| {}
        }

        for (names.items) |name| {
            var src_buf: [MAX_PATH_LEN]u8 = undefined;
            const src = std.fmt.bufPrint(&src_buf, "{s}/{s}", .{ folder_name, name }) catch return StorageError.IoError;
            self.tsk_dir.rename(src, name) catch |err| switch (err) {
                error.PathAlreadyExists => return StorageError.IssueAlreadyExists,
                else => return err,
            };
        }

        const moved = names.items.len;
        for (names.items) |name| self.allocator.free(name);
        names.deinit(self.allocator);

        self.tsk_dir.deleteDir(folder_name) catch |err| switch (err) {
            error.DirNotEmpty => {},
            error.FileNotFound => {},
            else => return err,
        };

        return moved;
    }

    fn getChildIssues(self: *Self, parent_id: []const u8) ![]Issue {
        var children: std.ArrayList(Issue) = .{};
        errdefer {
            for (children.items) |*c| c.deinit(self.allocator);
            children.deinit(self.allocator);
        }

        // Open parent folder
        var folder = self.tsk_dir.openDir(parent_id, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return children.toOwnedSlice(self.allocator),
            else => return err,
        };
        defer folder.close();

        var iter = folder.iterate();
        while (try iter.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".md")) {
                const id = entry.name[0 .. entry.name.len - 3];
                if (std.mem.eql(u8, id, parent_id)) continue; // Skip parent itself

                var path_buf: [MAX_PATH_LEN]u8 = undefined;
                const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ parent_id, entry.name }) catch return StorageError.IoError;

                const issue = self.readIssueFromPath(path, id) catch |err| switch (err) {
                    StorageError.InvalidFrontmatter, StorageError.InvalidStatus => continue,
                    else => return err,
                };
                children.append(self.allocator, issue) catch |err| {
                    issue.deinit(self.allocator);
                    return err;
                };
            }
        }

        // Sort by priority, then created_at
        std.mem.sort(Issue, children.items, {}, Issue.order);

        return children.toOwnedSlice(self.allocator);
    }

    /// Calculate the peer_index for a new issue based on positioning.
    /// - If after_id is set: insert after that issue (midpoint with next sibling, or +1 if last)
    /// - If before_id is set: insert before that issue (midpoint with prev sibling, or -1 if first)
    /// - Otherwise: append at end (max peer_index + 1, or 0 if no siblings)
    pub fn calculatePeerIndex(self: *Self, parent_id: ?[]const u8, after_id: ?[]const u8, before_id: ?[]const u8) !f64 {
        // Get siblings (sorted by peer_index)
        const siblings = if (parent_id) |pid|
            try self.getChildIssues(pid)
        else
            try self.getRootIssues();
        defer freeIssues(self.allocator, siblings);

        if (siblings.len == 0) {
            return 0.0;
        }

        if (after_id) |aid| {
            // Find the target issue and calculate midpoint with next sibling
            for (siblings, 0..) |sibling, i| {
                if (std.mem.eql(u8, sibling.id, aid)) {
                    if (i + 1 < siblings.len) {
                        // Midpoint between target and next sibling
                        return (sibling.peer_index + siblings[i + 1].peer_index) / 2.0;
                    } else {
                        // Last sibling, add 1
                        return sibling.peer_index + 1.0;
                    }
                }
            }
            return StorageError.IssueNotFound;
        }

        if (before_id) |bid| {
            // Find the target issue and calculate midpoint with previous sibling
            for (siblings, 0..) |sibling, i| {
                if (std.mem.eql(u8, sibling.id, bid)) {
                    if (i > 0) {
                        // Midpoint between previous sibling and target
                        return (siblings[i - 1].peer_index + sibling.peer_index) / 2.0;
                    } else {
                        // First sibling, subtract 1
                        return sibling.peer_index - 1.0;
                    }
                }
            }
            return StorageError.IssueNotFound;
        }

        // Default: append at end
        return siblings[siblings.len - 1].peer_index + 1.0;
    }

    pub fn getChildren(self: *Self, parent_id: []const u8) ![]ChildIssue {
        const all_issues = try self.listAllIssuesIncludingArchived();
        defer freeIssues(self.allocator, all_issues);

        var status_by_id = try self.buildStatusMap(all_issues);
        defer status_by_id.deinit();

        return try self.getChildrenWithStatusMap(parent_id, &status_by_id);
    }

    pub fn getChildrenWithStatusMap(self: *Self, parent_id: []const u8, status_by_id: *const StatusMap) ![]ChildIssue {
        const child_issues = try self.getChildIssues(parent_id);
        var transfer_done = false;
        errdefer if (!transfer_done) {
            for (child_issues) |*issue| issue.deinit(self.allocator);
            self.allocator.free(child_issues);
        };

        var children: std.ArrayList(ChildIssue) = .{};
        errdefer {
            for (children.items) |*c| c.deinit(self.allocator);
            children.deinit(self.allocator);
        }

        try children.ensureTotalCapacity(self.allocator, child_issues.len);
        for (child_issues) |issue| {
            const blocked = isBlockedByStatusMap(issue.blocks, status_by_id);
            children.appendAssumeCapacity(.{
                .issue = issue,
                .blocked = blocked,
            });
        }

        transfer_done = true;
        self.allocator.free(child_issues);

        return children.toOwnedSlice(self.allocator);
    }

    pub fn searchIssues(self: *Self, query: []const u8) ![]Issue {
        const all_issues = try self.listAllIssuesIncludingArchived();
        defer self.allocator.free(all_issues);

        var matches: std.ArrayList(Issue) = .{};
        errdefer {
            for (matches.items) |*iss| iss.deinit(self.allocator);
            matches.deinit(self.allocator);
        }

        matches.ensureTotalCapacity(self.allocator, all_issues.len) catch |err| {
            for (all_issues) |*issue| issue.deinit(self.allocator);
            return err;
        };

        for (all_issues) |issue| {
            const in_title = containsIgnoreCase(issue.title, query);
            const in_desc = containsIgnoreCase(issue.description, query);
            const in_reason = if (issue.close_reason) |r| containsIgnoreCase(r, query) else false;
            const in_created = containsIgnoreCase(issue.created_at, query);
            const in_closed = if (issue.closed_at) |c| containsIgnoreCase(c, query) else false;

            if (in_title or in_desc or in_reason or in_created or in_closed) {
                matches.appendAssumeCapacity(issue);
            } else {
                issue.deinit(self.allocator);
            }
        }

        return matches.toOwnedSlice(self.allocator);
    }

    fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
        if (needle.len == 0) return true;
        if (needle.len > haystack.len) return false;

        var i: usize = 0;
        while (i + needle.len <= haystack.len) : (i += 1) {
            if (asciiEqualIgnoreCase(haystack[i .. i + needle.len], needle)) return true;
        }
        return false;
    }

    fn asciiEqualIgnoreCase(a: []const u8, b: []const u8) bool {
        if (a.len != b.len) return false;
        for (a, b) |ac, bc| {
            if (std.ascii.toLower(ac) != std.ascii.toLower(bc)) return false;
        }
        return true;
    }

    pub fn addDependency(self: *Self, issue_id: []const u8, depends_on_id: []const u8, dep_type: []const u8) !void {
        // Validate IDs to prevent path traversal
        try validateId(issue_id);
        try validateId(depends_on_id);

        // Verify the dependency target exists
        if (!try self.issueExists(depends_on_id)) {
            return StorageError.DependencyNotFound;
        }

        // Validate dependency type
        const valid_dep_types = std.StaticStringMap(void).initComptime(.{
            .{ "blocks", {} },
            .{ "parent-child", {} },
        });
        if (valid_dep_types.get(dep_type) == null) {
            return StorageError.InvalidFrontmatter;
        }

        // For "blocks" type, add to the issue's blocks array
        if (std.mem.eql(u8, dep_type, "blocks")) {
            // Check for cycle
            if (try self.wouldCreateCycle(issue_id, depends_on_id)) {
                return StorageError.DependencyCycle;
            }

            const path = try self.findIssuePath(issue_id);
            defer self.allocator.free(path);

            var issue = try self.readIssueFromPath(path, issue_id);
            defer issue.deinit(self.allocator);

            // Check if already in blocks
            for (issue.blocks) |b| {
                if (std.mem.eql(u8, b, depends_on_id)) return; // Already exists
            }

            // Add to blocks array
            var new_blocks: std.ArrayList([]const u8) = .{};
            errdefer {
                for (new_blocks.items) |b| self.allocator.free(b);
                new_blocks.deinit(self.allocator);
            }

            for (issue.blocks) |b| {
                const duped = try self.allocator.dupe(u8, b);
                try new_blocks.append(self.allocator, duped);
            }
            const new_dep = try self.allocator.dupe(u8, depends_on_id);
            try new_blocks.append(self.allocator, new_dep);

            const blocks_slice = try new_blocks.toOwnedSlice(self.allocator);
            defer {
                for (blocks_slice) |b| self.allocator.free(b);
                self.allocator.free(blocks_slice);
            }

            const content = try serializeFrontmatter(self.allocator, issue.withBlocks(blocks_slice));
            defer self.allocator.free(content);

            try writeFileAtomic(self.tsk_dir, path, content);
        }
        // "parent-child" type is handled by file location, not frontmatter
    }

    fn wouldCreateCycle(self: *Self, from_id: []const u8, to_id: []const u8) !bool {
        // BFS from to_id following blocks dependencies
        // If we reach from_id, cycle would be created

        // Use arena for all BFS allocations - single free at end
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        var visited = std.StringHashMap(void).init(alloc);
        var queue: std.ArrayList([]const u8) = .{};

        // Must dupe to_id since it may outlive original
        try queue.append(alloc, try alloc.dupe(u8, to_id));

        // Use index instead of orderedRemove(0) for O(1) dequeue
        var head: usize = 0;
        while (head < queue.items.len) {
            const current = queue.items[head];
            head += 1;

            if (std.mem.eql(u8, current, from_id)) {
                return true; // Cycle detected
            }

            if (visited.contains(current)) continue;
            try visited.put(current, {});

            const issue = try self.getIssue(current) orelse continue;
            defer issue.deinit(self.allocator);

            for (issue.blocks) |blocker| {
                if (!visited.contains(blocker)) {
                    // Must dupe since issue will be freed
                    try queue.append(alloc, try alloc.dupe(u8, blocker));
                }
            }
        }

        return false;
    }

    pub fn purgeArchive(self: *Self) !void {
        // deleteTree succeeds silently if the directory doesn't exist
        try self.tsk_dir.deleteTree("archive");

        // Recreate empty archive directory (handle race if another process recreated it)
        self.tsk_dir.makeDir("archive") catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
    }

};
