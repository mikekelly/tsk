const std = @import("std");

const version = "0.3.1";

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Get git short hash (trimmed)
    const git_hash = std.mem.trim(u8, b.run(&.{ "git", "rev-parse", "--short", "HEAD" }), "\n\r ");

    const exe = b.addExecutable(.{
        .name = "dot",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });

    const options = b.addOptions();
    options.addOption([]const u8, "version", version);
    options.addOption([]const u8, "git_hash", git_hash);
    exe.root_module.addOptions("build_options", options);

    // Link SQLite - priority: source > static lib > system
    const sqlite_source = b.option([]const u8, "sqlite-source", "Path to sqlite3.c for static compilation");
    const use_system = b.option(bool, "system-sqlite", "Use system SQLite (dynamic linking)") orelse false;
    linkSqlite(b, exe, target, sqlite_source, use_system);

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run dot");
    run_step.dependOn(&run_cmd.step);

    // Tests - pass binary path as build option
    const install_prefix = b.install_prefix;
    const dot_path = b.fmt("{s}/bin/dot", .{install_prefix});

    const test_options = b.addOptions();
    test_options.addOption([]const u8, "dot_binary", dot_path);

    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/tests.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    test_mod.addOptions("build_options", test_options);

    const tests = b.addTest(.{
        .root_module = test_mod,
    });

    linkSqlite(b, tests, target, sqlite_source, use_system);

    const run_tests = b.addRunArtifact(tests);
    run_tests.step.dependOn(b.getInstallStep()); // Build main binary first

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_tests.step);
}

fn linkSqlite(
    b: *std.Build,
    step: *std.Build.Step.Compile,
    target: std.Build.ResolvedTarget,
    sqlite_source: ?[]const u8,
    use_system: bool,
) void {
    if (sqlite_source) |src_path| {
        step.addCSourceFile(.{
            .file = .{ .cwd_relative = src_path },
            .flags = &.{ "-DSQLITE_THREADSAFE=0", "-DSQLITE_OMIT_LOAD_EXTENSION" },
        });
        const src_dir = std.fs.path.dirname(src_path) orelse ".";
        step.root_module.addIncludePath(.{ .cwd_relative = src_dir });
        return;
    }

    if (use_system) {
        step.root_module.linkSystemLibrary("sqlite3", .{});
        return;
    }

    if (target.result.os.tag == .macos and linkHomebrewSqlite(b, step)) {
        return;
    }

    step.root_module.linkSystemLibrary("sqlite3", .{});
}

fn linkHomebrewSqlite(b: *std.Build, step: *std.Build.Step.Compile) bool {
    const prefixes = [_][]const u8{
        "/opt/homebrew/opt/sqlite",
        "/usr/local/opt/sqlite",
    };

    for (prefixes) |prefix| {
        const lib_path = b.fmt("{s}/lib/libsqlite3.a", .{prefix});
        if (std.fs.accessAbsolute(lib_path, .{})) |_| {
            step.addObjectFile(.{ .cwd_relative = lib_path });
            step.root_module.addIncludePath(.{ .cwd_relative = b.fmt("{s}/include", .{prefix}) });
            return true;
        } else |_| {}
    }

    return false;
}
