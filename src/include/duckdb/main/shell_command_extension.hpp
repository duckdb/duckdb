//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/shell_command_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class DatabaseInstance;
struct DBConfig;

struct ShellCommandExtensionInfo {
	virtual ~ShellCommandExtensionInfo() {
	}
};

enum class ShellCommandResult : uint8_t { SUCCESS = 0, FAIL = 1, EXIT = 2, PRINT_USAGE = 3 };

//! Standard callback — shell keeps database open.
//! args[0] is the command name, args[1] is the sub-command, args[2..] are arguments.
typedef ShellCommandResult (*shell_command_callback_t)(DatabaseInstance &db, const vector<string> &args,
                                                       optional_ptr<ShellCommandExtensionInfo> info);

//! Exclusive-access callback — shell closes database before calling.
//! The callback receives the database path and can open its own instance.
//! After return, the shell reopens the database.
typedef ShellCommandResult (*shell_exclusive_command_callback_t)(const string &db_path, const vector<string> &args,
                                                                 optional_ptr<ShellCommandExtensionInfo> info);

//! A single sub-command within an extension's dot command.
struct ShellSubCommand {
	//! Sub-command name, e.g., "version" for ".icu version"
	string sub_command;
	//! Argument synopsis for .help
	string usage;
	//! Short description for .help
	string description;

	//! Normal mode: shell keeps database open.
	shell_command_callback_t callback = nullptr;
	//! Exclusive mode: shell closes DB before call, passes path, reopens after.
	shell_exclusive_command_callback_t exclusive_callback = nullptr;
};

//! Top-level extension command registration.
//! An extension may register multiple ShellCommandExtensions (one per dot command).
class ShellCommandExtension {
public:
	//! Dot-command name, e.g., "icu" -> ".icu"
	string command;
	//! Argument synopsis for .help (used when no sub-commands)
	string usage;
	//! Short description for .help
	string description;

	//! Direct callback — used when sub_commands is empty, or as fallback.
	shell_command_callback_t callback = nullptr;
	//! For exclusive file access (shell closes DB, passes path, reopens after):
	shell_exclusive_command_callback_t exclusive_callback = nullptr;

	//! Optional structured sub-commands.
	vector<ShellSubCommand> sub_commands;

	shared_ptr<ShellCommandExtensionInfo> info;

	static void Register(DBConfig &config, ShellCommandExtension extension);
};

} // namespace duckdb
