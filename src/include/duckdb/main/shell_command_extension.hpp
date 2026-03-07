//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/shell_command_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/extension_callback_manager.hpp"

namespace duckdb {

struct BaseShellState {};

enum class ShellCommandResult : uint8_t { SUCCESS = 0, FAIL = 1, EXIT = 2, PRINT_USAGE = 3 };

typedef ShellCommandResult (*shell_command_t)(BaseShellState &state, const vector<string> &args);

struct ShellCommand {
	const char *command;
	idx_t argument_count;
	shell_command_t callback;
	const char *usage;
	const char *description;
	idx_t match_size;
	const char *extra_description;
	idx_t auxiliary_index;
};

struct DBConfig;
class DatabaseInstance;

//! Per-extension state kept alive for the lifetime of the database
struct ShellCommandExtensionInfo {
	virtual ~ShellCommandExtensionInfo() {
	}
};
//! Callback type for shell command extensions.
//! args[0] is the command name (without the leading '.'), args[1..] are the arguments.
//! The extension may write output directly to stdout/stderr.
typedef ShellCommandResult (*shell_command_callback_t)(DatabaseInstance &db, const vector<string> &args,
                                                       optional_ptr<ShellCommandExtensionInfo> info);

//! A shell command extension allows extensions to register custom '.' dot-commands
//! in the DuckDB interactive shell.  Registration follows the same pattern as
//! ParserExtension: call ShellCommandExtension::Register(config, ext) from within
//! an extension's Load() function.
class ShellCommandExtension {
public:
	//! The command name without the leading '.', e.g. "ducklake"
	string command;
	//! Argument synopsis shown in .help output, e.g. "SUBCOMMAND [ARGS...]"
	string usage;
	//! Short description shown in .help output
	string description;
	//! The handler called when the user types '.<command> [args...]'
	shell_command_callback_t callback = nullptr;

	mutable ShellCommand inner_command;
	void Finalize(idx_t index) const {
		inner_command.command = command.c_str();
		inner_command.argument_count = 0;
		inner_command.callback = nullptr;
		inner_command.usage = usage.c_str();
		inner_command.description = description.c_str();
		inner_command.match_size = 0;
		inner_command.extra_description = nullptr;
		inner_command.auxiliary_index = index;
	}

	//! Extension-owned state passed to every callback invocation
	shared_ptr<ShellCommandExtensionInfo> info;

	static void Register(DBConfig &config, ShellCommandExtension extension);
};

} // namespace duckdb
