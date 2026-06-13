//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/shell_extension.hpp
//
// Types for registering shell-level extension commands (dot-commands in the
// DuckDB CLI). Extensions include this header and register their commands
// via ShellExtensionRegistration::Register(). The CLI shell queries the
// ExtensionCallbackManager at command-dispatch time.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/extension_callback_manager.hpp"

namespace duckdb {

class Connection;
class DatabaseInstance;
class QueryResult;
struct DBConfig;

//===--------------------------------------------------------------------===//
// ShellExtensionData -- base class for extension-stored data
//===--------------------------------------------------------------------===//
//! Base class for data stored via ShellContext::SetExtensionData/GetExtensionData.
//! Extensions subclass this and cast back to their type on retrieval.
struct ShellExtensionData {
	virtual ~ShellExtensionData() = default;
};

//===--------------------------------------------------------------------===//
// ShellContext -- abstract interface to the shell environment
//===--------------------------------------------------------------------===//
//! ShellContext provides the abstract interface that shell extension commands
//! use to interact with the CLI shell. The concrete implementation lives in
//! tools/shell/ and wraps ShellState. Extensions never see ShellState directly.
class ShellContext {
public:
	virtual ~ShellContext() = default;

	//===--------------------------------------------------------------------===//
	// Output
	//===--------------------------------------------------------------------===//
	//! Print text to stdout
	virtual void Print(const string &text) = 0;
	//! Print text to stderr
	virtual void PrintError(const string &text) = 0;

	//===--------------------------------------------------------------------===//
	// Database access
	//===--------------------------------------------------------------------===//
	//! Get the active database connection
	virtual Connection &GetConnection() = 0;
	//! Get the database instance
	virtual DatabaseInstance &GetDatabaseInstance() = 0;

	//===--------------------------------------------------------------------===//
	// Shell state queries
	//===--------------------------------------------------------------------===//
	//! Whether the shell is in safe mode (external access disabled)
	virtual bool IsSafeMode() const = 0;
	//! Check if an interrupt has been received (Ctrl-C / Escape)
	virtual bool IsInterrupted() const = 0;
	//! Clear the interrupt flag
	virtual void ClearInterrupt() = 0;

	//===--------------------------------------------------------------------===//
	// Terminal
	//===--------------------------------------------------------------------===//
	//! Get the terminal width in columns (0 if unknown)
	virtual idx_t GetTerminalWidth() = 0;

	//===--------------------------------------------------------------------===//
	// Rendering
	//===--------------------------------------------------------------------===//
	//! Render a query result using the shell's current output mode and renderer
	virtual void RenderQueryResult(QueryResult &result) = 0;
	//! Apply syntax highlighting to a SQL string (in-place)
	virtual void HighlightSQL(string &sql) = 0;
	//! Whether syntax highlighting is currently enabled
	virtual bool IsHighlightingEnabled() const = 0;
	//! Enable or disable syntax highlighting
	virtual void SetHighlightingEnabled(bool enabled) = 0;

	//===--------------------------------------------------------------------===//
	// Interactive input (for extensions that run their own REPL sub-loops)
	//===--------------------------------------------------------------------===//
	//! Enter extension input mode: saves and disables all SQL-specific
	//! linenoise configuration (completion, formatting, error rendering,
	//! ForceSubmitOnEnter, highlighting). Returns a token to pass to
	//! LeaveExtensionInputMode for correct restoration.
	virtual idx_t EnterExtensionInputMode() = 0;
	//! Leave extension input mode: restores all saved linenoise configuration
	virtual void LeaveExtensionInputMode(idx_t token) = 0;
	//! Read a line of interactive input with the given prompt.
	//! Returns the input string, or an empty string if EOF/Ctrl-D was received.
	//! The caller should check for empty return to detect exit.
	virtual string ReadLine(const string &prompt) = 0;
	//! Returns true if the last ReadLine() returned due to EOF (Ctrl-D)
	virtual bool ReadLineEOF() const = 0;

	//===--------------------------------------------------------------------===//
	// History management (for custom REPL sub-loops)
	//===--------------------------------------------------------------------===//
	//! Add a line to the current input history
	virtual void AddHistory(const string &line) = 0;
	//! Clear the current history buffer
	virtual void ClearHistory() = 0;
	//! Load raw history entries from a file (each line = one entry, no SQL joining)
	virtual void LoadRawHistory(const string &path) = 0;
	//! Save the current history to a file
	virtual void SaveHistoryToFile(const string &path) = 0;
	//! Save the shell's SQL history to a file (call before clearing)
	virtual void SaveShellHistory(const string &path) = 0;
	//! Load the shell's SQL history from a file (call after restoring)
	virtual void LoadShellHistory(const string &path) = 0;

	//===--------------------------------------------------------------------===//
	// Extension data storage
	//===--------------------------------------------------------------------===//
	//! Store opaque extension data keyed by name. The data persists for the
	//! lifetime of the shell session. Extensions subclass ShellExtensionData
	//! for their state and are responsible for casting back to the correct type.
	virtual void SetExtensionData(const string &key, shared_ptr<ShellExtensionData> data) = 0;
	//! Retrieve previously stored extension data (nullptr if not found)
	virtual shared_ptr<ShellExtensionData> GetExtensionData(const string &key) = 0;
};

//===--------------------------------------------------------------------===//
// ShellCommandResult
//===--------------------------------------------------------------------===//
//! Result codes for shell extension commands. Values match the shell's
//! internal MetadataResult enum so they can be cast directly.
enum class ShellCommandResult : uint8_t { SUCCESS = 0, FAIL = 1, EXIT = 2, PRINT_USAGE = 3 };

//===--------------------------------------------------------------------===//
// ShellExtensionCommand
//===--------------------------------------------------------------------===//
//! Callback signature for shell extension commands
typedef ShellCommandResult (*shell_command_callback_t)(ShellContext &context, const vector<string> &args);

//! Describes a single dot-command provided by an extension. Mirrors the
//! shell's internal MetadataCommand struct but uses string members since
//! these are dynamically allocated.
struct ShellExtensionCommand {
	//! The command name (without the leading dot, e.g. "ask" for ".ask")
	string command;
	//! Number of required arguments (0 = variadic / flexible)
	idx_t argument_count = 0;
	//! The callback function invoked when the command is executed
	shell_command_callback_t callback = nullptr;
	//! Usage string shown in .help (e.g. "?MESSAGE?")
	string usage;
	//! Short description shown in .help listing
	string description;
	//! Minimum prefix length for abbreviated matching (0 = must match full name)
	idx_t match_size = 0;
	//! Extended description shown by .help --all or .help <command>
	string extra_description;
};

//===--------------------------------------------------------------------===//
// ShellExtensionRegistration
//===--------------------------------------------------------------------===//
//! Optional info struct that extensions can subclass to store static data
//! associated with their shell extension registration.
struct ShellExtensionInfo {
	virtual ~ShellExtensionInfo() = default;
};

//! Registered with ExtensionCallbackManager during extension Load().
//! Contains the set of dot-commands the extension provides.
class ShellExtensionRegistration {
public:
	virtual ~ShellExtensionRegistration() = default;

	//! The commands this extension provides
	vector<ShellExtensionCommand> commands;
	//! Optional extension info
	shared_ptr<ShellExtensionInfo> info;

	//! Register this shell extension with the given database config
	static void Register(DBConfig &config, shared_ptr<ShellExtensionRegistration> extension);
	//! Iterate all registered shell extensions
	static ExtensionCallbackIteratorHelper<shared_ptr<ShellExtensionRegistration>> Iterate(DatabaseInstance &db) {
		return ExtensionCallbackManager::Get(db).ShellExtensions();
	}
};

} // namespace duckdb
