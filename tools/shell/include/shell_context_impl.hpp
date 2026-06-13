//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_context_impl.hpp
//
// Concrete implementation of ShellContext that wraps ShellState.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/shell_extension.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb_shell {

struct ShellState;

//! Saved linenoise/shell input state for EnterExtensionInputMode/LeaveExtensionInputMode
struct SavedInputMode {
	void *completion_cb = nullptr;
	void *format_cb = nullptr;
	bool highlighting_enabled = true;
	bool force_submit_on_enter = false;
	int error_rendering = 1;
	int completion_rendering = 1;
};

//! Concrete implementation of ShellContext that delegates to ShellState.
//! One instance is lazily created per ShellState and persists for the
//! lifetime of the shell session (so extension data survives across
//! dot-command invocations).
class ShellContextImpl : public duckdb::ShellContext {
public:
	explicit ShellContextImpl(ShellState &state);
	~ShellContextImpl() override;

	// Output
	void Print(const duckdb::string &text) override;
	void PrintError(const duckdb::string &text) override;

	// Database access
	duckdb::Connection &GetConnection() override;
	duckdb::DatabaseInstance &GetDatabaseInstance() override;

	// Shell state queries
	bool IsSafeMode() const override;
	bool IsInterrupted() const override;
	void ClearInterrupt() override;

	// Terminal
	duckdb::idx_t GetTerminalWidth() override;

	// Rendering
	void RenderQueryResult(duckdb::QueryResult &result) override;
	void HighlightSQL(duckdb::string &sql) override;
	bool IsHighlightingEnabled() const override;
	void SetHighlightingEnabled(bool enabled) override;

	// Interactive input
	duckdb::idx_t EnterExtensionInputMode() override;
	void LeaveExtensionInputMode(duckdb::idx_t token) override;
	duckdb::string ReadLine(const duckdb::string &prompt) override;
	bool ReadLineEOF() const override;

	// History
	void AddHistory(const duckdb::string &line) override;
	void ClearHistory() override;
	void LoadRawHistory(const duckdb::string &path) override;
	void SaveHistoryToFile(const duckdb::string &path) override;
	void SaveShellHistory(const duckdb::string &path) override;
	void LoadShellHistory(const duckdb::string &path) override;

	// Extension data storage
	void SetExtensionData(const duckdb::string &key, duckdb::shared_ptr<duckdb::ShellExtensionData> data) override;
	duckdb::shared_ptr<duckdb::ShellExtensionData> GetExtensionData(const duckdb::string &key) override;

private:
	ShellState &state;
	//! Extension data, keyed by extension name
	duckdb::case_insensitive_map_t<duckdb::shared_ptr<duckdb::ShellExtensionData>> extension_data;
	//! Stack of saved input modes for nested EnterExtensionInputMode calls
	duckdb::vector<SavedInputMode> saved_input_modes;
	//! Whether the last ReadLine returned due to EOF
	bool last_readline_eof = false;
};

} // namespace duckdb_shell
