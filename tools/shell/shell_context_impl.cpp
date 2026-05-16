//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_context_impl.cpp
//
// Concrete implementation of ShellContext wrapping ShellState.
//
//===----------------------------------------------------------------------===//

#include "shell_context_impl.hpp"
#include "shell_state.hpp"
#include "shell_renderer.hpp"

#ifdef HAVE_LINENOISE
#include "linenoise.h"
#include "history.hpp"
#include "terminal.hpp"
#endif

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb_shell {

ShellContextImpl::ShellContextImpl(ShellState &state) : state(state) {
}

ShellContextImpl::~ShellContextImpl() {
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

void ShellContextImpl::Print(const duckdb::string &text) {
	state.Print(text);
}

void ShellContextImpl::PrintError(const duckdb::string &text) {
	state.Print(PrintOutput::STDERR, text);
}

// ---------------------------------------------------------------------------
// Database access
// ---------------------------------------------------------------------------

duckdb::Connection &ShellContextImpl::GetConnection() {
	return *state.conn;
}

duckdb::DatabaseInstance &ShellContextImpl::GetDatabaseInstance() {
	return *state.db->instance;
}

// ---------------------------------------------------------------------------
// Shell state queries
// ---------------------------------------------------------------------------

bool ShellContextImpl::IsSafeMode() const {
	return state.safe_mode;
}

bool ShellContextImpl::IsInterrupted() const {
	return state.seenInterrupt != 0;
}

void ShellContextImpl::ClearInterrupt() {
	state.ClearInterrupt();
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

duckdb::idx_t ShellContextImpl::GetTerminalWidth() {
#ifdef HAVE_LINENOISE
	auto term_size = duckdb::Terminal::GetTerminalSize();
	if (term_size.ws_col > 0) {
		return static_cast<duckdb::idx_t>(term_size.ws_col);
	}
#endif
	return 0;
}

void ShellContextImpl::RenderQueryResult(duckdb::QueryResult &result) {
	auto renderer = state.GetRenderer();
	state.RenderQueryResult(*renderer, result);
}

void ShellContextImpl::HighlightSQL(duckdb::string &sql) {
	state.HighlightSQL(sql);
}

bool ShellContextImpl::IsHighlightingEnabled() const {
	return state.highlighting_enabled;
}

void ShellContextImpl::SetHighlightingEnabled(bool enabled) {
	state.highlighting_enabled = enabled;
}

// ---------------------------------------------------------------------------
// Interactive input
// ---------------------------------------------------------------------------

duckdb::idx_t ShellContextImpl::EnterExtensionInputMode() {
	SavedInputMode saved;

#ifdef HAVE_LINENOISE
	// Save current linenoise configuration
	saved.completion_cb = reinterpret_cast<void *>(linenoiseGetCompletionCallback());
	saved.format_cb = reinterpret_cast<void *>(linenoiseGetFormatCallback());
	saved.force_submit_on_enter = duckdb::Terminal::ForceSubmitOnEnter();
	// Note: error_rendering and completion_rendering don't have getters,
	// so we assume the default (enabled=1) unless we know otherwise
	saved.error_rendering = 1;
	saved.completion_rendering = 1;
#endif
	saved.highlighting_enabled = state.highlighting_enabled;

#ifdef HAVE_LINENOISE
	// Disable all SQL-specific input features
	duckdb::Terminal::SetForceSubmitOnEnter(true);
	linenoiseSetCompletionCallback(nullptr);
	linenoiseSetFormatCallback(nullptr);
	linenoiseSetErrorRendering(0);
	linenoiseSetCompletionRendering(0);
#endif
	state.highlighting_enabled = false;

	duckdb::idx_t token = saved_input_modes.size();
	saved_input_modes.push_back(std::move(saved));
	return token;
}

void ShellContextImpl::LeaveExtensionInputMode(duckdb::idx_t token) {
	if (token >= saved_input_modes.size()) {
		return;
	}
	auto &saved = saved_input_modes[token];

#ifdef HAVE_LINENOISE
	duckdb::Terminal::SetForceSubmitOnEnter(saved.force_submit_on_enter);
	linenoiseSetCompletionCallback(reinterpret_cast<linenoiseCompletionCallback *>(saved.completion_cb));
	linenoiseSetFormatCallback(reinterpret_cast<linenoiseFormatCallback *>(saved.format_cb));
	linenoiseSetErrorRendering(saved.error_rendering);
	linenoiseSetCompletionRendering(saved.completion_rendering);
#endif
	state.highlighting_enabled = saved.highlighting_enabled;

	// Pop all modes from token onwards (handles nested calls correctly)
	saved_input_modes.resize(token);
}

duckdb::string ShellContextImpl::ReadLine(const duckdb::string &prompt) {
	last_readline_eof = false;
#ifdef HAVE_LINENOISE
	char *line = linenoise(prompt.c_str());
	if (!line) {
		last_readline_eof = true;
		return duckdb::string();
	}
	duckdb::string result(line);
	free(line);
	return result;
#else
	state.Print(prompt);
	fflush(stdout);
	char buf[4096];
	if (fgets(buf, sizeof(buf), stdin)) {
		return duckdb::string(buf);
	}
	last_readline_eof = true;
	return duckdb::string();
#endif
}

bool ShellContextImpl::ReadLineEOF() const {
	return last_readline_eof;
}

// ---------------------------------------------------------------------------
// History management
// ---------------------------------------------------------------------------

void ShellContextImpl::AddHistory(const duckdb::string &line) {
#ifdef HAVE_LINENOISE
	linenoiseHistoryAdd(line.c_str());
#endif
}

void ShellContextImpl::ClearHistory() {
#ifdef HAVE_LINENOISE
	duckdb::History::Clear();
#endif
}

void ShellContextImpl::LoadRawHistory(const duckdb::string &path) {
#ifdef HAVE_LINENOISE
	duckdb::History::LoadRaw(path.c_str());
#endif
}

void ShellContextImpl::SaveHistoryToFile(const duckdb::string &path) {
#ifdef HAVE_LINENOISE
	linenoiseHistorySave(path.c_str());
#endif
}

void ShellContextImpl::SaveShellHistory(const duckdb::string &path) {
	state.ShellSaveHistory(path.c_str());
}

void ShellContextImpl::LoadShellHistory(const duckdb::string &path) {
	state.ShellLoadHistory(path.c_str());
}

// ---------------------------------------------------------------------------
// Extension data storage
// ---------------------------------------------------------------------------

void ShellContextImpl::SetExtensionData(const duckdb::string &key,
                                        duckdb::shared_ptr<duckdb::ShellExtensionData> data) {
	extension_data[key] = std::move(data);
}

duckdb::shared_ptr<duckdb::ShellExtensionData> ShellContextImpl::GetExtensionData(const duckdb::string &key) {
	auto it = extension_data.find(key);
	if (it == extension_data.end()) {
		return nullptr;
	}
	return it->second;
}

} // namespace duckdb_shell
