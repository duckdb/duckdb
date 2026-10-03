//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_progress_bar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/progress_bar/display/terminal_progress_bar_display.hpp"
#include "shell_prompt.hpp"

namespace duckdb_shell {
struct ProgressBarPrompt;

struct ShellProgressBar {
	friend class ShellProgressBarDisplay;
	friend struct ProgressBarPrompt;

public:
	ShellProgressBar();
	~ShellProgressBar();

public:
	void AddComponent(const string &text);
	void ClearComponents();
	string GenerateProgressBar(ShellState &state, idx_t terminal_width);

private:
	vector<unique_ptr<ProgressBarPrompt>> components;
	duckdb::ProgressBarDisplayInfo display_info;
	int32_t percentage = 0;
	double estimated_remaining_seconds = 0;
	//! The status message shown next to the progress (see duckdb::ClientStatus), if any
	string status_message;
	unique_ptr<duckdb::Connection> connection;
};

//! Displays a status bar alongside the progress bar
class ShellProgressBarDisplay : public duckdb::TerminalProgressBarDisplay {
public:
	ShellProgressBarDisplay();

public:
	void Finish() override;
	//! Next to the progress, the message is shown at the next update: the progress components may run queries, which
	//! should not happen from the (possibly worker) thread that reports the message
	void UpdateStatus(const string &message) override;

protected:
	void PrintProgressInternal(int32_t percentage, double estimated_remaining_seconds, bool is_finished) override;
	void PrintStatusInternal(const string &message, double elapsed_seconds) override;

private:
	optional_idx previous_terminal_width;
};

//! Prints a progress line to stderr every few seconds, for consumers that read our output through a pipe (agents)
class AgentProgressBarDisplay : public duckdb::TerminalProgressBarDisplay {
public:
	//! Minimum time between two progress lines
	static constexpr double PRINT_INTERVAL_SECONDS = 5.0;

public:
	AgentProgressBarDisplay();

public:
	void Finish() override;
	//! Prints a status line to stderr when the message changes - a repeated message at most every few seconds
	void UpdateStatus(const string &message) override;

protected:
	void PrintProgressInternal(int32_t percentage, double estimated_remaining_seconds, bool is_finished) override;

private:
	//! When the previous line was printed (seconds since the display was created), if any
	double last_print_time = -1;
	//! The previous status line's message, and when it was printed
	string last_status;
	double last_status_time = -1;
};

} // namespace duckdb_shell
