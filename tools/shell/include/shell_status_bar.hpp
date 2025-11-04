//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_status_bar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/progress_bar/display/terminal_progress_bar_display.hpp"
#include "shell_prompt.hpp"

namespace duckdb_shell {
struct StatusBarPrompt;

struct StatusBar {
	friend class ShellStatusBarDisplay;
	friend struct StatusBarPrompt;

public:
	StatusBar();
	~StatusBar();

public:
	void AddComponent(const string &text);
	void ClearComponents();
	string GenerateStatusBar(ShellState &state);

private:
	vector<unique_ptr<StatusBarPrompt>> components;
	duckdb::ProgressBarDisplayInfo display_info;
	int32_t percentage = 0;
	double estimated_remaining_seconds = 0;
	unique_ptr<duckdb::Connection> connection;
};

//! Displays a status bar alongside the progress bar
class ShellStatusBarDisplay : public duckdb::TerminalProgressBarDisplay {
public:
	ShellStatusBarDisplay();

protected:
	void PrintProgressInternal(int32_t percentage, double estimated_remaining_seconds, bool is_finished) override;
};

} // namespace duckdb_shell
