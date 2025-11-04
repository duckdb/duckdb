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

enum class StatusBarPadding { NO_PADDING };

enum class StatusBarAlignment { LEFT, MIDDLE, RIGHT };

struct StatusBarComponent {
	~StatusBarComponent();

	//! Prompt used to render the content of this component
	unique_ptr<StatusBarPrompt> prompt;
	//! Alignment of the status bar relative to the total bar
	StatusBarAlignment alignment = StatusBarAlignment::LEFT;
	//! Padding of the content of the status bar
	StatusBarPadding padding_type = StatusBarPadding::NO_PADDING;
	//! In case there is padding (i.e. padding_type is not NO_PADDING) - where to position the content
	StatusBarAlignment content_alignment = StatusBarAlignment::LEFT;
};

struct StatusBar {
	friend class ShellStatusBarDisplay;
	friend struct StatusBarPrompt;

public:
	StatusBar();
	~StatusBar();

public:
	void ParseStatusBar(const string &text);
	string GenerateStatusBar(ShellState &state);

private:
	vector<unique_ptr<StatusBarComponent>> components;
	duckdb::ProgressBarDisplayInfo display_info;
	int32_t percentage = 0;
	double estimated_remaining_seconds = 0;
};

//! Displays a status bar alongside the progress bar
class ShellStatusBarDisplay : public duckdb::TerminalProgressBarDisplay {
public:
	ShellStatusBarDisplay();

protected:
	void PrintProgressInternal(int32_t percentage, double estimated_remaining_seconds, bool is_finished) override;
};

} // namespace duckdb_shell
