//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_status_bar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/progress_bar/display/terminal_progress_bar_display.hpp"

namespace duckdb_shell {

//! Displays a status bar alongside the progress bar
class ShellStatusBarDisplay : public duckdb::TerminalProgressBarDisplay {
public:
	void Update(double percentage) override;
	void Finish() override;

protected:
	void PrintProgressInternal(int32_t percentage, double estimated_remaining_seconds, bool is_finished) override;
};

} // namespace duckdb_shell
