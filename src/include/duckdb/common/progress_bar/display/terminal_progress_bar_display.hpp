//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/progress_bar/display/terminal_progress_bar_display.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/progress_bar/progress_bar_display.hpp"

namespace duckdb {

class TerminalProgressBarDisplay : public ProgressBarDisplay {
public:
	TerminalProgressBarDisplay();
	~TerminalProgressBarDisplay() override final;

public:
	void Update(double percentage) override;
	void Finish() override;
};

} // namespace duckdb
