//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/progress_bar/display/terminal_progress_bar_display.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/progress_bar/progress_bar_display.hpp"

namespace duckdb {

class TerminalProgressBarDisplay : public ProgressBarDisplay {
public:
	TerminalProgressBarDisplay() {
	}
	~TerminalProgressBarDisplay() override final {
	}

public:
	void Update(double percentage) override;
	void Finish() override;

private:
	static constexpr const idx_t PARTIAL_BLOCK_COUNT = 8;
#ifndef DUCKDB_ASCII_TREE_RENDERER
	const char *PROGRESS_EMPTY = " ";
	const char *PROGRESS_PARTIAL[PARTIAL_BLOCK_COUNT] {
	    " ",           "\xE2\x96\x8F", "\xE2\x96\x8E", "\xE2\x96\x8D", "\xE2\x96\x8C", "\xE2\x96\x8B", "\xE2\x96\x8A",
	    "\xE2\x96\x89"};
	const char *PROGRESS_BLOCK = "\xE2\x96\x88";
	const char *PROGRESS_START = "\xE2\x96\x95";
	const char *PROGRESS_END = "\xE2\x96\x8F";
#else
	const char *PROGRESS_EMPTY = " ";
	const char *PROGRESS_PARTIAL[PARTIAL_BLOCK_COUNT] {" ", " ", " ", " ", " ", " ", " ", " "};
	const char *PROGRESS_BLOCK = "=";
	const char *PROGRESS_START = "[";
	const char *PROGRESS_END = "]";
#endif
	static constexpr const idx_t PROGRESS_BAR_WIDTH = 60;

private:
	void PrintProgressInternal(int percentage);
};

} // namespace duckdb
