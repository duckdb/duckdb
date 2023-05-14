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
#include "duckdb/common/unicode_bar.hpp"

namespace duckdb {

class TerminalProgressBarDisplay : public ProgressBarDisplay {
public:
	TerminalProgressBarDisplay() {
	}
	~TerminalProgressBarDisplay() override {
	}

public:
	void Update(double percentage) override;
	void Finish() override;

private:
	static constexpr const idx_t PARTIAL_BLOCK_COUNT = UnicodeBar::PartialBlocksCount();
#ifndef DUCKDB_ASCII_TREE_RENDERER
	const char *PROGRESS_EMPTY = " ";
	const char *const *PROGRESS_PARTIAL = UnicodeBar::PartialBlocks();
	const char *PROGRESS_BLOCK = UnicodeBar::FullBlock();
	const char *PROGRESS_START = "\xE2\x96\x95";
	const char *PROGRESS_END = "\xE2\x96\x8F";
#else
	const char *PROGRESS_EMPTY = " ";
	const char *const PROGRESS_PARTIAL[PARTIAL_BLOCK_COUNT] = {" ", " ", " ", " ", " ", " ", " ", " "};
	const char *PROGRESS_BLOCK = "=";
	const char *PROGRESS_START = "[";
	const char *PROGRESS_END = "]";
#endif
	static constexpr const idx_t PROGRESS_BAR_WIDTH = 60;

private:
	void PrintProgressInternal(int percentage);
};

} // namespace duckdb
