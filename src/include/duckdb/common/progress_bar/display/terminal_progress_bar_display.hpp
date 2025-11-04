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
#include "duckdb/common/progress_bar/unscented_kalman_filter.hpp"
#include <chrono>

namespace duckdb {

struct TerminalProgressBarDisplayedProgressInfo {
	optional_idx percentage;
	optional_idx estimated_seconds_remaining;

	bool operator==(const TerminalProgressBarDisplayedProgressInfo &other) const {
		return percentage == other.percentage && estimated_seconds_remaining == other.estimated_seconds_remaining;
	}

	bool operator!=(const TerminalProgressBarDisplayedProgressInfo &other) const {
		return !(*this == other);
	}
};

class TerminalProgressBarDisplay : public ProgressBarDisplay {
public:
	TerminalProgressBarDisplay() {
		start_time = std::chrono::steady_clock::now();
		displayed_progress_info = {optional_idx(), optional_idx()};
	}

	~TerminalProgressBarDisplay() override {
	}

public:
	void Update(double percentage) override;
	void Finish() override;

private:
	void PeriodicUpdate();

	static constexpr const idx_t PARTIAL_BLOCK_COUNT = UnicodeBar::PartialBlocksCount();
#ifndef DUCKDB_ASCII_TREE_RENDERER
	const char *PROGRESS_EMPTY = " ";                                  // NOLINT
	const char *const *PROGRESS_PARTIAL = UnicodeBar::PartialBlocks(); // NOLINT
	const char *PROGRESS_BLOCK = UnicodeBar::FullBlock();              // NOLINT
	const char *PROGRESS_START = "\xE2\x96\x95";                       // NOLINT
	const char *PROGRESS_END = "\xE2\x96\x8F";                         // NOLINT
#else
	const char *PROGRESS_EMPTY = " ";
	const char *const PROGRESS_PARTIAL[PARTIAL_BLOCK_COUNT] = {" ", " ", " ", " ", " ", " ", " ", " "};
	const char *PROGRESS_BLOCK = "=";
	const char *PROGRESS_START = "[";
	const char *PROGRESS_END = "]";
#endif
	static constexpr const idx_t PROGRESS_BAR_WIDTH = 38;

private:
	static int32_t NormalizePercentage(double percentage);
	void PrintProgressInternal(int32_t percentage, double estimated_remaining_seconds, bool is_finished = false);
	double GetElapsedDuration() {
		auto now = std::chrono::steady_clock::now();
		return std::chrono::duration<double>(now - start_time).count();
	}
	void StopPeriodicUpdates();

private:
	UnscentedKalmanFilter ukf;
	std::chrono::steady_clock::time_point start_time;

	// track the progress info that has been previously
	// displayed to prevent redundant updates
	struct TerminalProgressBarDisplayedProgressInfo displayed_progress_info;
};

} // namespace duckdb
