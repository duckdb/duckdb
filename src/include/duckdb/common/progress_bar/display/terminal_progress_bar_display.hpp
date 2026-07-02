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

struct ProgressBarDisplayInfo {
	idx_t width = 38;
#ifndef DUCKDB_ASCII_TREE_RENDERER
	const char *progress_empty = " ";
	const char *const *progress_partial = UnicodeBar::PartialBlocks();
	idx_t partial_block_count = UnicodeBar::PartialBlocksCount();
	const char *progress_block = UnicodeBar::FullBlock();
	const char *progress_start = "\xE2\x96\x95";
	const char *progress_end = "\xE2\x96\x8F";
#else
	const char *progress_empty = " ";
	const char *const progress_partial[PARTIAL_BLOCK_COUNT] = {" ", " ", " ", " ", " ", " ", " ", " "};
	idx_t partial_block_count = 8;
	const char *progress_block = "=";
	const char *progress_start = "[";
	const char *progress_end = "]";
#endif
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
	static string FormatETA(double seconds, bool elapsed = false);
	static string FormatProgressBar(const ProgressBarDisplayInfo &display_info, int32_t percentage);

private:
	void PeriodicUpdate();

public:
	ProgressBarDisplayInfo display_info;

protected:
	virtual void PrintProgressInternal(int32_t percentage, double estimated_remaining_seconds,
	                                   bool is_finished = false);

	static int32_t NormalizePercentage(double percentage);
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
