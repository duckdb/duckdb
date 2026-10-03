//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/progress_bar/display/terminal_progress_bar_display.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/progress_bar/progress_bar_display.hpp"
#include "duckdb/common/unicode_bar.hpp"
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
	void UpdateStatus(const string &message) override;
	static string FormatETA(double seconds, bool elapsed = false);
	//! A short elapsed time for a status line: "42s", "3m 05s"
	static string FormatElapsed(double seconds);
	//! The spinner shown next to a status message while there is no progress, one frame per redraw
	static const char *SpinnerFrame(idx_t frame);
	static string FormatProgressBar(const ProgressBarDisplayInfo &display_info, int32_t percentage);
	static double EstimateRemainingSeconds(double percentage, double elapsed_seconds,
	                                       double observed_progress_per_second = 0.0);

private:
	void PeriodicUpdate();

public:
	ProgressBarDisplayInfo display_info;

protected:
	virtual void PrintProgressInternal(int32_t percentage, double estimated_remaining_seconds,
	                                   bool is_finished = false);

	//! Prints the status message on its own (while there is no progress), with a spinner and the elapsed time. An empty
	//! message clears the line
	virtual void PrintStatusInternal(const string &message, double elapsed_seconds);

	static int32_t NormalizePercentage(double percentage);

	//! The status message shown next to the progress, if any
	string status_message;
	//! Whether progress was displayed - the status message is then shown next to it
	bool has_percentage = false;
	int32_t displayed_percentage = 0;
	double displayed_remaining_seconds = 0;
	//! Whether the status message changed since the progress was printed - the next Update reprints it
	bool status_changed = false;
	//! The next spinner frame
	idx_t spinner_frame = 0;
	//! The render length of the previous status line, to clear what is left of it
	idx_t previous_status_length = 0;
	double GetElapsedDuration() {
		auto now = std::chrono::steady_clock::now();
		return std::chrono::duration<double>(now - start_time).count();
	}
	void StopPeriodicUpdates();

private:
	double UpdateEstimatedRemainingSeconds(double percentage, double elapsed_seconds);

private:
	std::chrono::steady_clock::time_point start_time;
	bool has_eta_sample = false;
	double last_eta_percentage = 0.0;
	double last_eta_sample_time = 0.0;
	double last_eta_update_time = 0.0;
	double smoothed_progress_per_second = 0.0;
	double estimated_completion_time = 0.0;

	// track the progress info that has been previously
	// displayed to prevent redundant updates
	struct TerminalProgressBarDisplayedProgressInfo displayed_progress_info;
};

} // namespace duckdb
