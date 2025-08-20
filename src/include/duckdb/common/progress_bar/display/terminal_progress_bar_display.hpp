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
#include <thread>

namespace duckdb {

class TerminalProgressBarDisplay : public ProgressBarDisplay {
private:
	UnscentedKalmanFilter ukf;
	std::chrono::steady_clock::time_point start_time;

	double GetElapsedDuration() {
		auto now = std::chrono::steady_clock::now();
		return std::chrono::duration<double>(now - start_time).count();
	}
	void StopPeriodicUpdates();

public:
	TerminalProgressBarDisplay() {
		start_time = std::chrono::steady_clock::now();
	}

	~TerminalProgressBarDisplay() override {
	}

public:
	void Update(double percentage) override;
	void Finish() override;

private:
	std::mutex mtx;
	std::thread periodic_update_thread;
	std::condition_variable cv;
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
};

} // namespace duckdb
