//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/progress_bar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"
#include "duckdb/execution/executor.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/profiler.hpp"

namespace duckdb {

class ProgressBar {
public:
	explicit ProgressBar(Executor &executor, idx_t show_progress_after, bool print_progress);

	//! Starts the thread
	void Start();
	//! Updates the progress bar and prints it to the screen
	void Update(bool final);
	//! Gets current percentage
	double GetCurrentPercentage();

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

	void PrintProgressInternal(int percentage);
	void PrintProgress(int percentage);
	void FinishProgressBarPrint();

private:
	//! The executor
	Executor &executor;
	//! The profiler used to measure the time since the progress bar was started
	Profiler profiler;
	//! The time in ms after which to start displaying the progress bar
	idx_t show_progress_after;
	//! The current progress percentage
	double current_percentage;
	//! Whether or not we print the progress bar
	bool print_progress;
	//! Whether or not profiling is supported for the current query
	bool supported = true;
};
} // namespace duckdb
