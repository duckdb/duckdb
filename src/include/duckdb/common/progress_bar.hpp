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
	const string PROGRESS_BAR_STRING = "============================================================";
	static constexpr const idx_t PROGRESS_BAR_WIDTH = 60;

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
