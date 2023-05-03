//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/progress_bar/progress_bar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"
#include "duckdb/execution/executor.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/common/progress_bar/progress_bar_display.hpp"

namespace duckdb {

struct ClientConfig;
typedef unique_ptr<ProgressBarDisplay> (*progress_bar_display_create_func_t)();

class ProgressBar {
public:
	static unique_ptr<ProgressBarDisplay> DefaultProgressBarDisplay();
	static void SystemOverrideCheck(ClientConfig &config);

	explicit ProgressBar(
	    Executor &executor, idx_t show_progress_after,
	    progress_bar_display_create_func_t create_display_func = ProgressBar::DefaultProgressBarDisplay);

	//! Starts the thread
	void Start();
	//! Updates the progress bar and prints it to the screen
	void Update(bool final);
	//! Gets current percentage
	double GetCurrentPercentage();

	void PrintProgressInternal(int percentage);
	void PrintProgress(int percentage);
	void FinishProgressBarPrint();
	bool ShouldPrint(bool final) const;
	bool PrintEnabled() const;

private:
	//! The executor
	Executor &executor;
	//! The profiler used to measure the time since the progress bar was started
	Profiler profiler;
	//! The time in ms after which to start displaying the progress bar
	idx_t show_progress_after;
	//! The current progress percentage
	double current_percentage;
	//! The display used to print the progress
	unique_ptr<ProgressBarDisplay> display;
	//! Whether or not profiling is supported for the current query
	bool supported = true;
	//! Whether the bar has already finished
	bool finished = false;
};
} // namespace duckdb
