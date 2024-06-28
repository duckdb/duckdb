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

struct QueryProgress {
	friend class ProgressBar;

public:
	QueryProgress();
	void Initialize();
	void Restart();
	double GetPercentage();
	uint64_t GetRowsProcesseed();
	uint64_t GetTotalRowsToProcess();
	QueryProgress &operator=(const QueryProgress &other);
	QueryProgress(const QueryProgress &other);

private:
	atomic<double> percentage;
	atomic<uint64_t> rows_processed;
	atomic<uint64_t> total_rows_to_process;
};

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
	QueryProgress GetDetailedQueryProgress();
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
	//! Keeps track of the total progress of a query
	QueryProgress query_progress;
	//! The display used to print the progress
	unique_ptr<ProgressBarDisplay> display;
	//! Whether or not profiling is supported for the current query
	bool supported = true;
	//! Whether the bar has already finished
	bool finished = false;
};
} // namespace duckdb
