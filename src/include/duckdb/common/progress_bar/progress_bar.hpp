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

#include <cmath>

namespace duckdb {

struct SquaredDistanceAccumulator {
	void AddSample(double x, double t) {
		stats[0] += x * x * (t - prev_t);
		stats[1] += x * t * (t - prev_t);
		stats[2] += t * t * (t - prev_t);
		stats[3] += 1.0 * (t - prev_t);
		prev_t = t;
	}
	double GetResult(double normalize_T = 1.0) {
		D_ASSERT(stats[3] >= 1.0);
		return std::sqrt((stats[0] + (stats[2] / normalize_T - 2.0 * stats[1]) / normalize_T) / stats[3]);
	}
	double prev_t = 0.0;
	double stats[4] = {0.0, 0.0, 0.0, 0.0};
	// Accumulated statistics are:
	//  stats[0] => sum(x^2)
	//  stats[1] => sum(x*2)
	//  stats[2] => sum(t^2)
	//  stats[3] => number of samples
	// Result we want to compute is sqrt(sum((x-t/T)^2)/N), and expanding (x-t/T)^2 you get
	//    sqrt(sum(x^2 - 2*x*t/T + t^2/T^2) / N) = sqrt( (sum(x^2) - 2.0*sum(x*t)/T + sum(t^2)/T^2) / N )
	// Trick here is doing normalization by (/T) on the accumulated sums, that allows to not known total time until the
	// very end
};

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
	bool ShouldPrint(bool final, double &elapsed_time) const;
	bool PrintEnabled() const;
	void IntializeStatCollection();

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
	//! Optional struct to hold accumulated data to compute squared distance
	unique_ptr<SquaredDistanceAccumulator> squared_distance_accumulator;
};
} // namespace duckdb
