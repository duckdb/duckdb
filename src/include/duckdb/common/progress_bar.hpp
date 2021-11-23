//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/progress_bar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifndef DUCKDB_NO_THREADS
#include "duckdb/common/thread.hpp"
#include <future>
#endif

#include "duckdb.h"
#include "duckdb/execution/executor.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
class ProgressBar {
public:
	explicit ProgressBar(Executor *executor, idx_t show_progress_after, idx_t time_update_bar = 100)
	    : executor(executor), show_progress_after(show_progress_after), time_update_bar(time_update_bar),
	      current_percentage(-1), stop(false) {
	}
	~ProgressBar();

	//! Starts the thread
	void Start();
	//! Stops the thread
	void Stop(bool success = true);
	//! Gets current percentage
	int GetCurrentPercentage();

	void Initialize(idx_t show_progress_after) {
		this->show_progress_after = show_progress_after;
	}

private:
	const string PROGRESS_BAR_STRING = "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||";
	static constexpr const idx_t PROGRESS_BAR_WIDTH = 60;
	Executor *executor = nullptr;
#ifndef DUCKDB_NO_THREADS
	thread progress_bar_thread;
	std::condition_variable c;
	mutex m;
#endif
	atomic<idx_t> show_progress_after;
	idx_t time_update_bar;
	atomic<int> current_percentage;
	atomic<bool> stop;
	//! In case our progress bar tries to use a scan operator that is not implemented we don't print anything
	bool supported = true;
	//! Starts the Progress Bar Thread that prints the progress bar
	void ProgressBarThread();

#ifndef DUCKDB_NO_THREADS
	template <class DURATION>
	bool WaitFor(DURATION duration) {
		unique_lock<mutex> l(m);
		return !c.wait_for(l, duration, [this]() { return stop.load(); });
	}
#endif
};
} // namespace duckdb