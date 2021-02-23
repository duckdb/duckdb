//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/progress_bar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <thread>
#include <future>
#include "duckdb.h"
#include "duckdb/execution/executor.hpp"

namespace duckdb {
class ProgressBar {
public:
	explicit ProgressBar(Executor *executor, idx_t show_progress_after, idx_t time_update_bar = 100)
	    : executor(executor), show_progress_after(show_progress_after), time_update_bar(time_update_bar) {

	                                                                    };

	//! Starts the thread
	void Start();
	//! Stops the thread
	void Stop();
	//! Gets current percentage
	int GetCurPercentage();
	//! If all values in the percentage were valid
	bool IsPercentageValid();

private:
	string pbstr = "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||";
	idx_t pbwidth = 60;
	Executor *executor = nullptr;
	std::thread progress_bar_thread;
	idx_t show_progress_after;
	idx_t time_update_bar;
	int cur_percentage = -1;
	std::atomic<bool> valid_percentage;
	std::condition_variable c;
	std::mutex m;
	bool stop = false;
	//! In case our progress bar tries to use a scan operator that is not implemented we don't print anything
	bool supported = true;
	//! Prints Progress
	void PrintProgress(int percentage);
	//! Prints an empty line when progress bar is done
	void FinishPrint();
	//! Starts the Progress Bar Thread that prints the progress bar
	void ProgressBarThread();
	template <class DURATION>
	bool WaitFor(DURATION duration) {
		std::unique_lock<std::mutex> l(m);
		return !c.wait_for(l, duration, [this]() { return stop; });
	}
};
} // namespace duckdb