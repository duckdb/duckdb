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
	explicit ProgressBar(Executor *executor, idx_t show_progress_after, idx_t time_update_bar, bool test = false)
	    : executor(executor), show_progress_after(show_progress_after), time_update_bar(time_update_bar),
	      running_test(test) {

	      };

	//! Starts the thread
	void Start();
	//! Stops the thread
	void Stop();
	//! If all values of the percentage were valid
	bool IsPercentageValid();

private:
	string pbstr = "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||";
	idx_t pbwidth = 60;
	Executor *executor = nullptr;
	std::thread progress_bar_thread;
	std::promise<void> exit_signal;
	std::future<void> future_obj;
	idx_t show_progress_after;
	idx_t time_update_bar;
	int cur_percentage = 0;
	std::atomic<bool> valid_percentage;
	bool running_test;
	//! In case our progress bar tries to use a scan operator that is not implemented we don't print anything
	bool supported = true;
	//! Prints Progress
	void PrintProgress(int percentage);
	//! Starts the Progress Bar Thread that prints the progress bar
	void ProgressBarThread();
	//! Starts the Progress Bar Thread that updates the cur_percentage (Used for testing)
	void ProgressBarThreadTest();
	//! Check if stop was requests
	bool StopRequested();
};
} // namespace duckdb