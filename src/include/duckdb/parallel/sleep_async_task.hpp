//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/sleep_async_task.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/async_result.hpp"

#include <chrono>
#include <thread>

namespace duckdb {

class SleepAsyncTask : public AsyncTask {
public:
	explicit SleepAsyncTask(idx_t sleep_for) : sleep_for(sleep_for) {
	}
	void Execute() override {
		std::this_thread::sleep_for(std::chrono::milliseconds(sleep_for));
	}
	const idx_t sleep_for;
};

class ThrowAsyncTask : public AsyncTask {
public:
	explicit ThrowAsyncTask(idx_t sleep_for) : sleep_for(sleep_for) {
	}
	void Execute() override {
		std::this_thread::sleep_for(std::chrono::milliseconds(sleep_for));
		throw NotImplementedException("ThrowAsyncTask: Test error handling when throwing mid-task");
	}
	const idx_t sleep_for;
};

} // namespace duckdb
