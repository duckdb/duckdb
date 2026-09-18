//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/callback_async_task.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/async_result.hpp"

#include <functional>

namespace duckdb {

//! Runs an arbitrary callback as an async task
class CallbackAsyncTask : public AsyncTask {
public:
	explicit CallbackAsyncTask(std::function<void()> callback_p, idx_t io_size_p = 0)
	    : callback(std::move(callback_p)), io_size(io_size_p) {
	}

	void Execute() override {
		callback();
	}

	idx_t GetIOSize() const override {
		return io_size;
	}

private:
	std::function<void()> callback;
	//! The number of bytes the callback reads, when known
	idx_t io_size;
};

} // namespace duckdb
