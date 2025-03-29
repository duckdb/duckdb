//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/task_error_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class TaskErrorManager {
public:
	TaskErrorManager() : has_error(false) {
	}

	void PushError(ErrorData error) {
		lock_guard<mutex> elock(error_lock);
		this->exceptions.push_back(std::move(error));
		has_error = true;
	}

	ErrorData GetError() {
		lock_guard<mutex> elock(error_lock);
		D_ASSERT(!exceptions.empty());

		// FIXME: Should we try to get the biggest priority error?
		// In case the first exception is a StandardException but a regular Exception or a FatalException occurred
		// Maybe we should throw the more critical exception instead, as that changes behavior.
		auto &entry = exceptions[0];
		return entry;
	}

	bool HasError() {
		return has_error;
	}

	void ThrowException() {
		lock_guard<mutex> elock(error_lock);
		D_ASSERT(!exceptions.empty());
		auto &entry = exceptions[0];
		entry.Throw();
	}

	void Reset() {
		lock_guard<mutex> elock(error_lock);
		exceptions.clear();
		has_error = false;
	}

private:
	mutex error_lock;
	//! Exceptions that occurred during the execution of the current query
	vector<ErrorData> exceptions;
	//! Lock-free error flag
	atomic<bool> has_error;
};
} // namespace duckdb
