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
#include "duckdb/common/vector.hpp"

namespace duckdb {

class TaskErrorManager {
public:
	void PushError(ErrorData error) {
		lock_guard<mutex> elock(error_lock);
		this->exceptions.push_back(std::move(error));
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
		lock_guard<mutex> elock(error_lock);
		return !exceptions.empty();
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
	}

private:
	mutex error_lock;
	//! Exceptions that occurred during the execution of the current query
	vector<ErrorData> exceptions;
};
} // namespace duckdb
