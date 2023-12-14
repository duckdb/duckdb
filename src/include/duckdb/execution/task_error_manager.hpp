//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/task_error_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/preserved_error.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class TaskErrorManager {
public:
	void PushError(PreservedError error) {
		lock_guard<mutex> elock(error_lock);
		this->exceptions.push_back(std::move(error));
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
	vector<PreservedError> exceptions;
};
} // namespace duckdb
