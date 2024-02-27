//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/batch_task_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/queue.hpp"

namespace duckdb {

template <class TASK>
class BatchTaskManager {
public:
	void AddTask(unique_ptr<TASK> task) {
		lock_guard<mutex> l(task_lock);
		task_queue.push(std::move(task));
	}

	unique_ptr<TASK> GetTask() {
		lock_guard<mutex> l(task_lock);
		if (task_queue.empty()) {
			return nullptr;
		}
		auto entry = std::move(task_queue.front());
		task_queue.pop();
		return entry;
	}

	idx_t TaskCount() {
		lock_guard<mutex> l(task_lock);
		return task_queue.size();
	}

private:
	mutex task_lock;
	//! The task queue for the batch copy to file
	queue<unique_ptr<TASK>> task_queue;
};

} // namespace duckdb
