//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/batch_task_helper.hpp
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

class BatchCopyTask {
public:
	virtual ~BatchCopyTask() {
	}

	virtual void Execute(const PhysicalOperator &op, ClientContext &context, GlobalSinkState &gstate_p) = 0;
};

class BatchTaskHelper {
public:
	void AddTask(unique_ptr<BatchCopyTask> task) {
		lock_guard<mutex> l(task_lock);
		task_queue.push(std::move(task));
	}

	unique_ptr<BatchCopyTask> GetTask() {
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
	queue<unique_ptr<BatchCopyTask>> task_queue;
};

} // namespace duckdb
