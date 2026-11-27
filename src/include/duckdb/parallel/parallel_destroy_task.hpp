//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/parallel_destroy_task.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/task_executor.hpp"

namespace duckdb {
template <class ITERABLE>
class ParallelDestroyTask : public BaseExecutorTask {
	using ITERATOR = typename ITERABLE::iterator;

public:
	ParallelDestroyTask(TaskExecutor &executor, ITERATOR begin_p, ITERATOR end_p)
	    : BaseExecutorTask(executor), begin(begin_p), end(end_p) {
	}

	void ExecuteTask() override {
		for (auto entry = begin; entry != end; ++entry) {
			entry->reset();
		}
	}

	void Cancel() override {
		// Finish releasing the range before the executor reports this task as complete.
		ExecuteTask();
	}

	static void Schedule(TaskScheduler &scheduler, ITERABLE &elements) {
		Schedule(scheduler, elements.begin(), elements.end(), elements.size());
	}

	//! The caller keeps the range stable until this call joins all scheduled tasks.
	static void Schedule(TaskScheduler &scheduler, ITERATOR begin, ITERATOR end, idx_t max_tasks) {
		if (begin == end) {
			return;
		}
		D_ASSERT(max_tasks > 0);
		// This is used in destructors and is therefore not allowed to throw
		// Similar approach to AttachedDatabase::Close(), just swallow the exception
		try {
			TaskExecutor executor(scheduler);
			const idx_t count = end - begin;
			const auto task_count = MinValue(count, max_tasks);
			const auto batch_size = (count + task_count - 1) / task_count;
			vector<unique_ptr<BaseExecutorTask>> tasks;
			tasks.reserve(task_count);
			for (idx_t offset = 0; offset < count; offset += batch_size) {
				tasks.push_back(make_uniq<ParallelDestroyTask>(executor, begin + offset,
				                                               begin + MinValue(count, offset + batch_size)));
			}
			executor.ScheduleTasks(std::move(tasks));
			executor.WorkOnTasks();
		} catch (...) { // NOLINT
		}
	}

private:
	ITERATOR begin;
	ITERATOR end;
};

} // namespace duckdb
