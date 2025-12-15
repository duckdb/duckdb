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
	using VALUE_TYPE = typename ITERABLE::value_type;

public:
	ParallelDestroyTask(TaskExecutor &executor, VALUE_TYPE &&elem_p)
	    : BaseExecutorTask(executor), elem(std::move(elem_p)) {
	}

	void ExecuteTask() override {
		elem.reset();
	}

	static void Schedule(const weak_ptr<DatabaseInstance> &db, ITERABLE &elements) {
		auto db_ref = db.lock();
		if (!db_ref) {
			return;
		}

		TaskExecutor executor(TaskScheduler::GetScheduler(*db_ref));
		for (auto &segment : elements) {
			auto destroy_task = make_uniq<ParallelDestroyTask>(executor, std::move(segment));
			executor.ScheduleTask(std::move(destroy_task));
		}
		executor.WorkOnTasks();
	}

private:
	VALUE_TYPE elem;
};

} // namespace duckdb
