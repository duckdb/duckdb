//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task_counter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

class TaskCounter {
public:
	explicit TaskCounter(TaskScheduler &scheduler_p)
	    : scheduler(scheduler_p), token(scheduler_p.CreateProducer()), task_count(0), tasks_completed(0) {
	}

	virtual void AddTask(shared_ptr<Task> task) {
		++task_count;
		scheduler.ScheduleTask(*token, std::move(task));
	}

	virtual void FinishTask() const {
		++tasks_completed;
	}

	virtual void Finish() {
		while (tasks_completed < task_count) {
			shared_ptr<Task> task;
			if (scheduler.GetTaskFromProducer(*token, task)) {
				task->Execute();
				task.reset();
			}
		}
	}

private:
	TaskScheduler &scheduler;
	unique_ptr<ProducerToken> token;
	size_t task_count;
	mutable atomic<size_t> tasks_completed;
};

} // namespace duckdb
